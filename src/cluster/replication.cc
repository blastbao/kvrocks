/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "replication.h"

#include <arpa/inet.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>

#include <algorithm>
#include <atomic>
#include <csignal>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include "commands/error_constants.h"
#include "event_util.h"
#include "fmt/format.h"
#include "io_util.h"
#include "logging.h"
#include "rocksdb/write_batch.h"
#include "rocksdb_crc32c.h"
#include "scope_exit.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "status.h"
#include "storage/batch_debugger.h"
#include "thread_util.h"
#include "time_util.h"
#include "unique_fd.h"

#ifdef ENABLE_OPENSSL
#include <event2/bufferevent_ssl.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#endif

Status FeedSlaveThread::Start() {
  auto s = util::CreateThread("feed-replica", [this] {
    sigset_t mask, omask;
    sigemptyset(&mask);
    sigemptyset(&omask);
    sigaddset(&mask, SIGCHLD);
    sigaddset(&mask, SIGHUP);
    sigaddset(&mask, SIGPIPE);
    pthread_sigmask(SIG_BLOCK, &mask, &omask);
    auto s = util::SockSend(conn_->GetFD(), redis::RESP_OK, conn_->GetBufferEvent());
    if (!s.IsOK()) {
      error("failed to send OK response to the replica: {}", s.Msg());
      return;
    }
    this->loop();
  });

  if (s) {
    t_ = std::move(*s);
  } else {
    conn_ = nullptr;  // prevent connection was freed when failed to start the thread
  }

  return std::move(s);
}

void FeedSlaveThread::Stop() {
  stop_ = true;
  warn("Slave thread was terminated, would stop feeding the slave: {}", conn_->GetAddr());
}

void FeedSlaveThread::Join() {
  if (auto s = util::ThreadJoin(t_); !s) {
    warn("Slave thread operation failed: {}", s.Msg());
  }
}

void FeedSlaveThread::checkLivenessIfNeed() {
  if (++interval_ % 1000) return;
  const auto ping_command = redis::BulkString("ping");
  auto s = util::SockSend(conn_->GetFD(), ping_command, conn_->GetBufferEvent());
  if (!s.IsOK()) {
    error("Ping slave [{}] err: {}, would stop the thread", conn_->GetAddr(), s.Msg());
    Stop();
  }
}

void FeedSlaveThread::loop() {
  // is_first_repl_batch was used to fix that replication may be stuck in a dead loop
  // when some seqs might be lost in the middle of the WAL log, so forced to replicate
  // first batch here to work around this issue instead of waiting for enough batch size.
  bool is_first_repl_batch = true;
  uint32_t yield_microseconds = 2 * 1000;
  std::string batches_bulk;
  size_t updates_in_batches = 0;
  while (!IsStopped()) {
    auto curr_seq = next_repl_seq_.load();

    if (!iter_ || !iter_->Valid()) {
      if (iter_) info("WAL was rotated, would reopen again");
      if (!srv_->storage->WALHasNewData(curr_seq) || !srv_->storage->GetWALIter(curr_seq, &iter_).IsOK()) {
        iter_ = nullptr;
        usleep(yield_microseconds);
        checkLivenessIfNeed();
        continue;
      }
    }
    // iter_ would be always valid here
    auto batch = iter_->GetBatch();
    if (batch.sequence != curr_seq) {
      error(
          "Fatal error encountered, WAL iterator is discrete, some seq might be lost, sequence {} expected, but got {}",
          curr_seq, batch.sequence);
      Stop();
      return;
    }
    updates_in_batches += batch.writeBatchPtr->Count();
    batches_bulk += redis::BulkString(batch.writeBatchPtr->Data());
    // 1. We must send the first replication batch, as said above.
    // 2. To avoid frequently calling 'write' system call to send replication stream,
    //    we pack multiple batches into one big bulk if possible, and only send once.
    //    But we should send the bulk of batches if its size exceed kMaxDelayBytes,
    //    16Kb by default. Moreover, we also send if updates count in all bathes is
    //    more that kMaxDelayUpdates, to void too many delayed updates.
    // 3. To avoid master don't send replication stream to slave since of packing
    //    batches strategy, we still send batches if current batch sequence is less
    //    kMaxDelayUpdates than latest sequence.
    if (is_first_repl_batch || batches_bulk.size() >= kMaxDelayBytes || updates_in_batches >= kMaxDelayUpdates ||
        srv_->storage->LatestSeqNumber() - batch.sequence <= kMaxDelayUpdates) {
      // Send entire bulk which contain multiple batches
      auto s = util::SockSend(conn_->GetFD(), batches_bulk, conn_->GetBufferEvent());
      if (!s.IsOK()) {
        error("Write error while sending batch to slave: {}. batches: 0x{}", s.Msg(), util::StringToHex(batches_bulk));
        Stop();
        return;
      }
      is_first_repl_batch = false;
      batches_bulk.clear();
      if (batches_bulk.capacity() > kMaxDelayBytes * 2) batches_bulk.shrink_to_fit();
      updates_in_batches = 0;
    }
    curr_seq = batch.sequence + batch.writeBatchPtr->Count();
    next_repl_seq_.store(curr_seq);
    while (!IsStopped() && !srv_->storage->WALHasNewData(curr_seq)) {
      usleep(yield_microseconds);
      checkLivenessIfNeed();
    }
    iter_->Next();
  }
}

void SendString(bufferevent *bev, const std::string &data) {
  auto output = bufferevent_get_output(bev);
  evbuffer_add(output, data.c_str(), data.length());
}

void ReplicationThread::CallbacksStateMachine::ConnEventCB(bufferevent *bev, int16_t events) {
  if (events & BEV_EVENT_CONNECTED) {
    // call write_cb when connected
    bufferevent_data_cb write_cb = nullptr;
    bufferevent_getcb(bev, nullptr, &write_cb, nullptr, nullptr);
    if (write_cb) write_cb(bev, this);
    return;
  }
  if (events & (BEV_EVENT_ERROR | BEV_EVENT_EOF)) {
    error("[replication] connection error/eof, reconnect the master");
    // Wait a bit and reconnect
    repl_->repl_state_.store(kReplConnecting, std::memory_order_relaxed);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    Stop();
    Start();
  }
}

void ReplicationThread::CallbacksStateMachine::SetReadCB(bufferevent *bev, bufferevent_data_cb cb) {
  bufferevent_enable(bev, EV_READ);
  bufferevent_setcb(bev, cb, nullptr, EventCallbackFunc<&CallbacksStateMachine::ConnEventCB>, this);
}

void ReplicationThread::CallbacksStateMachine::SetWriteCB(bufferevent *bev, bufferevent_data_cb cb) {
  bufferevent_enable(bev, EV_WRITE);
  bufferevent_setcb(bev, nullptr, cb, EventCallbackFunc<&CallbacksStateMachine::ConnEventCB>, this);
}

void ReplicationThread::CallbacksStateMachine::ReadWriteCB(bufferevent *bev) {
LOOP_LABEL:
  assert(handler_idx_ <= handlers_.size());
  debug("[replication] Execute handler[{}]", getHandlerName(handler_idx_));
  auto st = getHandlerFunc(handler_idx_)(repl_, bev);
  repl_->last_io_time_secs_.store(util::GetTimeStamp(), std::memory_order_relaxed);
  switch (st) {
    case CBState::NEXT:
      ++handler_idx_;
      [[fallthrough]];
    case CBState::PREV:
      if (st == CBState::PREV) --handler_idx_;
      if (getHandlerEventType(handler_idx_) == WRITE) {
        SetWriteCB(bev, EventCallbackFunc<&CallbacksStateMachine::ReadWriteCB>);
      } else {
        SetReadCB(bev, EventCallbackFunc<&CallbacksStateMachine::ReadWriteCB>);
      }
      // invoke the read handler (of next step) directly, as the bev might
      // have the data already.
      goto LOOP_LABEL;  // NOLINT
    case CBState::AGAIN:
      break;
    case CBState::QUIT:  // state that can not be retry, or all steps are executed.
      bufferevent_free(bev);
      bev_ = nullptr;
      repl_->repl_state_.store(kReplError, std::memory_order_relaxed);
      break;
    case CBState::RESTART:  // state that can be retried some time later
      Stop();
      if (repl_->stop_flag_) {
        info("[replication] Wouldn't restart while the replication thread was stopped");
        break;
      }
      repl_->repl_state_.store(kReplConnecting, std::memory_order_relaxed);
      info("[replication] Retry in 10 seconds");
      std::this_thread::sleep_for(std::chrono::seconds(10));
      Start();
  }
}

void ReplicationThread::CallbacksStateMachine::Start() {
  struct bufferevent *bev = nullptr;

  if (handlers_.empty()) {
    return;
  }

  // Note: It may cause data races to use 'masterauth' directly.
  // It is acceptable because password change is a low frequency operation.
  if (!repl_->srv_->GetConfig()->masterauth.empty()) {
    handlers_.emplace_front(CallbacksStateMachine::READ, "auth read", &ReplicationThread::authReadCB);
    handlers_.emplace_front(CallbacksStateMachine::WRITE, "auth write", &ReplicationThread::authWriteCB);
  }

  uint64_t last_connect_timestamp = 0;

  while (!repl_->stop_flag_ && bev == nullptr) {
    if (util::GetTimeStampMS() - last_connect_timestamp < 1000) {
      // prevent frequent re-connect when the master is down with the connection refused error
      sleep(1);
    }
    last_connect_timestamp = util::GetTimeStampMS();
    auto cfd = util::SockConnect(repl_->host_, repl_->port_, repl_->srv_->GetConfig()->replication_connect_timeout_ms);
    if (!cfd) {
      error("[replication] Failed to connect the master, err: {}", cfd.Msg());
      continue;
    }
#ifdef ENABLE_OPENSSL
    SSL *ssl = nullptr;
    if (repl_->srv_->GetConfig()->tls_replication) {
      ssl = SSL_new(repl_->srv_->ssl_ctx.get());
      if (!ssl) {
        error("Failed to construct SSL structure for new connection: {}", fmt::streamed(SSLErrors{}));
        evutil_closesocket(*cfd);
        return;
      }
      bev = bufferevent_openssl_socket_new(repl_->base_, *cfd, ssl, BUFFEREVENT_SSL_CONNECTING, BEV_OPT_CLOSE_ON_FREE);
    } else {
      bev = bufferevent_socket_new(repl_->base_, *cfd, BEV_OPT_CLOSE_ON_FREE);
    }
#else
    bev = bufferevent_socket_new(repl_->base_, *cfd, BEV_OPT_CLOSE_ON_FREE);
#endif
    if (bev == nullptr) {
#ifdef ENABLE_OPENSSL
      if (ssl) SSL_free(ssl);
#endif
      close(*cfd);
      error("[replication] Failed to create the event socket");
      continue;
    }
#ifdef ENABLE_OPENSSL
    if (repl_->srv_->GetConfig()->tls_replication) {
      bufferevent_openssl_set_allow_dirty_shutdown(bev, 1);
    }
#endif
  }
  if (bev == nullptr) {  // failed to connect the master and received the stop signal
    return;
  }

  handler_idx_ = 0;
  repl_->incr_state_ = Incr_batch_size;
  if (getHandlerEventType(0) == WRITE) {
    SetWriteCB(bev, EventCallbackFunc<&CallbacksStateMachine::ReadWriteCB>);
  } else {
    SetReadCB(bev, EventCallbackFunc<&CallbacksStateMachine::ReadWriteCB>);
  }
  bev_ = bev;
}

void ReplicationThread::CallbacksStateMachine::Stop() {
  if (bev_) {
    bufferevent_free(bev_);
    bev_ = nullptr;
  }
}

ReplicationThread::ReplicationThread(std::string host, uint32_t port, Server *srv)
    : host_(std::move(host)),
      port_(port),
      srv_(srv),
      storage_(srv->storage),
      repl_state_(kReplConnecting),
      psync_steps_(
          this,
          CallbacksStateMachine::CallbackList{
              CallbackType{CallbacksStateMachine::WRITE, "dbname write", &ReplicationThread::checkDBNameWriteCB},
              CallbackType{CallbacksStateMachine::READ, "dbname read", &ReplicationThread::checkDBNameReadCB},
              CallbackType{CallbacksStateMachine::WRITE, "replconf write", &ReplicationThread::replConfWriteCB},
              CallbackType{CallbacksStateMachine::READ, "replconf read", &ReplicationThread::replConfReadCB},
              CallbackType{CallbacksStateMachine::WRITE, "psync write", &ReplicationThread::tryPSyncWriteCB},
              CallbackType{CallbacksStateMachine::READ, "psync read", &ReplicationThread::tryPSyncReadCB},
              CallbackType{CallbacksStateMachine::READ, "batch loop", &ReplicationThread::incrementBatchLoopCB}}),
      fullsync_steps_(
          this, CallbacksStateMachine::CallbackList{
                    CallbackType{CallbacksStateMachine::WRITE, "fullsync write", &ReplicationThread::fullSyncWriteCB},
                    CallbackType{CallbacksStateMachine::READ, "fullsync read", &ReplicationThread::fullSyncReadCB}}) {}

Status ReplicationThread::Start(std::function<bool()> &&pre_fullsync_cb, std::function<void()> &&post_fullsync_cb) {
  pre_fullsync_cb_ = std::move(pre_fullsync_cb);
  post_fullsync_cb_ = std::move(post_fullsync_cb);

  // Clean synced checkpoint from old master because replica starts to follow new master
  auto s = rocksdb::DestroyDB(srv_->GetConfig()->sync_checkpoint_dir, rocksdb::Options());
  if (!s.ok()) {
    warn("Can't clean synced checkpoint from master, error: {}", s.ToString());
  } else {
    warn("Clean old synced checkpoint successfully");
  }

  // cleanup the old backups, so we can start replication in a clean state
  storage_->PurgeOldBackups(0, 0);

  t_ = GET_OR_RET(util::CreateThread("master-repl", [this] {
    this->run();
    assert(stop_flag_);
  }));

  return Status::OK();
}

void ReplicationThread::Stop() {
  if (stop_flag_) return;

  stop_flag_ = true;  // Stopping procedure is asynchronous,
                      // handled by timer
  if (auto s = util::ThreadJoin(t_); !s) {
    warn("Replication thread operation failed: {}", s.Msg());
  }
  info("[replication] Stopped");
}

/*
 * Run connect to master, and start the following steps
 * asynchronously
 *  - CheckDBName
 *  - TryPsync
 *  - - if ok, IncrementBatchLoop
 *  - - not, FullSync and restart TryPsync when done
 */
void ReplicationThread::run() {
  base_ = event_base_new();
  if (base_ == nullptr) {
    error("[replication] Failed to create new ev base");
    return;
  }
  psync_steps_.Start();

  auto timer = UniqueEvent(NewEvent(base_, -1, EV_PERSIST));
  timeval tmo{0, 100000};  // 100 ms
  evtimer_add(timer.get(), &tmo);

  event_base_dispatch(base_);
  timer.reset();
  event_base_free(base_);
}

ReplicationThread::CBState ReplicationThread::authWriteCB(bufferevent *bev) {
  SendString(bev, redis::ArrayOfBulkStrings({"AUTH", srv_->GetConfig()->masterauth}));
  info("[replication] Auth request was sent, waiting for response");
  repl_state_.store(kReplSendAuth, std::memory_order_relaxed);
  return CBState::NEXT;
}

inline bool ResponseLineIsOK(std::string_view line) { return line == RESP_PREFIX_SIMPLE_STRING "OK"; }

ReplicationThread::CBState ReplicationThread::authReadCB(bufferevent *bev) {  // NOLINT
  auto input = bufferevent_get_input(bev);
  UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return CBState::AGAIN;
  if (!ResponseLineIsOK(line.View())) {
    // Auth failed
    error("[replication] Auth failed: {}", line.get());
    return CBState::RESTART;
  }
  info("[replication] Auth response was received, continue...");
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::checkDBNameWriteCB(bufferevent *bev) {
  SendString(bev, redis::ArrayOfBulkStrings({"_db_name"}));
  repl_state_.store(kReplCheckDBName, std::memory_order_relaxed);
  info("[replication] Check db name request was sent, waiting for response");
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::checkDBNameReadCB(bufferevent *bev) {
  auto input = bufferevent_get_input(bev);
  UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return CBState::AGAIN;

  if (line[0] == '-') {
    if (isRestoringError(line.View())) {
      warn("The master was restoring the db, retry later");
    } else {
      error("Failed to get the db name, {}", line.get());
    }
    return CBState::RESTART;
  }
  std::string db_name = storage_->GetName();
  if (line.length == db_name.size() && !strncmp(line.get(), db_name.data(), line.length)) {
    // DB name match, we should continue to next step: TryPsync
    info("[replication] DB name is valid, continue...");
    return CBState::NEXT;
  }
  error("[replication] Mismatched the db name, local: {}, remote: {}", db_name, line.get());
  return CBState::RESTART;
}

ReplicationThread::CBState ReplicationThread::replConfWriteCB(bufferevent *bev) {
  auto config = srv_->GetConfig();

  auto port = config->replica_announce_port > 0 ? config->replica_announce_port : config->port;
  std::vector<std::string> data_to_send{"replconf", "listening-port", std::to_string(port)};
  if (!next_try_without_announce_ip_address_ && !config->replica_announce_ip.empty()) {
    data_to_send.emplace_back("ip-address");
    data_to_send.emplace_back(config->replica_announce_ip);
  }
  SendString(bev, redis::ArrayOfBulkStrings(data_to_send));
  repl_state_.store(kReplReplConf, std::memory_order_relaxed);
  info("[replication] replconf request was sent, waiting for response");
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::replConfReadCB(bufferevent *bev) {
  auto input = bufferevent_get_input(bev);
  UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return CBState::AGAIN;

  // on unknown option: first try without announce ip, if it fails again - do nothing (to prevent infinite loop)
  if (isUnknownOption(line.View()) && !next_try_without_announce_ip_address_) {
    next_try_without_announce_ip_address_ = true;
    warn("The old version master, can't handle ip-address, try without it again");
    // Retry previous state, i.e. send replconf again
    return CBState::PREV;
  }
  if (line[0] == '-' && isRestoringError(line.View())) {
    warn("The master was restoring the db, retry later");
    return CBState::RESTART;
  }
  if (!ResponseLineIsOK(line.View())) {
    warn("[replication] Failed to replconf: {}", line.get() + 1);
    //  backward compatible with old version that doesn't support replconf cmd
    return CBState::NEXT;
  } else {
    info("[replication] replconf is ok, start psync");
    return CBState::NEXT;
  }
}

ReplicationThread::CBState ReplicationThread::tryPSyncWriteCB(bufferevent *bev) {
  auto cur_seq = storage_->LatestSeqNumber();
  auto next_seq = cur_seq + 1;
  std::string replid;

  // Get replication id
  std::string replid_in_wal = storage_->GetReplIdFromWalBySeq(cur_seq);
  // Set if valid replication id
  if (replid_in_wal.length() == kReplIdLength) {
    replid = replid_in_wal;
  } else {
    // Maybe there is no WAL, we can get replication id from db since master
    // always write replication id into db before any operation when starting
    // new "replication history".
    std::string replid_in_db = storage_->GetReplIdFromDbEngine();
    if (replid_in_db.length() == kReplIdLength) {
      replid = replid_in_db;
    }
  }

  // To adapt to old master using old PSYNC, i.e. only use next sequence id.
  // Also use old PSYNC if replica can't find replication id from WAL and DB.
  if (!srv_->GetConfig()->use_rsid_psync || next_try_old_psync_ || replid.length() != kReplIdLength) {
    next_try_old_psync_ = false;  // Reset next_try_old_psync_
    SendString(bev, redis::ArrayOfBulkStrings({"PSYNC", std::to_string(next_seq)}));
    info("[replication] Try to use psync, next seq: {}", next_seq);
  } else {
    // NEW PSYNC "Unique Replication Sequence ID": replication id and sequence id
    SendString(bev, redis::ArrayOfBulkStrings({"PSYNC", replid, std::to_string(next_seq)}));
    info("[replication] Try to use new psync, current unique replication sequence id: {}:{}", replid, cur_seq);
  }
  repl_state_.store(kReplSendPSync, std::memory_order_relaxed);
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::tryPSyncReadCB(bufferevent *bev) {
  auto input = bufferevent_get_input(bev);
  UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return CBState::AGAIN;

  if (line[0] == '-' && isRestoringError(line.View())) {
    warn("The master was restoring the db, retry later");
    return CBState::RESTART;
  }

  if (line[0] == '-' && isWrongPsyncNum(line.View())) {
    next_try_old_psync_ = true;
    warn("The old version master, can't handle new PSYNC, try old PSYNC again");
    // Retry previous state, i.e. send PSYNC again
    return CBState::PREV;
  }

  if (!ResponseLineIsOK(line.View())) {
    // PSYNC isn't OK, we should use FullSync
    // Switch to fullsync state machine
    fullsync_steps_.Start();
    info("[replication] Failed to psync, error: {}, switch to fullsync", line.get());
    return CBState::QUIT;
  } else {
    // PSYNC is OK, use IncrementBatchLoop
    info("[replication] PSync is ok, start increment batch loop");
    return CBState::NEXT;
  }
}

/**
 * 增量批次循环回调函数
 *
 * @param bev bufferevent对象，用于网络通信
 * @return 返回状态机下一步状态
 *
 * 功能说明：
 * 1. 处理主节点发送的增量更新数据
 * 2. 按照Redis协议解析批量数据
 * 3. 将解析后的写批次应用到本地存储
 * 4. 处理主节点的心跳检测(ping)
 *
 * 协议格式：
 * 1. 批量数据大小行: $<size>\r\n
 * 2. 批量数据内容: <data>\r\n
 *
 * 状态转换：
 * - Incr_batch_size: 读取批量数据大小
 * - Incr_batch_data: 读取批量数据内容
 */
ReplicationThread::CBState ReplicationThread::incrementBatchLoopCB(bufferevent *bev) {
  // 设置复制状态为 "已连接"，表示增量复制阶段已经开始
  repl_state_.store(kReplConnected, std::memory_order_relaxed);

  // 获取输入缓冲区，读取主节点发送的写入数据
  auto input = bufferevent_get_input(bev);

  // 主循环，持续处理从主节点接收的增量写入数据
  while (true) {
    switch (incr_state_) {
      // 读取 `批量数据大小`
      case Incr_batch_size: {
        // 从缓冲区中读取一行，格式为 "$<size>\r\n"，表示接下来数据的长度（RESP 协议中的 Bulk String）
        UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
        if (!line) return CBState::AGAIN; // 数据不足，直接返回，等待下次回调来获取更多数据
        // 解析批量数据大小($<size>中的size, 需要跳过 '$' 开头) ，单位为字节数
        incr_bulk_len_ = line.length > 0 ? std::strtoull(line.get() + 1, nullptr, 10) : 0;
        // 验证数据大小有效性
        if (incr_bulk_len_ == 0) {
          error("[replication] Invalid increment data size");
          return CBState::RESTART;   // 无效数据，重启状态机
        }
        // 读取 `批量数据大小` 成功，进入读取 `批量数据内容` 阶段
        incr_state_ = Incr_batch_data;
        break;
      }
      // 读取 `批量数据内容`
      case Incr_batch_data:
        // 判断输入缓冲区是否包含足够的数据（incr_bulk_len_ + "\r\n"）
        if (incr_bulk_len_ + 2 > evbuffer_get_length(input)) {
          return CBState::AGAIN;  // 数据不足，直接返回，等待下次回调来获取更多数据
        }
        // 获取批量数据指针(避免拷贝)，数据中包括后续的 \r\n
        const char *bulk_data =
            reinterpret_cast<const char *>(evbuffer_pullup(input, static_cast<ssize_t>(incr_bulk_len_ + 2)));

        // 创建 bulk_string ，存储 bulk_data 前 bulk_len_ 字节，不含 \r\n
        std::string bulk_string = std::string(bulk_data, incr_bulk_len_);

        // 从缓冲区移除已处理数据
        evbuffer_drain(input, incr_bulk_len_ + 2);

        // 重置状态为读取下一个批量数据大小
        incr_state_ = Incr_batch_size;

        // 特殊处理 ping 包：主节点定期发送 "ping"，作为心跳检测，不应写入数据库
        if (bulk_string == "ping") {
          return CBState::AGAIN;  // 不处理 ping，仅保持连接活跃
        }

        // 基于 bulk_string 构造 RocksDB 的 WriteBatch
        rocksdb::WriteBatch batch(std::move(bulk_string));

        // 把 WriteBatch 应用到本地存储
        auto s = storage_->ReplicaApplyWriteBatch(&batch);
        if (!s.IsOK()) {
          error("[replication] CRITICAL - Failed to write batch to local, {}. batch: 0x{}", s.Msg(),
                util::StringToHex(batch.Data()));
          return CBState::RESTART;  // 写入失败，重启状态机
        }

        // 解析 WriteBatch，用于统计或进一步处理（如 Stream、PubSub）
        s = parseWriteBatch(batch);
        if (!s.IsOK()) {
          error("[replication] CRITICAL - failed to parse write batch 0x{}: {}", util::StringToHex(batch.Data()),
                s.Msg());
          return CBState::RESTART;
        }

        break;
    }
  }
}

ReplicationThread::CBState ReplicationThread::fullSyncWriteCB(bufferevent *bev) {
  SendString(bev, redis::ArrayOfBulkStrings({"_fetch_meta"}));
  repl_state_.store(kReplFetchMeta, std::memory_order_relaxed);
  info("[replication] Start syncing data with fullsync");
  return CBState::NEXT;
}

/**
 * 当从节点进入 fullsync 状态时，它将读取来自主节点发送的 元信息（meta）及 SST 文件清单，并随后开始并行下载数据文件，并恢复到本地 DB 。
 *
 * @param bev Libevent 缓冲事件对象，用于网络 IO
 * @return 状态机下一步动作 (AGAIN/RESTART/QUIT)
 */


// Q: 为啥需要清理检查点中的无效文件，检查点中都包含啥文件？
// A:
//  检查点（checkpoint） 包含数据库在某一时刻的完整快照文件，这些文件用于全量同步（full sync）时从主节点（master）复制到从节点（slave）。
//
//  检查点中的文件通常包括：
//  - SST 文件（.sst）: RocksDB 的实际数据文件，存储键值对数据。
//  - MANIFEST 文件: 记录 RocksDB 的版本变更信息，用于恢复数据库状态。
//  - CURRENT 文件: 指向当前有效的 MANIFEST 文件。
//  - OPTIONS 文件: 存储数据库配置选项。
//  - 其他可能的元数据文件: 如 IDENTITY、LOG 等。
//
//  为什么需要清理无效文件？
//  在复制过程中，从节点需要确保检查点目录中只包含主节点发送的有效文件，避免以下问题：
//  - 避免残留旧数据污染新同步
//    如果前一次同步失败，可能残留部分文件，如果不清理，可能导致数据不一致。
//    例如，旧 .sst 文件可能包含已删除的数据，影响新同步的正确性。
//  - CURRENT 文件必须清理
//    CURRENT 文件不包含文件编号（而其他文件如 MANIFEST-000123 有编号），无法通过文件名判断其是否属于当前有效数据。
//    如果不清理，可能导致从节点加载错误的 MANIFEST，进而恢复错误的数据库状态。
//  - 节省磁盘空间
//    无效文件占用磁盘空间，尤其是大容量数据库，清理可以避免浪费。
ReplicationThread::CBState ReplicationThread::fullSyncReadCB(bufferevent *bev) {
  auto input = bufferevent_get_input(bev);  // 获取输入缓冲区
  switch (fullsync_state_) {
    // 阶段1：获取主节点发来的 meta ID（RocksDB备份ID），新版本 kvrocks 没有此阶段
    case kFetchMetaID: {
      // 如果主节点是新版本（不使用 repl port），则跳过 meta_id 阶段
      if (!srv_->GetConfig()->master_use_repl_port) {
        fullsync_state_ = kFetchMetaContent;
        return CBState::AGAIN;
      }
      // 读取一行数据，从中解析出 backup ID
      UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
      if (!line) return CBState::AGAIN;  // 数据不足，等待下次回调
      if (line[0] == '-') { // 错误处理（主节点返回"-"开头表示错误）
        error("[replication] Failed to fetch meta id: {}", line.get());
        return CBState::RESTART;  // 需要重启同步流程
      }
      // 解析备份ID（RocksDB的BackupID）
      fullsync_meta_id_ = static_cast<rocksdb::BackupID>(line.length > 0 ? std::strtoul(line.get(), nullptr, 10) : 0);
      if (fullsync_meta_id_ == 0) {  // 校验有效性
        error("[replication] Invalid meta id received");
        return CBState::RESTART;
      }
      fullsync_state_ = kFetchMetaSize;  // 转移到下一状态
      info("[replication] Succeed fetching meta id: {}", fullsync_meta_id_);
      // 注意：此处故意省略break，实现自动状态流转
    }

    // 阶段2：获取主节点发来的 meta 文件大小
    case kFetchMetaSize: {
      UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
      if (!line) return CBState::AGAIN;
      if (line[0] == '-') {
        error("[replication] Failed to fetch meta size: {}", line.get());
        return CBState::RESTART;
      }

      fullsync_filesize_ = line.length > 0 ? std::strtoull(line.get(), nullptr, 10) : 0;
      if (fullsync_filesize_ == 0) {  // 校验文件大小有效性
        error("[replication] Invalid meta file size received");
        return CBState::RESTART;
      }

      fullsync_state_ = kFetchMetaContent;  // 状态转移
      info("[replication] Succeed fetching meta size: {}", fullsync_filesize_);
      // 注意：此处故意省略break，实现自动状态流转
    }

    // 阶段3：获取元数据内容（核心阶段）
    case kFetchMetaContent: {
      std::string target_dir;
      engine::Storage::ReplDataManager::MetaInfo meta;

      // 处理旧版本 master 的情况（通过备份端口）
      if (srv_->GetConfig()->master_use_repl_port) {
        // 检查是否接收完完整的元数据文件
        if (evbuffer_get_length(input) < fullsync_filesize_) {
          return CBState::AGAIN;
        }
        // 解析并保存元数据（包含SST文件列表等信息）
        auto s = engine::Storage::ReplDataManager::ParseMetaAndSave(storage_, fullsync_meta_id_, input, &meta);
        if (!s.IsOK()) {
          error("[replication] Failed to parse meta and save: {}", s.Msg());
          return CBState::AGAIN;
        }
        target_dir = srv_->GetConfig()->backup_sync_dir;  // 备份目录
      }
      // 处理新版本 master 的情况（通过常规复制端口）
      else {
        // 读取一行数据
        UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
        if (!line) return CBState::AGAIN;
        if (line[0] == '-') {
          error("[replication] Failed to fetch meta info: {}", line.get());
          return CBState::RESTART;
        }

        // 解析出需要同步的文件列表（逗号分隔）
        std::vector<std::string> need_files = util::Split(std::string(line.get()), ",");
        for (const auto &f : need_files) {
          meta.files.emplace_back(f, 0);  // 文件名+文件大小（0 表示未指定）
        }

        // 清理检查点目录中的无效文件，"CURRENT"文件必须被清理(因为 CURRENT 是软链接，必须重新生成)
        target_dir = srv_->GetConfig()->sync_checkpoint_dir;  // 检查点目录
        auto iter = std::find(need_files.begin(), need_files.end(), "CURRENT");
        if (iter != need_files.end()) need_files.erase(iter);
        // 清理不在 need_files 列表中的文件
        auto s = engine::Storage::ReplDataManager::CleanInvalidFiles(storage_, target_dir, need_files);

        // 清理失败时的降级处理：暴力删除整个目录，确保没有残留数据影响同步。
        if (!s.IsOK()) {
          warn("[replication] Failed to clean invalid files, attempting full cleanup");
          auto s = rocksdb::DestroyDB(target_dir, rocksdb::Options());
          if (!s.ok()) {
            warn("[replication] Full cleanup failed: {}", s.ToString());
          }
        }
      }

      assert(evbuffer_get_length(input) == 0);  // 确保缓冲区已完全消费
      fullsync_state_ = kFetchMetaID;  // 重置状态机
      info("[replication] Files info received, starting parallel download");

      // 如果配置要求全同步前清空本地数据库
      bool pre_fullsync_done = false;
      if (srv_->GetConfig()->slave_empty_db_before_fullsync) {
        if (!pre_fullsync_cb_()) return CBState::RESTART;  // 回调预处理
        pre_fullsync_done = true;
        storage_->EmptyDB();  // 清空数据库
      }

      // 并行下载 SST 文件（多线程加速）
      repl_state_.store(kReplFetchSST, std::memory_order_relaxed);
      auto s = parallelFetchFile(target_dir, meta.files);
      if (!s.IsOK()) {
        if (pre_fullsync_done) post_fullsync_cb_();  // 回滚预处理
        error("[replication] Parallel fetch failed: {}", s.Msg());
        return CBState::RESTART;
      }
      info("[replication] Files download completed, restoring data");

      // 执行预处理回调（如果之前未执行）
      if (!pre_fullsync_done && !pre_fullsync_cb_()) return CBState::RESTART;

      // 数据恢复：根据 master 版本选择不同的恢复方式，旧版本用 BackupEngine ，新版本用 Checkpoint
      if (srv_->GetConfig()->master_use_repl_port) {
        s = storage_->RestoreFromBackup();    // 从备份恢复(旧版本)
      } else {
        s = storage_->RestoreFromCheckpoint(); // 从检查点恢复(新版本)
      }
      if (!s.IsOK()) {
        error("[replication] Restore failed: {}, restarting", s.Msg());
        post_fullsync_cb_();  // 清理资源
        return CBState::RESTART;
      }
      info("[replication] Data restore completed");
      post_fullsync_cb_();  // 正常完成后的清理

      // 重新加载命名空间
      //  全同步时直接替换了底层 RocksDB 数据文件（SST），但内存中的 namespace 信息没更新。
      //  `LoadAndRewrite` 会解析所有命名空间，重建内存中的元数据。
      s = srv_->GetNamespace()->LoadAndRewrite();
      if (!s.IsOK()) {
        error("[replication] Namespace reload failed: {}", s.Msg());
      }

      // 切换回增量同步状态机
      psync_steps_.Start();
      return CBState::QUIT;  // 全量同步完成
    }
  }
  unreachable();  // 理论上不可达（状态机完整性保护）
}
// 并行地从 master 拉取多个文件
// 参数:
//   dir: 存储文件的目标目录
//   files: 需要拉取的文件列表，每个元素是 <文件名, CRC校验值> 对
Status ReplicationThread::parallelFetchFile(const std::string &dir,
                                            const std::vector<std::pair<std::string, uint32_t>> &files) {
  // 默认使用一个线程（串行），如果文件数超过 20，则使用 4 个线程并行拉取。
  size_t concurrency = 1;
  if (files.size() > 20) {
    concurrency = 4;
  }

  // 原子计数器：记录已拉取和跳过的文件数
  std::atomic<uint32_t> fetch_cnt = {0};
  std::atomic<uint32_t> skip_cnt = {0};

  // 存储异步操作结果的vector
  std::vector<std::future<Status>> results;

  // 创建多个线程执行拉取任务
  for (size_t tid = 0; tid < concurrency; ++tid) {
    results.push_back(
        std::async(std::launch::async, [this, dir, &files, tid, concurrency, &fetch_cnt, &skip_cnt]() -> Status {
          // 检查停止标志
          if (this->stop_flag_) {
            return {Status::NotOK, "replication thread was stopped"};
          }

          // SSL相关初始化
          ssl_st *ssl = nullptr;
#ifdef ENABLE_OPENSSL
          // 如果启用了 TLS，创建 SSL 上下文
          if (this->srv_->GetConfig()->tls_replication) {
            ssl = SSL_new(this->srv_->ssl_ctx.get());
          }
          // 使用 RAII 方式在退出时释放 SSL 资源
          auto exit = MakeScopeExit([ssl] { SSL_free(ssl); });
#endif

          // 与 master 建立连接（可使用 SSL）
          int sock_fd = GET_OR_RET(util::SockConnect(this->host_,
                                                     this->port_,
                                                     ssl,
                                                     this->srv_->GetConfig()->replication_connect_timeout_ms,
                                                     this->srv_->GetConfig()->replication_recv_timeout_ms)
                                       .Prefixed("connect the server err"));

#ifdef ENABLE_OPENSSL
          // 如果连接成功，不释放 SSL，后续仍需使用
          exit.Disable();
#endif

          UniqueFD unique_fd{sock_fd};  // 使用RAII管理socket

          // 发送认证信息
          auto s = this->sendAuth(sock_fd, ssl);
          if (!s.IsOK()) {
            return s.Prefixed("send the auth command err");
          }

          // 准备需要拉取的文件列表
          std::vector<std::string> fetch_files; // 文件名
          std::vector<uint32_t> crcs;           // 校验值

          // 分配任务给当前线程(按线程ID和并发数分配)
          for (auto f_idx = tid; f_idx < files.size(); f_idx += concurrency) {
            if (this->stop_flag_) {
              return {Status::NotOK, "replication thread was stopped"};
            }

            const auto &f_name = files[f_idx].first;
            const auto &f_crc = files[f_idx].second;

            // 检查文件是否已存在且 CRC 匹配
            if (engine::Storage::ReplDataManager::FileExists(this->storage_, dir, f_name, f_crc)) {
              skip_cnt.fetch_add(1);  // 增加跳过计数
              uint32_t cur_skip_cnt = skip_cnt.load();
              uint32_t cur_fetch_cnt = fetch_cnt.load();
              info("[skip] {} {}, skip count: {}, fetch count: {}, progress: {} / {}",
                   f_name, f_crc, cur_skip_cnt, cur_fetch_cnt,
                   (cur_skip_cnt + cur_fetch_cnt), files.size()); // 打印跳过信息
              continue;
            }

            // 添加到待拉取列表
            fetch_files.push_back(f_name);
            crcs.push_back(f_crc);
          }

          unsigned files_count = files.size();
          // 拉取完成后的回调函数
          FetchFileCallback fn = [&fetch_cnt, &skip_cnt, files_count](const std::string &fetch_file, uint32_t fetch_crc) {
            fetch_cnt.fetch_add(1);  // 增加拉取计数
            uint32_t cur_skip_cnt = skip_cnt.load();
            uint32_t cur_fetch_cnt = fetch_cnt.load();
            info("[fetch] Fetched {}, crc32 {}, skip count: {}, fetch count: {}, progress: {} / {}",
                fetch_file, fetch_crc, cur_skip_cnt, cur_fetch_cnt,
                cur_skip_cnt + cur_fetch_cnt, files_count); // 打印拉取信息
          };

          // 根据主节点版本选择拉取方式
          if (srv_->GetConfig()->master_use_repl_port) {
            // 旧版本主节点：每次只能拉取单个文件，需多次请求。
            for (unsigned i = 0; i < fetch_files.size(); i++) {
              s = this->fetchFiles(sock_fd, dir, {fetch_files[i]}, {crcs[i]}, fn, ssl);
              if (!s.IsOK()) break;
            }
          } else {
            // 新版本主节点：可以批量拉取文件
            if (!fetch_files.empty()) {
              s = this->fetchFiles(sock_fd, dir, fetch_files, crcs, fn, ssl);
            }
          }
          return s;
        }));
  }

  // 等待所有线程结束，任何一个线程失败则直接返回错误。
  for (auto &f : results) {
    Status s = f.get();
    if (!s.IsOK()) return s;
  }
  return Status::OK();
}

Status ReplicationThread::sendAuth(int sock_fd, ssl_st *ssl) {
  // Send auth when needed
  std::string auth = srv_->GetConfig()->masterauth;
  if (!auth.empty()) {
    UniqueEvbuf evbuf;
    const auto auth_command = redis::ArrayOfBulkStrings({"AUTH", auth});
    auto s = util::SockSend(sock_fd, auth_command, ssl);
    if (!s.IsOK()) return s.Prefixed("send auth command err");
    while (true) {
      if (auto s = util::EvbufferRead(evbuf.get(), sock_fd, -1, ssl); !s) {
        return std::move(s).Prefixed("read auth response err");
      }
      UniqueEvbufReadln line(evbuf.get(), EVBUFFER_EOL_CRLF_STRICT);
      if (!line) continue;
      if (!ResponseLineIsOK(line.View())) {
        return {Status::NotOK, "auth got invalid response"};
      }
      break;
    }
  }
  return Status::OK();
}

Status ReplicationThread::fetchFile(int sock_fd,
                                    evbuffer *evbuf,
                                    const std::string &dir,
                                    const std::string &file,
                                    uint32_t crc,
                                    const FetchFileCallback &fn,
                                    ssl_st *ssl) {
  size_t file_size = 0;

  // Read file size line
  while (true) {
    UniqueEvbufReadln line(evbuf, EVBUFFER_EOL_CRLF_STRICT);
    if (!line) {
      if (auto s = util::EvbufferRead(evbuf, sock_fd, -1, ssl); !s) {
        if (s.Is<Status::TryAgain>()) {
          if (stop_flag_) {
            return {Status::NotOK, "replication thread was stopped"};
          }
          continue;
        }
        return std::move(s).Prefixed("read size");
      }
      continue;
    }
    if (line[0] == '-') {
      std::string msg(line.get());
      return {Status::NotOK, msg};
    }
    file_size = line.length > 0 ? std::strtoull(line.get(), nullptr, 10) : 0;
    break;
  }

  // Write to tmp file
  auto tmp_file = engine::Storage::ReplDataManager::NewTmpFile(storage_, dir, file);
  if (!tmp_file) {
    return {Status::NotOK, "unable to create tmp file"};
  }

  size_t remain = file_size;
  uint32_t tmp_crc = 0;
  char data[16 * 1024];
  while (remain != 0) {
    if (evbuffer_get_length(evbuf) > 0) {
      auto data_len = evbuffer_remove(evbuf, data, remain > 16 * 1024 ? 16 * 1024 : remain);
      if (data_len == 0) continue;
      if (data_len < 0) {
        return {Status::NotOK, "read sst file data error"};
      }
      tmp_file->Append(rocksdb::Slice(data, data_len));
      tmp_crc = rocksdb::crc32c::Extend(tmp_crc, data, data_len);
      remain -= data_len;
    } else {
      if (auto s = util::EvbufferRead(evbuf, sock_fd, -1, ssl); !s) {
        if (s.Is<Status::TryAgain>()) {
          if (stop_flag_) {
            return {Status::NotOK, "replication thread was stopped"};
          }
          continue;
        }
        return std::move(s).Prefixed("read sst file");
      }
    }
  }
  // Verify file crc checksum if crc is not 0
  if (crc && crc != tmp_crc) {
    return {Status::NotOK, fmt::format("CRC mismatched, {} was expected but got {}", crc, tmp_crc)};
  }
  // File is OK, rename to formal name
  auto s = engine::Storage::ReplDataManager::SwapTmpFile(storage_, dir, file);
  if (!s.IsOK()) return s;

  // Call fetch file callback function
  fn(file, crc);
  return Status::OK();
}

Status ReplicationThread::fetchFiles(int sock_fd, const std::string &dir, const std::vector<std::string> &files,
                                     const std::vector<uint32_t> &crcs, const FetchFileCallback &fn, ssl_st *ssl) {
  std::string files_str;
  for (const auto &file : files) {
    files_str += file;
    files_str.push_back(',');
  }
  files_str.pop_back();

  const auto fetch_command = redis::ArrayOfBulkStrings({"_fetch_file", files_str});
  auto s = util::SockSend(sock_fd, fetch_command, ssl);
  if (!s.IsOK()) return s.Prefixed("send fetch file command");

  UniqueEvbuf evbuf;
  for (unsigned i = 0; i < files.size(); i++) {
    debug("[fetch] Start to fetch file {}", files[i]);
    s = fetchFile(sock_fd, evbuf.get(), dir, files[i], crcs[i], fn, ssl);
    if (!s.IsOK()) {
      s = Status(Status::NotOK, "fetch file err: " + s.Msg());
      warn("[fetch] Fail to fetch file {}, err: {}", files[i], s.Msg());
      break;
    }
    debug("[fetch] Succeed fetching file {}", files[i]);

    // Just for tests
    if (srv_->GetConfig()->fullsync_recv_file_delay) {
      sleep(srv_->GetConfig()->fullsync_recv_file_delay);
    }
  }
  return s;
}

// Check if stop_flag_ is set, when do, tear down replication
void ReplicationThread::TimerCB(int, int16_t) {
  // DLOG(INFO) << "[replication] timer";
  if (stop_flag_) {
    info("[replication] Stop ev loop");
    event_base_loopbreak(base_);
    psync_steps_.Stop();
    fullsync_steps_.Stop();
  }
}


/**
 * 解析主节点发送来的 WriteBatch 数据，并处理其中的特殊逻辑，比如：
 *  - 发布 Pub/Sub 消息；
 *  - 处理脚本传播；
 *  - 处理 Stream 消息追加事件；
 *  - 加载命名空间配置。
 *
 * @param write_batch RocksDB WriteBatch 对象，包含需要解析的操作数据
 * @return 返回执行状态，成功返回OK，失败返回错误信息
 *
 * 功能说明：
 * 1. 使用 WriteBatchHandler 迭代解析写批次中的操作
 * 2. 根据操作类型(kBatchTypePublish/kBatchTypePropagate/kBatchTypeStream)执行相应处理
 * 3. 处理发布订阅消息、传播命令和流数据等特殊操作
 *
 * 处理逻辑：
 * - 发布订阅消息: 通过 PublishMessage 广播消息
 * - 传播命令: 执行传播的命令或加载命名空间
 * - 流数据: 触发流条目添加事件
 * - 其他类型: 不做特殊处理
 */
Status ReplicationThread::parseWriteBatch(const rocksdb::WriteBatch &write_batch) {
  // 1. 创建自定义的 WriteBatchHandler，用于处理 WriteBatch 中的数据操作
  WriteBatchHandler write_batch_handler;
  // 2. 迭代处理 WriteBatch 中的数据操作
  auto db_status = write_batch.Iterate(&write_batch_handler);
  if (!db_status.ok())
    return {Status::NotOK, "failed to iterate over write batch: " + db_status.ToString()};

  // 3. 根据 WriteBatch 的类型执行相应处理
  switch (write_batch_handler.Type()) {
    // 3.1 发布订阅处理：
    //  - 当操作类型为 kBatchTypePublish 时，调用 PublishMessage 进行消息广播
    //  - 实现跨节点的发布订阅功能
    case kBatchTypePublish:
      // 主节点执行过 PUBLISH 命令，从节点收到后也触发本地发布，将 kv 发布消息到指定频道
      srv_->PublishMessage(write_batch_handler.Key(), write_batch_handler.Value());
      break;
    // 3.2 命令传播处理：
    //  - 处理传播的 Lua 脚本命令(engine::kPropagateScriptCommand)
    //  - 处理命名空间配置的动态加载(kNamespaceDBKey)
    case kBatchTypePropagate:
      if (write_batch_handler.Key() == engine::kPropagateScriptCommand) {    // Lua 脚本
        // 将 value 解析为 Redis 协议格式，再次执行
        std::vector<std::string> tokens = util::TokenizeRedisProtocol(write_batch_handler.Value());
        if (!tokens.empty()) {
          auto s = srv_->ExecPropagatedCommand(tokens);  // 执行实际命令
          if (!s.IsOK()) {
            return s.Prefixed("failed to execute propagate command");
          }
        }
      } else if (write_batch_handler.Key() == kNamespaceDBKey) { // namespace 配置命令（用于加载/同步命名空间信息）
        // 重新加载 namespace 信息
        auto s = srv_->GetNamespace()->LoadAndRewrite();
        if (!s.IsOK()) {
          return s.Prefixed("failed to load namespaces");
        }
      }
      break;
    // 3.3 流数据处理
    //  - 解析流条目键(包含命名空间和键名)
    //  - 从子键中提取流条目ID(时间戳+序列号)
    //  - 触发流条目添加事件通知
    case kBatchTypeStream: {
      // 从 batch 中获取当前写入的 key ，并解析内部 key（包含 slot、ns、key 等）
      auto key = write_batch_handler.Key();
      InternalKey ikey(key, storage_->IsSlotIdEncoded());
      // 提取 entry_id（包含时间戳+序列号）
      Slice entry_id = ikey.GetSubKey();
      redis::StreamEntryID id;
      GetFixed64(&entry_id, &id.ms);  // 解析时间戳，毫秒
      GetFixed64(&entry_id, &id.seq); // 解析序列号，同一毫秒内的序号
      // 调用 stream 数据追加的回调（通知监听器、触发事件）
      srv_->OnEntryAddedToStream(ikey.GetNamespace().ToString(), ikey.GetKey().ToString(), id);
      break;
    }
    // 3.4 无需特殊处理
    case kBatchTypeNone:
      break;
  }

  return Status::OK();  // 所有处理成功，返回 OK
}

bool ReplicationThread::isRestoringError(std::string_view err) {
  // err doesn't contain the CRLF, so cannot use redis::Error here.
  return err == RESP_PREFIX_ERROR + redis::StatusToRedisErrorMsg({Status::RedisLoading, redis::errRestoringBackup});
}

bool ReplicationThread::isWrongPsyncNum(std::string_view err) {
  // err doesn't contain the CRLF, so cannot use redis::Error here.
  return err == RESP_PREFIX_ERROR + redis::StatusToRedisErrorMsg({Status::NotOK, redis::errWrongNumOfArguments});
}

bool ReplicationThread::isUnknownOption(std::string_view err) {
  // err doesn't contain the CRLF, so cannot use redis::Error here.
  return err == RESP_PREFIX_ERROR + redis::StatusToRedisErrorMsg({Status::NotOK, redis::errUnknownOption});
}

// PutCF 解析 rocksdb 写入操作属于哪种语义类型，并将 key/value 缓存在 handler 内部字段中（kv_）以备后续 parseWriteBatch 使用
//
// 在 Kvrocks 中，为了实现 Redis 语义，设计了多个逻辑上的列族（Column Family），如：
//
// 列族名	枚举值	                        功能说明
// PubSub	ColumnFamilyID::PubSub	        存放主节点发布的 Pub/Sub 消息
// Propagate	ColumnFamilyID::Propagate	存放需要传播的命令（如脚本、namespace）
// Stream	ColumnFamilyID::Stream	        存放 Stream 的实际追加数据
//
// 通过这样的设计，主节点在执行命令时会把需要同步给从节点的副作用数据写入对应列族中，从节点在解析写批时就能知道它是何种语义，进而触发适当的行为。
rocksdb::Status WriteBatchHandler::PutCF(uint32_t column_family_id, const rocksdb::Slice &key, const rocksdb::Slice &value) {
  // 默认认为是普通写操作（无副作用）
  type_ = kBatchTypeNone;

  // 判断是否是 PubSub 消息列族
  if (column_family_id == static_cast<uint32_t>(ColumnFamilyID::PubSub)) {
    type_ = kBatchTypePublish;  // 标记为 PUBLISH 类型
    kv_ = std::make_pair(key.ToString(), value.ToString());  // 记录 key/value
    return rocksdb::Status::OK();
  // 判断是否是 Propagate（命令传播）列族
  } else if (column_family_id == static_cast<uint32_t>(ColumnFamilyID::Propagate)) {
    type_ = kBatchTypePropagate;  // 标记为传播命令
    kv_ = std::make_pair(key.ToString(), value.ToString());
    return rocksdb::Status::OK();
  // 判断是否是 Stream 列族（用于处理 Stream 数据追加）
  } else if (column_family_id == static_cast<uint32_t>(ColumnFamilyID::Stream)) {
    type_ = kBatchTypeStream;  // 标记为 Stream 类型
    kv_ = std::make_pair(key.ToString(), value.ToString());
    return rocksdb::Status::OK();
  }
  // 其他列族：默认不需要特殊处理，仅作为普通写入存在
  return rocksdb::Status::OK();
}

