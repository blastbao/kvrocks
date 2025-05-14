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

#include "commander.h"
#include "error_constants.h"
#include "io_util.h"
#include "scope_exit.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "thread_util.h"
#include "time_util.h"
#include "unique_fd.h"

namespace redis {

class CommandPSync : public Commander {
 public:

  // 参数解析
  //
  // 解析 PSYNC 命令参数，支持两种格式：
  //  - 旧版：PSYNC <repl_seq>
  //  - 新版：PSYNC <repl_id> <repl_seq>
  //
  // 参数验证：
  //  - 检查序列号是否为有效的无符号长整型
  //  - 检查复制ID长度是否正确(40字符)
  //
  // 设置成员变量：
  //  - next_repl_seq_: 从节点请求的序列号
  //  - new_psync_: 是否使用新版PSYNC协议
  //  - replica_replid_: 从节点的复制ID
  Status Parse(const std::vector<std::string> &args) override {
    size_t seq_arg = 1;
    if (args.size() == 3) {
      seq_arg = 2;
      new_psync_ = true;
    }

    auto parse_result = ParseInt<uint64_t>(args[seq_arg], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, "value is not an unsigned long long or out of range"};
    }

    next_repl_seq_ = static_cast<rocksdb::SequenceNumber>(*parse_result);
    if (new_psync_) {
      assert(args.size() == 3);
      replica_replid_ = args[1];
      if (replica_replid_.size() != kReplIdLength) {
        return {Status::RedisParseErr, "Wrong replication id length"};
      }
    }

    return Commander::Parse(args);
  }

  // 命令执行
  //
  // 当从节点需要与主节点同步时：
  //  - 从节点发送PSYNC命令，带上自己的复制状态
  //  - 主节点检查是否可以增量同步
  //    - 如果可以，建立增量同步通道
  //    - 如果不行，要求从节点执行全量同步
  //
  //
  // Q: 为什么需要 rsid_psync 机制？
  // A:
  //
  //
  //
  // Q: 为什么需要设置 conn->Detach() 等标记？
  // 在 Kvrocks 的 PSYNC 执行阶段（增量同步或全量同步）：
  //  - 主线程（Worker Thread）原本通过 libevent 管理每个连接的 I/O（非阻塞 + 事件驱动）。
  //  - PSYNC 后续需要主节点在一个独立线程中不断发送 binlog/WAL 给从节点，这个线程会用同步 I/O（阻塞写）方式发送数据流。
  // 为了避免主线程和新线程共同管理同一个连接对象 conn（尤其是 bufferevent）导致竞争或重复释放等问题，必须进行隔离。
  //
  // conn->Detach():
  //  从主线程的 event loop 中注销这个连接，libevent 不再自动监控此连接的读写事件，后续的 socket I/O 会完全由新线程手动控制（比如：手动调用 write() 或 send()）。
  //
  // conn->EnableFlag(redis::Connection::kSlave):
  //  给连接打上 slave 标识（用于后续逻辑分支判断）, Kvrocks 会根据这个标志在其他地方（比如清理、统计）做特殊处理。
  //
  // SockSetBlocking(conn->GetFD(), 1):
  //  把 socket 设置为阻塞模式。
  //  因为，在主线程中 libevent 是基于非阻塞 I/O 的，但我们现在切到子线程，由它同步循环地发送 wal 变更，
  //  所以，我们改为阻塞 socket，以便 write() 会自动阻塞直到数据发送完。
  //
  // conn->EnableFlag(redis::Connection::kCloseAsync);
  //  如果出错了，就标记连接需要异步关闭，确保在主线程之外也能安全关闭连接。

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    // 记录从节点连接信息和同步请求详情，包括从节点ip/port、请求同步的 WAL 序号、replica id 、本地 WAL 最新序号等。
    info(
        "Slave {}, listening port: {}, announce ip: {} asks for synchronization "
        "with next sequence: {}, replication id: {}, and local sequence: {}",
        conn->GetAddr(), conn->GetListeningPort(), conn->GetAnnounceIP(), next_repl_seq_,
        (replica_replid_.length() ? replica_replid_ : "not supported"), srv->storage->LatestSeqNumber());
    bool need_full_sync = false;

    // Check replication id of the last sequence log
    // 如果是新协议，且当前实例启用基于 ReplID 的增量同步校验机制，简称 rsid_psync（Replication Sequence ID PSYNC），则需要检查 repl_id 是否匹配；
    if (new_psync_ && srv->GetConfig()->use_rsid_psync) {
      // 过程:
      //  - 从节点请求以 next_repl_seq_ 开始同步；
      //  - 主节点查询 next_repl_seq_ - 1 的 WAL 记录对应的 replid；
      //  - 如果两者不匹配，说明主从历史不一致，需要全量同步。
      std::string replid_in_wal = srv->storage->GetReplIdFromWalBySeq(next_repl_seq_ - 1);
      info("Replication id in WAL: {}", replid_in_wal);
      // We check replication id only when WAL has this sequence, since there may be no WAL,
      // Or WAL may have nothing when starting from db of old version kvrocks.
      if (replid_in_wal.length() == kReplIdLength && replid_in_wal != replica_replid_) {
        *output = "wrong replication id of the last log";
        need_full_sync = true; // 需要全量同步
      }
    }

    // Check Log sequence
    // 检查 next_repl_seq_ 是否在当前 WAL 范围内，WAL 可能被滚动清理了
    if (!need_full_sync && !checkWALBoundary(srv->storage, next_repl_seq_).IsOK()) {
      *output = "sequence out of range, please use fullsync";
      need_full_sync = true; // 若不在，必须全量同步
    }

    // 如果需要全量同步，直接返回错误
    // 注意，主节点不会主动发起全量同步，而是提示从节点“重新发送 FULLSYNC”。
    if (need_full_sync) {
      srv->stats.IncrPSyncErrCount();
      return {Status::RedisExecErr, *output};
    }

    // Server would spawn a new thread to sync the batch, and connection would
    // be taken over, so should never trigger any event in worker thread.
    //
    // 设置连接选型，因为这个连接后续交给异步线程处理
    conn->Detach();
    conn->EnableFlag(redis::Connection::kSlave);
    auto s = util::SockSetBlocking(conn->GetFD(), 1);
    if (!s.IsOK()) {
      conn->EnableFlag(redis::Connection::kCloseAsync);
      return s.Prefixed("failed to set blocking mode on socket");
    }
    srv->stats.IncrPSyncOKCount();

    // 将当前连接加入主节点的 slave 列表，每个 slave 会创建一个 FeedSlaveThread 后台线程，用于同步增量变更给从节点
    s = srv->AddSlave(conn, next_repl_seq_);
    if (!s.IsOK()) {
      // 如果出错，返回错误给从节点
      std::string err = redis::Error(s);
      s = util::SockSend(conn->GetFD(), err, conn->GetBufferEvent());
      if (!s.IsOK()) {
        warn("failed to send error message to the replica: {}", s.Msg());
      }
      conn->EnableFlag(redis::Connection::kCloseAsync);
      warn("Failed to add replica: {} to start incremental syncing", conn->GetAddr());
    } else {
      info("New replica: {} was added, start incremental syncing", conn->GetAddr());
    }
    return s;
  }

 private:
  rocksdb::SequenceNumber next_repl_seq_ = 0;
  bool new_psync_ = false;
  std::string replica_replid_;

  // Return OK if the seq is in the range of the current WAL
  static Status checkWALBoundary(engine::Storage *storage, rocksdb::SequenceNumber seq) {
    // 正常请求
    if (seq == storage->LatestSeqNumber() + 1) {
      return Status::OK();
    }

    // Upper bound
    // 超过上界
    if (seq > storage->LatestSeqNumber() + 1) {
      return {Status::NotOK};
    }

    // Lower bound
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    auto s = storage->GetWALIter(seq, &iter);
    if (s.IsOK() && iter->Valid()) {
      auto batch = iter->GetBatch();
      if (seq != batch.sequence) {
        if (seq > batch.sequence) {
          error("checkWALBoundary with sequence: {}, but GetWALIter return older sequence: {}", seq, batch.sequence);
        }
        // 低于下界，报错
        return {Status::NotOK};
      }
      // 在 wal 中找到目标 seq 对应的 wal entry ，返回 OK
      return Status::OK();
    }

    // 查找失败，返回 Not OK
    return {Status::NotOK};
  }
};

class CommandReplConf : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 == 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    for (size_t i = 1; i < args.size(); i += 2) {
      Status s = ParseParam(util::ToLower(args[i]), args[i + 1]);
      if (!s.IsOK()) {
        return s;
      }
    }

    return Commander::Parse(args);
  }

  Status ParseParam(const std::string &option, const std::string &value) {
    if (option == "listening-port") {
      auto parse_result = ParseInt<int>(value, NumericRange<int>{1, PORT_LIMIT - 1}, 10);
      if (!parse_result) {
        return {Status::RedisParseErr, "listening-port should be number or out of range"};
      }

      port_ = *parse_result;
    } else if (option == "ip-address") {
      if (value == "") {
        return {Status::RedisParseErr, "ip-address should not be empty"};
      }
      ip_address_ = value;
    } else {
      return {Status::RedisParseErr, errUnknownOption};
    }

    return Status::OK();
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn, std::string *output) override {
    if (port_ != 0) {
      conn->SetListeningPort(port_);
    }
    if (!ip_address_.empty()) {
      conn->SetAnnounceIP(ip_address_);
    }
    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  int port_ = 0;
  std::string ip_address_;
};

class CommandFetchMeta : public Commander {
 public:
  Status Parse([[maybe_unused]] const std::vector<std::string> &args) override { return Status::OK(); }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn,
                 [[maybe_unused]] std::string *output) override {

    // 从 conn 获取文件描述符(repl_fd)和IP地址
    int repl_fd = conn->GetFD();
    std::string ip = conn->GetAnnounceIP();

    // 将 socket 设置为阻塞模式，后续要通过这个 socket 发送数据文件的元信息，阻塞模式能保证发送的完整性和可控性。
    auto s = util::SockSetBlocking(repl_fd, 1);
    if (!s.IsOK()) {
      return s.Prefixed("failed to set blocking mode on socket");
    }

    conn->NeedNotFreeBufferEvent(); // 指示不要自动释放  buffer event
    conn->EnableFlag(redis::Connection::kCloseAsync); // 启用异步关闭模式
    srv->stats.IncrFullSyncCount(); // 增加全量同步计数

    // Feed-replica-meta thread
    //
    // 启动一个独立的 feed-repl-info 的线程，处理耗时的文件信息获取和发送操作，避免阻塞主线程
    auto t = GET_OR_RET(util::CreateThread("feed-repl-info", [srv, repl_fd, ip, bev = conn->GetBufferEvent()] {
      // 增加 FetchFile 线程计数
      srv->IncrFetchFileThread();
      // 使用 RAII 模式确保线程退出时自动清理资源，不需要主线程参与资源回收，这样可以 detach 避免阻塞主线程
      auto exit = MakeScopeExit([srv, bev] {
        bufferevent_free(bev);  // 传输完成后自动释放buffer事件
        srv->DecrFetchFileThread();   // 减少线程计数
      });

      // 从 Storage 获取当前的 checkpoint 信息（例如 AOF/RDB 路径、大小、时间戳等）
      std::string files;
      auto s = engine::Storage::ReplDataManager::GetFullReplDataInfo(srv->storage, &files);
      if (!s.IsOK()) {
        warn("[replication] Failed to get full data file info: {}", s.Msg());
        s = util::SockSend(repl_fd, redis::Error({Status::RedisErrorNoPrefix, "can't create db checkpoint"}), bev);
        if (!s.IsOK()) {
          warn("[replication] Failed to send error response: {}", s.Msg());
        }
        return;
      }

      // Send full data file info
      //
      // 向从节点发送元信息（如文件名列表）；
      if (auto s = util::SockSend(repl_fd, files + CRLF, bev)) {
        info("[replication] Succeed sending full data file info to {}", ip);
      } else {
        warn("[replication] Fail to send full data file info {}, error: {}", ip, s.Msg());
      }

      // 更新 checkpoint 最近访问时间，可用于后续判断是否可以复用已有的 checkpoint。
      auto now_secs = static_cast<time_t>(util::GetTimeStamp());
      srv->storage->SetCheckpointAccessTimeSecs(now_secs);
    }));

    // 在 C++ 中，std::thread 可以以两种方式运行：
    //  - join 模式：主线程等待子线程执行完毕再继续。调用 thread.join()。
    //  - detach 模式：子线程自己运行，运行完自动清理资源。主线程不管它。调用 thread.detach()。
    //
    // 如果一个线程既不 join 也不 detach ，当线程结束时：
    //  - 在 Linux/Unix 系统上，线程状态会保留（类似僵尸进程）
    //  - 线程 ID 和部分资源(如TLS)会泄漏，直到主线程最终调用 join
    //
    // 即使通过 MakeScopeExit(...) 确保线程内部使用的资源释放（比如 buffer 和计数器），但仍无法释放线程对象本身占用的系统资源。
    if (auto s = util::ThreadDetach(t); !s) {
      return s;
    }

    return Status::OK();
  }
};



// Q: 为什么 PSync 需要 Detach ，而这里不需要？
// A:
//  是否需要 conn->Detach()，关键在于子线程是否绕过了 bufferevent、直接使用了 socket。
//    - 直接用 socket（阻塞 IO） → 一定要 Detach()，否则容易 race 。
//    - 继续用 bufferevent（线程安全） → 可以不 Detach()，但要用 NeedNotFreeBufferEvent() 防止主线程释放。
//
class CommandFetchFile : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    files_str_ = args[1];
    return Status::OK();
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn,
                 [[maybe_unused]] std::string *output) override {
    std::vector<std::string> files = util::Split(files_str_, ",");

    int repl_fd = conn->GetFD();
    std::string ip = conn->GetAnnounceIP();

    auto s = util::SockSetBlocking(repl_fd, 1);
    if (!s.IsOK()) {
      return s.Prefixed("failed to set blocking mode on socket");
    }

    conn->NeedNotFreeBufferEvent();  // Feed-replica-file thread will close the replica bufferevent
    conn->EnableFlag(redis::Connection::kCloseAsync);

    auto t = GET_OR_RET(util::CreateThread("feed-repl-file", [srv, repl_fd, ip, files, bev = conn->GetBufferEvent()]() {
      auto exit = MakeScopeExit([bev] { bufferevent_free(bev); });
      srv->IncrFetchFileThread();

      for (const auto &file : files) {
        if (srv->IsStopped()) break;

        uint64_t file_size = 0, max_replication_bytes = 0;
        if (srv->GetConfig()->max_replication_mb > 0 && srv->GetFetchFileThreadNum() != 0) {
          max_replication_bytes = (srv->GetConfig()->max_replication_mb * MiB) / srv->GetFetchFileThreadNum();
        }
        auto start = std::chrono::high_resolution_clock::now();
        auto fd = UniqueFD(engine::Storage::ReplDataManager::OpenDataFile(srv->storage, file, &file_size));
        if (!fd) break;

        // Send file size and content
        auto s = util::SockSend(repl_fd, std::to_string(file_size) + CRLF, bev);
        if (s) {
          s = util::SockSendFile(repl_fd, *fd, file_size, bev);
        }
        if (s) {
          info("[replication] Succeed sending file {} to {}", file, ip);
        } else {
          warn("[replication] Fail to send file {} to {}, error: {}", file, ip, s.Msg());
          break;
        }
        fd.Close();

        // Sleep if the speed of sending file is more than replication speed limit
        auto end = std::chrono::high_resolution_clock::now();
        uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        if (max_replication_bytes > 0) {
          auto shortest = static_cast<uint64_t>(static_cast<double>(file_size) / static_cast<double>(max_replication_bytes) * (1000 * 1000));
          if (duration < shortest) {
            info("[replication] Need to sleep {} ms since of sending files too quickly", (shortest - duration) / 1000);
            usleep(shortest - duration);
          }
        }
      }
      auto now_secs = util::GetTimeStamp<std::chrono::seconds>();
      srv->storage->SetCheckpointAccessTimeSecs(now_secs);
      srv->DecrFetchFileThread();
    }));

    if (auto s = util::ThreadDetach(t); !s) {
      return s;
    }

    return Status::OK();
  }

 private:
  std::string files_str_;
};

class CommandDBName : public Commander {
 public:
  Status Parse([[maybe_unused]] const std::vector<std::string> &args) override { return Status::OK(); }
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, [[maybe_unused]] std::string *output) override {
    conn->Reply(srv->storage->GetName() + CRLF);
    return Status::OK();
  }
};


REDIS_REGISTER_COMMANDS(Replication,
                        MakeCmdAttr<CommandReplConf>("replconf", -3, "read-only no-script", NO_KEY),
                        MakeCmdAttr<CommandPSync>("psync", -2, "read-only no-multi no-script", NO_KEY),
                        MakeCmdAttr<CommandFetchMeta>("_fetch_meta", 1, "read-only no-multi no-script", NO_KEY),
                        MakeCmdAttr<CommandFetchFile>("_fetch_file", 2, "read-only no-multi no-script", NO_KEY),
                        MakeCmdAttr<CommandDBName>("_db_name", 1, "read-only no-multi", NO_KEY), )

}  // namespace redis
