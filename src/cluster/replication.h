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

#pragma once

#include <event2/bufferevent.h>

#include <atomic>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "event_util.h"
#include "io_util.h"
#include "rocksdb/write_batch.h"
#include "server/redis_connection.h"
#include "status.h"
#include "storage/storage.h"

class Server;

enum ReplState {
  kReplConnecting = 1,    // 正在连接主节点
  kReplSendAuth,          // 向主节点发送 AUTH 命令（身份验证）
  kReplCheckDBName,       // 校验 DB 名称是否一致
  kReplReplConf,          // 向主节点发送 REPLCONF 命令
  kReplSendPSync,         // 向主节点发送 PSYNC 命令
  kReplFetchMeta,         // 进行全量同步，获取备份元信息
  kReplFetchSST,          // 下载主节点的 SST 文件（RocksDB 快照）
  kReplConnected,         // 主从复制已建立，进入增量复制阶段
  kReplError,             // 出现错误，复制失败
};

enum WriteBatchType {
  kBatchTypeNone = 0,     // 默认无效类型
  kBatchTypePublish,      // Pub/Sub 消息
  kBatchTypePropagate,    // 需要传播到从节点的写操作
  kBatchTypeStream,       // Stream 数据（例如模块内的数据流）
};

// 获取文件回调
using FetchFileCallback = std::function<void(const std::string &, uint32_t)>;

// 主节点上的线程，用于异步地向某个从节点推送增量更新的数据
class FeedSlaveThread {
 public:
  explicit FeedSlaveThread(Server *srv, redis::Connection *conn, rocksdb::SequenceNumber next_repl_seq)
      : srv_(srv), conn_(conn), next_repl_seq_(next_repl_seq) {}
  ~FeedSlaveThread() = default;

  // 线程控制
  Status Start();
  void Stop();
  void Join();

  // 状态查询
  bool IsStopped() { return stop_; }
  redis::Connection *GetConn() { return conn_.get(); }
  rocksdb::SequenceNumber GetCurrentReplSeq() {
    auto seq = next_repl_seq_.load();
    return seq == 0 ? 0 : seq - 1;
  }

 private:
  uint64_t interval_ = 0;                                           // 检查间隔
  std::atomic<bool> stop_ = false;                                  // 停止标志
  Server *srv_ = nullptr;                                           // Server 实例指针
  std::unique_ptr<redis::Connection> conn_ = nullptr;               // 从节点连接
  std::atomic<rocksdb::SequenceNumber> next_repl_seq_ = 0;          // 下一个复制序列号
  std::thread t_;                                                   // 工作线程
  std::unique_ptr<rocksdb::TransactionLogIterator> iter_ = nullptr; // RocksDB事务日志迭代器

  static const size_t kMaxDelayUpdates = 16;          // 最大延迟更新数
  static const size_t kMaxDelayBytes = 16 * 1024;     // 最大延迟字节数

  void loop();                // 主循环
  void checkLivenessIfNeed(); // 检查连接活性
};


// 从节点的复制线程，维护与主节点的连接，负责状态机控制
class ReplicationThread : private EventCallbackBase<ReplicationThread> {
 public:
  explicit ReplicationThread(std::string host, uint32_t port, Server *srv);
  Status Start(std::function<bool()> &&pre_fullsync_cb, std::function<void()> &&post_fullsync_cb);
  void Stop();
  bool IsStopped() const { return stop_flag_; }
  ReplState State() { return repl_state_.load(std::memory_order_relaxed); }
  int64_t LastIOTimeSecs() const { return last_io_time_secs_.load(std::memory_order_relaxed); }

  void TimerCB(int, int16_t);

 protected:
  event_base *base_ = nullptr;

  // The state machine to manage the asynchronous steps used in replication
  // 回调状态机，管理复制流程中的异步操作
  class CallbacksStateMachine {
   public:
    // 状态
    enum class State {
      NEXT,    // 转到下一步
      PREV,    // 返回上一步
      AGAIN,   // 重复当前步骤
      QUIT,    // 退出状态机
      RESTART, // 重启状态机
    };
    // 事件类型
    enum EventType {
      READ,   // 读事件
      WRITE,  // 写事件
    };
    // 回调函数类型
    using CallbackFunc = std::function<State(ReplicationThread *, bufferevent *)>;
    using CallbackType = std::tuple<EventType, std::string, CallbackFunc>;
    using CallbackList = std::deque<CallbackType>;

    CallbacksStateMachine(ReplicationThread *repl, CallbackList &&handlers)
        : repl_(repl), handlers_(std::move(handlers)) {}

    // 状态机控制方法
    void Start();
    void Stop();
    void ReadWriteCB(bufferevent *bev);
    void ConnEventCB(bufferevent *bev, int16_t events);
    void SetReadCB(bufferevent *bev, bufferevent_data_cb cb);
    void SetWriteCB(bufferevent *bev, bufferevent_data_cb cb);

   private:
    bufferevent *bev_ = nullptr;                // libevent.bufferevent 对象，管理 Socket 的读写事件和缓冲区。
    ReplicationThread *repl_;                   // 所属复制线程
    CallbackList handlers_;                     // 回调处理列表
    CallbackList::size_type handler_idx_ = 0;   // 当前处理索引

    EventType getHandlerEventType(CallbackList::size_type idx) { return std::get<0>(handlers_[idx]); }
    std::string getHandlerName(CallbackList::size_type idx) { return std::get<1>(handlers_[idx]); }
    CallbackFunc getHandlerFunc(CallbackList::size_type idx) { return std::get<2>(handlers_[idx]); }
  };

  using CallbackType = CallbacksStateMachine::CallbackType;

 private:
  // 成员变量
  std::thread t_;                                     // 工作线程
  std::atomic<bool> stop_flag_ = false;               // 停止标志
  std::string host_;                                  // 主节点主机
  uint32_t port_;                                     // 主节点端口
  Server *srv_ = nullptr;                             // Server实例
  engine::Storage *storage_ = nullptr;                // 存储引擎
  std::atomic<ReplState> repl_state_;                 // 复制状态
  std::atomic<int64_t> last_io_time_secs_ = 0;        // 最后IO时间
  bool next_try_old_psync_ = false;                   // 是否尝试旧版PSYNC
  bool next_try_without_announce_ip_address_ = false; // 是否不宣布IP地址

  // 回调函数
  std::function<bool()> pre_fullsync_cb_;  // 全量同步前回调
  std::function<void()> post_fullsync_cb_; // 全量同步后回调

  // Internal states managed by FullSync procedure
  // 全量同步状态
  enum FullSyncState {
    kFetchMetaID,      // 获取元数据ID
    kFetchMetaSize,    // 获取元数据大小
    kFetchMetaContent, // 获取元数据内容
  } fullsync_state_ = kFetchMetaID;
  rocksdb::BackupID fullsync_meta_id_ = 0;  // 元数据 ID ，RocksDB的BackupID
  size_t fullsync_filesize_ = 0;            // 元数据文件大小 ，

  // Internal states managed by IncrementBatchLoop procedure
  // 增量同步状态
  enum IncrementBatchLoopState {
    Incr_batch_size,  // 获取批次大小
    Incr_batch_data,  // 获取批次数据
  } incr_state_ = Incr_batch_size;
  size_t incr_bulk_len_ = 0;  // 批量数据长度

  // 状态机实例
  CallbacksStateMachine psync_steps_;     // PSYNC 步骤状态机
  CallbacksStateMachine fullsync_steps_;  // 全量同步步骤状态机

  // 主方法
  void run();

  // 各种回调方法
  using CBState = CallbacksStateMachine::State;
  CBState authWriteCB(bufferevent *bev);
  CBState authReadCB(bufferevent *bev);
  CBState checkDBNameWriteCB(bufferevent *bev);
  CBState checkDBNameReadCB(bufferevent *bev);
  CBState replConfWriteCB(bufferevent *bev);
  CBState replConfReadCB(bufferevent *bev);
  CBState tryPSyncWriteCB(bufferevent *bev);
  CBState tryPSyncReadCB(bufferevent *bev);
  CBState incrementBatchLoopCB(bufferevent *bev);
  CBState fullSyncWriteCB(bufferevent *bev);
  CBState fullSyncReadCB(bufferevent *bev);

  // Synchronized-Blocking ops
  // 同步阻塞操作
  Status sendAuth(int sock_fd, ssl_st *ssl);
  Status fetchFile(int sock_fd, evbuffer *evbuf, const std::string &dir, const std::string &file, uint32_t crc,
                   const FetchFileCallback &fn, ssl_st *ssl);
  Status fetchFiles(int sock_fd, const std::string &dir, const std::vector<std::string> &files,
                    const std::vector<uint32_t> &crcs, const FetchFileCallback &fn, ssl_st *ssl);
  Status parallelFetchFile(const std::string &dir, const std::vector<std::pair<std::string, uint32_t>> &files);

  // 静态辅助方法
  static bool isRestoringError(std::string_view err);
  static bool isWrongPsyncNum(std::string_view err);
  static bool isUnknownOption(std::string_view err);

  // 写批次解析
  Status parseWriteBatch(const rocksdb::WriteBatch &write_batch);
};

/*
 * An extractor to extract update from raw writebatch
 */
// 用于从 RocksDB WriteBatch 中提取更新操作
class WriteBatchHandler : public rocksdb::WriteBatch::Handler {
 public:
  rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice &key, const rocksdb::Slice &value) override;
  rocksdb::Status DeleteCF([[maybe_unused]] uint32_t column_family_id,
                           [[maybe_unused]] const rocksdb::Slice &key) override {
    return rocksdb::Status::OK();
  }
  rocksdb::Status DeleteRangeCF([[maybe_unused]] uint32_t column_family_id,
                                [[maybe_unused]] const rocksdb::Slice &begin_key,
                                [[maybe_unused]] const rocksdb::Slice &end_key) override {
    return rocksdb::Status::OK();
  }
  WriteBatchType Type() { return type_; }
  std::string Key() const { return kv_.first; }
  std::string Value() const { return kv_.second; }

 private:
  std::pair<std::string, std::string> kv_;
  WriteBatchType type_ = kBatchTypeNone;
};
