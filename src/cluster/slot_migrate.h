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

#include <rocksdb/db.h>
#include <rocksdb/status.h>
#include <rocksdb/transaction_log.h>
#include <rocksdb/write_batch.h>

#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "batch_sender.h"
#include "logging.h"
#include "server/server.h"
#include "status.h"
#include "storage/redis_db.h"
#include "unique_fd.h"


// 迁移类型：
//  - kRedisCommand（默认）: 通过生成类似 SET, HSET, ZADD 这类 Redis 命令，把已有的数据和日志转换成命令并在目标节点上重放。
//    优点：通用，目标节点不需要与源节点版本一致。
//    缺点：效率低，数据需要逐条处理和发送。
//  - kRawKeyValue : 使用 APPLYBATCH 命令，直接传输 RocksDB 的 key-value 数据（包括元数据和实际 value）。
//    优点：更快且节省网络开销。
//    缺点：如果目标节点不支持 APPLYBATCH（如版本不兼容），会自动退回到 kRedisCommand 模式。

enum class MigrationType {
  /// Use Redis commands to migrate data.
  /// It will try to extract commands from existing data and log, then replay
  /// them on the destination node.
  kRedisCommand = 0,
  /// Using raw key-value and "APPLYBATCH" command in kvrocks to migrate data.
  ///
  /// If downstream is not compatible with raw key-value, this migration type will
  /// auto switch to kRedisCommand.
  kRawKeyValue
};

// 迁移状态：
//  - kNone: 没有任务或任务尚未开始。
//  - kStarted: 正在进行 slot 迁移。
//  - kSuccess: 全部 key 成功迁移完成。
//  - kFailed: 中途发生错误（如网络中断、目标拒绝写入等），导致迁移失败。
enum class MigrationState { kNone = 0, kStarted, kSuccess, kFailed };

// 迁移阶段：
//  - kNone: 尚未开始。
//  - kStart: 正在准备迁移，初始化阶段。
//  - kSnapshot: 正在拷贝数据快照（类似 RDB 快照），这是 bulk 的 key-value 数据。
//  - kWAL: 拷贝快照后，再同步未被快照捕捉的 WAL 日志（类似 PSYNC 的增量补全）。
//  - kSuccess: 所有数据成功同步，目标节点已准备接管。
//  - kFailed: 发生错误，迁移失败。
//  - kClean: 清理阶段，比如释放迁移状态、关闭连接等。
enum class SlotMigrationStage { kNone, kStart, kSnapshot, kWAL, kSuccess, kFailed, kClean };

// key 的迁移结果：
//  - kMigrated: 成功迁移到了目标节点。
//  - kExpired: 在迁移前已经过期，不需处理。
//  - kUnderlyingStructEmpty: 比如一个 set/hash/zset 全部元素都已过期或删除，结构本身也可被跳过。
enum class KeyMigrationResult { kMigrated, kExpired, kUnderlyingStructEmpty };

struct SlotMigrationJob {
  SlotMigrationJob(const SlotRange &slot_range_in,
                   std::string dst_ip,
                   int dst_port,
                   int speed,
                   int pipeline_size,
                   int seq_gap)
      : slot_range(slot_range_in),
        dst_ip(std::move(dst_ip)),
        dst_port(dst_port),
        max_speed(speed),
        max_pipeline_size(pipeline_size),
        seq_gap_limit(seq_gap) {}
  SlotMigrationJob(const SlotMigrationJob &other) = delete;
  SlotMigrationJob &operator=(const SlotMigrationJob &other) = delete;
  ~SlotMigrationJob() = default;

  SlotRange slot_range;
  std::string dst_ip;
  int dst_port;
  int max_speed;
  int max_pipeline_size;
  int seq_gap_limit;
};

class SyncMigrateContext;

class SlotMigrator : public redis::Database {
 public:
  explicit SlotMigrator(Server *srv);
  SlotMigrator(const SlotMigrator &other) = delete;
  SlotMigrator &operator=(const SlotMigrator &other) = delete;
  ~SlotMigrator();

  Status CreateMigrationThread();
  Status PerformSlotRangeMigration(const std::string &node_id, std::string &dst_ip, int dst_port,
                                   const SlotRange &range, SyncMigrateContext *blocking_ctx = nullptr);
  void ReleaseForbiddenSlotRange();
  void SetMaxMigrationSpeed(int value) {
    if (value >= 0) max_migration_speed_ = value;
  }
  void SetMaxPipelineSize(int value) {
    if (value > 0) max_pipeline_size_ = value;
  }
  void SetSequenceGapLimit(int value) {
    if (value > 0) seq_gap_limit_ = value;
  }
  void SetMigrateBatchRateLimit(size_t bytes_per_sec) { migrate_batch_bytes_per_sec_ = bytes_per_sec; }
  void SetMigrateBatchSize(size_t size) { migrate_batch_size_bytes_ = size; }
  void SetStopMigrationFlag(bool value) { stop_migration_ = value; }
  bool IsMigrationInProgress() const { return migration_state_ == MigrationState::kStarted; }
  SlotMigrationStage GetCurrentSlotMigrationStage() const { return current_stage_; }
  SlotRange GetForbiddenSlotRange() const { return forbidden_slot_range_; }
  SlotRange GetMigratingSlotRange() const { return slot_range_; }
  std::string GetDstNode() const { return dst_node_; }
  void GetMigrationInfo(std::string *info) const;
  void CancelSyncCtx();

 private:
  void loop();
  void runMigrationProcess();
  bool isTerminated() const { return thread_state_ == ThreadState::Terminated; }
  Status startMigration();
  Status sendSnapshot();
  Status syncWAL();
  Status finishSuccessfulMigration();
  Status finishFailedMigration();
  void clean();

  Status authOnDstNode(int sock_fd, const std::string &password);
  Status setImportStatusOnDstNode(int sock_fd, int status);
  static StatusOr<bool> supportedApplyBatchCommandOnDstNode(int sock_fd);

  Status sendSnapshotByCmd();
  Status syncWALByCmd();
  Status checkSingleResponse(int sock_fd);
  Status checkMultipleResponses(int sock_fd, int total);

  StatusOr<KeyMigrationResult> migrateOneKey(const rocksdb::Slice &key, const rocksdb::Slice &encoded_metadata,
                                             std::string *restore_cmds);
  Status migrateSimpleKey(const rocksdb::Slice &key, const Metadata &metadata, const std::string &bytes,
                          std::string *restore_cmds);
  Status migrateComplexKey(const rocksdb::Slice &key, const Metadata &metadata, std::string *restore_cmds);
  Status migrateStream(const rocksdb::Slice &key, const StreamMetadata &metadata, std::string *restore_cmds);
  Status migrateBitmapKey(const InternalKey &inkey, std::unique_ptr<rocksdb::Iterator> *iter,
                          std::vector<std::string> *user_cmd, std::string *restore_cmds);

  Status sendCmdsPipelineIfNeed(std::string *commands, bool need);
  void applyMigrationSpeedLimit() const;
  Status generateCmdsFromBatch(rocksdb::BatchResult *batch, std::string *commands);
  Status migrateIncrementData(std::unique_ptr<rocksdb::TransactionLogIterator> *iter, uint64_t end_seq);
  Status syncWalBeforeForbiddingSlot();
  Status syncWalAfterForbiddingSlot();

  Status sendMigrationBatch(BatchSender *batch);
  Status sendSnapshotByRawKV();
  Status syncWALByRawKV();
  bool catchUpIncrementalWAL();
  Status migrateIncrementalDataByRawKV(uint64_t end_seq, BatchSender *batch_sender);

  void setForbiddenSlotRange(const SlotRange &slot_range);
  std::unique_lock<std::mutex> blockingLock() { return std::unique_lock<std::mutex>(blocking_mutex_); }

  void resumeSyncCtx(const Status &migrate_result);

  enum class ParserState { ArrayLen, BulkLen, BulkData, ArrayData, OneRspEnd };
  enum class ThreadState { Uninitialized, Running, Terminated };

  static constexpr int kDefaultMaxPipelineSize = 16;
  static constexpr int kDefaultMaxMigrationSpeed = 4096;
  static constexpr int kDefaultSequenceGapLimit = 10000;
  static constexpr int kMaxItemsInCommand = 16;  // number of items in every write command of complex keys
  static constexpr int kMaxLoopTimes = 10;

  Server *srv_;

  int max_migration_speed_ = kDefaultMaxMigrationSpeed;
  int max_pipeline_size_ = kDefaultMaxPipelineSize;
  uint64_t seq_gap_limit_ = kDefaultSequenceGapLimit;
  std::atomic<size_t> migrate_batch_bytes_per_sec_ = 1 * GiB;
  std::atomic<size_t> migrate_batch_size_bytes_;

  SlotMigrationStage current_stage_ = SlotMigrationStage::kNone;
  ParserState parser_state_ = ParserState::ArrayLen;
  std::atomic<ThreadState> thread_state_ = ThreadState::Uninitialized;
  std::atomic<MigrationState> migration_state_ = MigrationState::kNone;

  int current_pipeline_size_ = 0;
  uint64_t last_send_time_ = 0;

  std::thread t_;
  std::mutex job_mutex_;
  std::condition_variable job_cv_;
  std::unique_ptr<SlotMigrationJob> migration_job_;  // 当前正在处理的迁移任务，迁移完成后被 reset // GUARDED_BY(job_mutex_)

  std::string dst_node_;
  std::string dst_ip_;
  int dst_port_ = -1;
  UniqueFD dst_fd_;

  MigrationType migration_type_ = MigrationType::kRedisCommand;

  static_assert(std::atomic<SlotRange>::is_always_lock_free, "SlotRange is not lock free.");

  // 此变量作用：在迁移 slot 的最后阶段（增量同步完成后、开始切换拓扑前），临时禁止对该 slot 的写操作，确保迁移结束前数据不会再变动，保证数据一致性。
  // Q: 在迁移成功后，为什么 forbidden_slot_range_ 变量没有被重置？
  std::atomic<SlotRange> forbidden_slot_range_ = SlotRange{-1, -1};
  std::atomic<SlotRange> slot_range_ = SlotRange{-1, -1};
  std::atomic<SlotRange> migrate_failed_slot_range_ = SlotRange{-1, -1};

  std::atomic<bool> stop_migration_ = false;  // if is true migration will be stopped but the thread won't be destroyed
  const rocksdb::Snapshot *slot_snapshot_ = nullptr;
  uint64_t wal_begin_seq_ = 0;

  std::mutex blocking_mutex_;
  SyncMigrateContext *blocking_context_ = nullptr; // 当前正在处理的迁移任务的 blocking_ctx_ ，当迁移完成后，在 reset migration_job_ 前，会触发 blocking_context_ 并 reset 
};
