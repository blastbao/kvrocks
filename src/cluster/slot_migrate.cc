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

#include "slot_migrate.h"

#include <memory>
#include <utility>

#include "db_util.h"
#include "event_util.h"
#include "fmt/format.h"
#include "io_util.h"
#include "storage/batch_extractor.h"
#include "storage/iterator.h"
#include "storage/redis_metadata.h"
#include "sync_migrate_context.h"
#include "thread_util.h"
#include "time_util.h"
#include "types/redis_stream_base.h"

constexpr std::string_view errFailedToSendCommands = "failed to send commands to restore a key";
constexpr std::string_view errMigrationTaskCanceled = "key migration stopped due to a task cancellation";
constexpr std::string_view errFailedToSetImportStatus = "failed to set import status on destination node";
constexpr std::string_view errUnsupportedMigrationType = "unsupported migration type";

static std::map<RedisType, std::string> type_to_cmd = {
    {kRedisString, "set"},
    {kRedisList, "rpush"},
    {kRedisHash, "hmset"},
    {kRedisSet, "sadd"},
    {kRedisZSet, "zadd"},
    {kRedisBitmap, "setbit"},
    {kRedisSortedint, "siadd"},
    {kRedisStream, "xadd"},
};

SlotMigrator::SlotMigrator(Server *srv)
    : Database(srv->storage, kDefaultNamespace),
      srv_(srv),
      max_migration_speed_(srv->GetConfig()->migrate_speed),
      max_pipeline_size_(srv->GetConfig()->pipeline_size),
      seq_gap_limit_(srv->GetConfig()->sequence_gap),
      migrate_batch_bytes_per_sec_(srv->GetConfig()->migrate_batch_rate_limit_mb * MiB),
      migrate_batch_size_bytes_(srv->GetConfig()->migrate_batch_size_kb * KiB) {
  // Let metadata_cf_handle_ be nullptr, and get them in real time to avoid accessing invalid pointer,
  // because metadata_cf_handle_ and db_ will be destroyed if DB is reopened.
  // [Situation]:
  // 1. Start an empty slave server.
  // 2. Connect to master which has amounted of data, and trigger full synchronization.
  // 3. After replication, change slave to master and start slot migrate.
  // 4. It will occur segment fault when using metadata_cf_handle_ to create iterator of rocksdb.
  // [Reason]:
  // After full synchronization, DB will be reopened, db_ and metadata_cf_handle_ will be released.
  // Then, if we create rocksdb iterator with metadata_cf_handle_, it will go wrong.
  // [Solution]:
  // db_ and metadata_cf_handle_ will be replaced by storage_->GetDB() and storage_->GetCFHandle("metadata")
  // in all functions used in migration process.
  // [Note]:
  // This problem may exist in all functions of Database called in slot migration process.
  metadata_cf_handle_ = nullptr;

  if (srv->IsSlave()) {
    SetStopMigrationFlag(true);
  }
}

// 当 client 发送迁移命令 CLUSTERX MIGRATE 给 rocksdb 时，会调用此函数：
//  创建一个迁移任务 SlotMigrationJob 对象，将其保存到成员变量 migration_job_ 上（锁），然后触发信号量 job_cv_ 通知后台线程执行数据迁移。
//  创建、保存、触发迁移任务后，此函数即刻返回，如果 client 需要阻塞等待迁移完成，需要通过 blocking_ctx 轮训或者阻塞式等待迁移结果。
Status SlotMigrator::PerformSlotRangeMigration(const std::string &node_id,      // 目标节点 ID
                                               std::string &dst_ip,             // 目标节点 IP
                                               int dst_port,                    // 目标节点 PORT
                                               const SlotRange &slot_range,     // 迁移 slot range
                                               SyncMigrateContext *blocking_ctx // （可选）阻塞上下文，用于阻塞调用线程直到迁移完成（如用于同步命令）
                                               ) {
  // TODO: concurrent migration, multiple migration jobs
  // Only one slot migration job at the same time
  // 确保同一时间只会有一个迁移任务在执行
  //  - 使用原子操作检查当前 slot_range_ 是否为空，若为空则赋值为入参 slot_range ，否则 slot_range_ 非空意味着当前有任务在进行，直接返回错误。
  SlotRange empty_slot_range = {-1, -1};
  if (!slot_range_.compare_exchange_strong(empty_slot_range, slot_range)) {
    return {Status::NotOK, "There is already a migrating job"};
  }

  // forbidden_slot_range_ 存储着禁止迁移的槽位段（一般是已经迁移完成的或者故障恢复时保护的）。
  // 如果新任务的 slot_range 与 forbidden_slot_range_ 有交集，则说明 slot_range 包含不可迁移的槽位，重置 slot_range_ 并报错返回。
  if (slot_range.HasOverlap(forbidden_slot_range_)) {
    // Have to release migrate slot set above
    slot_range_ = empty_slot_range;
    return {Status::NotOK, "Can't migrate slot which has been migrated"};
  }

  // 更新迁移状态
  migration_state_ = MigrationState::kStarted;

  // 获取迁移配置
  auto speed = srv_->GetConfig()->migrate_speed;
  auto seq_gap = srv_->GetConfig()->sequence_gap;
  auto pipeline_size = srv_->GetConfig()->pipeline_size;
  if (speed <= 0) {
    speed = 0;
  }
  if (pipeline_size <= 0) {
    pipeline_size = kDefaultMaxPipelineSize;
  }
  if (seq_gap <= 0) {
    seq_gap = kDefaultSequenceGapLimit;
  }

  // [重要] 如果开启了同步迁移，这里调用 blocking_ctx->Suspend() 挂起，调用者会阻塞在 blocking_ctx 上直到后台线程完成迁移并调用 blocking_ctx->Resume()
  if (blocking_ctx) {
    std::unique_lock<std::mutex> lock(blocking_mutex_);
    blocking_context_ = blocking_ctx;
    blocking_context_->Suspend();
  }

  dst_node_ = node_id;

  // Create migration job
  //
  // 创建一个 SlotMigrationJob，其封装了迁移所需的参数；
  // 将 SlotMigrationJob 存入 migration_job_ ，通过条件变量 job_cv_ 通知后台线程立即开始执行迁移。
  auto job = std::make_unique<SlotMigrationJob>(slot_range, dst_ip, dst_port, speed, pipeline_size, seq_gap);
  {
    std::lock_guard<std::mutex> guard(job_mutex_);
    migration_job_ = std::move(job);
    job_cv_.notify_one();
  }
  info("[migrate] Start migrating slot(s) {} to {}:{}", slot_range.String(), dst_ip, dst_port);

  // 这里创建线程完成后立即返回，如果是同步迁移客户端会通过 blocking_ctx 去阻塞等待；
  return Status::OK();
}

SlotMigrator::~SlotMigrator() {
  if (thread_state_ == ThreadState::Running) {
    stop_migration_ = true;                             // 当前迁移任务被中止
    thread_state_ = ThreadState::Terminated;            // 当前迁移线程被终止
    job_cv_.notify_all();                               // 唤醒迁移线程让其退出
    if (auto s = util::ThreadJoin(t_); !s) {  // 等待迁移线程退出
      warn("Slot migrating thread operation failed: {}", s.Msg());
    }
  }
}

Status SlotMigrator::CreateMigrationThread() {
  t_ = GET_OR_RET(util::CreateThread("slot-migrate", [this] {
    thread_state_ = ThreadState::Running;
    this->loop();
  }));

  return Status::OK();
}

// 后台线程主循环：
//  监听信号量 job_cv_ ，当 job_cv_ 被触发意味着 migration_job_ 变量上保存了当前待处理的迁移任务。
void SlotMigrator::loop() {
  // 同时只有一个迁移任务在执行，它保存在 migration_job_ 变量上
  while (true) {
    // 监听信号量（配合 mutex 使用），被唤醒意味着有新迁移任务到达
    {
      std::unique_lock<std::mutex> ul(job_mutex_);
      job_cv_.wait(ul, [&] { return isTerminated() || migration_job_; });
    }

    if (isTerminated()) {
      clean();
      return;
    }

    info("[migrate] Migrating slot(s): {}, dst_ip: {}, dst_port: {}, max_speed: {}, max_pipeline_size: {}",
         migration_job_->slot_range.String(),
         migration_job_->dst_ip,
         migration_job_->dst_port,
         migration_job_->max_speed,
         migration_job_->max_pipeline_size);

    // 有新的迁移任务 migration_job_ 待执行，解析相关参数
    dst_ip_ = migration_job_->dst_ip;
    dst_port_ = migration_job_->dst_port;
    max_migration_speed_ = migration_job_->max_speed;
    max_pipeline_size_ = migration_job_->max_pipeline_size;
    seq_gap_limit_ = migration_job_->seq_gap_limit;

    // 执行迁移任务
    runMigrationProcess();
  }
}

void SlotMigrator::runMigrationProcess() {
  // 迁移开始
  current_stage_ = SlotMigrationStage::kStart;
  // 不断循环直到迁移完成（成功 or 失败）
  while (true) {
    if (isTerminated()) { // 线程被终止
      warn("[migrate] Will stop state machine, because the thread was terminated");
      clean();
      return;
    }

    // 状态机流转
    switch (current_stage_) {
      case SlotMigrationStage::kStart: { // 初始化：创建快照、建立连接、迁移握手
        auto s = startMigration();
        if (s.IsOK()) {
          info("[migrate] Succeed to start migrating slot(s) {}", slot_range_.load().String());
          current_stage_ = SlotMigrationStage::kSnapshot;
        } else {
          error("[migrate] Failed to start migrating slot(s) {}. Error: {}", slot_range_.load().String(), s.Msg());
          current_stage_ = SlotMigrationStage::kFailed;
          resumeSyncCtx(s);
        }
        break;
      }
      case SlotMigrationStage::kSnapshot: { // 从快照中提取指定 slot_range_ 的 kv 发送给目标节点
        auto s = sendSnapshot();
        if (s.IsOK()) {
          current_stage_ = SlotMigrationStage::kWAL;
        } else {
          error("[migrate] Failed to send snapshot of slot(s) {}. Error: {}", slot_range_.load().String(), s.Msg());
          current_stage_ = SlotMigrationStage::kFailed;
          resumeSyncCtx(s);
        }
        break;
      }
      case SlotMigrationStage::kWAL: {  // 从 wal 提取 snapshot sequence number 到 latest sequence number 之前的新增数据发送目标节点
        auto s = syncWAL();
        if (s.IsOK()) {
          info("[migrate] Succeed to sync from WAL for slot(s) {}", slot_range_.load().String());
          current_stage_ = SlotMigrationStage::kSuccess;
        } else {
          error("[migrate] Failed to sync from WAL for slot(s) {}. Error: {}", slot_range_.load().String(), s.Msg());
          current_stage_ = SlotMigrationStage::kFailed;
          resumeSyncCtx(s);
        }
        break;
      }
      case SlotMigrationStage::kSuccess: {  // 迁移成功，发送 cluster import success 命令通知目标节点导入完成，并更新当前节点的 migrated_slots_ 映射记录每个被迁移 slot 新节点的 ip:port
        auto s = finishSuccessfulMigration();
        if (s.IsOK()) {
          info("[migrate] Succeed to migrate slot(s) {}", slot_range_.load().String());
          current_stage_ = SlotMigrationStage::kClean; // 下一个阶段：clean
          migration_state_ = MigrationState::kSuccess;
          resumeSyncCtx(s);
        } else {
          error("[migrate] Failed to finish a successful migration of slot(s) {}. Error: {}",slot_range_.load().String(), s.Msg());
          current_stage_ = SlotMigrationStage::kFailed;
          resumeSyncCtx(s);
        }
        break;
      }
      case SlotMigrationStage::kFailed: { // 迁移失败，重试
        auto s = finishFailedMigration();
        if (!s.IsOK()) {
          error("[migrate] Failed to finish a failed migration of slot(s) {}. Error: {}", slot_range_.load().String(),s.Msg());
        }
        info("[migrate] Failed to migrate a slot(s) {}", slot_range_.load().String());
        migration_state_ = MigrationState::kFailed;
        current_stage_ = SlotMigrationStage::kClean;
        break;
      }
      case SlotMigrationStage::kClean: {
        clean();
        return;
      }
      default:
        error("[migrate] Unexpected state for the state machine: {}", static_cast<int>(current_stage_));
        clean();
        return;
    }
  }
}

Status SlotMigrator::startMigration() {
  // Get snapshot and sequence
  // 1. 获取 RocksDB Snapshot
  slot_snapshot_ = storage_->GetDB()->GetSnapshot();
  if (!slot_snapshot_) {
    return {Status::NotOK, "failed to create snapshot"};
  }

  // 2. 记录当前快照的 WAL 序列号，用于后续增量同步
  wal_begin_seq_ = slot_snapshot_->GetSequenceNumber();
  last_send_time_ = 0;


  // Connect to the destination node
  // 3. 创建到目标节点的 TCP 连接
  auto result = util::SockConnect(dst_ip_, dst_port_);
  if (!result.IsOK()) {
    return {Status::NotOK, fmt::format("failed to connect to the destination node: {}", result.Msg())};
  }
  dst_fd_.Reset(*result);

  // Auth first
  // 4. 如果配置文件中开启密码认证（requirepass），则对目标 Redis 节点进行 AUTH；
  std::string pass = srv_->GetConfig()->requirepass;
  if (!pass.empty()) {
    auto s = authOnDstNode(*dst_fd_, pass);
    if (!s.IsOK()) { // 检查错误码
      return s.Prefixed("failed to authenticate on destination node");
    }
  }

  // Set destination node import status to START
  // 5. 向目标节点发送 cluster import 命令，通知目标开始接收迁移数据；若目标报错，则停止迁移。
  auto s = setImportStatusOnDstNode(*dst_fd_, kImportStart);
  if (!s.IsOK()) {
    return s.Prefixed(errFailedToSetImportStatus);
  }

  // 6. 读取配置中指定的迁移类型，并检查目标节点是否支持该类型，从而确定最终的迁移类型
  //  - kRawKeyValue: 使用 Kvrocks 自定义的 APPLYBATCH 命令，批量写入底层 RocksDB；
  //  - kRedisCommand: 使用标准 Redis 命令（如 SET, RPUSH 等）逐条迁移。
  migration_type_ = srv_->GetConfig()->migrate_type;

  // If the APPLYBATCH command is not supported on the destination,
  // we will fall back to the redis-command migration type.
  if (migration_type_ == MigrationType::kRawKeyValue) {
    // 检查目标节点是否支持 APPLYBATCH
    //  - 如果当前使用的是 RawKeyValue 模式，则尝试检测目标是否支持 APPLYBATCH 命令；
    //  -如果目标版本不支持（比如是普通 Redis 实例），则自动降级为 RedisCommand 模式；
    bool supported = GET_OR_RET(supportedApplyBatchCommandOnDstNode(*dst_fd_));
    if (!supported) {
      info("APPLYBATCH command is not supported, use redis command for migration");
      migration_type_ = MigrationType::kRedisCommand;
    }
  }
  info("[migrate] Start migrating slot(s) {}, connect destination fd {}", slot_range_.load().String(), *dst_fd_);

  return Status::OK();
}

Status SlotMigrator::sendSnapshot() {
  if (migration_type_ == MigrationType::kRedisCommand) {
    return sendSnapshotByCmd();
  } else if (migration_type_ == MigrationType::kRawKeyValue) {
    return sendSnapshotByRawKV();
  }
  return {Status::NotOK, std::string(errUnsupportedMigrationType)};
}

Status SlotMigrator::syncWAL() {
  if (migration_type_ == MigrationType::kRedisCommand) {
    return syncWALByCmd();
  } else if (migration_type_ == MigrationType::kRawKeyValue) {
    return syncWALByRawKV();
  }
  return {Status::NotOK, std::string(errUnsupportedMigrationType)};
}

// 将某个 slot 区间内的 key 以 Redis 命令格式迁移到目标节点。
Status SlotMigrator::sendSnapshotByCmd() {
  // 变量初始化
  //  - 记录三种 key 类型的数量；
  //  - restore_cmds 存放待发送的 Redis 命令拼接；
  //  - 复制一个 slot_range_ 用于遍历。
  uint64_t migrated_key_cnt = 0;
  uint64_t expired_key_cnt = 0;
  uint64_t empty_key_cnt = 0;
  std::string restore_cmds;
  SlotRange slot_range = slot_range_;
  info("[migrate] Start migrating snapshot of slot(s): {}", slot_range.String());

  // Construct key prefix to iterate the keys belong to the target slot
  //
  // Kvrocks 的所有 key 以 {namespace}:{slot_id}|key 格式存储；
  // 这里根据 slot_range 范围构造 RocksDB 的遍历上下界，确保只遍历当前 slot_range 的 key。
  std::string prefix = ComposeSlotKeyPrefix(namespace_, slot_range.start);
  info("[migrate] Iterate keys of slot(s), key's prefix: {}", prefix);
  std::string upper_bound = ComposeSlotKeyUpperBound(namespace_, slot_range.end);

  // 设置迭代器快照、读取范围
  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions(); // 获取默认的扫描配置（包含缓存策略等设置）
  read_options.snapshot = slot_snapshot_;                             // 快照绑定
  Slice prefix_slice(prefix);
  Slice upper_bound_slice(upper_bound);
  read_options.iterate_lower_bound = &prefix_slice;                   // 槽范围起始键
  read_options.iterate_upper_bound = &upper_bound_slice;              // 槽范围结束键

  // 创建迭代器
  rocksdb::ColumnFamilyHandle *cf_handle = storage_->GetCFHandle(ColumnFamilyID::Metadata);
  auto iter = util::UniqueIterator(storage_->GetDB()->NewIterator(read_options, cf_handle));

  // Seek to the beginning of keys start with 'prefix' and iterate all these keys
  // 从 prefix 开始遍历，每次调用 .Next() 访问下一个 key，遇到 key 不属于本 slot 时就退出（提前终止）。
  int current_slot = slot_range.start;
  for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
    // The migrating task has to be stopped, if server role is changed from master to slave
    // or flush command (flushdb or flushall) is executed
    // 迁移任务被中止时停止遍历，比如角色变更或者执行了 flushdb/flushall 等命令
    if (stop_migration_) {
      return {Status::NotOK, std::string(errMigrationTaskCanceled)};
    }

    // Iteration is out of range
    // 判断 key 是否属于当前 slot
    // 备注：虽然已经用 iterate bound 来限定范围，但依然保险地做一次验证；
    current_slot = ExtractSlotId(iter->key());
    if (!slot_range.Contains(current_slot)) {
      break;
    }

    // Get user key
    // 提取用户原始 key
    //
    // 为什么要提取 user_key ？
    // 因为 RocksDB 的底层 key 是 Kvrocks 内部结构，以 Redis 协议来传递它，目标 Redis 实例不懂。
    // 必须从 RocksDB 的 key 中提取出用户原始 key（user_key），搭配 value，一起构造 Redis 的 RESTORE 或 SET 命令发送给目标实例。
    auto [_, user_key] = ExtractNamespaceKey(iter->key(), /*slot_id_encoded=*/true);

    // Add key's constructed commands to restore_cmds, send pipeline or not according to task's max_pipeline_size
    auto result = migrateOneKey(user_key, iter->value(), &restore_cmds);
    if (!result.IsOK()) {
      return {Status::NotOK, fmt::format("failed to migrate a key {}: {}", user_key, result.Msg())};
    }

    if (*result == KeyMigrationResult::kMigrated) { // 迁移成功
      info("[migrate] The key {} successfully migrated", user_key);
      migrated_key_cnt++;
    } else if (*result == KeyMigrationResult::kExpired) { // Key 已过期，无需迁移
      info("[migrate] The key {} is expired", user_key);
      expired_key_cnt++;
    } else if (*result == KeyMigrationResult::kUnderlyingStructEmpty) { // 空数据，无需迁移
      info("[migrate] The key {} has no elements", user_key);
      empty_key_cnt++;
    } else { // 错误
      error("[migrate] Migrated a key {} with unexpected result: {}", user_key, static_cast<int>(*result));
      return {Status::NotOK};
    }
  }

  // 运行到这里，有可能是 for loop 循环退出，也可能是遍历完当前 slot 而 break 退出；如果是前者，需要检查 iter 是否 valid ？
  // 在使用 RocksDB 迭代器 iter->Next() 遍历 key 的过程中，有可能发生底层错误（例如磁盘损坏、sst文件丢失等），RocksDB 不会主动抛异常，而是通过 iter->status() 返回状态，
  // 需要主动检查确认迭代器状态，如果忽略，可能会在迁移过程中错过一部分 key，但又不知道出了什么问题。
  if (auto s = iter->status(); !s.ok()) {
    auto err_str = s.ToString();
    error("[migrate] Failed to iterate keys of slot {}: {}", current_slot, err_str);
    return {Status::NotOK, fmt::format("failed to iterate keys of slot {}: {}", current_slot, err_str)};
  }

  // It's necessary to send commands that are still in the pipeline since the final pipeline may not be sent
  // while iterating keys because its size could be less than max_pipeline_size_
  //
  // 在遍历每个 key 时，我们会构造 Redis 命令并追加到 restore_cmds 中；只有当命令数达到 max_pipeline_size_ 时，才会触发发送（为了节省网络开销）。
  // 所以最后一批，要强制发送。
  auto s = sendCmdsPipelineIfNeed(&restore_cmds, true);
  if (!s.IsOK()) {
    return s.Prefixed(errFailedToSendCommands);
  }
  info("[migrate] Succeed to migrate slot(s) snapshot, slot(s): {}, Migrated keys: {}, Expired keys: {}, Empty keys: {}",slot_range.String(), migrated_key_cnt, expired_key_cnt, empty_key_cnt);
  return Status::OK();
}

Status SlotMigrator::syncWALByCmd() {
  // Send incremental data from WAL circularly until new increment less than a certain amount
  // 增量同步 wal ，直到追到距当前 lastest_seq 很近
  auto s = syncWalBeforeForbiddingSlot();
  if (!s.IsOK()) {
    return s.Prefixed("failed to sync WAL before forbidding a slot");
  }

  // 对正在迁移的 slot_range_ 加锁
  setForbiddenSlotRange(slot_range_);

  // Send last incremental data
  // 完成最后一部分的 wal 同步
  s = syncWalAfterForbiddingSlot();
  if (!s.IsOK()) {
    return s.Prefixed("failed to sync WAL after forbidding a slot");
  }

  return Status::OK();
}

Status SlotMigrator::finishSuccessfulMigration() {
  // 中止迁移？
  if (stop_migration_) {
    return {Status::NotOK, std::string(errMigrationTaskCanceled)};
  }

  // Set import status on the destination node to SUCCESS
  // 通知目标节点迁移完成，可以结束导入状态
  auto s = setImportStatusOnDstNode(*dst_fd_, kImportSuccess);
  if (!s.IsOK()) {
    return s.Prefixed(errFailedToSetImportStatus);
  }

  // 构造目标节点地址（IP:Port），用于记录和写入到集群元信息中。
  std::string dst_ip_port = dst_ip_ + ":" + std::to_string(dst_port_);

  // 更新 slot_range_ 内每个 slot 的新归属节点
  //
  // 重要，至此 slot 迁移成功，所有请求本 node 的 slot 请求会被重定向到新节点，以确保数据一致性。
  s = srv_->cluster->SetSlotRangeMigrated(slot_range_, dst_ip_port);
  if (!s.IsOK()) {
    return s.Prefixed(
        fmt::format("failed to set slot(s) {} as migrated to {}", slot_range_.load().String(), dst_ip_port));
  }

  // 重置变量
  migrate_failed_slot_range_ = {-1, -1};
  return Status::OK();
}

Status SlotMigrator::finishFailedMigration() {
  // Stop slot will forbid writing
  migrate_failed_slot_range_ = slot_range_.load();
  forbidden_slot_range_ = {-1, -1};

  // Set import status on the destination node to FAILED
  auto s = setImportStatusOnDstNode(*dst_fd_, kImportFailed);
  if (!s.IsOK()) {
    return s.Prefixed(errFailedToSetImportStatus);
  }

  return Status::OK();
}

void SlotMigrator::clean() {
  info("[migrate] Clean resources of migrating slot(s) {}", slot_range_.load().String());

  // 释放快照
  if (slot_snapshot_) {
    storage_->GetDB()->ReleaseSnapshot(slot_snapshot_);
    slot_snapshot_ = nullptr;
  }

  current_stage_ = SlotMigrationStage::kNone;
  current_pipeline_size_ = 0;
  wal_begin_seq_ = 0;
  std::lock_guard<std::mutex> guard(job_mutex_);
  migration_job_.reset();
  dst_fd_.Reset();
  slot_range_ = {-1, -1};
  SetStopMigrationFlag(false);
}

Status SlotMigrator::authOnDstNode(int sock_fd, const std::string &password) {
  std::string cmd = redis::ArrayOfBulkStrings({"auth", password});
  auto s = util::SockSend(sock_fd, cmd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to send AUTH command");
  }

  s = checkSingleResponse(sock_fd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to check the response of AUTH command");
  }

  return Status::OK();
}

Status SlotMigrator::setImportStatusOnDstNode(int sock_fd, int status) {
  // 检查 socket 文件描述符是否有效
  if (sock_fd <= 0) return {Status::NotOK, "invalid socket descriptor"};

  // 构造 Redis 命令：
  //    cluster import <slot_range> <status>
  // 其中：
  //  status：
  //   - 0: 开始导入
  //   - 1: 成功
  //   - 2: 失败
  //   - 3: 未知
  std::string cmd =
      redis::ArrayOfBulkStrings({"cluster", "import", slot_range_.load().String(), std::to_string(status)});

  // 发送到目标节点
  auto s = util::SockSend(sock_fd, cmd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to send command to the destination node");
  }

  // 读取响应
  s = checkSingleResponse(sock_fd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to check the response from the destination node");
  }

  return Status::OK();
}

StatusOr<bool> SlotMigrator::supportedApplyBatchCommandOnDstNode(int sock_fd) {
  std::string cmd = redis::ArrayOfBulkStrings({"command", "info", "applybatch"});
  auto s = util::SockSend(sock_fd, cmd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to send command info to the destination node");
  }

  UniqueEvbuf evbuf;
  if (evbuffer_read(evbuf.get(), sock_fd, -1) <= 0) {
    return Status::FromErrno("read response error");
  }

  UniqueEvbufReadln line(evbuf.get(), EVBUFFER_EOL_CRLF_STRICT);
  if (!line) {
    return Status::FromErrno("read empty response");
  }

  if (line[0] == '*') {
    line = UniqueEvbufReadln(evbuf.get(), EVBUFFER_EOL_LF);
    if (line && line[0] == '*') {
      return true;
    }
  }

  return false;
}

Status SlotMigrator::checkSingleResponse(int sock_fd) { return checkMultipleResponses(sock_fd, 1); }

// Commands  |  Response            |  Instance
// ++++++++++++++++++++++++++++++++++++++++
// set          Redis::Integer         :1\r\n
// hset         Redis::SimpleString    +OK\r\n
// sadd         Redis::Integer
// zadd         Redis::Integer
// siadd        Redis::Integer
// setbit       Redis::Integer
// expire       Redis::Integer
// lpush        Redis::Integer
// rpush        Redis::Integer
// ltrim        Redis::SimpleString    -Err\r\n
// linsert      Redis::Integer
// lset         Redis::SimpleString
// hdel         Redis::Integer
// srem         Redis::Integer
// zrem         Redis::Integer
// lpop         Redis::NilString       $-1\r\n
//          or  Redis::BulkString      $1\r\n1\r\n
// rpop         Redis::NilString
//          or  Redis::BulkString
// lrem         Redis::Integer
// sirem        Redis::Integer
// del          Redis::Integer
// xadd         Redis::BulkString
// bitfield     Redis::Array           *1\r\n:0
Status SlotMigrator::checkMultipleResponses(int sock_fd, int total) {
  if (sock_fd < 0 || total <= 0) {
    return {Status::NotOK, fmt::format("invalid arguments: sock_fd={}, count={}", sock_fd, total)};
  }

  // Set socket receive timeout first
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  // Start checking response
  size_t bulk_or_array_len = 0;
  int cnt = 0;
  parser_state_ = ParserState::ArrayLen;
  UniqueEvbuf evbuf;
  while (true) {
    // Read response data from socket buffer to the event buffer
    if (evbuffer_read(evbuf.get(), sock_fd, -1) <= 0) {
      return {Status::NotOK, fmt::format("failed to read response: {}", strerror(errno))};
    }

    // Parse response data in event buffer
    bool run = true;
    while (run) {
      switch (parser_state_) {
        // Handle single string response
        case ParserState::ArrayLen: {
          UniqueEvbufReadln line(evbuf.get(), EVBUFFER_EOL_CRLF_STRICT);
          if (!line) {
            info("[migrate] Event buffer is empty, read socket again");
            run = false;
            break;
          }

          if (line[0] == '-') {
            return {Status::NotOK, fmt::format("got invalid response of length {}: {}", line.length, line.get())};
          } else if (line[0] == '$' || line[0] == '*') {
            auto parse_result = ParseInt<uint64_t>(std::string(line.get() + 1, line.length - 1), 10);
            if (!parse_result) {
              return {Status::NotOK, "protocol error: expected integer value"};
            }

            bulk_or_array_len = *parse_result;
            if (bulk_or_array_len <= 0) {
              parser_state_ = ParserState::OneRspEnd;
            } else if (line[0] == '$') {
              parser_state_ = ParserState::BulkData;
            } else {
              parser_state_ = ParserState::ArrayData;
            }
          } else if (line[0] == '+' || line[0] == ':') {
            parser_state_ = ParserState::OneRspEnd;
          } else {
            return {Status::NotOK, fmt::format("got unexpected response of length {}: {}", line.length, line.get())};
          }

          break;
        }
        // Handle bulk string response
        case ParserState::BulkData: {
          if (evbuffer_get_length(evbuf.get()) < bulk_or_array_len + 2) {
            info("[migrate] Bulk data in event buffer is not complete, read socket again");
            run = false;
            break;
          }
          // TODO(chrisZMF): Check tail '\r\n'
          evbuffer_drain(evbuf.get(), bulk_or_array_len + 2);
          bulk_or_array_len = 0;
          parser_state_ = ParserState::OneRspEnd;
          break;
        }
        case ParserState::ArrayData: {
          while (run && bulk_or_array_len > 0) {
            evbuffer_ptr ptr = evbuffer_search_eol(evbuf.get(), nullptr, nullptr, EVBUFFER_EOL_CRLF_STRICT);
            if (ptr.pos < 0) {
              info("[migrate] Array data in event buffer is not complete, read socket again");
              run = false;
              break;
            }
            evbuffer_drain(evbuf.get(), ptr.pos + 2);
            --bulk_or_array_len;
          }
          if (run) {
            parser_state_ = ParserState::OneRspEnd;
          }
          break;
        }
        case ParserState::OneRspEnd: {
          cnt++;
          if (cnt >= total) {
            return Status::OK();
          }

          parser_state_ = ParserState::ArrayLen;
          break;
        }
        default:
          break;
      }
    }
  }
}

// 将一个 Key 从当前实例迁移到目标节点
StatusOr<KeyMigrationResult> SlotMigrator::migrateOneKey(const rocksdb::Slice &key,
                                                         const rocksdb::Slice &encoded_metadata,
                                                         std::string *restore_cmds) {

  // 将 RocksDB 中的 value 解码为一个 Metadata 对象，得到 key 的类型、大小、过期时间等信息。
  std::string bytes = encoded_metadata.ToString();
  Metadata metadata(kRedisNone, false);
  if (auto s = metadata.Decode(bytes); !s.ok()) {
    return {Status::NotOK, s.ToString()};
  }
  // 如果是非空类型但当前值为空，无需迁移。
  if (!metadata.IsEmptyableType() && metadata.size == 0) {
    return KeyMigrationResult::kUnderlyingStructEmpty;
  }
  // 如果该 Key 已经过期了，也不迁移。
  if (metadata.Expired()) {
    return KeyMigrationResult::kExpired;
  }

  // Construct command according to type of the key
  switch (metadata.Type()) {
    case kRedisString:
    case kRedisJson: {
      auto s = migrateSimpleKey(key, metadata, bytes, restore_cmds);
      if (!s.IsOK()) {
        return s.Prefixed("failed to migrate simple key");
      }
      break;
    }
    case kRedisList:
    case kRedisZSet:
    case kRedisBitmap:
    case kRedisHash:
    case kRedisSet:
    case kRedisSortedint: {
      auto s = migrateComplexKey(key, metadata, restore_cmds);
      if (!s.IsOK()) {
        return s.Prefixed("failed to migrate complex key");
      }
      break;
    }
    case kRedisStream: {
      StreamMetadata stream_md(false);
      if (auto s = stream_md.Decode(bytes); !s.ok()) {
        return {Status::NotOK, s.ToString()};
      }

      auto s = migrateStream(key, stream_md, restore_cmds);
      if (!s.IsOK()) {
        return s.Prefixed("failed to migrate stream key");
      }
      break;
    }
    case kRedisHyperLogLog: {
      // HyperLogLog migration by cmd is not supported,
      // since it's hard to restore the same key structure for HyperLogLog
      // commands.
      break;
    }
    default:
      break;
  }

  return KeyMigrationResult::kMigrated;
}

Status SlotMigrator::migrateSimpleKey(const rocksdb::Slice &key, const Metadata &metadata, const std::string &bytes, std::string *restore_cmds) {
  if (metadata.Type() == kRedisString) {
    // 构建 SET key value 命令
    std::vector<std::string> command = {"SET", key.ToString(), bytes.substr(Metadata::GetOffsetAfterExpire(bytes[0]))};
    // 如果有过期时间，添加 PXAT 参数
    if (metadata.expire > 0) {
      command.emplace_back("PXAT");
      command.emplace_back(std::to_string(metadata.expire));
    }
    // 将命令转换为 Redis 协议格式，存入 buffer ；
    // 例如：["SET","foo","bar"] → *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    *restore_cmds += redis::ArrayOfBulkStrings(command);
    // 命令计数，跟踪当前累积的命令数量，用于决定何时触发批量发送
    current_pipeline_size_++;
  } else if (metadata.Type() == kRedisJson) {
    // kRedisJson
    // 将原始 bytes 反序列化为 JsonValue 对象，这里 FromRawString 会跳过 metadata 部分
    JsonValue json_value;
    if (auto s = redis::Json::FromRawString(bytes, &json_value); !s.ok()) {
      return {Status::NotOK, s.ToString()};
    }
    // 将 JsonValue 对象转成标准 JSON 字符串
    auto json_bytes = GET_OR_RET(json_value.Dump());
    // 构造 Redis 命令：JSON.SET key $ <json-string>
    std::vector<std::string> command = {"JSON.SET", key.ToString(), "$", std::move(json_bytes)};
    // 将命令转换为 Redis 协议格式，存入 buffer
    *restore_cmds += redis::ArrayOfBulkStrings(command);
    // 命令计数，跟踪当前累积的命令数量，用于决定何时触发批量发送
    current_pipeline_size_++;
    // 如果有过期时间，附带一个 PEXPIREAT 命令，注意 metadata.expire 是绝对时间戳。
    if (metadata.expire > 0) {
      *restore_cmds += redis::ArrayOfBulkStrings({"PEXPIREAT", key.ToString(), std::to_string(metadata.expire)});
      current_pipeline_size_++;
    }
  } else {
    return {Status::NotOK, "unsupported simple key type"};
  }

  // Check whether pipeline needs to be sent
  // TODO(chrisZMF): Resend data if failed to send data
  auto s = sendCmdsPipelineIfNeed(restore_cmds, false);
  if (!s.IsOK()) {
    return s.Prefixed(errFailedToSendCommands);
  }

  return Status::OK();
}

// Kvrocks 的复杂 key（list、set、zset 等）是用多个底层 RocksDB key 来表示的：
//  - 逻辑的 redis key：存在 metadata 中
//  - 每个元素（subkey）作为一个独立的 entry 存在 RocksDB 中，格式是 InternalKey(namespace, key, subkey, version)
//
Status SlotMigrator::migrateComplexKey(const rocksdb::Slice &key, const Metadata &metadata, std::string *restore_cmds) {
  // 构造命令前缀，根据 meta.Type 确定：set -> SADD, zset -> ZADD, list -> RPUSH 等
  std::string cmd;
  {
    auto iter = type_to_cmd.find(metadata.Type());
    if (iter != type_to_cmd.end()) {
      cmd = iter->second;
    } else {
      // 不支持的类型立即返回错误
      if (metadata.Type() > RedisTypeNames.size()) {
        return {Status::NotOK, "unknown key type: " + std::to_string(metadata.Type())};
      }
      return {Status::NotOK, "unsupported complex key type: " + metadata.TypeName()};
    }
  }

  // 用户原始命令，其操作的是用户的原始 key ，不是 rocksdb 的存储 key
  std::vector<std::string> user_cmd = {cmd, key.ToString()};

  // Construct key prefix to iterate values of the complex type user key
  // 基于 key 构造 RocksDB 底层存储的 internal key 前缀，用于扫描该键的所有元素
  std::string slot_key = AppendNamespacePrefix(key);
  std::string prefix_subkey = InternalKey(slot_key, "", metadata.version, true).Encode();

  // 构造 RocksDB 迭代器（扫描子键）
  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.snapshot = slot_snapshot_;
  Slice prefix_slice(prefix_subkey);
  read_options.iterate_lower_bound = &prefix_slice;
  // Should use th raw db iterator to avoid reading uncommitted writes in transaction mode
  auto iter = util::UniqueIterator(storage_->GetDB()->NewIterator(read_options));
  int item_count = 0;

  // 遍历 RocksDB 中该 key 所有的子项
  for (iter->Seek(prefix_subkey); iter->Valid(); iter->Next()) {
    if (stop_migration_) {
      return {Status::NotOK, std::string(errMigrationTaskCanceled)};
    }

    if (!iter->key().starts_with(prefix_subkey)) {
      break;
    }

    // Parse values of the complex key
    // InternalKey is adopted to get complex key's value from the formatted key return by iterator of rocksdb
    //
    // 使用 InternalKey 解析出 sub_key
    InternalKey inkey(iter->key(), true);
    // 不同类型，用不同方式组装参数
    switch (metadata.Type()) {
      case kRedisSet: {
        // SADD key subkey1 subkey2
        user_cmd.emplace_back(inkey.GetSubKey().ToString());
        break;
      }
      case kRedisSortedint: {
        auto id = DecodeFixed64(inkey.GetSubKey().ToString().data());
        user_cmd.emplace_back(std::to_string(id));
        break;
      }
      case kRedisZSet: {
        // ZADD key score1 member1 score2 member2
        auto score = DecodeDouble(iter->value().ToString().data());
        user_cmd.emplace_back(util::Float2String(score));
        user_cmd.emplace_back(inkey.GetSubKey().ToString());
        break;
      }
      case kRedisBitmap: {
        auto s = migrateBitmapKey(inkey, &iter, &user_cmd, restore_cmds);
        if (!s.IsOK()) {
          return s.Prefixed("failed to migrate bitmap key");
        }
        break;
      }
      case kRedisHash: {
        // HMSET key field1 val1 field2 val2
        user_cmd.emplace_back(inkey.GetSubKey().ToString());
        user_cmd.emplace_back(iter->value().ToString());
        break;
      }
      case kRedisList: {
        // RPUSH key item1 item2
        user_cmd.emplace_back(iter->value().ToString());
        break;
      }
      case kRedisHyperLogLog: {
        break;
      }
      default:
        break;
    }

    // Check item count
    // Exclude bitmap because it does not have hmset-like command
    if (metadata.Type() != kRedisBitmap) {
      item_count++;
      if (item_count >= kMaxItemsInCommand) {
        // 每次到达批量阈值（比如 1024），将命令加到 restore_cmds 中。
        *restore_cmds += redis::ArrayOfBulkStrings(user_cmd);
        current_pipeline_size_++;
        item_count = 0;
        // Have to clear saved items
        // 清空 user_cmd 的参数部分（保留前两个元素：命令名和 key），准备下次拼接。
        user_cmd.erase(user_cmd.begin() + 2, user_cmd.end());

        // Send commands if the pipeline contains enough of them
        auto s = sendCmdsPipelineIfNeed(restore_cmds, false);
        if (!s.IsOK()) {
          return s.Prefixed(errFailedToSendCommands);
        }
      }
    }
  }

  if (auto s = iter->status(); !s.ok()) {
    return {Status::NotOK,
            fmt::format("failed to iterate values of the complex key {}: {}", key.ToString(), s.ToString())};
  }


  // 处理最后一批不满的子项...

  // Have to check the item count of the last command list
  // 每次到达批量阈值（比如 1024），将命令加到 restore_cmds 中。
  if (item_count % kMaxItemsInCommand != 0) {
    *restore_cmds += redis::ArrayOfBulkStrings(user_cmd);
    current_pipeline_size_++;
  }

  // Add TTL for complex key
  // 加上过期时间
  if (metadata.expire > 0) {
    *restore_cmds += redis::ArrayOfBulkStrings({"PEXPIREAT", key.ToString(), std::to_string(metadata.expire)});
    current_pipeline_size_++;
  }

  // Send commands if the pipeline contains enough of them
  // 根据当前 pipeline 是否达到了阈值，判断是否需要发送。
  auto s = sendCmdsPipelineIfNeed(restore_cmds, false);
  if (!s.IsOK()) {
    return s.Prefixed(errFailedToSendCommands);
  }

  return Status::OK();
}

Status SlotMigrator::migrateStream(const Slice &key, const StreamMetadata &metadata, std::string *restore_cmds) {
  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.snapshot = slot_snapshot_;
  std::string ns_key = AppendNamespacePrefix(key);
  // Construct key prefix to iterate values of the stream
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, true).Encode();
  rocksdb::Slice prefix_key_slice(prefix_key);
  read_options.iterate_lower_bound = &prefix_key_slice;

  // Should use th raw db iterator to avoid reading uncommitted writes in transaction mode
  auto iter =
      util::UniqueIterator(storage_->GetDB()->NewIterator(read_options, storage_->GetCFHandle(ColumnFamilyID::Stream)));

  std::vector<std::string> user_cmd = {type_to_cmd[metadata.Type()], key.ToString()};

  for (iter->Seek(prefix_key); iter->Valid(); iter->Next()) {
    if (stop_migration_) {
      return {Status::NotOK, std::string(errMigrationTaskCanceled)};
    }

    if (!iter->key().starts_with(prefix_key)) {
      break;
    }

    auto s = WriteBatchExtractor::ExtractStreamAddCommand(true, iter->key(), iter->value(), &user_cmd);
    if (!s.IsOK()) {
      return s;
    }
    *restore_cmds += redis::ArrayOfBulkStrings(user_cmd);
    current_pipeline_size_++;

    user_cmd.erase(user_cmd.begin() + 2, user_cmd.end());

    s = sendCmdsPipelineIfNeed(restore_cmds, false);
    if (!s.IsOK()) {
      return s.Prefixed(errFailedToSendCommands);
    }
  }

  if (auto s = iter->status(); !s.ok()) {
    return {Status::NotOK,
            fmt::format("failed to iterate values of the stream key {}: {}", key.ToString(), s.ToString())};
  }

  // commands like XTRIM and XDEL affect stream's metadata, but we use only XADD for a slot migration
  // XSETID is used to adjust stream's info on the destination node according to the current values on the source
  *restore_cmds += redis::ArrayOfBulkStrings({"XSETID", key.ToString(), metadata.last_generated_id.ToString(),
                                              "ENTRIESADDED", std::to_string(metadata.entries_added), "MAXDELETEDID",
                                              metadata.max_deleted_entry_id.ToString()});
  current_pipeline_size_++;

  // Add TTL
  if (metadata.expire > 0) {
    *restore_cmds += redis::ArrayOfBulkStrings({"PEXPIREAT", key.ToString(), std::to_string(metadata.expire)});
    current_pipeline_size_++;
  }

  auto s = sendCmdsPipelineIfNeed(restore_cmds, false);
  if (!s.IsOK()) {
    return s.Prefixed(errFailedToSendCommands);
  }

  return Status::OK();
}

Status SlotMigrator::migrateBitmapKey(const InternalKey &inkey, std::unique_ptr<rocksdb::Iterator> *iter,
                                      std::vector<std::string> *user_cmd, std::string *restore_cmds) {
  std::string index_str = inkey.GetSubKey().ToString();
  std::string fragment = (*iter)->value().ToString();
  auto parse_result = ParseInt<int>(index_str, 10);
  if (!parse_result) {
    return {Status::RedisParseErr, "index is not a valid integer"};
  }

  uint32_t index = *parse_result;

  // Bitmap does not have hmset-like command
  // TODO(chrisZMF): Use hmset-like command for efficiency
  for (int byte_idx = 0; byte_idx < static_cast<int>(fragment.size()); byte_idx++) {
    if (fragment[byte_idx] & 0xff) {
      for (int bit_idx = 0; bit_idx < 8; bit_idx++) {
        if (fragment[byte_idx] & (1 << bit_idx)) {
          uint32_t offset = (index * 8) + (byte_idx * 8) + bit_idx;
          user_cmd->emplace_back(std::to_string(offset));
          user_cmd->emplace_back("1");
          *restore_cmds += redis::ArrayOfBulkStrings(*user_cmd);
          current_pipeline_size_++;
          user_cmd->erase(user_cmd->begin() + 2, user_cmd->end());
        }
      }

      auto s = sendCmdsPipelineIfNeed(restore_cmds, false);
      if (!s.IsOK()) {
        return s.Prefixed(errFailedToSendCommands);
      }
    }
  }

  return Status::OK();
}

Status SlotMigrator::sendCmdsPipelineIfNeed(std::string *commands, bool need) {
  // 中止迁移？
  if (stop_migration_) {
    return {Status::NotOK, std::string(errMigrationTaskCanceled)};
  }

  // Check pipeline
  // 是否强制发送？是否攒够一批？
  if (!need && current_pipeline_size_ < max_pipeline_size_) {
    return Status::OK();
  }

  // 如果 pipeline 是空的，也无需发送
  if (current_pipeline_size_ == 0) {
    info("[migrate] No commands to send");
    return Status::OK();
  }

  // 控制发送速率 sleep a while
  applyMigrationSpeedLimit();

  // 发送数据
  auto s = util::SockSend(*dst_fd_, *commands);
  if (!s.IsOK()) {
    return s.Prefixed("failed to write data to a socket");
  }

  last_send_time_ = util::GetTimeStampUS();

  // 因为 Redis 是请求-响应协议，发送几条命令就必须读几条回复。
  s = checkMultipleResponses(*dst_fd_, current_pipeline_size_);
  if (!s.IsOK()) {
    return s.Prefixed("wrong response from the destination node");
  }

  // Clear commands and running pipeline
  // 变量重置，方便下次发送
  commands->clear();
  current_pipeline_size_ = 0;

  return Status::OK();
}

// 在 slot 迁移过程的后半段，为了保证数据一致性，源端必须禁止该 slot 上的所有写入。
// 因为快照数据已经迁移，增量数据也已通过 WAL 迁移，但是迁移过程中有源源不断的新写入，会导致 WAL 同步始终无法结束；
// 所以最终阶段必须加锁禁止写入，设置 “forbidden slot” 。
void SlotMigrator::setForbiddenSlotRange(const SlotRange &slot_range) {
  info("[migrate] Setting forbidden slot(s) {}", slot_range.String());
  // Block server to set forbidden slot
  uint64_t during = util::GetTimeStampUS();
  // 加锁保护 forbidden_slot_range_ 写操作，避免并发问题；
  //  - srv_->WorkExclusivityGuard() 返回一个 RAII 互斥锁，在大括号作用域内保护 forbidden_slot_range_ 并发访问。
  //  - forbidden_slot_range_ 成员变量用于标记 “当前禁止写入的槽位范围” ，避免始终无法完成迁移。
  {
    auto exclusivity = srv_->WorkExclusivityGuard();
    forbidden_slot_range_ = slot_range;
  }
  during = util::GetTimeStampUS() - during;
  info("[migrate] To set forbidden slot, server was blocked for {} us", during);
}

void SlotMigrator::ReleaseForbiddenSlotRange() {
  info("[migrate] Release forbidden slot(s) {}", forbidden_slot_range_.load().String());
  forbidden_slot_range_ = {-1, -1};
}

void SlotMigrator::applyMigrationSpeedLimit() const {
  if (max_migration_speed_ > 0) {
    uint64_t current_time = util::GetTimeStampUS();
    uint64_t per_request_time = 1000000 * max_pipeline_size_ / max_migration_speed_;
    if (per_request_time == 0) {
      per_request_time = 1;
    }
    if (last_send_time_ + per_request_time > current_time) {
      uint64_t during = last_send_time_ + per_request_time - current_time;
      info("[migrate] Sleep to limit migration speed for: {}", during);
      std::this_thread::sleep_for(std::chrono::microseconds(during));
    }
  }
}

Status SlotMigrator::generateCmdsFromBatch(rocksdb::BatchResult *batch, std::string *commands) {
  // Iterate batch to get keys and construct commands for keys
  WriteBatchExtractor write_batch_extractor(storage_->IsSlotIdEncoded(), slot_range_, false);
  rocksdb::Status status = batch->writeBatchPtr->Iterate(&write_batch_extractor);
  if (!status.ok()) {
    error("[migrate] Failed to parse write batch, Err: {}", status.ToString());
    return {Status::NotOK};
  }

  // Get all constructed commands
  auto resp_commands = write_batch_extractor.GetRESPCommands();
  for (const auto &iter : *resp_commands) {
    for (const auto &it : iter.second) {
      *commands += it;
      current_pipeline_size_++;
    }
  }

  return Status::OK();
}

Status SlotMigrator::migrateIncrementData(std::unique_ptr<rocksdb::TransactionLogIterator> *iter, uint64_t end_seq) {
  // 检查 WAL 迭代器是否有效，如果无效则无法读取增量日志，直接报错。
  if (!(*iter) || !(*iter)->Valid()) {
    error("[migrate] WAL iterator is invalid");
    return {Status::NotOK};
  }

  uint64_t next_seq = wal_begin_seq_ + 1; // 同步序号
  std::string commands; // 用于拼接待发送的 Redis 命令字符串（pipeline 模式）。

  while (true) {
    // 停止迁移？
    if (stop_migration_) {
      error("[migrate] Migration task end during migrating WAL data");
      return {Status::NotOK};
    }

    // 在 RocksDB 中 WAL 数据是以 WriteBatch 为单位的，每个 batch 表示一次用户操作（SET、DEL 等）提交的写集合。
    auto batch = (*iter)->GetBatch();
    // WAL 是严格按顺序追加的，正常情况下每个 batch 的序列号是连续的，如果出现跳跃，可能是：
    //  - RocksDB 被压缩、合并导致部分写被丢弃；
    //  - WAL 被截断；
    //  - 存储系统异常。
    // 一旦发现序号不连续，说明数据不完整，不能迁移。
    if (batch.sequence != next_seq) {
      error("[migrate] WAL iterator is discrete, some seq might be lost, expected sequence: {}, but got sequence: {}",
            next_seq, batch.sequence);
      return {Status::NotOK};
    }

    // Generate commands by iterating write batch
    // 将 WriteBatch 转换为 Redis 命令，追加到 commands 中（猜测：这里可能会过滤掉不属于当前 slot_range_ 的 keys）
    auto s = generateCmdsFromBatch(&batch, &commands);
    if (!s.IsOK()) {
      error("[migrate] Failed to generate commands from write batch");
      return {Status::NotOK};
    }

    // Check whether command pipeline should be sent
    // 尝试发送数据，如果积累的命令数量未达到阈值，不会立即发送，但也会返回 ok ；
    s = sendCmdsPipelineIfNeed(&commands, false);
    if (!s.IsOK()) {
      error("[migrate] Failed to send WAL commands pipeline");
      return {Status::NotOK};
    }

    // 更新下一个期望序列号，每个 batch 可能包含多个写操作。
    next_seq = batch.sequence + batch.writeBatchPtr->Count();
    // 如果我们已经处理完所有期望范围内的日志（从 wal_begin_seq_+1 到 end_seq），退出循环。
    if (next_seq > end_seq) {
      info("[migrate] Migrate incremental data an epoch OK, seq from {}, to {}", wal_begin_seq_, end_seq);
      break;
    }

    // 继续读取下一个 batch
    (*iter)->Next();
    if (!(*iter)->Valid()) { // 检查是否有效
      error("[migrate] WAL iterator is invalid, expected end seq: {}, next seq: {}", end_seq, next_seq);
      return {Status::NotOK};
    }
  }

  // Send the left data of this epoch
  // 发送最后一批命令
  auto s = sendCmdsPipelineIfNeed(&commands, true);
  if (!s.IsOK()) {
    error("[migrate] Failed to send WAL last commands in pipeline");
    return {Status::NotOK};
  }

  return Status::OK();
}


// 在迁移过程中，主流程分为两个阶段：
//  - 迁移 snapshot（旧数据）
//  - 迁移 WAL（增量写入数据）
// 在迁移完 snapshot 后，可能仍有客户端在写数据（写入 RocksDB -> WAL）。
// 此函数的作用就是：在当前节点禁用这个 slot 前，把 WAL 中最新写入的变更都尽量迁移出去，确保迁移数据不会漏。
Status SlotMigrator::syncWalBeforeForbiddingSlot() {
  uint32_t count = 0;

  // 设定最多循环次数，每次迭代会同步一批增量数据。
  while (count < kMaxLoopTimes) {
    // 获取当前最新的 WAL 序列号
    uint64_t latest_seq = storage_->GetDB()->GetLatestSequenceNumber();
    // 根据已同步完的 snapshot 序号计算 gap ，即这段时间新增的数据量
    uint64_t gap = latest_seq - wal_begin_seq_;
    // 如果 gap 很小，说明变更数据已经 “追得差不多了”，此时就可以放心地设置 forbid ，不用再等
    if (gap <= static_cast<uint64_t>(seq_gap_limit_)) {
      info("[migrate] Incremental data sequence: {}, less than limit: {}, go to set forbidden slot", gap,
           seq_gap_limit_);
      break;
    }

    // 创建一个 WAL 迭代器，从 wal_begin_seq_ + 1 开始读取变更并进行迁移
    std::unique_ptr<rocksdb::TransactionLogIterator> iter = nullptr;
    auto s = storage_->GetWALIter(wal_begin_seq_ + 1, &iter);
    if (!s.IsOK()) {
      error("[migrate] Failed to generate WAL iterator before setting forbidden slot, Err: {}", s.Msg());
      return {Status::NotOK};
    }

    // Iterate wal and migrate data
    // 执行 wal 的增量同步，完成 [wal_begin_seq_+1, latest_seq] 区间的数据同步
    s = migrateIncrementData(&iter, latest_seq);
    if (!s.IsOK()) {
      error("[migrate] Failed to migrate WAL data before setting forbidden slot");
      return {Status::NotOK};
    }

    // 因为同步过程中可能还有新数据写入，所以开始下一次增量同步，直到 gap 小于指定阈值
    wal_begin_seq_ = latest_seq;
    count++;
  }

  info("[migrate] Succeed to migrate incremental data before setting forbidden slot, end epoch: {}", count);
  return Status::OK();
}

// 在禁止写入某个 slot 之后，同步这期间产生的增量数据（通过 WAL 日志），完成最后的收尾工作。
Status SlotMigrator::syncWalAfterForbiddingSlot() {
  uint64_t latest_seq = storage_->GetDB()->GetLatestSequenceNumber();

  // No incremental data
  if (latest_seq <= wal_begin_seq_) return Status::OK();

  // Get WAL iter
  std::unique_ptr<rocksdb::TransactionLogIterator> iter = nullptr;
  auto s = storage_->GetWALIter(wal_begin_seq_ + 1, &iter);
  if (!s.IsOK()) {
    error("[migrate] Failed to generate WAL iterator after setting forbidden slot, Err: {}", s.Msg());
    return {Status::NotOK};
  }

  // Send incremental data
  s = migrateIncrementData(&iter, latest_seq);
  if (!s.IsOK()) {
    error("[migrate] Failed to migrate WAL data after setting forbidden slot");
    return {Status::NotOK};
  }

  return Status::OK();
}

void SlotMigrator::GetMigrationInfo(std::string *info) const {
  info->clear();
  if (!slot_range_.load().IsValid() &&
      !forbidden_slot_range_.load().IsValid() &&
      !migrate_failed_slot_range_.load().IsValid()) {
    return;
  }

  SlotRange slot_range;
  std::string task_state;

  switch (migration_state_.load()) {
    case MigrationState::kNone:
      task_state = "none";
      break;
    case MigrationState::kStarted:
      task_state = "start";
      slot_range = slot_range_;
      break;
    case MigrationState::kSuccess:
      task_state = "success";
      slot_range = forbidden_slot_range_;
      break;
    case MigrationState::kFailed:
      task_state = "fail";
      slot_range = migrate_failed_slot_range_;
      break;
    default:
      break;
  }

  *info = fmt::format("migrating_slot(s): {}\r\ndestination_node: {}\r\nmigrating_state: {}\r\n", slot_range.String(),
                      dst_node_, task_state);
}

void SlotMigrator::CancelSyncCtx() {
  std::unique_lock<std::mutex> lock(blocking_mutex_);
  blocking_context_ = nullptr;
}

void SlotMigrator::resumeSyncCtx(const Status &migrate_result) {
  std::unique_lock<std::mutex> lock(blocking_mutex_);
  if (blocking_context_) {
    blocking_context_->Resume(migrate_result); // [重要] 通知迁移已经完成
    blocking_context_ = nullptr;
  }
}

Status SlotMigrator::sendMigrationBatch(BatchSender *batch) {
  // user may dynamically change some configs, apply it when send data
  batch->SetMaxBytes(migrate_batch_size_bytes_);
  batch->SetBytesPerSecond(migrate_batch_bytes_per_sec_);
  return batch->Send();
}

Status SlotMigrator::sendSnapshotByRawKV() {
  uint64_t start_ts = util::GetTimeStampMS();
  auto slot_range = slot_range_.load();
  info("[migrate] Migrating snapshot of slot(s) {} by raw key value", slot_range.String());

  auto prefix = ComposeSlotKeyPrefix(namespace_, slot_range.start);
  auto upper_bound = ComposeSlotKeyUpperBound(namespace_, slot_range.end);

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.snapshot = slot_snapshot_;
  rocksdb::Slice prefix_slice(prefix);
  rocksdb::Slice upper_bound_slice(upper_bound);
  read_options.iterate_lower_bound = &prefix_slice;
  read_options.iterate_upper_bound = &upper_bound_slice;
  auto no_txn_ctx = engine::Context::NoTransactionContext(storage_);
  engine::DBIterator iter(no_txn_ctx, read_options);

  BatchSender batch_sender(*dst_fd_, migrate_batch_size_bytes_, migrate_batch_bytes_per_sec_);

  for (iter.Seek(prefix); iter.Valid(); iter.Next()) {
    // Iteration is out of range
    auto key_slot_id = ExtractSlotId(iter.Key());
    if (!slot_range.Contains(key_slot_id)) {
      break;
    }

    auto redis_type = iter.Type();
    std::string log_data;
    if (redis_type == RedisType::kRedisList) {
      redis::WriteBatchLogData batch_log_data(redis_type, {std::to_string(RedisCommand::kRedisCmdRPush)});
      log_data = batch_log_data.Encode();
    } else {
      redis::WriteBatchLogData batch_log_data(redis_type);
      log_data = batch_log_data.Encode();
    }
    batch_sender.SetPrefixLogData(log_data);

    GET_OR_RET(batch_sender.Put(storage_->GetCFHandle(ColumnFamilyID::Metadata), iter.Key(), iter.Value()));

    auto subkey_iter = iter.GetSubKeyIterator();
    if (!subkey_iter) {
      continue;
    }

    for (subkey_iter->Seek(); subkey_iter->Valid(); subkey_iter->Next()) {
      GET_OR_RET(batch_sender.Put(subkey_iter->ColumnFamilyHandle(), subkey_iter->Key(), subkey_iter->Value()));

      if (redis_type == RedisType::kRedisZSet) {
        InternalKey internal_key(subkey_iter->Key(), storage_->IsSlotIdEncoded());
        auto score_key = subkey_iter->Value().ToString();
        score_key.append(subkey_iter->UserKey().ToString());
        auto score_key_bytes =
            InternalKey(iter.Key(), score_key, internal_key.GetVersion(), storage_->IsSlotIdEncoded()).Encode();
        GET_OR_RET(batch_sender.Put(storage_->GetCFHandle(ColumnFamilyID::SecondarySubkey), score_key_bytes, Slice()));
      }

      if (batch_sender.IsFull()) {
        GET_OR_RET(sendMigrationBatch(&batch_sender));
      }
    }

    if (batch_sender.IsFull()) {
      GET_OR_RET(sendMigrationBatch(&batch_sender));
    }
  }

  GET_OR_RET(sendMigrationBatch(&batch_sender));

  auto elapsed = util::GetTimeStampMS() - start_ts;
  info(
      "[migrate] Succeed to migrate snapshot range, slot(s): {}, elapsed: {} ms, sent: {} bytes, rate: {:.2f} kb/s, "
      "batches: {}, entries: {}",
      slot_range.String(), elapsed, batch_sender.GetSentBytes(), batch_sender.GetRate(start_ts),
      batch_sender.GetSentBatchesNum(), batch_sender.GetEntriesNum());

  return Status::OK();
}

Status SlotMigrator::syncWALByRawKV() {
  uint64_t start_ts = util::GetTimeStampMS();
  info("[migrate] Syncing WAL of slot(s) {} by raw key value", slot_range_.load().String());
  BatchSender batch_sender(*dst_fd_, migrate_batch_size_bytes_, migrate_batch_bytes_per_sec_);

  int epoch = 1;
  uint64_t wal_incremental_seq = 0;

  while (epoch <= kMaxLoopTimes) {
    if (catchUpIncrementalWAL()) {
      break;
    }
    wal_incremental_seq = storage_->GetDB()->GetLatestSequenceNumber();
    auto s = migrateIncrementalDataByRawKV(wal_incremental_seq, &batch_sender);
    if (!s.IsOK()) {
      return {Status::NotOK, fmt::format("migrate incremental data failed, {}", s.Msg())};
    }
    info("[migrate] Migrated incremental data, epoch: {}, seq from {} to {}", epoch, wal_begin_seq_,
         wal_incremental_seq);
    wal_begin_seq_ = wal_incremental_seq;
    epoch++;
  }

  setForbiddenSlotRange(slot_range_);

  wal_incremental_seq = storage_->GetDB()->GetLatestSequenceNumber();
  if (wal_incremental_seq > wal_begin_seq_) {
    auto s = migrateIncrementalDataByRawKV(wal_incremental_seq, &batch_sender);
    if (!s.IsOK()) {
      return {Status::NotOK, fmt::format("migrate last incremental data failed, {}", s.Msg())};
    }
    info("[migrate] Migrated last incremental data after set forbidden slot, seq from {} to {}", wal_begin_seq_,
         wal_incremental_seq);
  }

  auto elapsed = util::GetTimeStampMS() - start_ts;
  info(
      "[migrate] Succeed to migrate incremental data, slot(s): {}, elapsed: {} ms, "
      "sent: {} bytes, rate: {:.2f} kb/s, batches: {}, entries: {}",
      slot_range_.load().String(), elapsed, batch_sender.GetSentBytes(), batch_sender.GetRate(start_ts),
      batch_sender.GetSentBatchesNum(), batch_sender.GetEntriesNum());

  return Status::OK();
}

bool SlotMigrator::catchUpIncrementalWAL() {
  uint64_t gap = storage_->GetDB()->GetLatestSequenceNumber() - wal_begin_seq_;
  if (gap <= seq_gap_limit_) {
    info("[migrate] Incremental data sequence gap: {}, less than limit: {}, set forbidden slot(s): {}", gap,
         seq_gap_limit_, slot_range_.load().String());
    return true;
  }
  return false;
}

Status SlotMigrator::migrateIncrementalDataByRawKV(uint64_t end_seq, BatchSender *batch_sender) {
  engine::WALIterator wal_iter(storage_, slot_range_);
  uint64_t start_seq = wal_begin_seq_ + 1;
  for (wal_iter.Seek(start_seq); wal_iter.Valid(); wal_iter.Next()) {
    if (wal_iter.NextSequenceNumber() > end_seq + 1) {
      break;
    }
    auto item = wal_iter.Item();
    switch (item.type) {
      case engine::WALItem::Type::kTypeLogData: {
        GET_OR_RET(batch_sender->PutLogData(item.key));
        break;
      }
      case engine::WALItem::Type::kTypePut: {
        if (item.column_family_id > kMaxColumnFamilyID) {
          info("[migrate] Invalid put column family id: {}", item.column_family_id);
          continue;
        }
        GET_OR_RET(batch_sender->Put(storage_->GetCFHandle(static_cast<ColumnFamilyID>(item.column_family_id)),
                                     item.key, item.value));
        break;
      }
      case engine::WALItem::Type::kTypeDelete: {
        if (item.column_family_id > kMaxColumnFamilyID) {
          info("[migrate] Invalid delete column family id: {}", item.column_family_id);
          continue;
        }
        GET_OR_RET(
            batch_sender->Delete(storage_->GetCFHandle(static_cast<ColumnFamilyID>(item.column_family_id)), item.key));
        break;
      }
      case engine::WALItem::Type::kTypeDeleteRange: {
        // Do nothing in DeleteRange due to it might cross multiple slots. It's only used in
        // FLUSHDB/FLUSHALL commands for now and maybe we can disable them while migrating.
      }
      default:
        break;
    }
    if (batch_sender->IsFull()) {
      GET_OR_RET(sendMigrationBatch(batch_sender));
    }
  }

  // send the remaining data
  return sendMigrationBatch(batch_sender);
}
