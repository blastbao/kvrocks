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

#include "slot_import.h"

SlotImport::SlotImport(Server *srv)
    : Database(srv->storage, kDefaultNamespace), srv_(srv), import_slot_range_(-1, -1), import_status_(kImportNone) {
  std::lock_guard<std::mutex> guard(mutex_);
  // Let metadata_cf_handle_ be nullptr, then get them in real time while use them.
  // See comments in SlotMigrator::SlotMigrator for detailed reason.
  metadata_cf_handle_ = nullptr;
}

// 在分布式 Key-Value 存储系统中（如 Redis Cluster、Kvrocks），key 会按哈希分布到若干 slot 中，slot 是数据分片的最小单位。
//
// Slot 导入（import）流程通常是指：
//  - 某个节点正在接收来自其他节点的数据迁移（迁入）。
//  - 在接收数据之前，应清空已有该 slot 的旧数据。
//  - 同一时间只能导入一个 slot-range，以避免数据混乱或冲突。
Status SlotImport::Start(const SlotRange &slot_range) {
  // 对 import_status_、import_slot_range_ 的访问需要加锁
  std::lock_guard<std::mutex> guard(mutex_);

  // 如果当前已经有导入任务在进行
  //  - 若是相同 slot_range，再次调用 Start 会直接返回 OK（幂等性）。
  //  - 若是不同 slot_range，则拒绝导入，报错表明当前已有其他导入任务在进行。
  if (import_status_ == kImportStart) {
    // return ok if the same slot is importing
    if (import_slot_range_ == slot_range) {
      return Status::OK();
    }
    return {Status::NotOK,fmt::format("only one importing job is allowed, current importing: {}", import_slot_range_.String())};
  }

  // Clean slot data first
  // 把当前节点上该 slot_range 的旧数据全部删除，保证后续导入过程中数据不会冲突或重复。
  engine::Context ctx(srv_->storage);
  auto s = ClearKeysOfSlotRange(ctx, namespace_, slot_range);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("clear keys of slot(s) error: {}", s.ToString())};
  }

  // 设置导入状态和 slot 范围
  import_status_ = kImportStart;
  import_slot_range_ = slot_range;
  return Status::OK();
}

Status SlotImport::Success(const SlotRange &slot_range) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (import_slot_range_ != slot_range) {
    return {Status::NotOK, fmt::format("mismatch slot, importing slot(s): {}, but got: {}", import_slot_range_.String(),
                                       slot_range.String())};
  }

  Status s = srv_->cluster->SetSlotRangeImported(import_slot_range_);
  if (!s.IsOK()) {
    return {Status::NotOK, fmt::format("unable to set imported status: {}", slot_range.String())};
  }

  import_status_ = kImportSuccess;
  return Status::OK();
}

Status SlotImport::Fail(const SlotRange &slot_range) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (import_slot_range_ != slot_range) {
    return {Status::NotOK, fmt::format("mismatch slot, importing slot(s): {}, but got: {}", import_slot_range_.String(),
                                       slot_range.String())};
  }

  // Clean imported slot data
  engine::Context ctx(srv_->storage);
  auto s = ClearKeysOfSlotRange(ctx, namespace_, slot_range);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("clear keys of slot(s) error: {}", s.ToString())};
  }

  import_status_ = kImportFailed;
  return Status::OK();
}

Status SlotImport::StopForLinkError() {
  // 对 import_status_/namespace_/import_slot_range_ 等变量的访问要加锁
  std::lock_guard<std::mutex> guard(mutex_);

  // We don't need to do anything if the importer is not started yet.
  // 如果当前没有导入任务，直接返回 OK
  if (import_status_ != kImportStart) return Status::OK();

  // Maybe server has failovered
  // Situation:
  // Refer to the situation described in SlotMigrator::SlotMigrator
  // 1. Change server to slave when it is importing data.
  // 2. Source server's migration process end after destination server has finished replication.
  // 3. The migration link closed by source server, then this function will be call by OnEvent.
  // 4. ClearKeysOfSlot can clear data although server is a slave, because ClearKeysOfSlot
  //    deletes data in rocksdb directly. Therefore, it is necessary to avoid clearing data gotten
  //    from new master.
  //
  // 在当前节点还是主节点时，才清除数据，防止误删主从同步来的数据。
  if (!srv_->IsSlave()) {
    // Clean imported slot data
    engine::Context ctx(srv_->storage);
    auto s = ClearKeysOfSlotRange(ctx, namespace_, import_slot_range_);
    if (!s.ok()) {
      return {Status::NotOK, fmt::format("clear keys of slot error: {}", s.ToString())};
    }
  }

  // 设置导入失败状态
  import_status_ = kImportFailed;
  return Status::OK();
}

SlotRange SlotImport::GetSlotRange() {
  std::lock_guard<std::mutex> guard(mutex_);
  // import_slot_ only be set when import_status_ is kImportStart
  if (import_status_ != kImportStart) {
    return {-1, -1};
  }
  return import_slot_range_;
}

int SlotImport::GetStatus() {
  std::lock_guard<std::mutex> guard(mutex_);
  return import_status_;
}

void SlotImport::GetImportInfo(std::string *info) {
  std::lock_guard<std::mutex> guard(mutex_);
  info->clear();
  if (!import_slot_range_.IsValid()) {
    return;
  }

  std::string import_stat;
  switch (import_status_) {
    case kImportNone:
      import_stat = "none";
      break;
    case kImportStart:
      import_stat = "start";
      break;
    case kImportSuccess:
      import_stat = "success";
      break;
    case kImportFailed:
      import_stat = "error";
      break;
    default:
      break;
  }

  *info = fmt::format("importing_slot(s): {}\r\nimport_state: {}\r\n", import_slot_range_.String(), import_stat);
}
