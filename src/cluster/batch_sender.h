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

#include <rocksdb/rate_limiter.h>
#include <rocksdb/write_batch.h>

#include "status.h"

// BatchSender 的作用是在迁移数据时把多条写操作（Put/Delete/LogData）批量打包成一个 RocksDB 的 WriteBatch，然后发送给目标实例，并支持速率限制。
//
// 成员变量
//    | 变量名                                                | 说明                                                  |
//    | ---------------------- ------------------------------| ---------------------------------------------------- |
//    | `dst_fd_`                                            | 目标 socket 文件描述符（目标实例的连接）                   |
//    | `write_batch_`                                       | 当前正在累积的 RocksDB `WriteBatch` 对象                |
//    | `max_bytes_`                                         | 每个批次最大字节数，超出则需要发送                         |
//    | `bytes_per_sec_`                                     | 限速值，单位 B/s（0 表示不限制）                          |
//    | `rate_limiter_`                                      | 限速器，使用 RocksDB 提供的 `GenericRateLimiter`        |
//    | `prefix_logdata_`                                    | 额外的 log 数据（如迁移的元信息），用于写 batch 前预设      |
//    | `sent_bytes_` / `sent_batches_num_` / `entries_num_` | 已发送的统计信息                                        |
//    | `pending_entries_`                                   | 当前未发送的 entry 数量（调用了 Put/Delete 但还没 Send）   |
//    | ---------------------- ------------------------------| ---------------------------------------------------- |
//
// 设计特点
//  - 批量处理：积累多个操作后一次性发送，提高效率
//  - 速率控制：通过令牌桶算法限制发送速率
//  - 日志支持：支持在数据前添加日志信息
//  - 错误处理：每个操作都有状态返回
//  - 统计功能：跟踪发送量、批次数量等指标
//
// 典型使用场景
//  这个类主要用于数据迁移场景，将RocksDB的变更批量发送到另一个节点，常见于：
//  - 数据库复制
//  - 数据备份
//  - 集群扩容时的数据迁移
class BatchSender {
 public:
  BatchSender() = default;
  BatchSender(int fd, size_t max_bytes, size_t bytes_per_sec)
      : dst_fd_(fd),
        max_bytes_(max_bytes),
        bytes_per_sec_(bytes_per_sec),
        rate_limiter_(std::unique_ptr<rocksdb::RateLimiter>(
            rocksdb::NewGenericRateLimiter(static_cast<int64_t>(bytes_per_sec_)))) {}

  ~BatchSender() = default;

  Status Put(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key, const rocksdb::Slice &value);
  Status Delete(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key);
  Status PutLogData(const rocksdb::Slice &blob);
  void SetPrefixLogData(const std::string &prefix_logdata);
  Status Send();

  void SetMaxBytes(size_t max_bytes) {
    if (max_bytes_ != max_bytes) max_bytes_ = max_bytes;
  }
  bool IsFull() const { return write_batch_.GetDataSize() >= max_bytes_; }
  uint64_t GetSentBytes() const { return sent_bytes_; }
  uint32_t GetSentBatchesNum() const { return sent_batches_num_; }
  uint32_t GetEntriesNum() const { return entries_num_; }
  void SetBytesPerSecond(size_t bytes_per_sec);
  double GetRate(uint64_t since) const;

 private:
  static Status sendApplyBatchCmd(int fd, const rocksdb::WriteBatch &write_batch);

  rocksdb::WriteBatch write_batch_{};
  std::string prefix_logdata_{};
  uint64_t sent_bytes_ = 0;
  uint32_t sent_batches_num_ = 0;
  uint32_t entries_num_ = 0;
  uint32_t pending_entries_ = 0;

  int dst_fd_;
  size_t max_bytes_;

  size_t bytes_per_sec_ = 0;  // 0 means no limit
  std::unique_ptr<rocksdb::RateLimiter> rate_limiter_;
};



