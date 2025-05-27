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

#include "redis_list.h"

#include <cstdlib>
#include <utility>

#include "db_util.h"

namespace redis {

// Redis 的 List 数据结构本质上是一个双端队列（deque），Kvrocks 通过将 index 与 sub_key 组合来唯一定位每个 list 元素，例如：
//
//  key = user:list
//  index = 10000
//  InternalKey = namespace + key + index + version + slotid

// 获取 List 结构的 Meta
rocksdb::Status List::GetMetadata(engine::Context &ctx, const Slice &ns_key, ListMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisList}, ns_key, metadata);
}

rocksdb::Status List::Size(engine::Context &ctx, const Slice &user_key, uint64_t *size) {
  *size = 0;

  // 获取 meta
  std::string ns_key = AppendNamespacePrefix(user_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  // 返回 size
  *size = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status List::Push(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &elems, bool left, uint64_t *new_size) {
  return push(ctx, user_key, elems, true, left, new_size);
}

rocksdb::Status List::PushX(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &elems, bool left, uint64_t *new_size) {
  return push(ctx, user_key, elems, false, left, new_size);
}

rocksdb::Status List::push(engine::Context &ctx,
                           const Slice &user_key,           // 用户提供的键（如 Redis 的 list key）
                           const std::vector<Slice> &elems, // 要插入的多个元素
                           bool create_if_missing,          // 如果 key 不存在，是否自动创建
                           bool left,                       // 插入方向, LPUSH or RPUSH
                           uint64_t *new_size) {            // 输出参数，返回插入后列表的新长度
  *new_size = 0;
  std::string ns_key = AppendNamespacePrefix(user_key); // 基于 user_key 生成以 ns 为前缀的实际存储键


  // 记录日志，用于 AOF 持久化和主从复制
  RedisCommand cmd = left ? kRedisCmdLPush : kRedisCmdRPush;
  WriteBatchLogData log_data(kRedisList, {std::to_string(cmd)});
  auto batch = storage_->GetWriteBatchBase();
  // Q: 为什么 PutLogData 只写了命令名（如 LPUSH），却没有把 elems（即 a b c 等待插入的元素）写进去？
  // A: Kvrocks 的 LogData 就像一个元信息 header ，只是作为额外的“标记”或“元信息”用于区分 Redis 命令类型，方便持久化日志分析、恢复或审计。
  //    kv 数据是在 WriteBatch 中写入的，LogData 会随 batch 一起写入 WAL（Write-Ahead Log），
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  // 读取元数据，
  ListMetadata metadata;
  s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !(create_if_missing && s.IsNotFound())) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  // 新插入元素的下标
  uint64_t index = left ? metadata.head - 1 : metadata.tail;

  for (const auto &elem : elems) {
    std::string index_buf;
    // 将 index 编码成 8 字节的字符串，作为 subkey
    PutFixed64(&index_buf, index);
    // 构造 list 子元素 elem 的 internal key
    std::string sub_key = InternalKey(ns_key, index_buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    // 以 key-value 格式存入 rocksdb
    s = batch->Put(sub_key, elem);
    if (!s.ok()) return s;
    // 根据是 LPUSH 还是 RPUSH，更新 index
    left ? --index : ++index;
  }

  // 更新 List 的元数据（head、tail、size、version 等）
  if (left) {
    metadata.head -= elems.size();
  } else {
    metadata.tail += elems.size();
  }
  metadata.size += elems.size();
  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;
  // 返回 list 新长度
  *new_size = metadata.size;
  // 提交整个 batch ，其中包含 LogData、elems、meta
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status List::Pop(engine::Context &ctx, const Slice &user_key, bool left, std::string *elem) {
  elem->clear();

  std::vector<std::string> elems;
  auto s = PopMulti(ctx, user_key, left, 1, &elems);
  if (!s.ok()) return s;

  *elem = std::move(elems[0]);
  return rocksdb::Status::OK();
}

rocksdb::Status List::PopMulti(engine::Context &ctx,
                               const rocksdb::Slice &user_key,
                               bool left,
                               uint32_t count,
                               std::vector<std::string> *elems) {
  elems->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);

  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  auto batch = storage_->GetWriteBatchBase();
  RedisCommand cmd = left ? kRedisCmdLPop : kRedisCmdRPop;
  WriteBatchLogData log_data(kRedisList, {std::to_string(cmd)});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  while (metadata.size > 0 && count > 0) {
    uint64_t index = left ? metadata.head : metadata.tail - 1;
    std::string buf;
    PutFixed64(&buf, index);
    std::string sub_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    std::string elem;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &elem);
    if (!s.ok()) {
      // FIXME: should be always exists??
      return s;
    }

    elems->push_back(elem);
    s = batch->Delete(sub_key);
    if (!s.ok()) return s;
    metadata.size -= 1;
    left ? ++metadata.head : --metadata.tail;
    --count;
  }

  if (metadata.size == 0) {
    s = batch->Delete(metadata_cf_handle_, ns_key);
    if (!s.ok()) return s;
  } else {
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

/*
 * LRem would remove which value is equal to elem, and count limit the remove number and direction
 * Caution: The LRem timing complexity is O(N), don't use it on a long list
 * The simplified description of LRem Algorithm follows those steps:
 * 1. find out all the index of elems to delete
 * 2. determine to move the remain elems from the left or right by the length of moving elems
 * 3. move the remain elems with overlay
 * 4. trim and delete
 * For example: lrem list hello 0
 * when the list was like this:
 * | E1 | E2 | E3 | hello | E4 | E5 | hello | E6 |
 * the index of elems to delete is [3, 6], left part size is 6 and right part size is 4,
 * so move elems from right to left:
 * => | E1 | E2 | E3 | E4 | E4 | E5 | hello | E6 |
 * => | E1 | E2 | E3 | E4 | E5 | E5 | hello | E6 |
 * => | E1 | E2 | E3 | E4 | E5 | E6 | hello | E6 |
 * then trim the list from tail with num of elems to delete, here is 2.
 * and list would become: | E1 | E2 | E3 | E4 | E5 | E6 |
 */
//
// LREM key count value
//
// 功能：
//  - 删除 list 中和 value 相等的元素。
// 参数说明：
//  count ：
//    - > 0：从 头部 开始，最多删除 count 个；
//    - < 0：从 尾部 开始，最多删除 count 个；
//    - = 0：删除所有匹配项。
// 执行过程：
//  - 找出所有要删除的元素下标；
//  - 根据左右剩余元素的数量，决定“从左移”还是“从右移”；
//  - 执行“覆盖式移动”：用剩下的元素覆盖被删元素；
//  - 更新 metadata（head/tail/size），并删除尾部无效数据。
//
// 举例说明：
//  - 原列表: E1 | E2 | E3 | hello | E4 | E5 | hello | E6
//  - 删除 hello 后:
//    1. 找到要删除的索引 [3,6]
//    2. 右侧元素较少(4个)，选择从右向左移动
//    3. 逐步移动 E4,E5,E6 覆盖 hello 的位置
//    4. 最终列表: E1 | E2 | E3 | E4 | E5 | E6
//
//
// 删除整个 List 是通过删除其元数据（metadata）来实现的
//  - Kvrocks 使用元数据（metadata）+ 多个子 key（subkey） 结构
//  - 删除 metadata 相当于让这个 list 在逻辑上“失效”，这些元素键虽然物理存在，但逻辑上已不可访问
//  - 后续对该 list 的操作（如 LPUSH, LRANGE）会自动使用新的 version(+1)；
//  - 老版本的子 key 即使还存在，也不会被访问；
//  - 后台 compact 时会清理这些无效数据。
rocksdb::Status List::Rem(engine::Context &ctx, const Slice &user_key, int count, const Slice &elem, uint64_t *removed_cnt) {
  *removed_cnt = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  uint64_t index = count >= 0 ? metadata.head : metadata.tail - 1;
  std::string buf;
  PutFixed64(&buf, index);
  std::string start_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  bool reversed = count < 0;
  std::vector<uint64_t> to_delete_indexes;
  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  // 遍历列表查找所有值为 elem 的元素，将下标记录到 to_delete_indexes 中，最多删除 count 个元素；
  auto iter = util::UniqueIterator(ctx, read_options);
  for (iter->Seek(start_key); iter->Valid() && iter->key().starts_with(prefix);!reversed ? iter->Next() : iter->Prev()) {
    if (iter->value() == elem) {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      Slice sub_key = ikey.GetSubKey();
      GetFixed64(&sub_key, &index);
      to_delete_indexes.emplace_back(index);
      if (static_cast<int>(to_delete_indexes.size()) == abs(count)) break;
    }
  }
  if (to_delete_indexes.empty()) {
    return rocksdb::Status::NotFound();
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisList, {std::to_string(kRedisCmdLRem), std::to_string(count), elem.ToString()});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  // 当要删除的元素数量等于列表大小时，直接删除整个列表
  if (to_delete_indexes.size() == metadata.size) {
    s = batch->Delete(metadata_cf_handle_, ns_key);
    if (!s.ok()) return s;
  } else {
    // 确定最优移动方向
    uint64_t min_to_delete_index = !reversed ? to_delete_indexes[0] : to_delete_indexes[to_delete_indexes.size() - 1];
    uint64_t max_to_delete_index = !reversed ? to_delete_indexes[to_delete_indexes.size() - 1] : to_delete_indexes[0];
    uint64_t left_part_len = max_to_delete_index - metadata.head;
    uint64_t right_part_len = metadata.tail - 1 - min_to_delete_index;
    reversed = left_part_len <= right_part_len;
    buf.clear();
    PutFixed64(&buf, reversed ? max_to_delete_index : min_to_delete_index);
    start_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    size_t processed = 0;
    // 移动非删除元素
    for (iter->Seek(start_key); iter->Valid() && iter->key().starts_with(prefix); !reversed ? iter->Next() : iter->Prev()) {
      if (iter->value() != elem || processed >= to_delete_indexes.size()) {
        // 将保留的元素移动到删除元素的位置上，实现元素的"覆盖"操作
        buf.clear();
        PutFixed64(&buf, reversed ? max_to_delete_index-- : min_to_delete_index++);
        std::string to_update_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
        s = batch->Put(to_update_key, iter->value());
        if (!s.ok()) return s;
      } else {
        processed++;
      }
    }

    // 删除多余元素
    for (uint64_t idx = 0; idx < to_delete_indexes.size(); ++idx) {
      buf.clear();
      PutFixed64(&buf, reversed ? (metadata.head + idx) : (metadata.tail - 1 - idx));
      std::string to_delete_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
      s = batch->Delete(to_delete_key);
      if (!s.ok()) return s;
    }

    // 更新元数据
    if (reversed) {
      metadata.head += to_delete_indexes.size();
    } else {
      metadata.tail -= to_delete_indexes.size();
    }
    metadata.size -= to_delete_indexes.size();
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }

  *removed_cnt = to_delete_indexes.size();
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}


// LINSERT mylist BEFORE|AFTER pivot value
//
// 插入过程：找到 pivot 所在位置，并移动其后（或前面）的元素为新元素腾出空间，插入该新元素，同时更新 metadata 和写入 RocksDB 。
rocksdb::Status List::Insert(engine::Context &ctx,
                             const Slice &user_key,  // 用户传入的 key
                             const Slice &pivot,     // 要插入位置参考的 pivot 元素
                             const Slice &elem,      // 要插入的新元素
                             bool before,            // 插入在前还是后
                             int *new_size) {        // 返回新列表长度（失败返回 -1）
  *new_size = 0;

  // 获取列表元数据（head, tail, size, version 等）
  std::string ns_key = AppendNamespacePrefix(user_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  std::string buf;
  uint64_t pivot_index = metadata.head - 1;  // 初始化 pivot 为 head-1
  PutFixed64(&buf, metadata.head); // 将 metadata.head 编码为 8 字节二进制存入 buf
  // 起始键，指向列表的第一个元素，包含命名空间、精确索引、版本号；通过起始键可以直接跳转到列表头部，避免全表扫描
  std::string start_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  // 前缀键，匹配当前版本下列表的所有元素键
  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  // 下一版本前缀键，作为迭代边界（iterate_upper_bound），防止跨版本扫描；RocksDB 迭代器需要显式指定范围，通过设置 upper_bound 为下一版本的起始键，确保只扫描当前版本的数据。
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();
  // 从 start_key 开始遍历列表
  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;
  auto iter = util::UniqueIterator(ctx, read_options);
  for (iter->Seek(start_key); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    // 找到目标 pivot 元素
    if (iter->value() == pivot) {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      Slice sub_key = ikey.GetSubKey();
      GetFixed64(&sub_key, &pivot_index);
      break;
    }
  }
  // 如果没找到，返回 NotFound
  if (pivot_index == (metadata.head - 1)) {
    *new_size = -1;
    return rocksdb::Status::NotFound();
  }

  // 准备写 rocksdb
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisList,{std::to_string(kRedisCmdLInsert), before ? "1" : "0", pivot.ToString(), elem.ToString()});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  // Kvrocks 中 List 在插入元素时，必须把相关元素向后或向前移动，为新元素让出下标位置。
  // 为了尽可能减少要 “搬移” 的元素数，要从两个方向计算需要移动的元素数目，选择移动更少的方案，减少需要更新的键值对数量。
  //
  // left_part_len: 计算 pivot 左侧(头部方向)的元素数量
  //  - 如果插入在 pivot 前(before=true)，则不包含 pivot 本身
  //  - 如果插入在 pivot 后(before=false)，则包含 pivot
  // right_part_len: 计算 pivot 右侧(尾部方向)的元素数量
  //  - 如果插入在 pivot 前(before=true)，则包含 pivot 本身
  //  - 如果插入在 pivot 后(before=false)，则不包含 pivot
  uint64_t left_part_len = pivot_index - metadata.head + (before ? 0 : 1);
  uint64_t right_part_len = metadata.tail - 1 - pivot_index + (before ? 1 : 0);
  // true  表示选择"向左移动元素"(从头部方向操作)
  // false 表示选择"向右移动元素"(从尾部方向操作)
  bool reversed = left_part_len <= right_part_len;
  // 当移动方向与插入方向一致时(从左侧操作且插入基准后 或者 从右侧操作且插入基准前)，直接使用 pivot 的位置，否则基于 pivot_index 做调整得到新元素位置
  uint64_t new_elem_index = 0;
  if ((reversed && !before) || (!reversed && before)) {
    new_elem_index = pivot_index;
  } else {
    new_elem_index = reversed ? --pivot_index : ++pivot_index;
    !reversed ? iter->Next() : iter->Prev(); // 因为 pivot 需要被移动，调整 iter
  }

  // 遍历需要移动的元素，把每个旧 key 的值重新写到新的 key（index ±1），所有元素整体前(后)移一格
  for (; iter->Valid() && iter->key().starts_with(prefix); !reversed ? iter->Next() : iter->Prev()) {
    buf.clear();
    PutFixed64(&buf, reversed ? --pivot_index : ++pivot_index);
    std::string to_update_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = batch->Put(to_update_key, iter->value());
    if (!s.ok()) return s;
  }

  // 插入新元素
  buf.clear();
  PutFixed64(&buf, new_elem_index);
  std::string to_update_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = batch->Put(to_update_key, elem);
  if (!s.ok()) return s;

  // 更新 meta ，包括 head(tail)、size
  if (reversed) {
    metadata.head--;
  } else {
    metadata.tail++;
  }
  metadata.size++;
  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;

  // 返回最新列表长度
  *new_size = static_cast<int>(metadata.size);
  // 写入 rocksdb
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status List::Index(engine::Context &ctx, const Slice &user_key, int index, std::string *elem) {
  elem->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  if (index < 0) index += static_cast<int>(metadata.size);
  if (index < 0 || index >= static_cast<int>(metadata.size)) return rocksdb::Status::NotFound();

  std::string buf;
  PutFixed64(&buf, metadata.head + index);
  std::string sub_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return storage_->Get(ctx, ctx.GetReadOptions(), sub_key, elem);
}

// The offset can also be negative, -1 is the last element, -2 the penultimate
// Out of range indexes will not produce an error.
// If start is larger than the end of the list, an empty list is returned.
// If stop is larger than the actual end of the list,
// Redis will treat it like the last element of the list.
rocksdb::Status List::Range(engine::Context &ctx, const Slice &user_key, int start, int stop,
                            std::vector<std::string> *elems) {
  elems->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  if (start < 0) start = static_cast<int>(metadata.size) + start;
  if (stop < 0) stop = static_cast<int>(metadata.size) + stop;
  if (start > static_cast<int>(metadata.size) || stop < 0 || start > stop) return rocksdb::Status::OK();
  if (start < 0) start = 0;

  std::string buf;
  PutFixed64(&buf, metadata.head + start);
  std::string start_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  for (iter->Seek(start_key); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice sub_key = ikey.GetSubKey();
    uint64_t index = 0;
    GetFixed64(&sub_key, &index);
    // index should be always >= start
    if (index > metadata.head + stop) break;
    elems->push_back(iter->value().ToString());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status List::Pos(engine::Context &ctx, const Slice &user_key, const Slice &elem, const PosSpec &spec,std::vector<int64_t> *indexes) {
  indexes->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  // A negative rank means start from the tail.
  int64_t rank = spec.rank;
  uint64_t start = metadata.head;
  bool reversed = false;
  if (rank < 0) {
    rank = -rank;
    start = metadata.tail - 1;
    reversed = true;
  }

  std::string buf;
  PutFixed64(&buf, start);
  std::string start_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  auto list_len = static_cast<int64_t>(metadata.size);
  int64_t max_len = spec.max_len;
  int64_t count = spec.count.value_or(-1);
  int64_t offset = 0, matches = 0;

  auto iter = util::UniqueIterator(ctx, read_options);
  iter->Seek(start_key);
  while (iter->Valid() && iter->key().starts_with(prefix) && (max_len == 0 || offset < max_len)) {
    if (iter->value() == elem) {
      matches++;
      if (matches >= rank) {
        int64_t pos = !reversed ? offset : list_len - offset - 1;
        indexes->push_back(pos);
        if (count != 0 && matches - rank + 1 >= count) {
          break;
        }
      }
    }
    offset++;
    !reversed ? iter->Next() : iter->Prev();
  }
  return rocksdb::Status::OK();
}

rocksdb::Status List::Set(engine::Context &ctx, const Slice &user_key, int index, Slice elem) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;
  if (index < 0) index += static_cast<int>(metadata.size);
  if (index < 0 || index >= static_cast<int>(metadata.size)) {
    return rocksdb::Status::InvalidArgument("index out of range");
  }

  std::string buf, value;
  PutFixed64(&buf, metadata.head + index);
  std::string sub_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value);
  if (!s.ok()) {
    return s;
  }
  if (value == elem) return rocksdb::Status::OK();

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisList, {std::to_string(kRedisCmdLSet), std::to_string(index)});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  s = batch->Put(sub_key, elem);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status List::LMove(engine::Context &ctx, const rocksdb::Slice &src, const rocksdb::Slice &dst, bool src_left,
                            bool dst_left, std::string *elem) {
  if (src == dst) {
    return lmoveOnSingleList(ctx, src, src_left, dst_left, elem);
  }
  return lmoveOnTwoLists(ctx, src, dst, src_left, dst_left, elem);
}

rocksdb::Status List::lmoveOnSingleList(engine::Context &ctx, const rocksdb::Slice &src, bool src_left, bool dst_left,
                                        std::string *elem) {
  std::string ns_key = AppendNamespacePrefix(src);

  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }

  elem->clear();

  uint64_t curr_index = src_left ? metadata.head : metadata.tail - 1;
  std::string curr_index_buf;
  PutFixed64(&curr_index_buf, curr_index);
  std::string curr_sub_key =
      InternalKey(ns_key, curr_index_buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = storage_->Get(ctx, ctx.GetReadOptions(), curr_sub_key, elem);
  if (!s.ok()) {
    return s;
  }

  if (src_left == dst_left) {
    // no-op
    return rocksdb::Status::OK();
  }

  if (metadata.size == 1) {
    // if there is only one element in the list - do nothing, just get it
    return rocksdb::Status::OK();
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisList, {std::to_string(kRedisCmdLMove), src.ToString(), src.ToString(),
                                          src_left ? "left" : "right", dst_left ? "left" : "right"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  s = batch->Delete(curr_sub_key);
  if (!s.ok()) return s;

  if (src_left) {
    ++metadata.head;
    ++metadata.tail;
  } else {
    --metadata.head;
    --metadata.tail;
  }

  uint64_t new_index = src_left ? metadata.tail - 1 : metadata.head;
  std::string new_index_buf;
  PutFixed64(&new_index_buf, new_index);
  std::string new_sub_key = InternalKey(ns_key, new_index_buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = batch->Put(new_sub_key, *elem);
  if (!s.ok()) return s;

  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status List::lmoveOnTwoLists(engine::Context &ctx, const rocksdb::Slice &src, const rocksdb::Slice &dst,
                                      bool src_left, bool dst_left, std::string *elem) {
  std::string src_ns_key = AppendNamespacePrefix(src);
  std::string dst_ns_key = AppendNamespacePrefix(dst);

  ListMetadata src_metadata(false);
  auto s = GetMetadata(ctx, src_ns_key, &src_metadata);
  if (!s.ok()) {
    return s;
  }

  ListMetadata dst_metadata(false);
  s = GetMetadata(ctx, dst_ns_key, &dst_metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  elem->clear();

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisList, {std::to_string(kRedisCmdLMove), src.ToString(), dst.ToString(),
                                          src_left ? "left" : "right", dst_left ? "left" : "right"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  uint64_t src_index = src_left ? src_metadata.head : src_metadata.tail - 1;
  std::string src_buf;
  PutFixed64(&src_buf, src_index);
  std::string src_sub_key =
      InternalKey(src_ns_key, src_buf, src_metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = storage_->Get(ctx, ctx.GetReadOptions(), src_sub_key, elem);
  if (!s.ok()) {
    return s;
  }

  s = batch->Delete(src_sub_key);
  if (!s.ok()) return s;
  if (src_metadata.size == 1) {
    s = batch->Delete(metadata_cf_handle_, src_ns_key);
    if (!s.ok()) return s;
  } else {
    std::string bytes;
    src_metadata.size -= 1;
    src_left ? ++src_metadata.head : --src_metadata.tail;
    src_metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, src_ns_key, bytes);
    if (!s.ok()) return s;
  }

  uint64_t dst_index = dst_left ? dst_metadata.head - 1 : dst_metadata.tail;
  std::string dst_buf;
  PutFixed64(&dst_buf, dst_index);
  std::string dst_sub_key =
      InternalKey(dst_ns_key, dst_buf, dst_metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = batch->Put(dst_sub_key, *elem);
  if (!s.ok()) return s;
  dst_left ? --dst_metadata.head : ++dst_metadata.tail;

  std::string bytes;
  dst_metadata.size += 1;
  dst_metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, dst_ns_key, bytes);
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

// Caution: trim the big list may block the server
rocksdb::Status List::Trim(engine::Context &ctx, const Slice &user_key, int start, int stop) {
  uint32_t trim_cnt = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  // 获取元数据
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  // 处理边界：Redis 支持负数索引（例如 -1 表示最后一个元素）
  if (start < 0) start += static_cast<int>(metadata.size);
  if (stop < 0) stop = static_cast<int>(metadata.size) >= -1 * stop ? static_cast<int>(metadata.size) + stop : -1;

  // the result will be empty list when start > stop, or start is larger than the end of list
  // 当 start > stop 时，表示无效范围，直接删除整个列表
  if (start > stop) {
    return storage_->Delete(ctx, storage_->DefaultWriteOptions(), metadata_cf_handle_, ns_key);
  }
  // 确保 start 不小于 0
  if (start < 0) start = 0;

  auto batch = storage_->GetWriteBatchBase();
  // 记录操作日志
  WriteBatchLogData log_data(kRedisList, std::vector<std::string>{std::to_string(kRedisCmdLTrim), std::to_string(start), std::to_string(stop)});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  // 删除左侧元素：从 head 到 start 前的元素全部删除
  uint64_t left_index = metadata.head + start;
  uint64_t right_index = metadata.head + stop + 1;
  for (uint64_t i = metadata.head; i < left_index; i++) {
    std::string buf;
    PutFixed64(&buf, i);
    std::string sub_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = batch->Delete(sub_key);
    if (!s.ok()) return s;
    metadata.head++;
    trim_cnt++;
  }
  // 删除右侧元素：从 stop 后到 tail 的元素全部删除
  auto tail = metadata.tail;
  for (uint64_t i = right_index; i < tail; i++) {
    std::string buf;
    PutFixed64(&buf, i);
    std::string sub_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = batch->Delete(sub_key);
    if (!s.ok()) return s;
    metadata.tail--;
    trim_cnt++;
  }

  // 更新元数据
  if (metadata.size >= trim_cnt) {
    metadata.size -= trim_cnt;
  } else {
    metadata.size = 0;
  }
  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}
}  // namespace redis
