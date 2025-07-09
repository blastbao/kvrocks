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

#include <encoding.h>
#include <storage/redis_metadata.h>

#include <memory>

namespace redis {

enum class IndexOnDataType : uint8_t {
  HASH = kRedisHash,
  JSON = kRedisJson,
};

inline constexpr auto kErrorInsufficientLength = "insufficient length while decoding metadata";
inline constexpr auto kErrorIncorrectLength = "length is too short or too long to be parsed as a vector";

class IndexMetadata {
 public:
  uint8_t flag = 0;                 // 标记位，当前未使用 // all reserved
  IndexOnDataType on_data_type;     // 数据类型

  const std::string &OnDataTypeName() const { return RedisTypeNames[(size_t)on_data_type]; }

  void Encode(std::string *dst) const {
    PutFixed8(dst, flag);
    PutFixed8(dst, uint8_t(on_data_type));
  }

  rocksdb::Status Decode(Slice *input) {
    if (!GetFixed8(input, &flag)) {
      return rocksdb::Status::InvalidArgument(kErrorInsufficientLength);
    }

    if (!GetFixed8(input, reinterpret_cast<uint8_t *>(&on_data_type))) {
      return rocksdb::Status::InvalidArgument(kErrorInsufficientLength);
    }

    return rocksdb::Status::OK();
  }
};

enum class SearchSubkeyType : uint8_t {
  INDEX_META = 0, // 索引元信息

  PREFIXES = 1,

  // field metadata
  FIELD_META = 2, // 字段元信息

  // field indexing data
  FIELD = 3,  // 字段索引数据

  // field alias
  FIELD_ALIAS = 4,
};

enum class IndexFieldType : uint8_t {
  TAG = 1,      // 0b0001

  NUMERIC = 2,  // 0b0010

  VECTOR = 3,   // 0b0011
};

enum class VectorType : uint8_t {
  FLOAT64 = 1,
};

inline const char *VectorTypeToString(VectorType type) {
  switch (type) {
    case VectorType::FLOAT64:
      return "FLOAT64";
  }

  return "unknown";
}

enum class DistanceMetric : uint8_t {
  L2 = 0,
  IP = 1,
  COSINE = 2,
};

inline const char *DistanceMetricToString(DistanceMetric dm) {
  switch (dm) {
    case DistanceMetric::L2:
      return "L2";
    case DistanceMetric::IP:
      return "IP";
    case DistanceMetric::COSINE:
      return "COSINE";
  }

  return "unknown";
}

enum class HnswLevelType : uint8_t {
  NODE = 1,
  EDGE = 2,
};

struct SearchKey {
  std::string_view ns;      // 命名空间
  std::string_view index;   // 索引名
  std::string_view field;   // 字段名

  SearchKey(std::string_view ns, std::string_view index) : ns(ns), index(index) {}
  SearchKey(std::string_view ns, std::string_view index, std::string_view field) : ns(ns), index(index), field(field) {}

  void PutNamespace(std::string *dst) const {
    PutFixed8(dst, ns.size());
    dst->append(ns);
  }

  static void PutType(std::string *dst, SearchSubkeyType type) { PutFixed8(dst, uint8_t(type)); }

  void PutIndex(std::string *dst) const { PutSizedString(dst, index); }

  static void PutHnswLevelType(std::string *dst, HnswLevelType type) { PutFixed8(dst, uint8_t(type)); }

  // <namespace_len><namespace>
  // <type: SearchSubkeyType>
  // <index_len><index>
  // <field_len><field>
  // <level>
  void PutHnswLevelPrefix(std::string *dst, uint16_t level) const {
    PutNamespace(dst);
    PutType(dst, SearchSubkeyType::FIELD);
    PutIndex(dst);
    PutSizedString(dst, field);
    PutFixed16(dst, level);
  }

  void PutHnswLevelNodePrefix(std::string *dst, uint16_t level) const {
    PutHnswLevelPrefix(dst, level);
    PutHnswLevelType(dst, HnswLevelType::NODE);
  }

  void PutHnswLevelEdgePrefix(std::string *dst, uint16_t level) const {
    PutHnswLevelPrefix(dst, level);
    PutHnswLevelType(dst, HnswLevelType::EDGE);
  }



  // 构造索引元数据：ns + type(INDEX_META) + index
  std::string ConstructIndexMeta() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::INDEX_META);
    PutIndex(&dst);
    return dst;
  }

  // 构造索引前缀：ns + type(PREFIXES) + index
  std::string ConstructIndexPrefixes() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::PREFIXES);
    PutIndex(&dst);
    return dst;
  }

  // 构造某个字段的元信息：ns + type(FIELD_META) + index
  std::string ConstructFieldMeta() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD_META);
    PutIndex(&dst);
    PutSizedString(&dst, field);
    return dst;
  }

  std::string ConstructAllFieldMetaBegin() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD_META);
    PutIndex(&dst);
    PutFixed32(&dst, 0);
    return dst;
  }

  std::string ConstructAllFieldMetaEnd() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD_META);
    PutIndex(&dst);
    PutFixed32(&dst, (uint32_t)(-1));
    return dst;
  }

  std::string ConstructAllFieldDataBegin() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD);
    PutIndex(&dst);
    PutFixed32(&dst, 0);
    return dst;
  }

  std::string ConstructAllFieldDataEnd() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD);
    PutIndex(&dst);
    PutFixed32(&dst, (uint32_t)(-1));
    return dst;
  }

  // ns + type + index + field + tag(string) + key
  std::string ConstructTagFieldData(std::string_view tag, std::string_view key) const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD);
    PutIndex(&dst);
    PutSizedString(&dst, field);
    PutSizedString(&dst, tag);
    PutSizedString(&dst, key);
    return dst;
  }

  // ns + type + index + field + num(double) + key
  std::string ConstructNumericFieldData(double num, std::string_view key) const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD);
    PutIndex(&dst);
    PutSizedString(&dst, field);
    PutDouble(&dst, num);
    PutSizedString(&dst, key);
    return dst;
  }

  std::string ConstructHnswLevelNodePrefix(uint16_t level) const {
    std::string dst;
    PutHnswLevelNodePrefix(&dst, level);
    return dst;
  }

  std::string ConstructHnswNode(uint16_t level, std::string_view key) const {
    std::string dst;
    PutHnswLevelNodePrefix(&dst, level);
    PutSizedString(&dst, key);
    return dst;
  }

  std::string ConstructHnswEdgeWithSingleEnd(uint16_t level, std::string_view key) const {
    std::string dst;
    PutHnswLevelEdgePrefix(&dst, level);
    PutSizedString(&dst, key);
    return dst;
  }

  std::string ConstructHnswEdge(uint16_t level, std::string_view key1, std::string_view key2) const {
    std::string dst;
    PutHnswLevelEdgePrefix(&dst, level);
    PutSizedString(&dst, key1);
    PutSizedString(&dst, key2);
    return dst;
  }
};

struct IndexPrefixes {
  std::vector<std::string> prefixes;

  static inline const std::string all[] = {""};

  auto begin() const {  // NOLINT
    return prefixes.empty() ? std::begin(all) : prefixes.data();
  }

  auto end() const {  // NOLINT
    return prefixes.empty() ? std::end(all) : prefixes.data() + prefixes.size();
  }

  void Encode(std::string *dst) const {
    for (const auto &prefix : prefixes) {
      PutFixed32(dst, prefix.size());
      dst->append(prefix);
    }
  }

  rocksdb::Status Decode(Slice *input) {
    uint32_t size = 0;

    while (GetFixed32(input, &size)) {
      if (input->size() < size) return rocksdb::Status::Corruption(kErrorInsufficientLength);
      prefixes.emplace_back(input->data(), size);
      input->remove_prefix(size);
    }

    return rocksdb::Status::OK();
  }
};

struct IndexFieldMetadata {
  bool noindex = false;
  IndexFieldType type;

  explicit IndexFieldMetadata(IndexFieldType type) : type(type) {}

  // 构造 flag ：最低 1 位为 noindex ，次低 4 位为 type ，高 3 位保留
  //
  // flag: <noindex: 1 bit> <type: 4 bit> <reserved: 3 bit>
  uint8_t MakeFlag() const { return noindex | (uint8_t)type << 1; }

  void DecodeFlag(uint8_t flag) {
    noindex = flag & 1;       // 取最低位
    type = DecodeType(flag);  // 取次 4 位
  }

  static IndexFieldType DecodeType(uint8_t flag) { return IndexFieldType(flag >> 1); }

  virtual ~IndexFieldMetadata() = default;

  std::string_view Type() const {
    switch (type) {
      case IndexFieldType::TAG:
        return "tag";
      case IndexFieldType::NUMERIC:
        return "numeric";
      case IndexFieldType::VECTOR:
        return "vector";
      default:
        return "unknown";
    }
  }

  virtual void Encode(std::string *dst) const { PutFixed8(dst, MakeFlag()); }

  virtual rocksdb::Status Decode(Slice *input) {
    uint8_t flag = 0;
    if (!GetFixed8(input, &flag)) {
      return rocksdb::Status::Corruption(kErrorInsufficientLength);
    }

    DecodeFlag(flag);
    return rocksdb::Status::OK();
  }

  virtual bool IsSortable() const { return false; }

  static inline rocksdb::Status Decode(Slice *input, std::unique_ptr<IndexFieldMetadata> &ptr);
};

struct TagFieldMetadata : IndexFieldMetadata {
  char separator = ',';
  bool case_sensitive = false;

  TagFieldMetadata() : IndexFieldMetadata(IndexFieldType::TAG) {}

  void Encode(std::string *dst) const override {
    IndexFieldMetadata::Encode(dst);
    PutFixed8(dst, separator);
    PutFixed8(dst, case_sensitive);
  }

  rocksdb::Status Decode(Slice *input) override {
    if (auto s = IndexFieldMetadata::Decode(input); !s.ok()) {
      return s;
    }

    if (input->size() < 2) {
      return rocksdb::Status::Corruption(kErrorInsufficientLength);
    }

    GetFixed8(input, (uint8_t *)&separator);
    GetFixed8(input, (uint8_t *)&case_sensitive);
    return rocksdb::Status::OK();
  }
};

struct NumericFieldMetadata : IndexFieldMetadata {
  NumericFieldMetadata() : IndexFieldMetadata(IndexFieldType::NUMERIC) {}

  bool IsSortable() const override { return true; }
};

struct HnswVectorFieldMetadata : IndexFieldMetadata {
  VectorType vector_type;          // 向量类型，如 float32、float64 等
  uint16_t dim;                    // 向量维度
  DistanceMetric distance_metric;  // 距离指标

  uint32_t initial_cap = 500000;   // 初始向量容量       // Initial vector capacity
  uint16_t m = 16;                 // 最大出边数         // Max allowed outgoing edges per node
  uint32_t ef_construction = 200;  // 最大的潜在出边数    // Max potential outgoing edge candidates during construction
  uint32_t ef_runtime = 10;        // 候选集大小         // Max top candidates held during KNN search
  double epsilon = 0.01;           // 相关因子           // Relative factor setting search boundaries in range queries
  uint16_t num_levels = 0;         // 最大层数           // Number of levels in the HNSW graph

  HnswVectorFieldMetadata() : IndexFieldMetadata(IndexFieldType::VECTOR) {}

  bool IsSortable() const override { return true; }

  void Encode(std::string *dst) const override {
    IndexFieldMetadata::Encode(dst);
    PutFixed8(dst, uint8_t(vector_type));
    PutFixed16(dst, dim);
    PutFixed8(dst, uint8_t(distance_metric));
    PutFixed32(dst, initial_cap);
    PutFixed16(dst, m);
    PutFixed32(dst, ef_construction);
    PutFixed32(dst, ef_runtime);
    PutDouble(dst, epsilon);
    PutFixed16(dst, num_levels);
  }

  rocksdb::Status Decode(Slice *input) override {
    if (auto s = IndexFieldMetadata::Decode(input); !s.ok()) {
      return s;
    }

    constexpr size_t required_size = sizeof(uint8_t) + sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint32_t) +
                                     sizeof(uint16_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t) +
                                     sizeof(uint16_t);

    if (input->size() < required_size) {
      return rocksdb::Status::Corruption(kErrorInsufficientLength);
    }

    GetFixed8(input, (uint8_t *)(&vector_type));
    GetFixed16(input, &dim);
    GetFixed8(input, (uint8_t *)(&distance_metric));
    GetFixed32(input, &initial_cap);
    GetFixed16(input, &m);
    GetFixed32(input, &ef_construction);
    GetFixed32(input, &ef_runtime);
    GetDouble(input, &epsilon);
    GetFixed16(input, &num_levels);
    return rocksdb::Status::OK();
  }
};

struct HnswNodeFieldMetadata {
  uint16_t num_neighbours;      // 当前节点在某一层中的邻居数量。用于维护图的稀疏度，边删除时也会更新。
  std::vector<double> vector;   // 节点向量，参与向量相似度计算。

  HnswNodeFieldMetadata() = default;
  HnswNodeFieldMetadata(uint16_t num_neighbours, std::vector<double> vector)
      : num_neighbours(num_neighbours), vector(std::move(vector)) {}


  // example:
  //  [num_neighbours:2B][dim:2B][v[0]:8B][v[1]:8B][v[2]:8B]...
  void Encode(std::string *dst) const {
    // 写入邻居数目，2B
    PutFixed16(dst, num_neighbours);
    // 写入向量长度，2B
    PutFixed16(dst, static_cast<uint16_t>(vector.size()));
    // 依次写入 vector 中的元素，每个元素是个 double (8B)
    for (double element : vector) {
      PutDouble(dst, element);
    }
  }

  rocksdb::Status Decode(Slice *input) {
    // 至少包含 4B
    if (input->size() < 2 + 2) {
      return rocksdb::Status::Corruption(kErrorInsufficientLength);
    }

    // 读取 num_neighbours
    GetFixed16(input, (uint16_t *)(&num_neighbours));
    // 读取 dim
    uint16_t dim = 0;
    GetFixed16(input, (uint16_t *)(&dim));
    // 检查剩下字节数是否等于 dim * sizeof(double)
    if (input->size() != dim * sizeof(double)) {
      return rocksdb::Status::Corruption(kErrorIncorrectLength);
    }

    //  逐个读取 double 值填入 vector 数组
    vector.resize(dim);
    for (auto i = 0; i < dim; ++i) {
      GetDouble(input, &vector[i]);
    }
    return rocksdb::Status::OK();
  }
};

inline rocksdb::Status IndexFieldMetadata::Decode(Slice *input, std::unique_ptr<IndexFieldMetadata> &ptr) {
  // 数据长度不足
  if (input->size() < 1) {
    return rocksdb::Status::Corruption(kErrorInsufficientLength);
  }

  // 首字节为字段类型
  switch (DecodeType((*input)[0])) {
    case IndexFieldType::TAG:
      ptr = std::make_unique<TagFieldMetadata>();
      break;
    case IndexFieldType::NUMERIC:
      ptr = std::make_unique<NumericFieldMetadata>();
      break;
    case IndexFieldType::VECTOR:
      ptr = std::make_unique<HnswVectorFieldMetadata>();
      break;
    default:
      return rocksdb::Status::Corruption("encountered unknown field type");
  }

  // 调用实际类对象的 Decode() 进行解析
  return ptr->Decode(input);

}

}  // namespace redis
