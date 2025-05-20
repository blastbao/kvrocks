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

#include "cluster.h"

#include <config/config_util.h>

#include <array>
#include <cstring>
#include <fstream>
#include <memory>
#include <vector>

#include "cluster/cluster_defs.h"
#include "commands/commander.h"
#include "common/io_util.h"
#include "fmt/format.h"
#include "parse_util.h"
#include "replication.h"
#include "server/server.h"
#include "string_util.h"
#include "time_util.h"

ClusterNode::ClusterNode(std::string id, std::string host, int port, int role, std::string master_id,
                         const std::bitset<kClusterSlots> &slots)
    : id(std::move(id)), host(std::move(host)), port(port), role(role), master_id(std::move(master_id)), slots(slots) {}

Cluster::Cluster(Server *srv, std::vector<std::string> binds, int port)
    : srv_(srv), binds_(std::move(binds)), port_(port) {
  for (auto &slots_node : slots_nodes_) {
    slots_node = nullptr;
  }
}

// We access cluster without lock, actually we guarantee data-safe by work threads
// ReadWriteLockGuard, CLUSTER command doesn't have 'exclusive' attribute, i.e.
// CLUSTER command can be executed concurrently, but some subcommand may change
// cluster data, so these commands should be executed exclusively, and ReadWriteLock
// also can guarantee accessing data is safe.
bool Cluster::SubCommandIsExecExclusive(const std::string &subcommand) {
  std::array subcommands = {"setnodes", "setnodeid", "setslot", "import", "reset"};

  return std::any_of(std::begin(subcommands),
                     std::end(subcommands),
                     [&subcommand](const std::string &val) { return util::EqualICase(val, subcommand); }
                     );
}

Status Cluster::SetNodeId(const std::string &node_id) {
  if (node_id.size() != kClusterNodeIdLen) {
    return {Status::ClusterInvalidInfo, errInvalidNodeID};
  }

  myid_ = node_id;
  // Already has cluster topology
  if (version_ >= 0 && nodes_.find(node_id) != nodes_.end()) {
    myself_ = nodes_[myid_];
  } else {
    myself_ = nullptr;
  }

  // Set replication relationship
  return SetMasterSlaveRepl();
}

// The reason why the new version MUST be +1 of current version is that,
// the command changes topology based on specific topology (also means specific
// version), we must guarantee current topology is exactly expected, otherwise,
// this update may make topology corrupt, so base topology version is very important.
// This is different with CLUSTERX SETNODES commands because it uses new version
// topology to cover current version, it allows kvrocks nodes lost some topology
// updates since of network failure, it is state instead of operation.
//
// 更新槽(slot)分配关系
Status Cluster::SetSlotRanges(const std::vector<SlotRange> &slot_ranges, const std::string &node_id, int64_t new_version) {
  // 版本号线性递增
  if (new_version <= 0 || new_version != version_ + 1) {
    return {Status::NotOK, errInvalidClusterVersion};
  }

  // 节点 ID 固定长度
  if (node_id.size() != kClusterNodeIdLen) {
    return {Status::NotOK, errInvalidNodeID};
  }

  // Get the node which we want to assign slots into it
  // 目标节点存在
  std::shared_ptr<ClusterNode> to_assign_node = nodes_[node_id];
  if (to_assign_node == nullptr) {
    return {Status::NotOK, "No this node in the cluster"};
  }

  // 目标节点是主节点
  if (to_assign_node->role != kClusterMaster) {
    return {Status::NotOK, errNoMasterNode};
  }

  // Update version
  version_ = new_version;

  // Update topology
  //  1. Remove the slot from old node if existing
  //  2. Add the slot into to-assign node
  //  3. Update the map of slots to nodes.
  // remember: The atomicity of the process is based on
  // the transactionality of ClearKeysOfSlotRange().
  engine::Context ctx(srv_->storage);
  for (auto [s_start, s_end] : slot_ranges) {
    for (int slot = s_start; slot <= s_end; slot++) {
      // 1. 从旧节点移除槽位
      std::shared_ptr<ClusterNode> old_node = slots_nodes_[slot];
      if (old_node != nullptr) {
        old_node->slots[slot] = false;
      }
      // 2. 向新节点添加槽位
      to_assign_node->slots[slot] = true;
      slots_nodes_[slot] = to_assign_node;

      // Clear data of migrated slot or record of imported slot
      // 3. 如果是本节点迁出的 slot，还需要清理旧数据
      if (old_node == myself_ && old_node != to_assign_node) {
        // If slot is migrated from this node
        if (migrated_slots_.count(slot) > 0) {
          // 清理本节点中该 slot 的 key 数据
          auto s = srv_->slot_migrator->ClearKeysOfSlotRange(ctx, kDefaultNamespace, SlotRange::GetPoint(slot));
          if (!s.ok()) {
            error("failed to clear data of migrated slot: {}", s.ToString());
          }
          migrated_slots_.erase(slot);
        }
        // If slot is imported into this node
        // 清除导入记录
        if (imported_slots_.count(slot) > 0) {
          imported_slots_.erase(slot);
        }
      }
    }
  }

  return Status::OK();
}

// cluster setnodes $all_nodes_info $version $force
// one line of $all_nodes: $node_id $host $port $role $master_node_id $slot_range
/**
 * 设置集群节点配置信息
 *
 * @param nodes_str 集群节点信息字符串，格式为多行文本，每行描述一个节点
 * @param version 集群配置版本号，必须大于当前版本(除非force=true)
 * @param force 是否强制更新，即使版本号不递增
 *
 * @return 执行状态，成功返回OK，失败返回错误信息
 *
 * 功能说明：
 * 1. 解析并验证节点信息字符串
 * 2. 更新集群拓扑结构和版本号
 * 3. 建立主从复制关系
 * 4. 清理迁移/导入槽位相关数据
 *
 * 节点信息字符串格式示例：
 * <node_id> <host:port> <role> <master_id> <slots>
 *
 * 注意事项：
 * 1. 版本号必须递增(除非force=true)
 * 2. 会自动识别当前节点ID(匹配IP和端口)
 * 3. 会清除已迁移槽位的数据
 */
// 官网：
//  Kvrocks 提供了 CLUSTERX SETNODES 命令设置拓扑结构，需要注意的是，由于集群中的节点不会相互进行通信，
//  所以该命令中的集群拓扑是整个集群的拓扑结构，而且也需要对集群中的所有节点都得执行拓扑结构的设置。
//
//  - VERSION: 新拓扑结构的版本号，为了避免拓扑被无序的错误修改，只有当新的拓扑结构版本号大于当前拓扑结构的版本号时才能更新集群拓扑结构。
//  - FORCE: 强制更新，命令中带有 force flag 时 Kvrocks 不会进行 VERSION 校验， 用于在集群拓扑结构管理混乱或出错时，强行设置集群拓扑结构。
Status Cluster::SetClusterNodes(const std::string &nodes_str, int64_t version, bool force) {
  // 版本号检查
  if (version < 0) return {Status::NotOK, errInvalidClusterVersion};

  if (!force) {
    // 非强制模式下，版本号必须递增
    if (version_ > version) {
      return {Status::NotOK, errInvalidClusterVersion};
    }

    // 相同版本无需更新
    if (version_ == version) return Status::OK();
  }

  // 解析节点信息字符串
  ClusterNodes nodes;                                            // 节点信息映射表 [ node_id -> ClusterNode ]
  std::unordered_map<int, std::string> slots_nodes;              // 槽位分配映射表 [ slot -> node_id ]
  Status s = parseClusterNodes(nodes_str, &nodes, &slots_nodes); // 执行解析
  if (!s.IsOK()) return s;

  // 更新集群版本和拓扑结构
  version_ = version;
  nodes_ = nodes;
  size_ = 0;

  // 遍历每个 slot 及其绑定的 node
  for (const auto &[slot, node_id] : slots_nodes) {
    // 更新 slot => node 映射
    slots_nodes_[slot] = nodes_[node_id];
  }

  // 遍历集群每个 node
  for (const auto &[node_id, node] : nodes_) {
    // 把 slave 绑定到 master
    if (node->role == kClusterSlave) {
      if (nodes_.find(node->master_id) != nodes_.end()) {
        nodes_[node->master_id]->replicas.push_back(node_id);
      }
    }
    // 统计有槽位的主节点数
    if (node->role == kClusterMaster && node->slots.count() > 0) {
      size_++;
    }
  }

  // 获取当前节点 node_id ，存入 myid_
  if (myid_.empty() || force) {
    for (const auto &[node_id, node] : nodes_) {
      if (node->port == port_ && util::MatchListeningIP(binds_, node->host)) {
        myid_ = node_id;
        break;
      }
    }
  }

  // 获取当前节点 node ，存入 myself_
  myself_ = nullptr;
  if (!myid_.empty() && nodes_.find(myid_) != nodes_.end()) {
    myself_ = nodes_[myid_];
  }

  // 建立主从复制关系
  if (auto s = SetMasterSlaveRepl(); !s.IsOK()) {
    return s.Prefixed("failed to set master-replica replication");
  }

  // 清理已迁移槽位的数据
  if (!migrated_slots_.empty()) {
    engine::Context ctx(srv_->storage);
    for (const auto &[slot, _] : migrated_slots_) {
      if (slots_nodes_[slot] != myself_) {
        // 清理本节点中该 slot 的 key 数据
        auto s = srv_->slot_migrator->ClearKeysOfSlotRange(ctx, kDefaultNamespace, SlotRange::GetPoint(slot));
        if (!s.ok()) {
          error("failed to clear data of migrated slots: {}", s.ToString());
        }
      }
    }
  }

  // 清除迁移/导入槽位记录
  migrated_slots_.clear();
  imported_slots_.clear();

  return Status::OK();
}

// Set replication relationship by cluster topology setting
Status Cluster::SetMasterSlaveRepl() {
  if (!srv_) return Status::OK();

  // If the node is not in the cluster topology, remove the master replication if it's a replica.
  // 如果 myself_ 不存在，说明此节点尚未加入集群，如果之前是 Slave，则需要移除主从关系
  if (!myself_) {
    if (auto s = srv_->RemoveMaster(); !s.IsOK()) {
      return s.Prefixed("failed to remove master");
    }
    return Status::OK();
  }

  bool is_slave = srv_->IsSlave();                              // 当前节点是否是 Slave
  bool is_cluster_enabled = srv_->GetConfig()->cluster_enabled; // 集群模式是否启用

  // 如果当前节点在集群拓扑中是 Master
  if (myself_->role == kClusterMaster) {
    // Master mode
    auto s = srv_->RemoveMaster();
    if (!s.IsOK()) {
      return s.Prefixed("failed to remove master");
    }
    info("MASTER MODE enabled by cluster topology setting");
    // ???
    if (srv_->slot_migrator && is_cluster_enabled && is_slave) {
      // Slave -> Master
      srv_->slot_migrator->SetStopMigrationFlag(false);
      info("Change server role to master, restart migration task");
    }
    return Status::OK();
  }

  // 如果当前节点在集群拓扑中是 Slave

  // 查找当前节点归属的 Master 节点
  auto it = nodes_.find(myself_->master_id);
  // 找到 Master 节点
  if (it != nodes_.end()) {
    // Replica mode and master node is existing
    // 建立主从复制关系
    std::shared_ptr<ClusterNode> master = it->second;
    auto s = srv_->AddMaster(master->host, master->port, false);
    if (!s.IsOK()) {
      warn("SLAVE OF {}:{} wasn't enabled by cluster topology setting, encounter error: {}", master->host, master->port,
           s.Msg());
      return s.Prefixed("failed to add master");
    }
    // ???
    if (srv_->slot_migrator && is_cluster_enabled && !is_slave) {
      // Master -> Slave
      srv_->slot_migrator->SetStopMigrationFlag(true);
      info("Change server role to slave, stop migration task");
    }
    info("SLAVE OF {}:{} enabled by cluster topology setting", master->host, master->port);
  }

  return Status::OK();
}

bool Cluster::IsNotMaster() { return myself_ == nullptr || myself_->role != kClusterMaster || srv_->IsSlave(); }


// 将指定的 slot_range 标记为已迁移成功，并更新本节点的 migrated_slots_ 映射表，指明这些 slot 现在归属于哪个目标节点 ip_port 。
// 当前节点在收到这些 slot 请求时，返回 -MOVED ，并提供目标节点地址（如：-MOVED 12345 10.1.1.33:6666）；
Status Cluster::SetSlotRangeMigrated(const SlotRange &slot_range, const std::string &ip_port) {
  if (!slot_range.IsValid()) {
    return {Status::NotOK, errSlotRangeInvalid};
  }

  // It is called by slot-migrating thread which is an asynchronous thread.
  // Therefore, it should be locked when a record is added to 'migrated_slots_'
  // which will be accessed when executing commands.
  auto exclusivity = srv_->WorkExclusivityGuard();
  for (auto slot = slot_range.start; slot <= slot_range.end; slot++) {
    migrated_slots_[slot] = ip_port;
  }
  return Status::OK();
}

Status Cluster::SetSlotRangeImported(const SlotRange &slot_range) {
  if (!slot_range.IsValid()) {
    return {Status::NotOK, errSlotRangeInvalid};
  }

  // It is called by command 'cluster import'. When executing the command, the
  // exclusive lock has been locked. Therefore, it can't be locked again.
  for (auto slot = slot_range.start; slot <= slot_range.end; slot++) {
    imported_slots_.insert(slot);
  }
  return Status::OK();
}


// CMD: CLUSTERX MIGRATE $slot_range $dst_nodeid $dst_nodeid
//
// 将当前节点（myself）负责的一段 slot 范围迁移到另一个 master 节点。
//
// 官网说明:
//   DBA 通过 CLUSTERX MIGRATE 命令进行 slot 迁移，即可完成一个特定 slot 数据的迁移。
//   该命令会先迁移该 slot 的全量数据，再迁移增量数据，从而保证数据的正确性。
//   当 slot 数据迁移完成后，数据迁移的源节点会回复 MOVED 错误告知 SDK 需要重定向到目标节点。
//   为了保证拓扑结构的正确性，Kvrocks 不会直接更改拓扑结构，也不会清理数据，需要管控节通过 CLUSTER SETSLOT 命令更改，当然也需要对集群中所有节点执行该命令。
Status Cluster::MigrateSlotRange(const SlotRange &slot_range, const std::string &dst_node_id, SyncMigrateContext *blocking_ctx) {
  // 目标节点是否存在
  if (nodes_.find(dst_node_id) == nodes_.end()) {
    return {Status::NotOK, "Can't find the destination node id"};
  }
  // 检查 slot 范围是否合法
  if (!slot_range.IsValid()) {
    return {Status::NotOK, errSlotRangeInvalid};
  }
  // 检查 slot 是否已经迁移过（防止重复迁移）
  if (!migrated_slots_.empty() && slot_range.HasOverlap({migrated_slots_.begin()->first, migrated_slots_.rbegin()->first})) {
    return {Status::NotOK, "Can't migrate slot which has been migrated"};
  }
  // 检查每个 slot 是否属于当前节点，存在不属于当前节点的 slot 就报错
  for (auto slot = slot_range.start; slot <= slot_range.end; slot++) {
    if (slots_nodes_[slot] != myself_) {
      return {Status::NotOK, "Can't migrate slot which doesn't belong to me"};
    }
  }
  // 只有 master 才能执行迁移操作
  if (IsNotMaster()) {
    return {Status::NotOK, "Slave can't migrate slot"};
  }
  // 目标节点也必须是 master
  if (nodes_[dst_node_id]->role != kClusterMaster) {
    return {Status::NotOK, "Can't migrate slot to a slave"};
  }
  // 不能迁移给自己
  if (nodes_[dst_node_id] == myself_) {
    return {Status::NotOK, "Can't migrate slot to myself"};
  }

  // 创建后台迁移任务，立即返回
  const auto &dst = nodes_[dst_node_id];
  Status s = srv_->slot_migrator->PerformSlotRangeMigration(dst_node_id, dst->host, dst->port, slot_range, blocking_ctx);
  return s;
}

// CMD: CLUSTER IMPORT $slot_range $state
//
// It is an internal command to notify the destination server to prepare for data importing.
// This command cannot be used by clients.
Status Cluster::ImportSlotRange(redis::Connection *conn, const SlotRange &slot_range, int state) {
  // 只有 master 才能导入 slot
  if (IsNotMaster()) {
    return {Status::NotOK, "Slave can't import slot"};
  }
  // 检查 slot 范围合法性
  if (!slot_range.IsValid()) {
    return {Status::NotOK, errSlotRangeInvalid};
  }
  // 不能导入已经属于本节点的 slot ，否则可能导致数据覆盖。
  for (auto slot = slot_range.start; slot <= slot_range.end; slot++) {
    auto source_node = srv_->cluster->slots_nodes_[slot];
    if (source_node && source_node->id == myid_) {
      return {Status::NotOK, "Can't import slot which belongs to me"};
    }
  }

  // 根据导入状态执行不同逻辑
  Status s;
  switch (state) {
    case kImportStart: // 开始导入
      s = srv_->slot_import->Start(slot_range);   // 设置状态、清理数据
      if (!s.IsOK()) return s;

      // Set link importing
      conn->SetImporting();                       // 给连接打上 Importing 标记
      myself_->importing_slot_range = slot_range; // 记录正在导入的 slot 范围

      // Set link error callback
      // 设置连接关闭回调（异常清理）
      conn->close_cb = [object_ptr = srv_->slot_import.get(), slot_range]([[maybe_unused]] int fd) {
        auto s = object_ptr->StopForLinkError();
        if (!s.IsOK()) {
          error("[import] Failed to stop importing slot(s) {}: {}", slot_range.String(), s.Msg());
        }
      };

      // Stop forbidding writing slot to accept write commands
      // ???
      if (slot_range.HasOverlap(srv_->slot_migrator->GetForbiddenSlotRange())) {
        // This approach assumes a shard only handles one migration task at a time.
        // When executing the import logic, the absence of other outgoing migrations on this shard justifies safely
        // removing the forbidden slot. A more robust solution would be required if concurrent slot migrations are
        // supported in the future.
        srv_->slot_migrator->ReleaseForbiddenSlotRange();
      }

      info("[import] Start importing slot(s) {}", slot_range.String());
      break;
    case kImportSuccess:  // 导入完成
      s = srv_->slot_import->Success(slot_range);
      if (!s.IsOK()) return s;
      info("[import] Mark the importing slot(s) {} as succeed", slot_range.String());
      break;
    case kImportFailed:   // 导入失败
      s = srv_->slot_import->Fail(slot_range);
      if (!s.IsOK()) return s;
      info("[import] Mark the importing slot(s) {} as failed", slot_range.String());
      break;
    default:
      return {Status::NotOK, errInvalidImportState};
  }

  return Status::OK();
}

Status Cluster::GetClusterInfo(std::string *cluster_infos) {
  if (version_ < 0) {
    return {Status::RedisClusterDown, errClusterNoInitialized};
  }

  cluster_infos->clear();

  int ok_slot = 0;
  for (auto &slots_node : slots_nodes_) {
    if (slots_node != nullptr) ok_slot++;
  }

  *cluster_infos =
      "cluster_state:ok\r\n"
      "cluster_slots_assigned:" +
      std::to_string(ok_slot) +
      "\r\n"
      "cluster_slots_ok:" +
      std::to_string(ok_slot) +
      "\r\n"
      "cluster_slots_pfail:0\r\n"
      "cluster_slots_fail:0\r\n"
      "cluster_known_nodes:" +
      std::to_string(nodes_.size()) +
      "\r\n"
      "cluster_size:" +
      std::to_string(size_) +
      "\r\n"
      "cluster_current_epoch:" +
      std::to_string(version_) +
      "\r\n"
      "cluster_my_epoch:" +
      std::to_string(version_) + "\r\n";

  if (myself_ != nullptr && myself_->role == kClusterMaster && !srv_->IsSlave()) {
    // Get migrating status
    std::string migrate_infos;
    srv_->slot_migrator->GetMigrationInfo(&migrate_infos);
    *cluster_infos += migrate_infos;

    // Get importing status
    std::string import_infos;
    srv_->slot_import->GetImportInfo(&import_infos);
    *cluster_infos += import_infos;
  }

  return Status::OK();
}

// Format: 1) 1) start slot
//            2) end slot
//            3) 1) master IP
//               2) master port
//               3) node ID
//            4) 1) replica IP
//               2) replica port
//               3) node ID
//          ... continued until done
Status Cluster::GetSlotsInfo(std::vector<SlotInfo> *slots_infos) {
  if (version_ < 0) {
    return {Status::RedisClusterDown, errClusterNoInitialized};
  }

  slots_infos->clear();

  int start = -1;
  std::shared_ptr<ClusterNode> n = nullptr;
  for (int i = 0; i <= kClusterSlots; i++) {
    // Find start node and slot id
    if (n == nullptr) {
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
      continue;
    }
    // Generate slots info when occur different node with start or end of slot
    if (i == kClusterSlots || n != slots_nodes_[i]) {
      slots_infos->emplace_back(genSlotNodeInfo(start, i - 1, n));
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
    }
  }

  return Status::OK();
}

SlotInfo Cluster::genSlotNodeInfo(int start, int end, const std::shared_ptr<ClusterNode> &n) {
  std::vector<SlotInfo::NodeInfo> vn;
  vn.push_back({n->host, n->port, n->id});  // itself

  for (const auto &id : n->replicas) {  // replicas
    if (nodes_.find(id) == nodes_.end()) continue;
    vn.push_back({nodes_[id]->host, nodes_[id]->port, nodes_[id]->id});
  }

  return {start, end, vn};
}

// $node $host:$port@$cport $role $master_id/$- $ping_sent $ping_received
// $version $connected $slot_range
Status Cluster::GetClusterNodes(std::string *nodes_str) {
  if (version_ < 0) {
    return {Status::RedisClusterDown, errClusterNoInitialized};
  }

  *nodes_str = genNodesDescription();
  return Status::OK();
}

StatusOr<std::string> Cluster::GetReplicas(const std::string &node_id) {
  if (version_ < 0) {
    return {Status::RedisClusterDown, errClusterNoInitialized};
  }

  auto item = nodes_.find(node_id);
  if (item == nodes_.end()) {
    return {Status::InvalidArgument, errInvalidNodeID};
  }

  auto node = item->second;
  if (node->role != kClusterMaster) {
    return {Status::InvalidArgument, errNoMasterNode};
  }

  auto now = util::GetTimeStampMS();
  std::string replicas_desc;
  for (const auto &replica_id : node->replicas) {
    auto n = nodes_.find(replica_id);
    if (n == nodes_.end()) {
      continue;
    }

    auto replica = n->second;

    std::string node_str;
    // ID, host, port
    node_str.append(
        fmt::format("{} {}:{}@{} ", replica_id, replica->host, replica->port, replica->port + kClusterPortIncr));

    // Flags
    node_str.append(fmt::format("slave {} ", node_id));

    // Ping sent, pong received, config epoch, link status
    node_str.append(fmt::format("{} {} {} connected", now - 1, now, version_));

    replicas_desc.append(node_str + "\n");
  }

  return replicas_desc;
}

std::string Cluster::getNodeIDBySlot(int slot) const {
  if (slot < 0 || slot >= kClusterSlots || !slots_nodes_[slot]) return "";
  return slots_nodes_[slot]->id;
}

std::string Cluster::genNodesDescription() {
  auto slots_infos = getClusterNodeSlots();

  auto now = util::GetTimeStampMS();
  std::string nodes_desc;
  for (const auto &[_, node] : nodes_) {
    std::string node_str;
    // ID, host, port
    node_str.append(node->id + " ");
    node_str.append(fmt::format("{}:{}@{} ", node->host, node->port, node->port + kClusterPortIncr));

    // Flags
    if (node->id == myid_) node_str.append("myself,");
    if (node->role == kClusterMaster) {
      node_str.append("master - ");
    } else {
      node_str.append("slave " + node->master_id + " ");
    }

    // Ping sent, pong received, config epoch, link status
    node_str.append(fmt::format("{} {} {} connected", now - 1, now, version_));

    if (node->role == kClusterMaster) {
      auto iter = slots_infos.find(node->id);
      if (iter != slots_infos.end() && !iter->second.empty()) {
        node_str.append(" " + iter->second);
      }
    }

    // Just for MYSELF node to show the importing/migrating slot
    if (node->id == myid_) {
      if (srv_->slot_migrator) {
        auto migrating_slot_range = srv_->slot_migrator->GetMigratingSlotRange();
        if (migrating_slot_range.IsValid()) {
          node_str.append(fmt::format(" [{}->-{}]", migrating_slot_range.String(), srv_->slot_migrator->GetDstNode()));
        }
      }
      if (srv_->slot_import) {
        auto importing_slot_range = srv_->slot_import->GetSlotRange();
        if (importing_slot_range.IsValid()) {
          node_str.append(
              fmt::format(" [{}-<-{}]", importing_slot_range.String(), getNodeIDBySlot(importing_slot_range.start)));
        }
      }
    }
    nodes_desc.append(node_str + "\n");
  }
  return nodes_desc;
}

std::map<std::string, std::string, std::less<>> Cluster::getClusterNodeSlots() const {
  int start = -1;
  // node id => slots info string
  std::map<std::string, std::string, std::less<>> slots_infos;

  std::shared_ptr<ClusterNode> n = nullptr;
  for (int i = 0; i <= kClusterSlots; i++) {
    // Find start node and slot id
    if (n == nullptr) {
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
      continue;
    }
    // Generate slots info when occur different node with start or end of slot
    if (i == kClusterSlots || n != slots_nodes_[i]) {
      if (start == i - 1) {
        slots_infos[n->id] += fmt::format("{} ", start);
      } else {
        slots_infos[n->id] += fmt::format("{}-{} ", start, i - 1);
      }
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
    }
  }

  for (auto &[_, info] : slots_infos) {
    if (info.size() > 0) info.pop_back();  // Remove last space
  }
  return slots_infos;
}

std::string Cluster::genNodesInfo() const {
  auto slots_infos = getClusterNodeSlots();

  std::string nodes_info;
  for (const auto &[_, node] : nodes_) {
    std::string node_str;
    node_str.append("node ");
    // ID
    node_str.append(node->id + " ");
    // Host + Port
    node_str.append(fmt::format("{} {} ", node->host, node->port));

    // Role
    if (node->role == kClusterMaster) {
      node_str.append("master - ");
    } else {
      node_str.append("slave " + node->master_id + " ");
    }

    // Slots
    if (node->role == kClusterMaster) {
      auto iter = slots_infos.find(node->id);
      if (iter != slots_infos.end() && !iter->second.empty()) {
        node_str.append(" " + iter->second);
      }
    }
    nodes_info.append(node_str + "\n");
  }
  return nodes_info;
}

Status Cluster::DumpClusterNodes(const std::string &file) {
  // Parse and validate the cluster nodes string before dumping into file
  std::string tmp_path = file + ".tmp";
  remove(tmp_path.data());
  std::ofstream output_file(tmp_path, std::ios::out);
  output_file << fmt::format("version {}\n", version_);
  output_file << fmt::format("id {}\n", myid_);
  output_file << genNodesInfo();
  output_file.close();
  if (rename(tmp_path.data(), file.data()) < 0) {
    return {Status::NotOK, fmt::format("rename file encounter error: {}", strerror(errno))};
  }
  return Status::OK();
}

// LoadClusterNodes 用于 Kvrocks 从 nodes.conf 等配置文件中恢复集群节点信息，包括本节点 ID、版本号和集群节点列表等。
Status Cluster::LoadClusterNodes(const std::string &file_path) {
  // 如果文件不存在，不报错 —— 视为首次启动；可使用 CLUSTERX 命令重新配置集群。
  if (rocksdb::Env::Default()->FileExists(file_path).IsNotFound()) {
    info("The cluster nodes file {} is not found. Use CLUSTERX subcommands to specify it.", file_path);
    return Status::OK();
  }

  std::ifstream file;
  file.open(file_path);
  if (!file.is_open()) { // 打开文件失败
    return {Status::NotOK, fmt::format("error opening the file '{}': {}", file_path, strerror(errno))};
  }

  int64_t version = -1;
  std::string id, nodes_info;
  std::string line;
  // 逐行解析文件内容
  while (file.good() && std::getline(file, line)) {
    // 返回 std::pair<string, string>
    auto parsed = ParseConfigLine(line);
    if (!parsed) return parsed.ToStatus().Prefixed("malformed line");
    if (parsed->first.empty() || parsed->second.empty()) continue;

    // 处理支持的键值对
    auto key = parsed->first;
    if (key == "version") {
      auto parse_result = ParseInt<int64_t>(parsed->second, 10);
      if (!parse_result) {
        return {Status::NotOK, errInvalidClusterVersion};
      }
      version = *parse_result;
    } else if (key == "id") {
      id = parsed->second;
      if (id.length() != kClusterNodeIdLen) { // 必须是固定长度
        return {Status::NotOK, errInvalidNodeID};
      }
    } else if (key == "node") {
      nodes_info.append(parsed->second + "\n");
    } else {
      return {Status::NotOK, fmt::format("unknown key: {}", key)};
    }
  }

  // 设置当前节点 ID
  myid_ = id;
  // 更新内存中节点配置信息
  return SetClusterNodes(nodes_info, version, false);
}

/**
 * 解析集群节点信息字符串
 *
 * @param nodes_str 输入-集群节点字符串（多行），每行表示一个节点
 * @param nodes 输出-节点信息映射表 [ node_id -> ClusterNode ]
 * @param slots_nodes 输出-槽位分配映射表 [ slot -> node_id ]
 *
 * @return 执行状态，成功返回OK，失败返回错误信息
 *
 * 功能说明：
 * 1. 解析集群节点信息字符串，构建节点拓扑结构
 * 2. 验证节点信息的完整性和有效性
 * 3. 记录槽位分配关系
 *
 * 输入字符串格式示例：
 * <node_id> <host> <port> <role> <master_id> [<slot_range>...]
 *
 * master: aaaaa 127.0.0.1 6379 master - 0-5460 5461-10922 ，对于 master 节点 <master_id> 字段为 - ；
 * slave: bbbbb 127.0.0.1 6380 slave aaaaa...aaaa ，对于 slave 节点 slot range 数组为空，因为 slave 节点不会持有槽位；
 *
 * 字段说明：
 * 1. node_id: 40 字符长度的节点 ID
 * 2. host/port: 节点地址
 * 3. role: 节点角色(master/slave/replica)
 * 4. master_id: 主节点 ID (从节点需要)
 * 5. slot_range: 槽位范围(主节点需要)，可以是单个槽位或范围(如 0 或 0-8191)
 */
Status Cluster::parseClusterNodes(const std::string &nodes_str,
                                  ClusterNodes *nodes,
                                  std::unordered_map<int, std::string> *slots_nodes) {
  // 按行切分节点
  std::vector<std::string> nodes_info = util::Split(nodes_str, "\n");
  if (nodes_info.empty()) {
    return {Status::ClusterInvalidInfo, errInvalidClusterNodeInfo};
  }

  nodes->clear(); // 清空输出参数

  // 逐个解析节点
  for (const auto &node_str : nodes_info) {
    // 按空格分拆字段
    std::vector<std::string> fields = util::Split(node_str, " ");
    if (fields.size() < 5) { // 至少需要5个字段
      return {Status::ClusterInvalidInfo, errInvalidClusterNodeInfo};
    }

    /* 1. 解析节点ID */
    if (fields[0].size() != kClusterNodeIdLen) { // 节点 ID 必须是固定长度，40字符
      return {Status::ClusterInvalidInfo, errInvalidNodeID};
    }
    std::string id = fields[0];

    /* 2. 解析主机地址 */
    std::string host = fields[1]; // TODO: 需要增加host有效性检查

    /* 3. 解析端口号 */
    auto parse_result = ParseInt<uint16_t>(fields[2], 10);
    if (!parse_result) {
      return {Status::ClusterInvalidInfo, "Invalid cluster node port"};
    }
    int port = *parse_result;

    /* 4. 解析节点角色 */
    int role = 0;
    if (util::EqualICase(fields[3], "master")) {
      role = kClusterMaster;
    } else if (util::EqualICase(fields[3], "slave") || util::EqualICase(fields[3], "replica")) {
      role = kClusterSlave;
    } else {
      return {Status::ClusterInvalidInfo, "Invalid cluster node role"};
    }

    /* 5. 解析主节点ID */
    std::string master_id = fields[4];
    // 主节点 master_id 必须为 "-" ，从节点必须为有效节点 ID
    if ((role == kClusterMaster && master_id != "-") ||
        (role == kClusterSlave && master_id.size() != kClusterNodeIdLen)) {
      return {Status::ClusterInvalidInfo, errInvalidNodeID};
    }

    std::bitset<kClusterSlots> slots; // 初始化槽位位图

    /* 处理从节点(不需要槽位信息) */
    if (role == kClusterSlave) {
      if (fields.size() != 5) { // 从节点只能有5个字段
        return {Status::ClusterInvalidInfo, errInvalidClusterNodeInfo};
      }
      // 创建从节点对象并保存到映射表中
      (*nodes)[id] = std::make_shared<ClusterNode>(id, host, port, role, master_id, slots);
      // 因为从节点不包含 slot range ，直接处理下一个节点
      continue;
    }

    /* 6. 处理主节点的槽位分配信息 */
    auto valid_range = NumericRange<int>{0, kClusterSlots - 1};  // 槽位有效范围
    const std::regex node_id_regex(R"(\b[a-fA-F0-9]{40}\b)"); // 节点 ID 正则，正则检查避免 node_id 粘连导致 slot 误识别

    // 遍历槽位信息字段(从第 6 个字段开始)
    for (unsigned i = 5; i < fields.size(); i++) {
      std::vector<std::string> ranges = util::Split(fields[i], "-");

      /* 情况 A：单个槽位，如 "1024" */
      if (ranges.size() == 1) {
        // 检查是否是误写的节点ID
        if (std::regex_match(fields[i], node_id_regex)) {
          return {Status::ClusterInvalidInfo,
                  "Invalid nodes definition: Missing newline between node entries."};
        }

        // 解析槽位号
        auto parse_start = ParseInt<int>(ranges[0], valid_range, 10);
        if (!parse_start) {
          return {Status::ClusterInvalidInfo, errSlotOutOfRange};
        }
        int start = *parse_start;

        // 设置槽位位图
        slots.set(start, true);
        if (role == kClusterMaster) {
          // 查看 slot -> node_id 映射表，检查槽位是否已分配；若某个槽已被其他节点声明则报错，确保槽位不会重叠。
          if (slots_nodes->find(start) != slots_nodes->end()) {
            return {Status::ClusterInvalidInfo, errSlotOverlapped};
          }
          // 若未分配，更新 slot -> node_id 映射表
          (*slots_nodes)[start] = id;
        }
      }
      /* 情况 B：slot 范围，如 "0-8191" */
      else if (ranges.size() == 2) {
        // 解析起始和结束槽位
        auto parse_start = ParseInt<int>(ranges[0], valid_range, 10);
        auto parse_stop = ParseInt<int>(ranges[1], valid_range, 10);
        if (!parse_start || !parse_stop || *parse_start >= *parse_stop) {
          return {Status::ClusterInvalidInfo, errSlotOutOfRange};
        }

        // 设置范围内的所有槽位
        int start = *parse_start;
        int stop = *parse_stop;
        for (int j = start; j <= stop; j++) {
          slots.set(j, true);
          if (role == kClusterMaster) {
            // 查看 slot -> node_id 映射表，检查槽位是否已分配；若某个槽已被其他节点声明则报错，确保槽位不会重叠。
            if (slots_nodes->find(j) != slots_nodes->end()) {
              return {Status::ClusterInvalidInfo, errSlotOverlapped};
            }
            // 更新 slot -> node_id 映射表
            (*slots_nodes)[j] = id;
          }
        }
      } else {
        return {Status::ClusterInvalidInfo, errSlotOutOfRange};
      }
    }

    // 创建主节点对象
    (*nodes)[id] = std::make_shared<ClusterNode>(id, host, port, role, master_id, slots);
  }

  return Status::OK();
}

bool Cluster::IsWriteForbiddenSlot(int slot) const {
  return srv_->slot_migrator->GetForbiddenSlotRange().Contains(slot);
}

Status Cluster::CanExecByMySelf(const redis::CommandAttributes *attributes,
                                const std::vector<std::string> &cmd_tokens,
                                redis::Connection *conn,
                                lua::ScriptRunCtx *script_run_ctx) {
  // 提取命令中的 key 的位置
  std::vector<int> key_indexes;

  attributes->ForEachKeyRange(
      [&](const std::vector<std::string> &, redis::CommandKeyRange key_range) {
        key_range.ForEachKeyIndex([&](int i) { key_indexes.push_back(i); }, cmd_tokens.size());
      },
      cmd_tokens);

  if (key_indexes.empty()) return Status::OK();


  // 所有 key 必须属于 同一个 slot
  // Redis Cluster 限制单条命令只能操作同一个 slot 中的 key ，否则报错 CROSSSLOT 。
  int slot = -1;
  for (auto i : key_indexes) {
    if (i >= static_cast<int>(cmd_tokens.size())) break;

    int cur_slot = GetSlotIdFromKey(cmd_tokens[i]);
    if (slot == -1) slot = cur_slot;
    if (slot != cur_slot) {
      return {Status::RedisCrossSlot, "Attempted to access keys that don't hash to the same slot"};
    }
  }
  if (slot == -1) return Status::OK();

  // 如果该 slot 没有任何节点负责，报 CLUSTERDOWN
  if (slots_nodes_[slot] == nullptr) {
    return {Status::RedisClusterDown, "Hash slot not served"};
  }

  bool cross_slot_ok = false;

  // 对 Lua 脚本特殊处理：
  //  - 脚本中的多个命令需要 使用相同的 slot；
  //  - 如果不是，只有设置了 kScriptAllowCrossSlotKeys 才允许；但是这些 slot 仍然落在同一个节点上。
  if (script_run_ctx) {
    if (script_run_ctx->current_slot != -1 && script_run_ctx->current_slot != slot) {
      if (getNodeIDBySlot(script_run_ctx->current_slot) != getNodeIDBySlot(slot)) {
        return {Status::RedisMoved, fmt::format("{} {}:{}", slot, slots_nodes_[slot]->host, slots_nodes_[slot]->port)};
      }
      if (!(script_run_ctx->flags & lua::ScriptFlagType::kScriptAllowCrossSlotKeys)) {
        return {Status::RedisCrossSlot, "Script attempted to access keys that do not hash to the same slot"};
      }
    }

    script_run_ctx->current_slot = slot;
    cross_slot_ok = true;
  }

  uint64_t flags = attributes->GenerateFlags(cmd_tokens);

  // 本节点是 slot 的拥有者 && 没有迁移中，就允许执行
  if (myself_ && myself_ == slots_nodes_[slot]) {
    // We use central controller to manage the topology of the cluster.
    // Server can't change the topology directly, so we record the migrated slots
    // to move the requests of the migrated slots to the destination node.
    if (migrated_slots_.count(slot) > 0) {  // I'm not serving the migrated slot
      return {Status::RedisMoved, fmt::format("{} {}", slot, migrated_slots_[slot])}; // 我已经迁走这个 slot
    }
    // To keep data consistency, slot will be forbidden write while sending the last incremental data.
    // During this phase, the requests of the migrating slot has to be rejected.
    //
    // [重要] 这里可见，migrate 的最后阶段是加锁的，直接拒绝写操作！
    if ((flags & redis::kCmdWrite) && IsWriteForbiddenSlot(slot)) {
      return {Status::RedisTryAgain, "Can't write to slot being migrated which is in write forbidden phase"}; // 正在迁移数据，写请求不允许
    }

    return Status::OK();  // I'm serving this slot
  }

  // 虽然 slot 尚未归属我，但我允许 ASKING 或 importing 请求
  //  - ASKING 命令是 Redis Cluster 的一种机制；让即将成为 slot 主节点的目标可以临时接受该 slot 的请求。
  if (myself_ && myself_->importing_slot_range.Contains(slot) &&
      (conn->IsImporting() || conn->IsFlagEnabled(redis::Connection::kAsking))) {
    // While data migrating, the topology of the destination node has not been changed.
    // The destination node has to serve the requests from the migrating slot,
    // although the slot is not belong to itself. Therefore, we record the importing slot
    // and mark the importing connection to accept the importing data.
    return Status::OK();  // I'm serving the importing connection or asking connection
  }

  // slot 已导入但尚未更新拓扑：接受请求
  if (myself_ && imported_slots_.count(slot)) {
    // After the slot is migrated, new requests of the migrated slot will be moved to
    // the destination server. Before the central controller change the topology, the destination
    // server should record the imported slots to accept new data of the imported slots.
    return Status::OK();  // I'm serving the imported slot
  }

  // 我是 slave，但是读请求，并且我的主节点是 slot 拥有者，且客户端设置了 readonly ，此时可以从我这里读
  if (myself_
      && myself_->role == kClusterSlave
      && !(flags & redis::kCmdWrite)
      && nodes_.find(myself_->master_id) != nodes_.end()
      && nodes_[myself_->master_id] == slots_nodes_[slot]
      && conn->IsFlagEnabled(redis::Connection::kReadOnly)) {
    return Status::OK();  // My master is serving this slot =>  主节点拥有该 slot 的从节点可读
  }

  // 默认返回 MOVED，指向 slot 的主节点地址
  if (!cross_slot_ok) {
    return {Status::RedisMoved, fmt::format("{} {}:{}", slot, slots_nodes_[slot]->host, slots_nodes_[slot]->port)};
  }

  return Status::OK();
}

// Only HARD mode is meaningful to the Kvrocks cluster,
// so it will force clearing all information after resetting.
Status Cluster::Reset() {
  if (srv_->slot_migrator && srv_->slot_migrator->GetMigratingSlotRange().IsValid()) {
    return {Status::NotOK, "Can't reset cluster while migrating slot"};
  }
  if (srv_->slot_import && srv_->slot_import->GetSlotRange().IsValid()) {
    return {Status::NotOK, "Can't reset cluster while importing slot"};
  }
  if (!srv_->storage->IsEmptyDB()) {
    return {Status::NotOK, "Can't reset cluster while database is not empty"};
  }
  if (srv_->IsSlave()) {
    auto s = srv_->RemoveMaster();
    if (!s.IsOK()) return s;
  }

  version_ = -1;
  size_ = 0;
  myid_.clear();
  myself_.reset();

  nodes_.clear();
  for (auto &n : slots_nodes_) {
    n = nullptr;
  }
  migrated_slots_.clear();
  imported_slots_.clear();

  // unlink the cluster nodes file if exists
  unlink(srv_->GetConfig()->NodesFilePath().data());
  return Status::OK();
}
