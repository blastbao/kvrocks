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

#include "cluster/sync_migrate_context.h"



// SyncMigrateContext 是 Kvrocks 中用于实现 同步迁移命令 CLUSTERX MIGRATE 的一套“伪阻塞”机制。
// 虽然名为“同步”，但它并没有真的阻塞线程，而是通过 libevent + 回调 + 定时器 实现“挂起当前连接、等待迁移完成、再恢复处理”的逻辑。
//
//
// Kvrocks 的 CLUSTERX MIGRATE 命令是用户发起的同步操作，意味着用户希望命令在数据真正迁移完成后返回；
// 但迁移是个过程，可能需要时间；能直接 sleep() 或 while(...) 等待阻塞主线程，那样会阻塞整个服务；
//
// 所以，Kvrocks 用 SyncMigrateContext 来：
//  - 将连接暂时挂起（Suspend）
//  - 等待迁移线程完成后恢复（Resume）或者等待超时（TimerCB）
//  - 恢复连接，向客户端返回结果。
//
//
//
// 工作流程
//  - 客户端发起迁移：CLUSTERX MIGRATE slot 1-1000 node2 6379
//  - 服务端处理：
//    - 创建 SyncMigrateContext
//    - 调用Suspend()挂起连接
//    - 启动后台迁移任务
//  - 迁移完成：
//    - 迁移线程调用Resume(OK)
//    - 事件循环触发OnWrite
//    - 返回客户端+OK
//  - 异常情况：
//    - 网络中断触发OnEvent
//    - 超时触发TimerCB
//    - 均会清理迁移上下文


// 让这个客户端连接暂时“挂起”——不处理读写请求，只在迁移完成或超时后恢复。
void SyncMigrateContext::Suspend() {
  // 设置读写事件回调
  auto bev = conn_->GetBufferEvent();
  SetCB(bev);
  // 如果设置了超时时间，则启动一个定时器。
  if (timeout_ > 0) {
    timer_.reset(NewTimer(bufferevent_get_base(bev)));
    timeval tm = {timeout_, 0};
    evtimer_add(timer_.get(), &tm);
  }
}

void SyncMigrateContext::Resume(const Status &migrate_result) {
  // 保存迁移结果
  migrate_result_ = migrate_result;
  // 手动激活该连接的写事件，会触发 OnWrite() 回调，向客户端回复结果。
  auto s = conn_->Owner()->EnableWriteEvent(conn_->GetFD());
  if (!s.IsOK()) {
    error("[server] Failed to enable write event on the sync migrate connection {}: {}", conn_->GetFD(), s.Msg());
  }
}

// 处理底层事件，如连接关闭、错误等：
void SyncMigrateContext::OnEvent(bufferevent *bev, int16_t events) {
  auto &&slot_migrator = srv_->slot_migrator;

  if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    timer_.reset(); // 清理定时器
    slot_migrator->CancelSyncCtx(); // 通知迁移逻辑终止等待
  }
  conn_->OnEvent(bev, events);
}

void SyncMigrateContext::TimerCB(int, [[maybe_unused]] int16_t events) {
  auto &&slot_migrator = srv_->slot_migrator;

  conn_->Reply(conn_->NilString());  // 回复客户端 nil
  timer_.reset();

  slot_migrator->CancelSyncCtx(); // 通知迁移逻辑终止等待

  // 恢复读取事件监听
  auto bev = conn_->GetBufferEvent();
  conn_->SetCB(bev);
  bufferevent_enable(bev, EV_READ);
}

void SyncMigrateContext::OnWrite(bufferevent *bev) {
  if (migrate_result_) {
    conn_->Reply(redis::RESP_OK);  // 迁移成功
  } else {
    conn_->Reply(redis::Error(migrate_result_)); // 迁移失败
  }

  timer_.reset(); // 取消定时器

  // 恢复读取状态
  conn_->SetCB(bev);
  bufferevent_enable(bev, EV_READ);
  bufferevent_trigger(bev, EV_READ, BEV_TRIG_IGNORE_WATERMARKS);
}
