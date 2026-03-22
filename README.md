# 🚀 Distributed KV Store based on Raft (MIT 6.5840 / 6.824)

## 📌 项目简介
本项目实现了 **MIT 6.5840 (原 6.824) Distributed Systems** 的核心实验，旨在基于 **Raft Consensus Algorithm** 构建一个强一致性、高可用的分布式分片 Key-Value 存储系统。

### 系统核心能力：
* **分布式一致性**：基于 Raft 协议保证多副本数据同步。
* **容错机制**：支持节点崩溃恢复，自动选举新 Leader。
* **高并发处理**：优化锁粒度，支持客户端并发请求。
* **动态分片 (Sharding)**：支持数据分片存储与集群间的动态迁移。
* **日志压缩 (Snapshot)**：实现快照机制，防止日志无限增长。

✅ **验证情况**：已通过官方全部测试用例，并完成 **50 次高强度压力测试** 确保无死锁与竞态。

---

## 🧱 项目结构
```text
.
├── kvraft/ # 基于 Raft 的 KV 存储
├── shardkv/ # 分片 KV 系统（动态迁移）
├── shardmaster/ # 分片配置管理（Shard 分配）
├── raft/ # Raft 共识算法实现
├── labrpc/ # RPC 框架
├── labgob/ # 序列化模块
├── porcupine/ # 线性一致性验证工具
├── models/ # 数据模型
├── mr/ # MapReduce 实现
├── mrapps/ # MapReduce 应用示例
├── main/ # 主程序入口
├── .gitignore
├── go.mod # Go 模块依赖
├── Makefile # 编译脚本
└── .check-build # 构建检查

```
---

## ⚙️ 核心功能

### 1️⃣ Raft 共识模块（raft）

实现完整 Raft 协议：

- **Leader Election（选举）**
  - 随机选举超时
  - 候选人投票机制
  - 任期（Term）管理
- **Log Replication（日志复制）**
  - AppendEntries RPC
  - 日志一致性检查
  - 冲突优化（ConflictTerm / ConflictIndex）
- **Safety（安全性）**
  - Leader Completeness
  - Log Matching Property
- **Snapshot（快照）**
  - 日志压缩（防止 log 无限增长）
  - InstallSnapshot RPC
  - 状态恢复

## 🏗️ Raft 架构流程图
```test
ticker (选举/心跳)
│
│ 触发选举或心跳
▼
replicatorLoop (每个 peer 一个独立 goroutine)
│
│ 发送日志复制请求
▼
replicateTo
│
│ 调用 RPC
▼
AppendEntries RPC
│
│ 处理请求
▼
Follower
│
│ commitIndex 变化
▼
applyCond.Broadcast
│
│ 唤醒 apply goroutine
▼
applier
│
│ 应用已提交的日志
▼
applyCh (输出到上层 KVServer)
```

## 📋 核心组件说明

| 组件 | 职责 |
|------|------|
| **ticker** | 周期性触发，负责 Leader 心跳发送或 Follower 选举超时 |
| **replicatorLoop** | 每个 follower 一个独立 goroutine，持续进行日志同步 |
| **replicateTo** | 执行 AppendEntries RPC 的具体逻辑，处理响应和冲突 |
| **AppendEntries RPC** | Raft 日志复制 RPC，由 Leader 调用，Follower 处理 |
| **Follower** | 接收并处理 AppendEntries 请求，更新 commitIndex |
| **applyCond.Broadcast** | 条件变量广播，通知 applier 有新的已提交日志 |
| **applier** | 应用已提交的日志条目到状态机 |
| **applyCh** | 与上层 KVServer 的通信通道，传递已提交的操作 |


### 2️⃣ KV 存储（kvraft）

构建在 Raft 之上的强一致 KV 系统：

- **支持操作**
  - Get
  - Put
  - Append
- **一致性保证**
  - 所有写操作通过 Raft 提交
  - 线性一致性（Linearizability）
- **去重机制（Exactly-once）**
  - 基于 (ClientID, RequestID)
  - 防止重复执行
- **并发控制**
  - channel + waitCh 机制等待日志提交
  - apply goroutine 异步执行状态机

### 3️⃣ 分片管理（shardmaster）

负责：

- Shard → Group 的映射管理

- **支持操作**
  - Join
  - Leave
  - Move
  - Query
- **关键点**
  - Config 版本管理
  - Shard 重分配（rebalance）
  - 历史配置查询

### 4️⃣ 分片 KV（shardkv）

在 KV 基础上扩展：

- **动态分片**
  - Shard 迁移
  - Group 之间数据搬迁
- **容错**
  - 基于 Raft 的多副本容错
- **一致性**
  - 分片迁移过程中仍保证正确性

---

## 🔄 系统架构
```text
Client
│
▼
KVServer (Leader)
│
▼
Raft Layer
│
▼
Log Replication
│
▼
applyCh
│
▼
State Machine (KV DB)
```

---

## 🔁 请求执行流程（以 Put 为例）

1. Client → KVServer (RPC)
2. KVServer → Raft.Start(op)
3. Leader 复制日志到多数节点
4. 日志 commit
5. applyCh 通知 KVServer
6. apply goroutine 执行 KV 操作
7. 返回客户端结果

---

## 🧵 并发模型

系统包含多个 goroutine：

- **Raft 层**
  - 选举线程（ticker）
  - 日志复制线程（replicator）
- **KVServer 层**
  - RPC handler（每个请求一个 goroutine）
  - applyLoop（核心状态机执行）

---

## 💡 技术亮点

⭐ **1. 高性能日志复制**  
使用 per-peer replicator goroutine，减少锁竞争（细粒度锁）

⭐ **2. 快照优化**  
自动触发 snapshot，避免日志无限增长

⭐ **3. 去重机制**  
实现 exactly-once 语义，防止客户端重试导致数据错误

⭐ **4. 分片负载均衡**  
Shard 均匀分配，最小迁移策略（优化版）

---

## 🧪 测试情况

- ✔ Lab 1: RPC & 基础组件
- ✔ Lab 2: Raft（A/B/C）全部通过
- ✔ Lab 3: KVServer（A/B）通过
- ✔ Lab 4: ShardKV 通过
- ✔ 50 次压力测试稳定通过

---

## 🛠️ 技术栈

- Go（Golang）
- RPC（模拟网络）
- 分布式一致性算法（Raft）
- 并发控制（goroutine + channel + mutex）

---

## 📉 测试与验证 (Reliability & Testing)

分布式系统的正确性极难通过单次运行保证。本项目通过编写 Bash 自动化脚本，对各个 Lab 进行了高强度的循环压力测试，以确保在极端的网络延迟和节点崩溃情况下，系统仍能保持线性一致性。

### 如何运行测试：

#### 1. 基础单次测试

进入对应组件目录，使用 `GO111MODULE=off` 环境运行 Go Test：

```test
# 测试 ShardMaster (Lab 4)
cd src/shardmaster
GO111MODULE=off go test -v

# 测试 Raft (Lab 2)
cd src/raft
GO111MODULE=off go test -v
```
#### 2. 压力测试 (Stress Test)
为了排查海量并发下的 Heisenbugs（难以复现的 Bug），建议运行以下脚本进行 50 次以上的连续测试：
```test
# 压力测试脚本示例
for i in {1..50}; do
    echo "--- RUNNING TEST ITERATION $i ---"
    GO111MODULE=off go test
    if [ $? -ne 0 ]; then
        echo "FAILED AT ITERATION $i"
        exit 1
    fi
done
echo "ALL TESTS PASSED!"
```
###测试覆盖范围：
Lab 1 (MapReduce)：容错处理、多 Worker 协调、任务重分配

Lab 2 (Raft)：选举竞争、日志一致性、网络分区恢复、快照与持久化

Lab 3 (Fault-tolerant KV)：线性化读写、重复请求去重、节点崩溃恢复

Lab 4 (Sharded KV)：配置动态变更、分片原子迁移、负载均衡测试

##📈 项目收获
深入理解 Raft 共识算法

掌握分布式系统一致性设计

熟悉高并发系统架构

理解分片、负载均衡与数据迁移

提升 Go 并发编程能力

