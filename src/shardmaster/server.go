package shardmaster

import (
	"sync"

	"time"

	"sort"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int                // 节点自身索引
	rf      *raft.Raft         // 底层 Raft 实例，用于共识
	applyCh chan raft.ApplyMsg // 接受 Raft 提交的信息

	// Your data here.
	configs []Config // 历史所有的配置记录,configs[0]是初始空配置
	// 每次修改分片分配（Join/Leave/Move），都会生成一个新的 Config 并推入数组
	waitCh map[int]chan Op // RPC 协程等待 Raft 达成共识的通知通道
	// 通过 Raft Log Index 来匹配请求和响应(和notifyChan一样)
}

// Raft 状态机执行的操作命令
type Op struct {
	// Your data here.
	Type string // 操作类型："Join","Leave","Move","Query"

	Servers map[int][]string // Join 参数：GID->服务器
	GIDs    []int            // Leave 参数：要移除的 GID列表
	Shard   int              // Move 参数：分片编号
	GID     int              // Move 参数：目标组 ID
	Num     int              // Query 参数：想要查询的配置版本号
}

// getWaitCh 获取或创建一个用于等待特定日志索引被应用的通道
func (sm *ShardMaster) getWaitCh(index int) chan Op {
	if _, ok := sm.waitCh[index]; !ok {
		sm.waitCh[index] = make(chan Op, 1) // 缓冲为 1 防止阻塞
	}
	return sm.waitCh[index]
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type:    "Join",
		Servers: args.Servers,
	}

	index, _, isLeader := sm.rf.Start(op) // 尝试提交给 Raft

	if !isLeader {
		reply.WrongLeader = true
		return
	}
	// 在对应的Index上“挂号”等待结果
	sm.mu.Lock()
	ch := sm.getWaitCh(index)
	sm.mu.Unlock()

	select {
	case appliedOp := <-ch:
		// 校验：确保索引应用的确实是刚才发起的请求(防止 Leader 变更导致的索引覆盖)
		if appliedOp.Type == op.Type {
			reply.WrongLeader = false
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(500 * time.Millisecond):
		// 超时处理
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type: "Leave",
		GIDs: args.GIDs,
	}

	index, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	ch := sm.getWaitCh(index)
	sm.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if appliedOp.Type == op.Type {
			reply.WrongLeader = false
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(500 * time.Millisecond):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:  "Move",
		Shard: args.Shard,
		GID:   args.GID,
	}

	index, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	ch := sm.getWaitCh(index)
	sm.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if appliedOp.Type == op.Type {
			reply.WrongLeader = false
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(500 * time.Millisecond):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Type: "Query",
		Num:  args.Num,
	}

	index, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	ch := sm.getWaitCh(index)
	sm.mu.Unlock()

	select {
	case appliedOp := <-ch:

		if appliedOp.Type != op.Type {
			reply.WrongLeader = true
			return
		}
		// Query 是只读的，从已应用的 configs 中直接读取
		sm.mu.Lock()
		if args.Num == -1 || args.Num >= len(sm.configs) {
			reply.Config = sm.configs[len(sm.configs)-1] // 返回最新配置
		} else {
			reply.Config = sm.configs[args.Num] // 返回指定版本
		}
		sm.mu.Unlock()

		reply.WrongLeader = false

	case <-time.After(500 * time.Millisecond):
		reply.WrongLeader = true
	}
}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) applyJoin(op Op) {
	// 获取当前最新配置
	last := sm.configs[len(sm.configs)-1]

	newConfig := Config{}
	newConfig.Num = last.Num + 1 // 版本号+1
	newConfig.Groups = map[int][]string{}
	//newConfig.Shards = last.Shards
	// 修改：深拷贝,逐个复制,防止后面rebalance会覆盖
	// 避免直接引用导致历史配置被修改
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = last.Shards[i]
	}
	// 拷贝旧 groups
	for gid, servers := range last.Groups {
		newConfig.Groups[gid] = servers
	}
	// 加入新 group
	for gid, servers := range op.Servers {
		newConfig.Groups[gid] = servers
	}

	sm.rebalance(&newConfig) // 加入新组后重新均衡分片

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) applyLeave(op Op) {

	last := sm.configs[len(sm.configs)-1]

	newConfig := Config{}
	newConfig.Num = last.Num + 1
	newConfig.Groups = map[int][]string{}
	//newConfig.Shards = last.Shards
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = last.Shards[i]
	}

	for gid, servers := range last.Groups {
		newConfig.Groups[gid] = servers
	}
	// 移除指定的副本组
	for _, gid := range op.GIDs {
		delete(newConfig.Groups, gid)
	}

	sm.rebalance(&newConfig) // 移除组后，原本属于它的分片需要分给别人

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) applyMove(op Op) {

	last := sm.configs[len(sm.configs)-1]

	newConfig := Config{}
	newConfig.Num = last.Num + 1
	//newConfig.Shards = last.Shards
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = last.Shards[i]
	}
	newConfig.Groups = map[int][]string{}

	for gid, servers := range last.Groups {
		newConfig.Groups[gid] = servers
	}
	// 手动指定分配归属，不需要调用rebalance
	newConfig.Shards[op.Shard] = op.GID

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) rebalance(config *Config) {
	// 无 group 分片归零
	if len(config.Groups) == 0 {
		return
	}

	// 统计每个 group 的 shard 数
	count := map[int]int{}
	for gid := range config.Groups {
		count[gid] = 0
	}

	// 统计当前各组拥有的分片数
	for _, gid := range config.Shards {
		if _, ok := count[gid]; ok {
			count[gid]++
		}
	}

	// 找出没有分配的 shard
	// 处理孤儿分片：如果某个分片指向的 GID 已经不存在了，先标记为待分配 (0)
	for i, gid := range config.Shards {
		if _, ok := config.Groups[gid]; !ok {
			config.Shards[i] = 0
		}
	}

	// 重新统计
	count = map[int]int{}
	for gid := range config.Groups {
		count[gid] = 0
	}

	for _, gid := range config.Shards {
		if gid != 0 {
			count[gid]++
		}
	}

	// 计算每个 group 应该拥有多少 shard
	n := len(config.Groups)
	avg := NShards / n
	extra := NShards % n

	target := map[int]int{}
	for gid := range config.Groups {
		target[gid] = avg
		if extra > 0 {
			target[gid]++
			extra--
		}
	}

	// 收集多余 shard：将那些超过 target 的分片拿出来放进 free 池
	free := []int{}
	for i, gid := range config.Shards {
		if gid == 0 {
			free = append(free, i)
			continue
		}
		if count[gid] > target[gid] {
			free = append(free, i)
			count[gid]--
		}
	}

	// 1. 先提取所有的 GID
	gids := make([]int, 0)
	for gid := range config.Groups {
		gids = append(gids, gid)
	}

	// 2. 显式排序（这就是保证确定性的关键行！）
	sort.Ints(gids)

	// 3. 按照排好序的 GID 顺序进行分配
	// 分配 shard：将 free 池的分片补给那些还没达标的组
	/*
		// 注意：Go 的 map 遍历是随机的，这保证了分配的相对随机和公平性
		for gid := range config.Groups {
			for count[gid] < target[gid] {
				shard := free[0]
				free = free[1:]

				config.Shards[shard] = gid
				count[gid]++
			}
		}*/
	for _, gid := range gids { // 这里的顺序现在是固定且确定的了
		for count[gid] < target[gid] {
			if len(free) == 0 {
				break
			}
			shard := free[0]
			free = free[1:]
			config.Shards[shard] = gid
			count[gid]++
		}
	}
}

func (sm *ShardMaster) applier() {

	for msg := range sm.applyCh {

		if msg.CommandValid {
			op := msg.Command.(Op)
			sm.mu.Lock()
			// 根据指令类型执行对应更新
			switch op.Type {
			case "Join": // 加入
				sm.applyJoin(op)
			case "Leave":
				sm.applyLeave(op) // 移除
			case "Move":
				sm.applyMove(op) // 迁移
				// Query 不修改状态机，只需通过 waitCh 告知已应用到此索引
			}
			index := msg.CommandIndex
			// 唤醒阻塞在相应Index上的 RPC 协程
			if ch, ok := sm.waitCh[index]; ok {
				ch <- op
				delete(sm.waitCh, index) // 清理waitch防止内存泄漏
			}
			sm.mu.Unlock()
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.waitCh = make(map[int]chan Op)

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	go sm.applier()

	return sm
}
