package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob" // 6.824 专用的序列化库
	"../labrpc" // 模拟网络 RPC 的库
	"../raft"   // 底层 Raft 协议实现
)

const Debug = 0

// DPrintf 用于调试输出，只有当 Debug > 0 时才会打印日志
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Op 定义了 Raft 提交的具体操作指令
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Type     string // 操作类型："Get", "Put", "Append"
	ClientId int64  // 客户端唯一标识，用于幂等性
	SeqId    int    //客户端请求序列号，用于幂等性
}

// KVServer 结构体定义
type KVServer struct {
	mu      sync.Mutex
	me      int                // 节点编号
	rf      *raft.Raft         // Raft 提交日志后通过此 Channel 通知 KVServer
	applyCh chan raft.ApplyMsg // 节点是否存活的标志位
	dead    int32              // set by Kill()

	// 触发快照的阈值。如果 Raft 日志大小超过此值，则进行快照
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap          map[string]string    // 核心存储:内存中的 Key-Value 映射
	lastOperations map[int64]LastOp     // 幂等性去重表:记录每个客户端最后一次执行的操作及结果
	notifyChans    map[int]chan OpReply // 响应通知：Key 为 log index，用于通知正在阻塞等待结果的 RPC 协程

	lastApplied int // 记录最后应用的 log index(防止日志回退)
}

// LastOp 存储客户端最后一次操作的元数据
type LastOp struct {
	SeqId int     // 已处理的最大序列号
	Reply OpReply // 缓存上次结果，Get 需要返回 Value
}

// OpReply 应用层响应内容
type OpReply struct {
	Err   Err    // 错误码：OK, ErrNoKey, ErrWrongLeader, ErrTimeout
	Value string // Get 操作返回的值
}

// Get RPC 处理函数
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 1. 预检查：如果不是 Leader，直接返回
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 2. 封装操作指令
	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	// 3. 将操作提交给 Raft。Start() 立即返回，不等待共识达成
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 4. 创建一个用于接收结果的 Channel，并注册到 notifyChans 中
	kv.mu.Lock()
	ch := make(chan OpReply, 1) // 使用缓冲 channel 防止死锁(applier阻塞)
	// 创建信箱
	kv.notifyChans[index] = ch
	// 信箱放进map里对应的位置
	kv.mu.Unlock()

	// 5. 阻塞等待：等待 applier 协程应用日志后的通知，或超时
	// 等待信箱回信
	select {
	case res := <-ch:
		// 关键：达成共识后必须检查任期是否改变，防止 Leader 换届导致的过时指令执行
		// 拿到结果，返回给客户端
		currentTerm, _ := kv.rf.GetState()
		if currentTerm != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = res.Err
			reply.Value = res.Value
		}
	case <-time.After(500 * time.Millisecond): // 设置合理超时
		reply.Err = ErrTimeout
	}

	// 6. 清理现场:删除已经处理完的通知 Channel，释放内存
	kv.mu.Lock()
	delete(kv.notifyChans, index)
	kv.mu.Unlock()
}

// PutAppend RPC 处理函数（逻辑与 Get 基本一致）
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 提前检查：如果是重复请求，且在去重表里有记录，可以直接返回（可选优化）
	// 但最稳妥的是依然走一遍 Raft 流程，在 applier 中统一去重

	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan OpReply, 1)
	kv.notifyChans[index] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		currentTerm, _ := kv.rf.GetState()
		if currentTerm != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = res.Err
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	delete(kv.notifyChans, index)
	kv.mu.Unlock()
}

// makeSnapshot 序列化当前状态机，准备持久化
func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// 按照顺序序列化 对 Map 和进度进行编码
	if e.Encode(kv.kvMap) != nil ||
		e.Encode(kv.lastOperations) != nil ||
		e.Encode(kv.lastApplied) != nil {
		return nil
	}
	return w.Bytes()
}

// ingestSnapshot 解码快照数据并恢复状态机,反序列化
func (kv *KVServer) ingestSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvMap map[string]string
	var lastOps map[int64]LastOp
	var lastApplied int // 定义局部变量用于接收解码值
	// 解码顺序必须与编码顺序一致
	if d.Decode(&kvMap) != nil || d.Decode(&lastOps) != nil || d.Decode(&lastApplied) != nil { // 记得解码它
		log.Fatalf("Server %v decode snapshot error", kv.me)
		return
	}
	kv.kvMap = kvMap
	kv.lastOperations = lastOps
	kv.lastApplied = lastApplied // 新增
}

// applier 后台协程：不断从 applyCh 读取 Raft 提交的消息并应用到状态机
func (kv *KVServer) applier() {
	for !kv.killed() {
		msg, ok := <-kv.applyCh
		if !ok {
			return
		}
		// 处理快照消息
		if msg.SnapshotValid {
			kv.mu.Lock()
			// 方案 B: 直接强制安装，避开繁琐且易死锁的 CondInstallSnapshot
			// 防御式编程：仅当快照比当前状态更新时才覆盖
			if msg.SnapshotIndex > kv.lastApplied {
				kv.ingestSnapshot(msg.Snapshot)
				kv.lastApplied = msg.SnapshotIndex
			}
			kv.mu.Unlock()
			continue
		}
		// 处理普通指令消息
		if msg.CommandValid {
			kv.mu.Lock()
			// 过滤旧日志，确保线性一致性
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = msg.CommandIndex

			op := msg.Command.(Op)
			var reply OpReply

			// 幂等性与状态机应用
			// 幂等性逻辑检查：查看该客户端是否已经执行过此 SeqId
			lastOp, ok := kv.lastOperations[op.ClientId]
			if op.Type != "Get" && ok && op.SeqId <= lastOp.SeqId {
				// 如果是重复的写请求，直接返回缓存的结果
				reply = lastOp.Reply
			} else {
				// 否则，应用到内存 Map 中
				reply = kv.applyToStateMachine(op)
				// 更新该客户端的操作记录（Get 请求一般不记录，因为不改变状态）
				if op.Type != "Get" {
					kv.lastOperations[op.ClientId] = LastOp{SeqId: op.SeqId, Reply: reply}
				}
			}

			// 唤醒挂起的 RPC 协程
			if ch, ok := kv.notifyChans[msg.CommandIndex]; ok {
				ch <- reply
				// 发送后立即删除，防止内存泄漏
				delete(kv.notifyChans, msg.CommandIndex)
			}

			// 检查是否需要触发快照（在锁内进行以保证原子性）
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
				snapshot := kv.makeSnapshot()
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
			}
			kv.mu.Unlock()
		}
	}
}

// 执行具体的读写内存操作
func (kv *KVServer) applyToStateMachine(op Op) OpReply {
	reply := OpReply{Err: OK}
	switch op.Type {
	case "Put":
		kv.kvMap[op.Key] = op.Value
	case "Append":
		kv.kvMap[op.Key] += op.Value
	case "Get":
		if val, ok := kv.kvMap[op.Key]; ok {
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
		}
	}
	return reply
}

// kill关闭服务
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 创建并启动一个 KVServer 实例
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// 告诉序列化库如何处理 Op 结构体
	labgob.Register(Op{})

	// 初始化内存数据结构
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)

	// You may need initialization code here.

	// 1. 先初始化所有 Map
	kv.kvMap = make(map[string]string)
	kv.lastOperations = make(map[int64]LastOp)
	kv.notifyChans = make(map[int]chan OpReply)

	// 2. 在启动 Raft 之前或之后立即恢复快照
	kv.ingestSnapshot(persister.ReadSnapshot())
	// 3. 启动 Raft
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 启动应用日志的后台协程
	go kv.applier()

	return kv
}
