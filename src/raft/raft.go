package raft

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

type Raft struct {
	mu        sync.Mutex          // 保护共享状态的锁 Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // 集群中的所有RPC节点 RPC end points of all peers
	persister *Persister          // 负责持久化存储的组件 Object to hold this peer's persisted state
	me        int                 // 当前节点我在beers中的索引 this peer's index into peers[]
	dead      int32               // 原子操作位1表示这个节点已关闭 set by Kill()
	// Figure 2 里的状态
	// Figure 2 持久化状态
	currentTerm int        // 服务器已知当前最新的任期
	votedFor    int        // 给谁投了票 在当前任期内收到选票的候选人ID（-1表示无）
	log         []LogEntry // 日志条目数组 索引从1开始（0放占位符）
	// Figure 2 易失性状态
	commitIndex int // 已知已提交的最后一条日志索引
	lastApplied int // 已经被应用到状态机的最后一条日志索引
	// Leader 专属易失性状态
	nextIndex       []int         // 发给每个人的下一条日志索引
	matchIndex      []int         // 每个人的匹配索引
	state           NodeState     // 自定义状态：Follower, Candidate, Leader
	lastHeartbeat   time.Time     // 上次收到有效leader心跳的时间
	electionTimeout time.Duration // 计时器 随机化的选举超时时间
	// 同步控制器
	// 用于每个 peer 的独立同步控制
	replicatorLock []sync.Mutex // 每个 peer 专属的小锁，减少大锁的竞争
	applyCond      *sync.Cond   // 条件变量：用于唤醒 applier （将日志应用到状态机）协程
	// 新增：用于每个 Peer 的复制触发信号
	replicatorCond []*sync.Cond // 条件变量：用于触发特定 peer 的日志复制过程
	// 快照
	lastIncludedIndex int // 快照覆盖的最后一条日志索引
	lastIncludedTerm  int // 快照覆盖的最后一条日志任期

	applyCh chan ApplyMsg
}
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	// 新增以下四个字段用于处理快照
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// 让 Raft 动起来：计时器循环 (ticker)
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		// 调试日志：在锁内打印状态
		//DPrintf("[S%d][Term %d][Commit %d] State: %v", rf.me, rf.currentTerm, rf.commitIndex, rf.state)
		// 逻辑 A: 非 Leader 检查选举超时（有无收到心跳）
		if rf.state != Leader {
			if time.Since(rf.lastHeartbeat) > rf.electionTimeout {
				rf.startElection() //超时，发起选举
			}
		} else {
			// 逻辑 B: Leader 检查心跳时间，唤醒所有 replicator
			// （周期性）每隔一段时间就 Signal 一次，确保即便没有新日志，也会发心跳
			for i := range rf.peers {
				if i != rf.me {
					rf.replicatorCond[i].Signal() //唤醒复制线程
				}
			}
		}
		rf.mu.Unlock()
		// mit官方推荐 heartbeat interval ≈ 100ms
		time.Sleep(100 * time.Millisecond)
	}
}

// 发起选举
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++          // 任期加 1
	rf.votedFor = rf.me       // 给自己投一票
	rf.persist()              // 2C:currentTerm 和 votedFor改变 必须持久化
	rf.resetElectionTimeout() // 重置自己的闹钟
	term := rf.currentTerm
	votesCount := 1 // 总票数 初始票数（自己那一票）
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			if rf.killed() {
				return
			}
			// 构造参数：候选人信息+最后一条日志的索引和任期
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastLogIndex(), // 5.4 比较日志新旧 确保安全性 告诉别人我有多强（日志新旧）
				LastLogTerm:  rf.getLastLogTerm(),
			}
			reply := RequestVoteReply{}
			// 发送RPC:调用远程节点的RequestVote方法
			// RPC 调用不能带锁
			if rf.sendRequestVote(p, &args, &reply) {
				rf.mu.Lock() //收到回复 涉及状态修改 必须加锁
				defer rf.mu.Unlock()
				// 收到回复后立即检查状态：如果身份已变、任期已过、或节点已死，直接退出
				if rf.state != Candidate || rf.currentTerm != term || rf.killed() {
					return
				}
				// 检查任期：如果发现别人的任期比我大，我立刻认怂变回 Follower
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					return
				}
				// 统计票数：对方同意投票
				if reply.VoteGranted {
					votesCount++
					// 拿到超过半数票（Majority），原地登基！
					// 并且确保 rf.state 依然是 Candidate 才能变 Leader
					if votesCount > len(rf.peers)/2 && rf.state == Candidate { //整数除法 3/5,2/3
						rf.state = Leader    // 立即改变状态，防止第二次进来 同一任期内多次执行leader初始化
						rf.convertToLeader() // 正式登基 初始化leader状态
					}
				}
			}
		}(peer)
	}
}

// 降级变为Follower
func (rf *Raft) convertToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.resetElectionTimeout() // 新增 防止刚降级马上又选举
	rf.persist()
}

// 升级为Leader
func (rf *Raft) convertToLeader() {
	// 转变身份
	// 此时ticker协程检测到state变化会开始发送心跳信号
	rf.state = Leader
	// 获取当前leader日志的最后索引
	// 此时leader认为自己的日志最全(基于选举安全性保证)
	lastIndex := rf.getLastLogIndex()
	// 遍历集群所有节点，初始化leader维护的进度表
	for i := range rf.peers {
		// 论文：初始化为 Leader 最后一条日志索引 + 1
		// 逻辑：Leader 默认所有人都有完整日志，先发一个空心跳探路
		rf.nextIndex[i] = lastIndex + 1 // 准备发给每个服务器的下一条日志索引
		rf.matchIndex[i] = 0            // 已知已同步到每个服务器的最高日志索引:初始化为 0（因为还没有任何新确认的日志同步）
	}
	// leader 自己已经拥有全部日志
	rf.matchIndex[rf.me] = lastIndex
	/* 注意：这里没有显式启动复制协程。
	   因为在 Make 函数里已经为每个 peer 预先启动了 leaderReplicationLoop。
	   一旦这里的 rf.state 变为 Leader，那些正在 Wait 的协程就会被 Signal 唤醒并开始工作。
	*/
}

/*
func (rf *Raft) convertToLeader() {
  rf.state = Leader
  for i := range rf.peers {
    rf.nextIndex[i] = len(rf.log) // 初始化准备发给别人的下一条日志
    rf.matchIndex[i] = 0          // 初始化已知别人拥有的日志
  }
    // 启动一个专门针对每个 peer 的同步器
    for peer := range rf.peers {
      if peer != rf.me {
        go rf.leaderReplicationLoop(peer)
      }
    }
}
*/
// 专门的同步循环
// 每个peer开启一个独立的异步循环
func (rf *Raft) leaderReplicationLoop(peer int) {
	//平时阻塞在 Wait 上，由心跳计时器或新日志进入时唤醒
	// 节点没挂协程就永远运行
	for rf.killed() == false {
		rf.replicatorLock[peer].Lock() // 用peer专属小锁不用 rf.mu 大锁，是为了避免 RPC 阻塞时影响整个 Raft 实例的运行
		// 如果不是 Leader，就一直等（直到被 Signal 且身份检查通过）
		// 这里的循环是为了应对“虚假唤醒”
		for rf.killed() == false {
			rf.mu.Lock()
			isLeader := (rf.state == Leader) // leader才发日志给别人
			rf.mu.Unlock()
			if isLeader {
				break //是leader跳出等待循环执行下面的复制逻辑
			}
			rf.replicatorCond[peer].Wait() //等待循环
		}
		if rf.killed() {
			rf.replicatorLock[peer].Unlock() // 检查节点生命状态 防止wait期间被kill
			return
		}
		// 释放小锁去执行 RPC 复制
		rf.replicatorLock[peer].Unlock()
		// replicateTo 内部会自己拿 rf.mu 大锁来构造参数,会处理具体日志切片构造+RPC调用
		// 执行同步动作,会构造 AppendEntriesArgs 并通过网络发给对应的 peer
		rf.replicateTo(peer)
		// 关键：在下一次循环开始前稍微歇一下，防止在死循环中极速发送 RPC,避免“忙等”
		// 或者直接再次进入 Wait 状态，等待 ticker 或 Start() 的下一次 Signal
		rf.replicatorLock[peer].Lock()
		// 这里可以加一个短时间的 Wait 或者直接让它回到循环顶部 Wait
		// 动作完成后，再次进入 Wait 状态，等待下一次信号（Signal）
		// 信号来源：
		//   A. ticker() 每 100ms 发出的心跳信号
		//   B. Start() 收到用户新指令时的即时触发
		//   C. replicateTo() 失败后需要立即重试的信号
		rf.replicatorCond[peer].Wait()
		rf.replicatorLock[peer].Unlock()
	}
}
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// 可以处理快照元数据
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log

	if len(rf.log) == 0 {
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{Term: lastIncludedTerm}
	}

	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	// 重要：重启后，应用进度不能从0开始，要从快照点开始
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
}

type RequestVoteArgs struct {
	Term         int // 候选人的任期
	CandidateId  int // 候选人 ID
	LastLogIndex int // 候选人最后一条日志的索引
	LastLogTerm  int // 候选人最后一条日志的任期
}
type RequestVoteReply struct {
	Term        int  // 当前任期，供候选人更新自己
	VoteGranted bool // 是否获得投票
}

// 投票规则
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 规则 1：如果你的任期比我小，不投
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 规则 2：如果我发现你的任期比我大，我更新任期并变回 Follower
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	// 规则 3：如果我还没投过票（或者投的就是你），且你的日志不比我旧
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args) {
		rf.votedFor = args.CandidateId
		rf.persist()                  // 2C:votedFor改变 必须持久化
		rf.lastHeartbeat = time.Now() // 既然投了票，说明认可你，重置我的闹钟
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // 前一条日志的索引
	PrevLogTerm  int // 前一条日志的任期
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int // Follower 冲突日志的任期
	ConflictIndex int // Follower 冲突任期的第一条日志索引
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// 实际触发复制的函数
// 持有锁构造参数 -> 释放锁发送 RPC -> 重新加锁处理回复
func (rf *Raft) replicateTo(server int) {
	// 阶段 1：加锁构造函数(Snapshot)
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock() // 只有 Leader 能发心跳/发起复制
		return
	}
	// 检查是否需要发快照
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		// 情况：Leader 准备发给 Follower 的下一条日志索引已经被快照截断了
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.persister.ReadSnapshot(), // 从存储中读取最近一次快照
		}
		rf.mu.Unlock() // 锁外发送 RPC
		reply := &InstallSnapshotReply{}
		if rf.sendInstallSnapshot(server, args, reply) {
			rf.mu.Lock()
			// 回复后处理：任期检查与进度更新
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
			} else {
				// 安装成功，同步进度直接跳到快照点
				rf.matchIndex[server] = args.LastIncludedIndex
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}
			rf.mu.Unlock()
		}
		return
	}

	// === Lab 2B 日志切片构造逻辑 ===
	// 计算前一条日志的信息(用于follower的一致性检查)
	prevLogIndex := rf.nextIndex[server] - 1
	var prevLogTerm int
	if prevLogIndex == rf.lastIncludedIndex {
		// 修改：如果前一条刚好是快照点，直接取快照元数据
		prevLogTerm = rf.lastIncludedTerm
	} else {
		// 修改：使用 realIndex 转换物理下标
		prevLogTerm = rf.log[rf.realIndex(prevLogIndex)].Term
	}
	// 2. 准备要发送的日志条目（深度拷贝）
	// 构造 entries (注意切片起始点)
	entries := make([]LogEntry, 0)
	if rf.getLastLogIndex() >= rf.nextIndex[server] {
		// 修改：切片时必须转换物理下标
		entries = append(entries, rf.log[rf.realIndex(rf.nextIndex[server]):]...)
	}
	// 将这个独立的副本交给 args
	// 封装成RPC参数
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,   // 论文规则：用于 Follower 匹配日志切片的起始点
		PrevLogTerm:  prevLogTerm,    // 论文规则：用于 Follower 校验该起始点任期是否一致
		Entries:      entries,        // 携带的真正日志内容
		LeaderCommit: rf.commitIndex, // 告诉 Follower 哪些日志已经可以提交了
	}
	// 打印发送日志：目标节点、当前任期、前一条日志索引、发送条数
	//DPrintf("[S%d -> S%d] Sending AE: Term %d, PrevIdx %d, PrevTerm %d, EntriesLen %d, LeaderCommit %d",rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
	// 阶段 2：释放大锁，执行网络调用
	// RPC 调用可能耗时很久（网络丢包、对方宕机），必须在锁外进行，否则整个集群会卡死
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(server, args, reply) {
		// 阶段 3：重新加锁，处理 RPC 回复
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 3. 处理任期冲突
		if reply.Term > rf.currentTerm {
			//DPrintf("[S%d <- S%d] AE Reply Higher Term: %d, Converting to Follower", rf.me, server, reply.Term)
			rf.convertToFollower(reply.Term)
			return
		}
		// 4. 收到回复后的安全性检查
		if rf.state != Leader || rf.currentTerm != args.Term {
			// 确保在 RPC 期间，我依然是这一任期的 Leader。
			return
			// 如果身份变了，之前的 reply 信息已经过时，不能用来更新 matchIndex
		}
		// === Lab 2B 进度更新逻辑 ===
		if reply.Success {
			/*DPrintf("[Leader %d] success replicate to %d match=%d next=%d",
			rf.me, server, rf.matchIndex[server], rf.nextIndex[server])*/
			// 算出该 peer 应该到达的最新的匹配位置
			newMatch := args.PrevLogIndex + len(args.Entries)
			if newMatch > rf.matchIndex[server] {
				rf.matchIndex[server] = newMatch
				// 下一次发送从匹配点的下一位开始
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}
			//DPrintf("[S%d <- S%d] AE Success: MatchIdx %d, NextIdx %d", rf.me, server, rf.matchIndex[server], rf.nextIndex[server])
			// 尝试推进整个集群的提交点
			rf.advanceCommitIndex()
			// 成功复制日志后继续同步后续日志
			if len(args.Entries) > 0 {
				rf.replicatorCond[server].Signal() //通过信号唤醒loop
			}
		} else {
			// 打印冲突详情
			//DPrintf("[S%d <- S%d] AE Conflict! ConflictTerm %d, ConflictIndex %d",rf.me, server, reply.ConflictTerm, reply.ConflictIndex)
			DPrintf("[Leader %d] commit -> %d", rf.me, rf.commitIndex)
			// 处理失败：日志冲突，进行快速回退
			// 情况 A: Follower 日志太短
			if reply.ConflictTerm == -1 {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				// 情况 B: 任期冲突。优化：Leader 先看自己有没有这个任期
				foundIndex := -1
				for i := len(rf.log) - 1; i >= 1; i-- {
					if rf.log[i].Term == reply.ConflictTerm {
						foundIndex = i
						break
					}
				}
				if foundIndex != -1 {
					// 如果 Leader 有这个任期，尝试从 Leader 该任期的下一条开始发
					rf.nextIndex[server] = foundIndex + 1
				} else {
					// 如果 Leader 没有这个任期，才跳到 Follower 那个任期的起始位置
					rf.nextIndex[server] = reply.ConflictIndex
				}
			}
			// 边界保护
			if rf.nextIndex[server] < 1 {
				rf.nextIndex[server] = 1
			}
			//DPrintf("[S%d <- S%d] NextIndex adjusted to %d", rf.me, server, rf.nextIndex[server])
			// 新增：立即重试,而不是等 100ms 心跳
			rf.replicatorCond[server].Signal()
		}
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()                // 加锁保护本地状态
	defer rf.mu.Unlock()        // 函数结束自动放锁
	reply.Term = rf.currentTerm // 默认先返回自己的当前任期
	// 规则 1: 如果 Leader 的任期小于自己的当前任期，拒绝请求
	if args.Term < rf.currentTerm {
		//DPrintf("[S%d] Reject AE from S%d: Old Term (Mine:%d, His:%d)", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Success = false
		return
	}
	// 只要 Leader 任期合法，就认可它的地位，重置计时器(防止我发起选举)
	rf.lastHeartbeat = time.Now()
	// 对方任期高 更新自己的任期 退位follower
	if args.Term > rf.currentTerm {
		//DPrintf("[S%d] AE Term Higher: %d, Converting to Follower", rf.me, args.Term)
		rf.convertToFollower(args.Term)
	}
	// 即使任期相等，也要确保自己从 Candidate 变回 Follower（如果有必要）
	if rf.state != Follower {
		rf.state = Follower
	}
	// 如果 PrevLogIndex 比快照还旧，直接告诉 Leader 从快照后开始发
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}
	// 规则 2: 一致性检查 (Consistency Check)
	// 情况 A：我的日志太短了，根本没有 PrevLogIndex 这一条
	if rf.getLastLogIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}
	// 情况 B：我有这条日志，但任期对不上（日志冲突）
	if rf.log[rf.realIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.log[rf.realIndex(args.PrevLogIndex)].Term
		// 快速回退逻辑
		index := args.PrevLogIndex
		for index > rf.lastIncludedIndex && rf.log[rf.realIndex(index-1)].Term == reply.ConflictTerm {
			index--
		}
		reply.ConflictIndex = index
		return
	}
	// 规则 3 & 4: 追加日志条目
	// 走到这里说明 PrevLogIndex 处的日志是一致的
	// 覆盖日志 (注意 realIndex)
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index <= rf.getLastLogIndex() && rf.log[rf.realIndex(index)].Term != entry.Term {
			rf.log = rf.log[:rf.realIndex(index)]
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		} else if index > rf.getLastLogIndex() {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	// 规则 5: 更新 commitIndex
	// Leader 告诉我已经可以提交到 LeaderCommit 了
	if args.LeaderCommit > rf.commitIndex {
		// 修改：使用 getLastLogIndex() 获取逻辑上的最后一条，而不是 len(log)-1
		lastIdx := rf.getLastLogIndex()
		if args.LeaderCommit < lastIdx {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIdx
		}
		rf.applyCond.Broadcast()
	}
	reply.Success = true
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//不再直接开协程，而是通过信号唤醒
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	// 调试日志：
	// [Role] ID: me, Term: currentTerm, Commit: commitIndex, Action: New Log
	//DPrintf("[%v] ID: %d, Term: %d, Commit: %d, LogLen: %d, Action: Start New Command",rf.state, rf.me, rf.currentTerm, rf.commitIndex, len(rf.log))
	newEntry := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, newEntry)
	rf.persist()
	// 返回逻辑索引，而不是物理长度
	index := rf.getLastLogIndex()
	// 加速同步：不再等待 ticker，立即唤醒一次复制
	// 唤醒所有复制协程立即工作，而不是等那 30ms
	for i := range rf.peers {
		if i != rf.me {
			rf.replicatorCond[i].Signal()
		}
	}
	return index, rf.currentTerm, true
}

func (rf *Raft) Kill() {
	// 1. 原子操作：将 dead 状态标记为 1
	// 使用 atomic 保证多协程可见性，其他协程调用 killed() 时会发现为 true 并退出循环
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	// 2. 唤醒 applier 协程
	// applier 阻塞在 rf.applyCond.Wait()。如果不 Broadcast，它可能永远睡在 Wait 里
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
	// 3. 遍历唤醒所有 peer 的复制协程 (replicator goroutines)
	for i := range rf.peers {
		if i != rf.me && rf.replicatorCond[i] != nil {
			// 注意：Signal/Broadcast 之前必须持有对应的锁
			// 必须先获取对应 peer 的专属小锁，这是 sync.Cond 的使用规范
			rf.replicatorLock[i].Lock()
			// 4. 唤醒阻塞在 replicatorCond[i].Wait() 上的 leaderReplicationLoop 协程
			// 唤醒后，该协程醒来会检查 rf.killed()，发现为 true 后会 return 退出
			rf.replicatorCond[i].Broadcast()
			rf.replicatorLock[i].Unlock()
		}
	}
}
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// 选举超时
func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Duration(500+rand.Intn(500)) * time.Millisecond
	rf.lastHeartbeat = time.Now()
}
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex
}
func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// 日志新旧判断：Figure 2 核心逻辑
func (rf *Raft) isLogUpToDate(args *RequestVoteArgs) bool {
	lastIndex := rf.getLastLogIndex()
	lastTerm := rf.getLastLogTerm()
	if args.LastLogTerm != lastTerm {
		return args.LastLogTerm > lastTerm
	}
	return args.LastLogIndex >= lastIndex
}

/*
	func (rf *Raft) isLogUpToDate(args *RequestVoteArgs) bool {
	  lastIndex, lastTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	  if args.LastLogTerm != lastTerm {
	    return args.LastLogTerm > lastTerm
	  }
	  return args.LastLogIndex >= lastIndex
	}
*/
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// 具体的推进逻辑函数
func (rf *Raft) advanceCommitIndex() {
	// 仅leader有权推进提交点
	if rf.state != Leader {
		return
	}
	// 从最后一条日志的逻辑索引开始找
	for n := rf.getLastLogIndex(); n > rf.commitIndex; n-- {
		if n <= rf.lastIncludedIndex {
			break
		}
		if rf.log[rf.realIndex(n)].Term == rf.currentTerm {
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = n
				rf.applyCond.Broadcast()
				break
			}
		}
	}
}

// applier 协程代码:负责将已经达成共识（committed）的日志发送到应用层
func (rf *Raft) applier(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		rf.mu.Lock()
		// 如果没有日志需要应用，就 Wait，释放锁并阻塞
		for rf.commitIndex <= rf.lastApplied && rf.killed() == false {
			rf.applyCond.Wait()
		}
		// 检查生命状态 防止Wait期间节点被kill
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		// 批量获取待提交的日志
		// 技巧：在锁内只负责构造消息数组，不真正发送，为了缩短持有锁的时间
		msgs := make([]ApplyMsg, 0)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,                                         // 标记这是一条普通的指令日志
				Command:      rf.log[rf.realIndex(rf.lastApplied)].Command, // 日志携带的具体内容（如 KV 操作）
				CommandIndex: rf.lastApplied,                               // 该日志在 Raft 中的索引位置
			})
		}
		// 关键：绝对不能持锁向 channel 发送数据！
		// 因为 applyCh 如果满了会阻塞，持锁阻塞会导致整个 Raft 节点死锁
		rf.mu.Unlock()
		// 在锁外发送，将消息依次送入通道，防止阻塞
		// 应用层（如 Key-Value Server）会从这个通道读取并执行指令
		for _, msg := range msgs {
			applyCh <- msg
		}
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte // 快照原始数据
}

type InstallSnapshotReply struct {
	Term int
}

// 编码 Raft 状态（不含快照本身数据，只含元数据和剩余日志）
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	return w.Bytes()
}

// Follower 处理 InstallSnapshot
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	// 任期检查
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return // 拒绝安装
	}
	// 更新任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	rf.lastHeartbeat = time.Now() // 收到 leader snapshot，重置选举时间

	// 3. 如果快照比本地已提交的还旧，没必要安装
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	// 4. 构造新的 log（必须保留 sentinel）
	// 截断 log
	if args.LastIncludedIndex < rf.getLastLogIndex() {
		// 如果快照点还在当前日志范围内，保留后续日志
		offset := args.LastIncludedIndex - rf.lastIncludedIndex

		newLog := make([]LogEntry, 1)
		newLog[0] = LogEntry{Term: args.LastIncludedTerm}
		// 把快照点之后的日志拼接到新日志后面
		newLog = append(newLog, rf.log[offset+1:]...)
		rf.log = newLog

	} else {
		// 如果快照比当前所有日志都新，直接清空旧日志，只留一个哨兵位
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}

	}
	// 5. 更新 snapshot 元数据,确保 commitIndex 和 lastApplied 不会回退
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// 修改：更新 commit/applied 确保不会回流
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}

	// 原子化持久化 Raft 状态和快照二进制数据
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Data)

	// 核心：通过 applyCh 通知 KVServer 立即覆盖状态机
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()
	// 修改：先解锁再发 channel，防止 KVServer 在读取 applyCh 时需要回头调 Raft 导致死锁
	rf.applyCh <- msg
}

// 获取真实的逻辑 Index
func (rf *Raft) realIndex(index int) int {
	return index - rf.lastIncludedIndex
}

// 获取逻辑长度
func (rf *Raft) getTotalLogLen() int {
	return len(rf.log) + rf.lastIncludedIndex
}

// GetRaftStateSize 返回当前持久化状态的大小，供 KVServer 判断是否触发快照
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. 安全检查：如果快照点已经落后于当前的 lastIncludedIndex，直接返回
	if index <= rf.lastIncludedIndex {
		return
	}

	// 2. 截断日志
	// 哨兵位逻辑：newLog[0] 存储快照点元数据，不存储实际 Command
	// 保留 index 之后的日志，index 本身变为新的 rf.log[0]
	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{Term: rf.log[rf.realIndex(index)].Term}
	// Term 必须取逻辑索引 index 对应的任期

	// 将逻辑索引 index 之后的日志全部拷贝到新日志中
	for i := index + 1; i <= rf.getLastLogIndex(); i++ {
		newLog = append(newLog, rf.log[rf.realIndex(i)])
	}

	// 更新快照元数据
	rf.lastIncludedTerm = rf.log[rf.realIndex(index)].Term
	rf.lastIncludedIndex = index
	rf.log = newLog

	// 3. 持久化 Raft 状态和快照
	rf.persistWithSnapshot(snapshot)
}

// 封装持久化动作：将状态和快照二进制数据一起存入 persister
func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftState := w.Bytes()
	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
}

// CondInstallSnapshot 简单实现：总是返回 true
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) GetLastIncludedIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastIncludedIndex
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	// 1. 初始化 Figure 2 中的持久化状态
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0} //占位
	// 2. 初始化 Figure 2 中的易失性状态
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	// 3. 初始化 Leader 专属状态
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// 4. 初始化选举计时器
	rand.Seed(time.Now().UnixNano() + int64(rf.me))
	rf.resetElectionTimeout()
	rf.readPersist(persister.ReadRaftState())
	// 5.为每一个 Peer 初始化独立的锁和条件变量
	rf.replicatorLock = make([]sync.Mutex, len(peers))
	rf.replicatorCond = make([]*sync.Cond, len(peers))
	for i := range peers {
		if i != rf.me {
			// 注意：每个 Cond 需要绑定一把锁，这里可以直接复用 rf.mu
			// 但为了并发性能，也可以给每个 replicator 准备一把小锁，
			// 将 Cond 绑定到对应 peer 的小锁上
			rf.replicatorCond[i] = sync.NewCond(&rf.replicatorLock[i])
			// 启动该 peer 的专属同步循环
			go rf.leaderReplicationLoop(i)
		}
	}
	// 6. 从持久化数据中恢复
	go rf.ticker()
	// 7. 启动日志应用协程
	go rf.applier(applyCh)
	return rf
}
