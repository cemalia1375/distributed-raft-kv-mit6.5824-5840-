package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId       int64
	seqId          int
	recentLeaderId int // 缓存上次成功的 Leader ID
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand() // 生成唯一 ID
	ck.seqId = 0          // 序列号从 0 开始
	ck.recentLeaderId = 0 // 默认从 0 号开始尝试
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seqId++ // Get 也需要递增序列号以保证线性一致性
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		reply := GetReply{}
		// 1. 使用缓存的 Leader ID 发送请求
		ok := ck.servers[ck.recentLeaderId].Call("KVServer.Get", &args, &reply)

		// 2. 只有成功且不是 ErrWrongLeader 才返回
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			return reply.Value
		}

		// 3. 失败（超时、网络断开、或者对方不是 Leader）
		ck.recentLeaderId = (ck.recentLeaderId + 1) % len(ck.servers)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++ // 每次请求递增序列号
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		reply := PutAppendReply{}
		// 尝试发送给当前认定的 Leader
		ok := ck.servers[ck.recentLeaderId].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.Err == OK {
			return
		}
		if ok && reply.Err == ErrNoKey {
			return
		}

		// 如果返回 ErrWrongLeader 或 RPC 失败（超时等）
		ck.recentLeaderId = (ck.recentLeaderId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
