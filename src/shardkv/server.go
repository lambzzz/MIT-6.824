package shardkv

import (
	"bytes"
	// "fmt"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Iscfg bool
	Type string
	ShardId int
	ConfigId int

	Key string
	Value string

	ClientId int64
	RequestId int64

	MoveShard map[string]string
	Config shardctrler.Config
	ClientSeq map[int64]int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Data []map[string]string

	persister *raft.Persister
	mck *shardctrler.Clerk

	Clientseq map[int64]int64
	ConfigSeq map[int64]int64
	rpcreturn map[int64]map[int64]chan Applyresult

	lastAppliedIndex int
	config shardctrler.Config
	ownedShards []bool
}

type Applyresult struct {
	Err Err
	Value string
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	_, _, isLeader := kv.rf.Start(Op{Iscfg: false,
									 Type: "Get",
									 ShardId: args.ShardId,
									 ConfigId: args.ConfigId,
									 Key: args.Key,
									 ClientId: args.ClientId,
									 RequestId: args.RequestId,})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	if _, ok := kv.rpcreturn[args.ClientId]; !ok {
		kv.rpcreturn[args.ClientId] = make(map[int64]chan Applyresult)
	}
	kv.rpcreturn[args.ClientId][args.RequestId] = make(chan Applyresult)
	applychan := kv.rpcreturn[args.ClientId][args.RequestId]
	kv.mu.Unlock()

	select {
	case applyresult := <-applychan:
		if applyresult.Err == OK {
			reply.Value = applyresult.Value
			reply.Err = OK
		} else if applyresult.Err == ErrWrongGroup {
			reply.Err = ErrWrongGroup
		}
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.rpcreturn[args.ClientId], args.RequestId)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	_, _, isLeader := kv.rf.Start(Op{Iscfg: false,
									 Type: args.Op,
									 ShardId: args.ShardId,
									 ConfigId: args.ConfigId,
									 Key: args.Key,
									 Value: args.Value,
									 ClientId: args.ClientId,
									 RequestId: args.RequestId,})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.rpcreturn[args.ClientId]; !ok {
		kv.rpcreturn[args.ClientId] = make(map[int64]chan Applyresult)
	}
	kv.rpcreturn[args.ClientId][args.RequestId] = make(chan Applyresult)
	applychan := kv.rpcreturn[args.ClientId][args.RequestId]
	kv.mu.Unlock()

	select {
	case applyresult := <-applychan:
		if applyresult.Err == OK {
			reply.Err = OK
		} else if applyresult.Err == ErrWrongGroup {
			reply.Err = ErrWrongGroup
		}
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.rpcreturn[args.ClientId], args.RequestId)
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) applyOploop() {
	for {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			op, ok := applyMsg.Command.(Op)
			if !ok {
				continue
			}
			kv.mu.Lock()
			rpcreturnval := Applyresult{Err: OK, Value: ""}
			if !op.Iscfg && kv.Clientseq[op.ClientId] < op.RequestId {
				kv.Clientseq[op.ClientId] = op.RequestId
				switch op.Type {
				case "Get":
					if kv.ownedShards[op.ShardId] {
						// 重复的Get操作不应该被RequestId影响而返回空值，因此不在此处赋值
					} else {
						rpcreturnval.Err = ErrWrongGroup
						kv.Clientseq[op.ClientId] = op.RequestId-1
					}
				case "Put":
					if kv.ownedShards[op.ShardId] {
						kv.Data[op.ShardId][op.Key] = op.Value
					} else {
						rpcreturnval.Err = ErrWrongGroup
						kv.Clientseq[op.ClientId] = op.RequestId-1
					}
				case "Append":
					if kv.ownedShards[op.ShardId] {
						kv.Data[op.ShardId][op.Key] += op.Value
					} else {
						rpcreturnval.Err = ErrWrongGroup
						kv.Clientseq[op.ClientId] = op.RequestId-1
					}
				default:
				}

			} else if op.Iscfg && kv.ConfigSeq[op.ClientId] < op.RequestId {
				kv.ConfigSeq[op.ClientId] = op.RequestId
				switch op.Type {
				case "LeaveShard":
					if kv.config.Num == int(op.RequestId) {
						kv.Data[op.ShardId] = make(map[string]string)
						kv.ownedShards[op.ShardId] = false
					}
				case "MoveShard":
					if kv.config.Num == int(op.RequestId) {
						kv.ownedShards[op.ShardId] = false
						if _, isLeader := kv.rf.GetState(); isLeader{
							sendgid := kv.config.Shards[op.ShardId]
							servers := kv.config.Groups[sendgid]
							var sharddata map[string]string = make(map[string]string)
							var clientseq map[int64]int64 = make(map[int64]int64)
							for key, value := range kv.Data[op.ShardId] {
								sharddata[key] = value
							}
							for key, value := range kv.Clientseq {
								clientseq[key] = value
							}

							go kv.SendTransShard(servers, sendgid, kv.config.Num, op.ShardId, sharddata, clientseq)
						}
						kv.Data[op.ShardId] = make(map[string]string)
					}
				case "JoinShard":
					if kv.config.Num == int(op.RequestId) && !kv.ownedShards[op.ShardId] {
						kv.Data[op.ShardId] = make(map[string]string)
						for key, value := range op.MoveShard {
							kv.Data[op.ShardId][key] = value
						}
						for key, value := range op.ClientSeq {
							if value > kv.Clientseq[key] {
								kv.Clientseq[key] = value
							}
						}
						kv.ownedShards[op.ShardId] = true
					} else if kv.config.Num < int(op.RequestId) {
						rpcreturnval.Err = ErrWrongGroup
						// 此处接收方处于旧配置需要重试，由于以ConfigId作为序列号，因此不更新RequestId
						kv.ConfigSeq[op.ClientId] = op.RequestId-1
					}
				case "SyncConfig":
					if op.Config.Num > kv.config.Num {
						kv.config = op.Config
					}
				default:
				}
			}

			kv.lastAppliedIndex = applyMsg.CommandIndex

			if _, ok = kv.rpcreturn[op.ClientId][op.RequestId]; ok {
				if op.Type == "Get" {
					// Get操作与其他不同，重复操作也需要返回value；首次Get操作成功而RPC返回失败，此时再次Get视为重复操作；重复操作时需要再次判断是否在当前配置下
					// 若为首次Get操作，该部分与上面代码不冲突；若为重复Get操作且前后Err不一致，则当前config比Get新，Get操作不需要序列号回退
					if kv.ownedShards[op.ShardId] {
						rpcreturnval.Err = OK
						rpcreturnval.Value = kv.Data[op.ShardId][op.Key]
					} else {
						rpcreturnval.Err = ErrWrongGroup
					}
				}
				select {
				case kv.rpcreturn[op.ClientId][op.RequestId] <-rpcreturnval:
				default:
				}
			}
			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			kv.mu.Lock()
			var data []map[string]string
			var clientseq map[int64]int64
			var config shardctrler.Config
			var ownedShards []bool
			buf := bytes.NewBuffer(applyMsg.Snapshot)
			decoder := labgob.NewDecoder(buf)
			if decoder.Decode(&data) != nil ||
			   decoder.Decode(&clientseq) != nil ||
			   decoder.Decode(&config) != nil ||
			   decoder.Decode(&ownedShards) != nil {
			} else {
				kv.Data = data
				kv.Clientseq = clientseq
				kv.config = config
				kv.ownedShards = ownedShards
			}
			kv.lastAppliedIndex = applyMsg.SnapshotIndex
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) snapshotloop() {
	for {
		kv.mu.Lock()
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
			buf := new(bytes.Buffer)
			encoder := labgob.NewEncoder(buf)
			encoder.Encode(kv.Data)
			encoder.Encode(kv.Clientseq)
			encoder.Encode(kv.config)
			encoder.Encode(kv.ownedShards)
			data := buf.Bytes()
			kv.rf.Snapshot(kv.lastAppliedIndex, data)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

}

func (kv *ShardKV) recoverSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	snapshot := kv.persister.ReadSnapshot()
	var data []map[string]string
	var clientseq map[int64]int64
	var config shardctrler.Config
	var ownedShards []bool
	buf := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buf)
	if decoder.Decode(&data) != nil ||
	   decoder.Decode(&clientseq) != nil ||
	   decoder.Decode(&config) != nil ||
	   decoder.Decode(&ownedShards) != nil {

	} else {
		kv.Data = data
		kv.Clientseq = clientseq
		kv.config = config
		kv.ownedShards = ownedShards
	}
}

func (kv *ShardKV) updateConfig(){
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState();!isLeader{
		kv.mu.Unlock()
		return
	}

	config := kv.mck.Query(kv.config.Num + 1)
	if config.Num > kv.config.Num {
		// 检查前一次配置是否完成
		for shardId := 0; shardId < len(kv.config.Shards); shardId++ {
			if (kv.config.Shards[shardId] == kv.gid && !kv.ownedShards[shardId]) ||
			   (kv.ownedShards[shardId] && kv.config.Shards[shardId] != kv.gid) {
				kv.mu.Unlock()
				return
			}
		}

		kv.rf.Start(Op{Iscfg: true,
					   Type: "SyncConfig",
					   Config: config,
					   ClientId: int64(kv.gid),
					   RequestId: int64(config.Num)})

		for shardId := 0; shardId < len(kv.config.Shards); shardId++ {
			if kv.config.Shards[shardId] == kv.gid {
				// 当前持有的shard
				if config.Shards[shardId] == kv.gid {
					// 持有权不变
					continue
				} else if config.Shards[shardId] == 0 {
					// 不持有
					kv.rf.Start(Op{Iscfg: true,
								   Type: "LeaveShard",
								   ShardId: shardId,
								   ConfigId: config.Num,
								   ClientId: int64(shardId),
								   RequestId: int64(config.Num)})
				} else {
					// 转移
					kv.rf.Start(Op{Iscfg: true,
								   Type: "MoveShard",
								   ShardId: shardId,
								   ConfigId: config.Num,
								   ClientId: int64(shardId),
								   RequestId: int64(config.Num)})
				}
				
			} else if kv.config.Shards[shardId] == 0 && config.Shards[shardId] == kv.gid {
				// 无人持有->新持有
				kv.rf.Start(Op{Iscfg: true,
							   Type: "JoinShard",
							   ShardId: shardId,
							   ConfigId: config.Num,
							   MoveShard: make(map[string]string),
							   ClientId: int64(shardId),
							   RequestId: int64(config.Num)})
			}
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) updateConfigloop(){
	for {
		kv.updateConfig()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) TransShard(args *TransShardArgs, reply *TransShardReply) {
	kv.mu.Lock()

	if _, isLeader := kv.rf.GetState();!isLeader{
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if args.ConfigId < kv.config.Num {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	_, _, isLeader := kv.rf.Start(Op{Iscfg: true,
									 Type: "JoinShard",
									 ShardId: args.ShardId,
									 ConfigId: args.ConfigId,
									 MoveShard: args.Data,
									 ClientSeq: args.ClientSeq,
									 ClientId: int64(args.ShardId),
									 RequestId: int64(args.ConfigId)})
	if !isLeader{
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	if _, ok := kv.rpcreturn[int64(args.ShardId)]; !ok {
		kv.rpcreturn[int64(args.ShardId)] = make(map[int64]chan Applyresult)
	}
	kv.rpcreturn[int64(args.ShardId)][int64(args.ConfigId)] = make(chan Applyresult)
	applychan := kv.rpcreturn[int64(args.ShardId)][int64(args.ConfigId)]
	kv.mu.Unlock()

	select {
	case applyresult := <-applychan:
		if applyresult.Err == OK {
			reply.Err = OK
		} else if applyresult.Err == ErrWrongGroup {
			reply.Err = ErrWrongGroup
		}
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.rpcreturn[int64(args.ShardId)], int64(args.ConfigId))
	kv.mu.Unlock()
}

func (kv *ShardKV) SendTransShard(servers []string, gid int, configId int, shardId int, data map[string]string, clientseq map[int64]int64) {
	args := &TransShardArgs{}
	args.GID = gid
	args.ConfigId = configId
	args.ShardId = shardId
	args.Data = data
	args.ClientSeq = clientseq
	for {
		for si := 0; si < len(servers); si = (si + 1) % len(servers) {
			srv := kv.make_end(servers[si])
			reply := &TransShardReply{}
			ok := srv.Call("ShardKV.TransShard", args, reply)
			if ok && reply.Err == OK {
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				break
			}
			// ... not ok, or ErrWrongLeader
		}
		time.Sleep(100 * time.Millisecond)
	}
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.persister = persister
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.Data = make([]map[string]string, shardctrler.NShards)
	for i := range kv.Data {
		kv.Data[i] = make(map[string]string)
	}
	kv.Clientseq = make(map[int64]int64)
	kv.ConfigSeq = make(map[int64]int64)
	kv.rpcreturn = make(map[int64]map[int64]chan Applyresult)
	kv.config = kv.mck.Query(0)
	kv.ownedShards = make([]bool, shardctrler.NShards)

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.recoverSnapshot()
	go kv.applyOploop()
	go kv.snapshotloop()
	go kv.updateConfigloop()

	return kv
}
