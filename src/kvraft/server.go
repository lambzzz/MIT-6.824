package kvraft

import (
	// "fmt"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string
	Key string
	Value string
	ClientId int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Data map[string]string
	Clientseq map[int64]int64
	rpcreturn map[int64]map[int64]chan string

	persister *raft.Persister
	lastAppliedIndex int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	_, _, isLeader := kv.rf.Start(Op{Type: "Get",
									 Key: args.Key,
									 ClientId: args.ClientId,
									 RequestId: args.RequestId,})
	if !isLeader{
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.rpcreturn[args.ClientId]; !ok {
		kv.rpcreturn[args.ClientId] = make(map[int64]chan string)
	}
	kv.rpcreturn[args.ClientId][args.RequestId] = make(chan string)
	applychan := kv.rpcreturn[args.ClientId][args.RequestId]
	kv.mu.Unlock()

	select {
	case reply.Value = <-applychan:
		reply.Err = OK
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrWrongLeader
		DPrintf("server %d timeout\n", kv.me)
	}
	kv.mu.Lock()
	delete(kv.rpcreturn[args.ClientId], args.RequestId)
	kv.mu.Unlock()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	_, _, isLeader := kv.rf.Start(Op{Type: args.Op,
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
		kv.rpcreturn[args.ClientId] = make(map[int64]chan string)
	}
	kv.rpcreturn[args.ClientId][args.RequestId] = make(chan string)
	applychan := kv.rpcreturn[args.ClientId][args.RequestId]
	kv.mu.Unlock()

	select {
	case <-applychan:
		reply.Err = OK
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrWrongLeader
		DPrintf("server %d timeout\n", kv.me)
	}
	kv.mu.Lock()
	delete(kv.rpcreturn[args.ClientId], args.RequestId)
	kv.mu.Unlock()

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyOploop() {
	for {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			op, ok := applyMsg.Command.(Op)
			if !ok {
				log.Fatal("Command is not of type Op")
			}
			kv.mu.Lock()
			if kv.Clientseq[op.ClientId] >= op.RequestId {
				DPrintf("Command %v has been applied", op.RequestId)
			} else {
				kv.Clientseq[op.ClientId] = op.RequestId
				switch op.Type {
				case "Get":
					
				case "Put":
					kv.Data[op.Key] = op.Value
				case "Append":
					kv.Data[op.Key] += op.Value
				default:
					DPrintf("op.Type error: %v", op.Type)
				}
			}

			kv.lastAppliedIndex = applyMsg.CommandIndex

			if _, ok = kv.rpcreturn[op.ClientId][op.RequestId]; ok {
				if op.Type == "Get" {
					select {
					case kv.rpcreturn[op.ClientId][op.RequestId] <-kv.Data[op.Key]:
					default:
					}
				} else {
					select {
					case kv.rpcreturn[op.ClientId][op.RequestId] <-"":
					default:
					}
				}
			}
			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			kv.mu.Lock()
			var data map[string]string
			var clientseq map[int64]int64
			buf := bytes.NewBuffer(applyMsg.Snapshot)
			decoder := labgob.NewDecoder(buf)
			if decoder.Decode(&data) != nil ||
			   decoder.Decode(&clientseq) != nil {
			} else {
				kv.Data = data
				kv.Clientseq = clientseq
			}
			kv.lastAppliedIndex = applyMsg.SnapshotIndex
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) snapshotloop() {
	for {
		kv.mu.Lock()
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
			buf := new(bytes.Buffer)
			encoder := labgob.NewEncoder(buf)
			encoder.Encode(kv.Data)
			encoder.Encode(kv.Clientseq)
			data := buf.Bytes()
			kv.rf.Snapshot(kv.lastAppliedIndex, data)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

}

func (kv *KVServer) recoverSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	snapshot := kv.persister.ReadSnapshot()
	var data map[string]string
	var clientseq map[int64]int64
	buf := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buf)
	if decoder.Decode(&data) != nil ||
	   decoder.Decode(&clientseq) != nil {

	} else {
		kv.Data = data
		kv.Clientseq = clientseq
	}
}

// func (kv *KVServer) cleancache() {
// 	for {
// 		kv.mu.Lock()
// 		for clientId := range kv.rpcreturn {
// 			if len(kv.rpcreturn[clientId]) == 0 {
// 				delete(kv.rpcreturn, clientId)
// 				delete(kv.Clientseq, clientId)
// 			}
// 		}
// 		kv.mu.Unlock()
// 		time.Sleep(1 * time.Minute)
// 	}
// }

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.Data = make(map[string]string)
	kv.Clientseq = make(map[int64]int64)
	kv.rpcreturn = make(map[int64]map[int64]chan string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	// You may need initialization code here.
	kv.recoverSnapshot()
	go kv.applyOploop()
	go kv.snapshotloop()
	// go kv.cleancache()

	return kv
}
