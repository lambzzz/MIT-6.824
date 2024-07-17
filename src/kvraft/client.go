package kvraft

import (
	"crypto/rand"
	// "fmt"
	"math/big"

	"6.824/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	requestId int64
	lastLeader int
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
	ck.lastLeader = 0
	ck.clientId = nrand()
	ck.requestId = 1
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	clientId := ck.clientId
	requestId := ck.requestId
	lastLeader := ck.lastLeader
	serverslen := len(ck.servers)

	args := GetArgs{Key: key, 
					ClientId: clientId, 
					RequestId: requestId}
	reply := GetReply{}
	for {
		reply = GetReply{}
		ok := ck.servers[lastLeader].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.lastLeader = lastLeader
				ck.requestId++
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.lastLeader = lastLeader
				ck.requestId++
				return ""
			} else {
				lastLeader = (lastLeader + 1) % serverslen
			}
		} else {
			lastLeader = (lastLeader + 1) % serverslen
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	clientId := ck.clientId
	requestId := ck.requestId
	lastLeader := ck.lastLeader
	serverslen := len(ck.servers)

	args := PutAppendArgs{Key: key, 
						  Value: value, 
						  Op: op, 
						  ClientId: clientId, 
						  RequestId: requestId}
	reply := PutAppendReply{}
	for {
		reply = PutAppendReply{}
		ok := ck.servers[lastLeader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			ck.lastLeader = lastLeader
			ck.requestId++
			return
		} else {
			lastLeader = (lastLeader + 1) % serverslen
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
