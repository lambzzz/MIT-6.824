package shardctrler

import (
	// "fmt"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	Clientseq map[int64]int64
	rpcreturn map[int64]map[int64]chan Config
}


type Op struct {
	// Your data here.
	Type string
	ClientId int64
	RequestId int64

	JoinServers   map[int][]string
	LeaveGIDs  []int
	MoveShard int
	MoveGID int
	QueryNum int
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	_, _, isLeader := sc.rf.Start(Op{Type: "Join",
									 JoinServers: args.Servers,
									 ClientId: args.ClientId,
									 RequestId: args.RequestId})
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	if _, ok := sc.rpcreturn[args.ClientId]; !ok {
		sc.rpcreturn[args.ClientId] = make(map[int64]chan Config)
	}
	sc.rpcreturn[args.ClientId][args.RequestId] = make(chan Config)
	applychan := sc.rpcreturn[args.ClientId][args.RequestId]
	sc.mu.Unlock()

	select {
	case <-applychan:
		reply.WrongLeader = false
		reply.Err = OK
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = "timeout"
	}
	sc.mu.Lock()
	delete(sc.rpcreturn[args.ClientId], args.RequestId)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	_, _, isLeader := sc.rf.Start(Op{Type: "Leave",
									 LeaveGIDs: args.GIDs,
									 ClientId: args.ClientId,
									 RequestId: args.RequestId})
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	if _, ok := sc.rpcreturn[args.ClientId]; !ok {
		sc.rpcreturn[args.ClientId] = make(map[int64]chan Config)
	}
	sc.rpcreturn[args.ClientId][args.RequestId] = make(chan Config)
	applychan := sc.rpcreturn[args.ClientId][args.RequestId]
	sc.mu.Unlock()

	select {
	case <-applychan:
		reply.WrongLeader = false
		reply.Err = OK
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = "timeout"
	}
	sc.mu.Lock()
	delete(sc.rpcreturn[args.ClientId], args.RequestId)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	_, _, isLeader := sc.rf.Start(Op{Type: "Move",
									 MoveShard: args.Shard,
									 MoveGID: args.GID,
									 ClientId: args.ClientId,
									 RequestId: args.RequestId})
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	if _, ok := sc.rpcreturn[args.ClientId]; !ok {
		sc.rpcreturn[args.ClientId] = make(map[int64]chan Config)
	}
	sc.rpcreturn[args.ClientId][args.RequestId] = make(chan Config)
	applychan := sc.rpcreturn[args.ClientId][args.RequestId]
	sc.mu.Unlock()

	select {
	case <-applychan:
		reply.WrongLeader = false
		reply.Err = OK
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = "timeout"
	}
	sc.mu.Lock()
	delete(sc.rpcreturn[args.ClientId], args.RequestId)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	_, _, isLeader := sc.rf.Start(Op{Type: "Query",
									 QueryNum: args.Num,
									 ClientId: args.ClientId,
									 RequestId: args.RequestId})
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	if _, ok := sc.rpcreturn[args.ClientId]; !ok {
		sc.rpcreturn[args.ClientId] = make(map[int64]chan Config)
	}
	sc.rpcreturn[args.ClientId][args.RequestId] = make(chan Config)
	applychan := sc.rpcreturn[args.ClientId][args.RequestId]
	sc.mu.Unlock()

	select {
	case reply.Config = <-applychan:
		reply.WrongLeader = false
		reply.Err = OK
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = "timeout"
	}
	sc.mu.Lock()
	delete(sc.rpcreturn[args.ClientId], args.RequestId)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) applyOploop() {
	for {
		applyMsg := <-sc.applyCh
		if applyMsg.CommandValid {
			op, ok := applyMsg.Command.(Op)
			if !ok {
				continue
			}
			sc.mu.Lock()
			cfg := Config{}
			if sc.Clientseq[op.ClientId] >= op.RequestId {
				
			} else {
				sc.Clientseq[op.ClientId] = op.RequestId
				switch op.Type {
				case "Join":
					sc.JoinOp(&op)
				case "Leave":
					sc.LeaveOp(&op)
				case "Move":
					sc.MoveOp(&op)
				case "Query":
					cfg = sc.QueryOp(&op)
				default:
				}
			}

			if _, ok = sc.rpcreturn[op.ClientId][op.RequestId]; ok {
				select {
				case sc.rpcreturn[op.ClientId][op.RequestId] <-cfg:
				default:
				}
			}
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) JoinOp(op *Op) {
	// 新建一个Config，将原来的Config复制过来
	cfg := Config{}
	cfg.Num = len(sc.configs)
	cfg.Shards = sc.configs[len(sc.configs)-1].Shards
	originalGroups := sc.configs[len(sc.configs)-1].Groups
	cfg.Groups = make(map[int][]string)
	for gid, servers := range originalGroups {
		cfg.Groups[gid] = servers
	}

	// 分组信息
	groupnumcur := len(cfg.Groups) + len(op.JoinServers)
	for gid, servers := range op.JoinServers {
		cfg.Groups[gid] = servers
	}
	if len(cfg.Groups) - len(op.JoinServers) >= NShards {
		sc.configs = append(sc.configs, cfg)
		return
	}
	shardspergroup := NShards / groupnumcur
	numovershard := NShards % groupnumcur

	// 统计当前每个组的Shards数目
	maps := make(map[int]int)
	for gid := range cfg.Groups{
		maps[gid] = 0
	}
	for i := 0; i < NShards; i++ {
		if cfg.Shards[i] != 0 {
			maps[cfg.Shards[i]]++
		}
	}

	// 将原来的Shards按照Shards数目从小到大排序
	var ss [][]int
	for k, v := range maps {
		ss = append(ss, []int{k, v})
	}
	sort.Slice(ss, func(i, j int) bool {
		if ss[i][1] != ss[j][1] {
			return ss[i][1] < ss[j][1]
		} else {
			return ss[i][0] < ss[j][0]
		}
	})
	// 数量为0的组放在最后
	for i := 0; i < len(ss); i++ {
		if ss[i][1] > 0 {
			ss = append(ss[i:], ss[:i]...)
			break
		}
	}

	// 统计Shards移入和移出的组与数目
	moveout, movein := make(map[int]int), make([][2]int, 0)
	for i := 0; i < numovershard; i++ {
		ss[i][1] -= shardspergroup + 1
		if ss[i][1] > 0 {
			moveout[ss[i][0]] = ss[i][1]
		} else if ss[i][1] < 0 {
			movein = append(movein, [2]int{ss[i][0], ss[i][1]})
		}
	}
	for i := numovershard; i < groupnumcur; i++ {
		ss[i][1] -= shardspergroup
		if ss[i][1] > 0 {
			moveout[ss[i][0]] = ss[i][1]
		} else if ss[i][1] < 0 {
			movein = append(movein, [2]int{ss[i][0], ss[i][1]})
		}
	}
	
	// 重新分配Shards
	for i := 0; i < NShards; i++ {
		if _, ok := moveout[cfg.Shards[i]]; ok {
			moveout[cfg.Shards[i]]--
			if moveout[cfg.Shards[i]] == 0 {
				delete(moveout, cfg.Shards[i])
			}
			cfg.Shards[i] = 0
		}
	}
	for i := 0; i < NShards; i++ {
		if cfg.Shards[i] == 0 {
			cfg.Shards[i] = movein[0][0]
			movein[0][1]++
			if movein[0][1] == 0 {
				movein = movein[1:]
			}
		}
	}
	sc.configs = append(sc.configs, cfg)
}

func (sc *ShardCtrler) LeaveOp(op *Op) {
	// 新建一个Config，将原来的Config复制过来 
	cfg := Config{}
	cfg.Num = len(sc.configs)
	cfg.Shards = sc.configs[len(sc.configs)-1].Shards
	originalGroups := sc.configs[len(sc.configs)-1].Groups
	cfg.Groups = make(map[int][]string)
	for gid, servers := range originalGroups {
		cfg.Groups[gid] = servers
	}

	// 分组信息
	delgids := op.LeaveGIDs
	groupnumcur := len(cfg.Groups) - len(op.LeaveGIDs)
	for _, gid := range op.LeaveGIDs {
		delete(cfg.Groups, gid)
	}
	if groupnumcur == 0 {
		cfg.Shards = [NShards]int{}
		sc.configs = append(sc.configs, cfg)
		return
	}
	shardspergroup := NShards / groupnumcur
	numovershard := NShards % groupnumcur

	// 统计当前每个组的Shards数目
	maps := make(map[int]int)
	for gid := range cfg.Groups{
		maps[gid] = 0
	}
	for i := 0; i < NShards; i++ {
		maps[cfg.Shards[i]]++
	}

	// 将原来的Shards按照Shards数目从大到小排序
	var ss [][]int
	mappos, mapneg := make(map[int]int), make([][2]int, 0)
	for i := 0; i < len(delgids); i++ { 
		if maps[delgids[i]] > 0 {
			mappos[delgids[i]] = maps[delgids[i]]
		}
		delete(maps, delgids[i])
	}
	for k, v := range maps {
		ss = append(ss, []int{k, v})
	}
	sort.Slice(ss, func(i, j int) bool {
		if ss[i][1] != ss[j][1] {
			return ss[i][1] > ss[j][1]
		} else {
			return ss[i][0] > ss[j][0]
		}
	})
	// 数量为0的组放在最后
	for i := 0; i < len(ss); i++ {
		if ss[i][1] > 0 {
			ss = append(ss[i:], ss[:i]...)
			break
		}
	}

	// 统计Shards移入和移出的组与数目
	for i := 0; i < groupnumcur-numovershard; i++ {
		ss[i][1] -= shardspergroup
		if ss[i][1] > 0 {
			mappos[ss[i][0]] = ss[i][1]
		} else if ss[i][1] < 0 {
			mapneg = append(mapneg, [2]int{ss[i][0], ss[i][1]})
		}
	}
	for i := groupnumcur-numovershard; i < groupnumcur; i++ {
		ss[i][1] -= shardspergroup + 1
		if ss[i][1] > 0 {
			mappos[ss[i][0]] = ss[i][1]
		} else if ss[i][1] < 0 {
			mapneg = append(mapneg, [2]int{ss[i][0], ss[i][1]})
		}
	}
	
	// 重新分配Shards
	for i := 0; i < NShards; i++ {
		if _, ok := mappos[cfg.Shards[i]]; ok {
			mappos[cfg.Shards[i]]--
			if mappos[cfg.Shards[i]] == 0 {
				delete(mappos, cfg.Shards[i])
			}
			cfg.Shards[i] = 0
		}
	}
	for i := 0; i < NShards; i++ {
		if cfg.Shards[i] == 0 {
			cfg.Shards[i] = mapneg[0][0]
			mapneg[0][1]++
			if mapneg[0][1] == 0 {
				mapneg = mapneg[1:]
			}
		}
	}
	sc.configs = append(sc.configs, cfg)
}

func (sc *ShardCtrler) MoveOp(op *Op) {
	// 新建一个Config，将原来的Config复制过来 
	cfg := Config{}
	cfg.Num = len(sc.configs)
	cfg.Shards = sc.configs[len(sc.configs)-1].Shards
	originalGroups := sc.configs[len(sc.configs)-1].Groups
	cfg.Groups = make(map[int][]string)
	for gid, servers := range originalGroups {
		cfg.Groups[gid] = servers
	}

	cfg.Shards[op.MoveShard] = op.MoveGID
	sc.configs = append(sc.configs, cfg)
}

func (sc *ShardCtrler) QueryOp(op *Op) Config {
	cfgidx := op.QueryNum
	if cfgidx == -1 || cfgidx >= len(sc.configs) {
		cfgidx = len(sc.configs) - 1
	}
	return sc.configs[cfgidx]
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.Clientseq = make(map[int64]int64)
	sc.rpcreturn = make(map[int64]map[int64]chan Config)

	go sc.applyOploop()

	return sc
}
