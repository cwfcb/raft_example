package raft_example

import (
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

// 节点结构定义
type Node struct {
	Connect bool
	Address string
}

func NewNode(address string) *Node {
	return &Node{Address: address}
}

// 节点状态定义
type nodeState int32

const (
	_ nodeState = iota
	Follower
	Candidate
	Leader
)

var (
	heartTimeout        = 300 // 心跳超时时间300ms
	randHeartTimeoutAdd = 200 // 心跳超时时间最大累加值

	electionTimeout        = 300 // 选举超时时间300ms
	randElectionTimeoutAdd = 200 // 选举超时时间最大累加值
	heartSleepTime         = 100 // leader心跳发送间隔
)

type Raft struct {
	Self        int32           // 当前节点id
	Peers       map[int32]*Node // 除了当前节点外，其它节点信息
	NodeState   nodeState       // 当前节点状态
	CurrentTerm int32           // 当前节点的任期
	VotedFor    int32           // 当前任期票投给了谁，-1表示没有投票
	VoteCount   int32           // 当前任期获得的投票数量
	HeartBeatC  chan bool       // 心跳检测机制
	ToLeaderC   chan bool       //
}

func (r *Raft) Start() {
	// 初始化raft节点
	r.NodeState = Follower
	r.CurrentTerm = 0
	r.VotedFor = -1
	r.HeartBeatC = make(chan bool)
	r.ToLeaderC = make(chan bool)

	// 节点状态变更以及RPC通讯
	go func() {
		rand.Seed(time.Now().UnixNano())
		// 不间断处理节点行为和RPC
		for {
			switch r.NodeState {
			case Follower:
				select {
				case <-r.HeartBeatC:
					log.Printf("follower-%d reveived heartbeat\n", r.Self)
				case <-time.After(time.Duration(rand.Intn(randHeartTimeoutAdd)+heartTimeout) * time.Millisecond):
					r.NodeState = Candidate
				}
			case Candidate:
				r.CurrentTerm++
				r.VotedFor = r.Self
				r.VoteCount = 1
				go r.broadVoteRequest()
				select {
				case <-time.After(time.Duration(rand.Intn(randElectionTimeoutAdd)+electionTimeout) * time.Millisecond):
					r.NodeState = Follower
				case <-r.ToLeaderC:
					r.NodeState = Leader
					log.Printf("node-%d is leader now for the %d term", r.Self, r.CurrentTerm)
				}
			case Leader:
				r.broadHeartbeatRequest()
				time.Sleep(time.Duration(heartSleepTime) * time.Millisecond)
			}
		}
	}()
	return
}

func (r *Raft) broadVoteRequest() {
	req := &VoteReq{
		Term:        r.CurrentTerm,
		CandidateID: r.Self,
	}
	for _, peer := range r.Peers {
		var resp VoteResp
		go func(addr string) {
			r.sendVoteRequest(addr, req, &resp)
		}(peer.Address)
	}
}

func (r *Raft) sendVoteRequest(addr string, req *VoteReq, resp *VoteResp) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		// 如果节点下线，可能连接不上
		log.Printf("dial %s error: %v", addr, err)
		return
	}
	defer client.Close()

	err = client.Call("Raft.VoteRequest", req, resp)
	if err != nil {
		log.Printf("call Raft.VoteRequest error: %v", err)
		return
	}
	// 其它节点的任期比candidate大，转成follower
	if resp.Term > r.CurrentTerm {
		r.CurrentTerm = resp.Term
		r.NodeState = Follower
		r.VotedFor = -1
		return
	}
	// 获得投票
	if resp.VotedGranted {
		r.VoteCount++
		if r.VoteCount > int32(len(r.Peers)/2+1) {
			r.ToLeaderC <- true
		}
	}
}

// 投票处理
func (r *Raft) VoteRequest(req *VoteReq, resp *VoteResp) error {

	if req.Term < r.CurrentTerm {
		resp.Term = r.CurrentTerm
		resp.VotedGranted = false
		return nil
	}
	if r.VotedFor == -1 {
		r.CurrentTerm = req.Term
		r.VotedFor = req.CandidateID

		resp.Term = r.CurrentTerm
		resp.VotedGranted = true
		return nil
	}
	resp.Term = r.CurrentTerm
	resp.VotedGranted = false
	return nil
}

func (r *Raft) broadHeartbeatRequest() {
	for _, peer := range r.Peers {
		req := &HeartbeatReq{
			Term:     r.CurrentTerm,
			LeaderID: r.Self,
		}
		go func(addr string) {
			var resp HeartbeatResp
			r.sendHeartBeatRequest(addr, req, &resp)
		}(peer.Address)
	}
}

func (r *Raft) sendHeartBeatRequest(addr string, req *HeartbeatReq, resp *HeartbeatResp) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		// 如果节点下线，可能连接不上
		log.Printf("dial %s error: %v", addr, err)
		return
	}
	defer client.Close()

	err = client.Call("Raft.HeartBeatRequest", req, resp)
	if err != nil {
		log.Printf("call Raft.VoteRequest error: %v", err)
		return
	}
	if resp.Term > r.CurrentTerm {
		r.CurrentTerm = resp.Term
		r.NodeState = Follower
		r.VotedFor = -1
	}

}

func (r *Raft) HeartBeatRequest(req *HeartbeatReq, resp *HeartbeatResp) error {
	if req.Term < r.CurrentTerm {
		resp.Term = r.CurrentTerm
		return nil
	}
	if req.Term > r.CurrentTerm {
		r.CurrentTerm = req.Term
		r.NodeState = Follower
		r.VotedFor = -1
	}
	resp.Term = r.CurrentTerm
	r.HeartBeatC <- true
	return nil
}

func (r *Raft)RegisterRPC(port string)  {
	rpc.Register(r)
	rpc.HandleHTTP()
	listen, err := net.Listen("tcp", port)
	if err != nil{
		log.Fatalf("listen error: %v", err)
	}
	go http.Serve(listen, nil)
}
