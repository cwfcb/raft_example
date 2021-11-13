package raft_example

type VoteReq struct {
	Term        int32 //   当前任期
	CandidateID int32 // 候选人id
}

type VoteResp struct {
	Term         int32 // 其它节点的任期号，如果大于Candidate任期，需要候选人更新
	VotedGranted bool  // Candidate是否获得投票
}

type HeartbeatReq struct {
	Term     int32 // 当前leader任期
	LeaderID int32 // 当前leader id
}

type HeartbeatResp struct {
	Term int32 // 当前follower的任期
}
