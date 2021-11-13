package main

import (
	"flag"
	"github.com/cwfcb/raft_example"
	"strings"
)

func main() {
	port := flag.String("port", ":9091", "rpc listen port")
	cluster := flag.String("cluster", "127.0.0.1:9091", "comma sep")
	id := flag.Int("id", 1, "node ID")

	flag.Parse()
	clusters := strings.Split(*cluster, ",")
	nodes := make(map[int32]*raft_example.Node, len(clusters))
	for i, cluster := range clusters {
		nodes[int32(i)] = raft_example.NewNode(cluster)
	}
	raft := &raft_example.Raft{
		Self:  int32(*id),
		Peers: nodes,
	}
	raft.RegisterRPC(*port)
	raft.Start()
}
