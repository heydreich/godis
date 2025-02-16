package cluster

import (
	"godis/datastruct/dict"
	"godis/interface/cluster"
	"godis/lib/consistenthash"

	"hash/crc32"

	"github.com/bwmarrin/snowflake"
)

const (
	replicasNum = 16
)

type Cluster struct {
	self    string                        // 本机地址，如 127.0.0.1:6107
	peers   cluster.PeerPicker            // 一致性哈希，用于选择节点
	getters map[string]cluster.PeerGetter // 用于和远程节点通信

	idGenerator    *snowflake.Node  // snowflake id生成器，用于生成分布式事务的id
	transactionMap *dict.SimpleDict // 记录所有的分布式事务（本地）
	coordinatorMap *dict.SimpleDict // 记录事务协调者
}

func NewCluster(self string) *Cluster {
	if self == "" {
		return nil
	}

	node, _ := snowflake.NewNode(int64(crc32.ChecksumIEEE([]byte(self))) % 1024)

	return &Cluster{
		self:    self,
		peers:   consistenthash.New(replicasNum, nil),
		getters: make(map[string]cluster.PeerGetter),

		idGenerator:    node,
		transactionMap: dict.MakeSimpleDict(),
		coordinatorMap: dict.MakeSimpleDict(),
	}
}

func (cluster *Cluster) AddPeers(peers ...string) {
	for _, peer := range peers {
		cluster.getters[peer] = newGetter(peer)
	}
	cluster.peers.AddNodes(cluster.self)
	cluster.peers.AddNodes(peers...)
}

func (cluster *Cluster) Close() {
	for _, g := range cluster.getters {
		g.Close()
	}
}
