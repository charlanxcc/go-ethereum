/* cpadmin.go */

package cpadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/etcdhelper"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
)

var (
	modifiedBlock = 0
	nodes map[string]*Node
)

var CpAdminAbi = `[
{
  "inputs":[],
  "name":"modifiedBlock",
  "outputs":[
    {"name":"","type":"uint256"}],
  "type":"function"
},
{
  "inputs":[],
  "name":"firstNode",
  "outputs":[
    {"name":"present","type":"bool"},
    {"name":"json","type":"string"}],
  "type":"function"
},
{
  "inputs":[{"name":"_id","type":"bytes"}],
  "name":"getNode",
  "outputs":[
    {"name":"present","type":"bool"},
    {"name":"json","type":"string"}],
  "type":"function"
}]`

type Node struct {
	Partner		bool	`json:"Partner"`
	Name		string	`json:"name"`
	Id			string	`json:"id"`
	Ip			string	`json:"ip"`
	Port		int		`json:"port"`
	NotBefore	int		`json:"notBefore"`
	NotAfter	int		`json:"notAfter"`
	Prev		string	`json:"prev"`
	Next		string	`json:"next"`
}

func (n *Node) eq(m *Node) bool {
	if n.Partner == m.Partner && n.Name == m.Name && n.Id == m.Id &&
		n.Ip == m.Ip && n.Port == m.Port && n.NotBefore == m.NotBefore &&
		n.NotAfter == m.NotAfter {
		return true
	} else {
		return false
	}
}

type CpAdmin struct {
	nodeInfo		*p2p.NodeInfo
	dataDir			string
	from			common.Address
	to				common.Address
	Updates			chan bool
	rpcCli			*rpc.Client
	cli				*ethclient.Client
	abi				abi.ABI

	modifiedBlock	int
	self			*Node
	nodes			map[string]*Node
}

var (
	cpAdmin	*CpAdmin
)

func StartCpAdmin(stack *node.Node, address string, dataDir string) {
	//address = "0x48537bd1e56b9c8e8599c298c4e1e8fbbeed40f5"

	rpcClient, err := stack.Attach()
	if err != nil {
		utils.Fatalf("Failed to attach to self: %v", err)
	}
	abi, err := abi.JSON(strings.NewReader(CpAdminAbi))
	if err != nil {
		utils.Fatalf("Invalid abi: %v", err)
	}

	cpAdmin = &CpAdmin{
		dataDir: dataDir,
		from: common.Address{},
		to: common.HexToAddress(address),
		Updates: make(chan bool, 10),
		rpcCli: rpcClient,
		cli: ethclient.NewClient(rpcClient),
		abi: abi,
	}

	go cpAdmin.run()
}

func (ca *CpAdmin) getModifiedBlock(ctx context.Context, block *big.Int) (int, error) {
	input, err := ca.abi.Pack("modifiedBlock")
	if err != nil {
		return 0, err
	}

	msg := ethereum.CallMsg{From: ca.from, To: &ca.to, Data: input}
	output, err := ca.cli.CallContract(ctx, msg, block)
	if err != nil {
		return 0, err
	}

	v := new(big.Int)
	err = ca.abi.Unpack(&v, "modifiedBlock", output)
	if err != nil {
		return 0, err
	} else {
		return int(v.Int64()), nil
	}
}

func (ca *CpAdmin) getNode(ctx context.Context, id string, block *big.Int) (*Node, error) {
	var method string
	var input []byte
	var err error

	if len(id) == 0 {
		method = "firstNode"
		input, err = ca.abi.Pack(method)
	} else {
		method = "getNode"
		input, err = ca.abi.Pack(method, []byte(id))
	}

	msg := ethereum.CallMsg{From: ca.from, To: &ca.to, Data: input}
	output, err := ca.cli.CallContract(ctx, msg, block)
	if err != nil {
		return nil, err
	}

	var present bool
	var jsonOut string
	o := []interface{}{&present, &jsonOut}
	err = ca.abi.Unpack(&o, method, output)
	if err != nil {
		return nil, err
	}

	// trim the json output
	if ix := strings.Index(jsonOut, "\000"); ix >= 0 {
		jsonOut = jsonOut[:ix]
	}

	node := new(Node)
	err = json.Unmarshal([]byte(jsonOut), node)
	return node, err
}

func (ca *CpAdmin) getNodes() (modifiedBlock int, nodes, addedNodes, updatedNodes, deletedNodes []*Node, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	block, err := ca.cli.HeaderByNumber(ctx, nil)
	if err != nil {
		return
	}

	modifiedBlock, err = ca.getModifiedBlock(ctx, block.Number)
	if err != nil {
		return
	}

	if ca.modifiedBlock == modifiedBlock {
		//return
	}

	var node *Node
	for {
		if node == nil {
			node, err = ca.getNode(ctx, "", block.Number)
		} else if len(node.Next) == 0 {
			break
		} else {
			node, err = ca.getNode(ctx, node.Next, block.Number)
		}

		if err != nil {
			return
		} else {
			nodes = append(nodes, node)
		}
	}

	var oldNodes []*Node
	for _, i := range ca.nodes {
		oldNodes = append(oldNodes, i)
	}

	sort.Slice(oldNodes, func(i, j int) bool {
		return oldNodes[i].Id < oldNodes[j].Id
	})
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Id < nodes[j].Id
	})

	i, j := 0, 0
	for {
		if i >= len(oldNodes) || j >= len(nodes) {
			break
		}
		v := strings.Compare(oldNodes[i].Id, nodes[j].Id)
		if v == 0 {
			if !oldNodes[i].eq(nodes[j]) {
				updatedNodes = append(updatedNodes, nodes[j])
			}
			i++
			j++
		} else if v < 0 {
			deletedNodes = append(deletedNodes, oldNodes[i])
			i++
		} else if v > 0 {
			addedNodes = append(addedNodes, nodes[j])
			j++
		}
	}

	if i < len(oldNodes) {
		for ; i < len(oldNodes); i++ {
			deletedNodes = append(deletedNodes, oldNodes[i])
		}
	}
	if j < len(nodes) {
		for ; j < len(nodes); j++ {
			addedNodes = append(addedNodes, nodes[j])
		}
	}

	return
}

func (ca *CpAdmin) getNodeInfo() (*p2p.NodeInfo, error) {
	var nodeInfo *p2p.NodeInfo
	ctx, cancel := context.WithCancel(context.Background())
	err := ca.rpcCli.CallContext(ctx, &nodeInfo, "admin_nodeInfo")
	cancel()
	if err != nil {
		log.Error(fmt.Sprintf("Cannot get node info: %v", err))
	}
	return nodeInfo, err
}

func (ca *CpAdmin) startEtcd() {
	var ns []*Node
	for _, i := range ca.nodes {
		ns = append(ns, i)
	}
	sort.Slice(ns, func (i, j int) bool {
		return ns[i].Name < ns[j].Name
	})

	var cluster bytes.Buffer
	for _, i := range ns {
		if cluster.Len() != 0 {
			cluster.WriteString(",")
		}
		cluster.WriteString(fmt.Sprintf("%s=http://%s:%d",
			i.Name, i.Ip, i.Port + 2))
	}

	etcdhelper.StartEtcd(ca.self.Name, cluster.String(), ca.dataDir, false)
}

func (ca *CpAdmin) addPeer(node *Node, updateEtcd bool) error {
	if node.Id == ca.nodeInfo.ID {
		return nil
	}

	var v *bool
	ctx, cancel := context.WithCancel(context.Background())
	id := fmt.Sprintf("enode://%s@%s:%d", node.Id, node.Ip, node.Port)
	e := ca.rpcCli.CallContext(ctx, &v, "admin_addPeer", id)
	cancel()
	if e != nil || !*v {
		log.Error(fmt.Sprintf("Cannot add peer %s: %v", id, e))
	} else {
		if updateEtcd {
		}
		log.Info(fmt.Sprintf("Added %s.", id))
	}

	return nil
}

func (ca *CpAdmin) run() {
	for {
		if ca.nodeInfo == nil {
			nodeInfo, err := ca.getNodeInfo()
			if err != nil {
				log.Error(fmt.Sprintf("Failed to get node info: %v", err))
			} else {
				ca.nodeInfo = nodeInfo
			}
		}

		if ca.nodeInfo != nil {
			modifiedBlock, nodes, addedNodes, updatedNodes, deletedNodes, err := ca.getNodes()
			if err != nil {
				log.Error(fmt.Sprintf("Failed to get nodes: %v\n", err))
			} else if ca.modifiedBlock != modifiedBlock {
				log.Debug(fmt.Sprintf("Modified Block: %d\n", modifiedBlock))

				self := ca.self

				ca.modifiedBlock = modifiedBlock
				ca.nodes = map[string]*Node{}
				for _, i := range nodes {
					ca.nodes[i.Id] = i
					if i.Id == ca.nodeInfo.ID {
						ca.self = i
					}
				}

				needToStartEtcd := self == nil && ca.self != nil
				if needToStartEtcd {
					ca.startEtcd()
				}

				log.Debug(fmt.Sprintf("Added:\n"))
				for _, i := range addedNodes {
					log.Debug(fmt.Sprintf("%v\n", i))
					ca.addPeer(i, !needToStartEtcd)
				}
				log.Debug(fmt.Sprintf("Updated:\n"))
				for _, i := range updatedNodes {
					log.Debug(fmt.Sprintf("%v\n", i))
				}
				log.Debug(fmt.Sprintf("Deleted:\n"))
				for _, i := range deletedNodes {
					log.Debug(fmt.Sprintf("%v\n", i))
				}
			}
		}

		to := make(chan bool, 1)
		go func() {
			time.Sleep(5 * time.Second)
			to <- true
		}()
		select {
		case <- ca.Updates:
		case <- to:
		}
	}
}

func IsPartner(id string) bool {
	if cpAdmin == nil || len(cpAdmin.nodes) <= 0 {
		return false
	}
	if id[:2] == "0x" {
		id = id[2:]
	}
	return cpAdmin.nodes[id] != nil
}

func AddEtcdMember(name string) (string, error) {
	var node *Node
	for _, i := range cpAdmin.nodes {
		if i.Name == name {
			node = i
			break
		}
	}
	if node == nil {
		return "", fmt.Errorf("Unknown Name")
	}

	return etcdhelper.AddMember(name, fmt.Sprintf("http://%s:%d", node.Ip, node.Port))
}

/* EOF */
