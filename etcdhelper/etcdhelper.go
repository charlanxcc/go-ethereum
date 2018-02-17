/* ethcdhelper.go */

package etcdhelper

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/etcdserver/api/v3client"
	"github.com/coreos/etcd/etcdserver/membership"
	"github.com/coreos/etcd/pkg/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

var (
	operationTimeout      = 30 * time.Second
	etcdName, etcdCluster string
	etcd                  *embed.Etcd
	etcdcli               *clientv3.Client
	mypeerurl             string
	lockSession           *concurrency.Session
	lock                  *concurrency.Mutex
	yieldCount            int64

	// notification when a new transaction arrives
	TxNotifier = make(chan bool, 1)
)

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "Fatal: "+format+"\n", args...)
	os.Exit(1)
}

func StartEtcd(name, cluster, datadir string, newCluster bool) error {
	if etcd != nil {
		return fmt.Errorf("Etcd is up and running already.")
	}

	etcdName, etcdCluster = name, cluster
	var lip string

	port := 0
	{
		ls := strings.Split(etcdCluster, ",")
		for _, i := range ls {
			i = strings.TrimSpace(i)
			l := strings.Split(i, "=")
			if len(l) != 2 || l[0] != name {
				continue
			}

			mypeerurl = l[1]
			var err error
			u, err := url.Parse(mypeerurl)
			if err != nil {
				return fmt.Errorf("Invalid endpoint for %s: %s, %s\n",
					l[0], l[1], err)
			}

			port, err = strconv.Atoi(u.Port())
			lip = u.Hostname()
		}
	}
	if port == 0 {
		return fmt.Errorf("etcd failed to start: port is not given\n")
	}

	cfg := embed.NewConfig()
	cfg.Dir = fmt.Sprintf("%s/default.etcd", datadir)
	cfg.Name = etcdName
	u, _ := url.Parse(fmt.Sprintf("http://%s:%d", lip, port))
	cfg.LPUrls = []url.URL{*u}
	cfg.APUrls = []url.URL{*u}
	u, _ = url.Parse(fmt.Sprintf("http://%s:%d", lip, port+1))
	cfg.LCUrls = []url.URL{*u}
	cfg.ACUrls = []url.URL{*u}
	if newCluster {
		cfg.ClusterState = "new"
	} else {
		cfg.ClusterState = "existing"
	}
	cfg.InitialCluster = cluster
	cfg.InitialClusterToken = "etcd"

	var err error
	etcd, err = embed.StartEtcd(cfg)
	if err != nil {
		return fmt.Errorf("FATAL: etcd failed to start: %v", err)
	} else {
		log.Info(fmt.Sprintf("etcd server %s succeeded\n", etcd.Server.Cfg.Name))
	}

	etcdcli = v3client.New(etcd.Server)
	return nil
/*
	// lock for block generation concurrency
	lockSession, err = concurrency.NewSession(etcdcli)
	if err != nil {
		fatalf("FATAL: etcd failed to create new sesson: %v", err)
	}
	lock = concurrency.NewMutex(lockSession, "block-generator")
*/
}

func GetCluster() string {
	if etcd == nil {
		return ""
	}

	var ms []*membership.Member
	for _, i := range etcd.Server.Cluster().Members() {
		ms = append(ms, i)
	}
	sort.Slice(ms, func (i, j int) bool {
		return ms[i].Attributes.Name < ms[j].Attributes.Name;
	})

	var bb bytes.Buffer
	for _, i := range ms {
		if bb.Len() > 0 {
			bb.WriteString(",")
		}
		bb.WriteString(fmt.Sprintf("%s=%s", i.Attributes.Name,
			i.RaftAttributes.PeerURLs[0]))
	}
	return bb.String()
}

func Info() interface{} {
	if etcd == nil {
		return "Etcd is not initialzed."
	}

	getMemberInfo := func(member *membership.Member) *map[string]interface{} {
		return &map[string]interface{}{
			"name": member.Attributes.Name,
			"id": member.ID.String(),
			"clientUrls": strings.Join(member.Attributes.ClientURLs, ","),
			"peerUrls": strings.Join(member.RaftAttributes.PeerURLs, ","),
		}
	}

	var ms []*membership.Member
	for _, i := range etcd.Server.Cluster().Members() {
		ms = append(ms, i)
	}
	sort.Slice(ms, func (i, j int) bool {
		return ms[i].Attributes.Name < ms[j].Attributes.Name;
	})

	var bb bytes.Buffer
	var self, leader *membership.Member
	var members []interface{}
	for _, i := range ms {
		if i.ID == etcd.Server.ID() {
			self = i
		}
		if i.ID == etcd.Server.Leader() {
			leader = i
		}
		members = append(members, getMemberInfo(i))
		if bb.Len() > 0 {
			bb.WriteString(",")
		}
		bb.WriteString(fmt.Sprintf("%s=%s", i.Attributes.Name,
			i.RaftAttributes.PeerURLs[0]))
	}

	info := map[string]interface{}{
		"cluster": bb.String(),
		"contract": params.AdminContractAddress,
		"members": members,
	}
	if self != nil {
		info["self"] = &map[string]interface{}{
			"name": self.Attributes.Name,
			"id": self.ID.String(),
		}
	}
	if leader != nil {
		info["leader"] = &map[string]interface{}{
			"name": leader.Attributes.Name,
			"id": leader.ID.String(),
		}
	}

	return info
}

func IsLeader() bool {
	if etcd == nil {
		return false
	} else {
		return etcd.Server.ID() == etcd.Server.Leader()
	}
}

func YieldLeadership() {
	if !IsLeader() {
		return
	}

	yieldCount = 0

	members := etcd.Server.Cluster().Members()
	ix := 0
	for i := 0; i < len(members); i++ {
		if members[i].ID == etcd.Server.ID() {
			ix = i
			break
		}
	}

	for j := ix + 1; ; j++ {
		j %= len(members)
		if j == ix {
			break
		}

		tm := etcd.Server.Cfg.ReqTimeout()
		ctx, cancel := context.WithTimeout(context.Background(), tm)
		err := etcd.Server.MoveLeader(ctx, etcd.Server.Lead(),
			uint64(members[j].ID))
		cancel()
		if err == nil {
			log.Info(fmt.Sprintf("Yielded leadership to %s:%s\n",
				members[j].Attributes.Name, members[j].ID))
			return
		}
	}
	log.Error(fmt.Sprintf("Failed to yield\n"))
}

func MoveLeader(id string) error {
	mid, err := types.IDFromString(id)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to move etcd leader to %s: %v", id, err))
		return err
	}

	tm := etcd.Server.Cfg.ReqTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), tm)
	err = etcd.Server.MoveLeader(ctx, etcd.Server.Lead(),
		uint64(mid))
	if err != nil {
		log.Error(fmt.Sprintf("Failed to move etcd leader to %s: %v", id, err))
	} else {
		log.Info(fmt.Sprintf("%s is new etcd leader.", id))
	}
	cancel()
	return err
}

func AddMember(name, peerURL string) (string, error) {
	cluster := GetCluster()
	_, err := etcdcli.MemberAdd(context.Background(), []string{peerURL})
	if err != nil {
		log.Error(fmt.Sprintf("Failed to add member %s: %v", peerURL, err))
		return "", err
	} else {
		var comma string
		if len(cluster) > 0 {
			comma = ","
		}
		cluster = fmt.Sprintf("%s%s%s=%s", cluster, comma, name, peerURL)
		rs := fmt.Sprintf("Run admin.etcdJoin('%s', '%s');", name, cluster)
		log.Info(fmt.Sprintf("%s=%s is new etcd member.", name, peerURL))
		return rs, err
	}
}

func Put(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	_, err := etcdcli.Put(ctx, key, value)
	cancel()
	return err
}

func Get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	rsp, err := etcdcli.Get(ctx, key)
	cancel()
	if err != nil {
		return "", err
	} else if rsp.Count == 0 {
		return "", nil
	} else {
		var v string
		for _, kv := range rsp.Kvs {
			v = string(kv.Value)
		}
		return v, nil
	}
}

func Lock() error {
	t := time.Now()
	err := lock.Lock(context.TODO())
	dt := float64(time.Now().Sub(t).Nanoseconds()) / 1000000.0
	if err == nil {
		log.Error(fmt.Sprintf("XXX %s: lock   (%.03f ms)", etcdName, dt))
		//Put("log", fmt.Sprintf("%s: lock  ", etcdName))
	} else {
		log.Error(fmt.Sprintf("XXX %s: lock   (%.03f ms) failed: %s", etcdName, err))
		//Put("log", fmt.Sprintf("%s: lock   failed: %s", etcdName, err))
	}
	return err
}

func Unlock() error {
	err := lock.Unlock(context.TODO())
	if err == nil {
		log.Error(fmt.Sprintf("XXX %s: unlock", etcdName))
		//Put("log", fmt.Sprintf("%s: unlock", etcdName))
	} else {
		log.Error(fmt.Sprintf("XXX %s: unlock failed: %s", etcdName, err))
		//Put("log", fmt.Sprintf("%s: unlock failed: %s", etcdName, err))
	}
	return err
}

func LogBlock(number uint64, hash string, count int) {
	if atomic.AddInt64(&yieldCount, 1) >= int64(params.LeaderYieldAfter) && count == 0 {
		go YieldLeadership()
	}

	var id, leader types.ID
	if etcd != nil {
		id = etcd.Server.ID()
		leader = etcd.Server.Leader()
	}
	Put("log", fmt.Sprintf("%s: mined %dth hash=%s %d txs. id=0x%x leader=0x%x\n", etcdName, number, hash, count, id, leader))
}

func TxNotify() {
	TxNotifier <- true
}

/* EOF */
