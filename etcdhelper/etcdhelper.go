/* ethcdhelper.go */

package etcdhelper

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/etcdserver/api/v3client"
	"github.com/coreos/etcd/pkg/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"gopkg.in/urfave/cli.v1"
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

func StartEtcd(ctx *cli.Context, name, cluster, datadir string) {
	etcdName, etcdCluster = name, cluster
	var lip string
	{
		host, err := os.Hostname()
		if err != nil {
			fatalf("Cannot get hostname: %v", err)
		}
		addrs, err := net.LookupIP(host)
		if err != nil {
			fatalf("Cannot get ip address for %s: %v", host, err)
		}
		for _, i := range addrs {
			if x := i.To4(); x != nil {
				lip = x.String()
				break
			}
		}
	}

	if lip == "" {
		fatalf("etcd failed to start: can't figure out local ip address\n")
	}

	port := 0
	//"etcd1.1=http://bmk1:20010,etcd1.2=http://bmk1:20020,etcd1.3=http://bmk1\:20030"
	{
		ls := strings.Split(etcdCluster, ",")
		for _, i := range ls {
			i = strings.TrimSpace(i)
			l := strings.Split(i, "=")
			if len(l) != 2 || l[0] != name {
				continue
			}
			m := regexp.MustCompile("^*[^:]+:([0-9]+)$").FindStringSubmatch(l[1])
			if len(m) != 2 {
				continue
			}
			var err error
			port, err = strconv.Atoi(m[1])
			if err != nil {
				port = 0
			}
			mypeerurl = l[1]
		}
	}
	if port == 0 {
		fatalf("etcd failed to start: port is not given\n")
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
	cfg.ClusterState = "new"
	cfg.InitialCluster = etcdCluster
	cfg.InitialClusterToken = "etcd"

	var err error
	etcd, err = embed.StartEtcd(cfg)
	if err != nil {
		fatalf("FATAL: etcd failed to start: %v", err)
	} else {
		fmt.Printf("etcd server %s succeeded\n", etcd.Server.Cfg.Name)
	}

	etcdcli = v3client.New(etcd.Server)

	// lock for block generation concurrency
	lockSession, err = concurrency.NewSession(etcdcli)
	if err != nil {
		fatalf("FATAL: etcd failed to create new sesson: %v", err)
	}
	lock = concurrency.NewMutex(lockSession, "block-generator")
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
