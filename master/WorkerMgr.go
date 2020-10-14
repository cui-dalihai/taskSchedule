package master

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"taskSchedule/prj_src/taskSchedule/common"
	"time"
)

type WorkerMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
}

var G_workerMgr *WorkerMgr

func (wm *WorkerMgr) WorkerList() (workerList []string, err error) {

	var (
		getRes   *clientv3.GetResponse
		workerId *mvccpb.KeyValue
		wId      string
	)

	if getRes, err = wm.kv.Get(context.TODO(), common.JOB_WORKER_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	for _, workerId = range getRes.Kvs {

		wId = common.ExtractWorker(string(workerId.Key))
		workerList = append(workerList, wId)
	}

	return
}

func InitWorkerMgr() (err error) {
	var (
		client *clientv3.Client
		kv     clientv3.KV
	)

	config := clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	if client, err = clientv3.New(config); err != nil {
		return
	}

	kv = clientv3.NewKV(client)

	G_workerMgr = &WorkerMgr{
		client: client,
		kv:     kv,
	}

	return
}
