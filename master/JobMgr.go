package master

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"taskSchedule/prj_src/taskSchedule/common"
	"time"
)

type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var G_jobMgr *JobMgr

func InitMgr() (err error) {

	var client *clientv3.Client

	config := clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	if client, err = clientv3.New(config); err != nil {
		return
	}

	kv := client.KV
	lease := client.Lease

	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}

// JobSave 任务保存/cron/jobs/
func (jm *JobMgr) JobSave(job *common.Job) (prevJob *common.Job, err error) {

	var (
		jobKey string
		jobVal []byte
		putRes *clientv3.PutResponse
	)

	jobKey = common.JOB_SAVE_DIR + job.Name
	if jobVal, err = json.Marshal(job); err != nil {
		return
	}

	if putRes, err = jm.kv.Put(context.TODO(), jobKey, string(jobVal), clientv3.WithPrevKV()); err != nil {
		return
	}

	if putRes.PrevKv != nil {
		if err = json.Unmarshal(putRes.PrevKv.Value, &prevJob); err != nil {
			err = nil
			return
		}
	}
	return
}

// JobDelete 任务删除/cron/jobs/
func (jm *JobMgr) JobDelete(name string) (prevJob *common.Job, err error) {

	var (
		jobKey string
		delObj *clientv3.DeleteResponse
	)

	jobKey = common.JOB_SAVE_DIR + name

	if delObj, err = jm.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	if len(delObj.PrevKvs) != 0 {
		if err = json.Unmarshal(delObj.PrevKvs[0].Value, &prevJob); err != nil {
			err = nil
			return
		}
	}
	return
}

// JobList 任务列表/cron/jobs/
func (jm *JobMgr) JobList() (jobList []*common.Job, err error) {

	var (
		getRes *clientv3.GetResponse
		kvPair *mvccpb.KeyValue
		dirKey string
		job    *common.Job
	)

	dirKey = common.JOB_SAVE_DIR

	if getRes, err = G_jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	jobList = make([]*common.Job, 0)
	for _, kvPair = range getRes.Kvs {
		job = &common.Job{}

		// 反序列化出错直接跳过这个job, 继续输出剩余的job
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

// JobKill 任务强杀，put /cron/killer/来通知worker, 同时put key尽快过期掉
func (jm *JobMgr) JobKill(name string) (err error) {

	var (
		killName      string
		leaseGrantRes *clientv3.LeaseGrantResponse
		leaseId       clientv3.LeaseID
	)

	killName = common.JOB_KILLER_DIR + name

	if leaseGrantRes, err = jm.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	leaseId = leaseGrantRes.ID

	if _, err = jm.kv.Put(context.TODO(), killName, "", clientv3.WithLease(leaseId)); err != nil {
		fmt.Println(err.Error())
		return
	}
	return
}
