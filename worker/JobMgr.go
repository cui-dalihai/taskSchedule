package worker

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"taskSchedule/prj_src/taskSchedule/common"
	"time"
)

type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var G_jobMgr *JobMgr

// CreateJobLock 创建某个任务创建一个锁
func (jm *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock) {
	jobLock = InitJobLock(jobName, jm.kv, jm.lease)
	return
}

// WatchKiller 监听强杀目录
func (jm *JobMgr) WatchKiller() (err error) {

	var (
		killKey   string
		event     *clientv3.Event
		job       *common.Job
		jobEvent  *common.JobEvent
		jobName   string
		watchChan clientv3.WatchChan
		watchRes  clientv3.WatchResponse
	)

	killKey = common.JOB_KILLER_DIR

	go func() {
		if watchChan = jm.watcher.Watch(context.TODO(), killKey, clientv3.WithPrefix()); err != nil {
			return
		}

		for watchRes = range watchChan {

			for _, event = range watchRes.Events {
				switch event.Type {
				case mvccpb.PUT:

					jobName = common.ExtractKillName(string(event.Kv.Key))
					job = &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL, job)
					G_scheduler.PushJob(jobEvent)
				case mvccpb.DELETE:
				}
			}
		}

	}()

	return
}

// WatchJobs 监听任务目录
func (jm *JobMgr) WatchJobs() (err error) {

	var (
		getRes     *clientv3.GetResponse
		kv         *mvccpb.KeyValue
		job        *common.Job
		jobName    string
		jobEvent   *common.JobEvent
		startVer   int64
		watchChan  clientv3.WatchChan
		watchRes   clientv3.WatchResponse
		watchEvent *clientv3.Event
	)

	// 1. 从/cron/jobs/拉取全量任务

	if getRes, err = jm.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	for _, kv = range getRes.Kvs {
		fmt.Printf("存量任务: %s \n", string(kv.Key))
		if job, err = common.UnpackJob(kv.Value); err != nil {
			fmt.Printf("Invalid Json Job %s \n", kv.Key)
			continue
		}
		fmt.Println(*job) // job是一个json指针, 使用指针求值符可以得到指针指向的变量的值

		jobEvent = common.BuildJobEvent(common.JOB_EVETN_SAVE, job)

		// 2. 解析后发送给调度协程
		G_scheduler.PushJob(jobEvent)
	}

	// 3. 继续监听后续变化
	go func() {
		// 起始监听版本
		startVer = getRes.Header.Revision + 1
		watchChan = jm.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix(), clientv3.WithRev(startVer))

		for watchRes = range watchChan {

			// 每个watchRes可能包含多个事件
			for _, watchEvent = range watchRes.Events {
				switch watchEvent.Type {

				case mvccpb.PUT:
					fmt.Printf("监听到创建事件: %s \n", watchEvent.Kv.Key)

					if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
						fmt.Printf("Invalid Job %s \n", watchEvent.Kv.Key)
						continue
					}

					jobEvent = common.BuildJobEvent(common.JOB_EVETN_SAVE, job)

				case mvccpb.DELETE:
					fmt.Printf("监听到删除事件: %s \n", watchEvent.Kv.Key)

					jobName = common.ExtractName(string(watchEvent.Kv.Key))
					job = &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
				}

				G_scheduler.PushJob(jobEvent)
			}
		}

	}()

	return
}

// InitMgr 连接etcd
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
	watcher := client.Watcher

	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	// WatchJobs中加载存量Jobs可能出错, err会从这里返回到worker的main函数, 导致main直接退出
	if err = G_jobMgr.WatchJobs(); err != nil {
		return
	}

	if err = G_jobMgr.WatchKiller(); err != nil {
		return
	}

	return
}
