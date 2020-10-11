package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"taskSchedule/prj_src/taskSchedule/common"
)

// JobLock 分布式锁(基于etcd的TXN机制)
type JobLock struct {
	kv    clientv3.KV
	lease clientv3.Lease

	jobName    string             // 每个任务不能并发, 不同任务之间可以并发
	cancelFunc context.CancelFunc // 取消自动续租
	leaseId    clientv3.LeaseID   // 租约ID
	isLocked   bool               // 是否上锁成功
}

// Lock 上锁
func (jobLock *JobLock) Lock() (err error) {

	var (
		leaseGrant       *clientv3.LeaseGrantResponse
		cancelCtx        context.Context
		cancelFunc       context.CancelFunc
		leaseId          clientv3.LeaseID
		keepAliveResChan <-chan *clientv3.LeaseKeepAliveResponse
		txn              clientv3.Txn
		lockKey          string
		txnRes           *clientv3.TxnResponse
	)

	// 创建租约 5秒
	if leaseGrant, err = jobLock.lease.Grant(context.TODO(), 5); err != nil {
		return
	}

	leaseId = leaseGrant.ID
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())

	// 自动续租
	if keepAliveResChan, err = jobLock.lease.KeepAlive(cancelCtx, leaseId); err != nil {
		goto FAIL
	}

	// 处理续租应答的协程
	go func() {
		var (
			keepAliveRes *clientv3.LeaseKeepAliveResponse
		)

		for {
			select {
			case keepAliveRes = <-keepAliveResChan:
				// 续租响应为空指针, 续租失败
				if keepAliveRes == nil {
					goto END
				}
			}
		}
	END:
	}()

	// 创建事物
	txn = jobLock.kv.Txn(context.TODO())
	lockKey = common.JOB_LOCK_DIR + jobLock.jobName

	// 事物抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))

	if txnRes, err = txn.Commit(); err != nil {
		// 可能是未写入就失败, 可能是写入后返回时失败, 两种情况下都使用FAIL回滚, 释放这个任务的锁
		goto FAIL
	}

	// 成功返回, 失败就立即释放租约
	if !txnRes.Succeeded {
		err = common.ERR_LOCK_ALREADY_IN_USED
		goto FAIL
	}

	jobLock.leaseId = leaseId
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true

	return
FAIL:
	cancelFunc()
	jobLock.lease.Revoke(context.TODO(), leaseId)
	return
}

func (jobLock *JobLock) UnLock() {
	if jobLock.isLocked {
		jobLock.cancelFunc()
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseId)
	}
}

func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) *JobLock {
	return &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}
}
