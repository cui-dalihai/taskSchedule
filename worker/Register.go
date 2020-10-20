package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	guuid "github.com/google/uuid"
	"net"
	"taskSchedule/prj_src/taskSchedule/common"
	"time"
)

var G_register *Register

type Register struct {
	WorkerId string
	WorkerIp string

	Kv    clientv3.KV
	Lease clientv3.Lease
}

func (register *Register) keepAlive() {

	var (
		leaseGrantRes     *clientv3.LeaseGrantResponse
		leaseId           clientv3.LeaseID
		err               error
		keepAliveChan     <-chan *clientv3.LeaseKeepAliveResponse
		leaseKeepAliveRes *clientv3.LeaseKeepAliveResponse
		regKey            string
		ctx               context.Context
		cancelFunc        context.CancelFunc
	)

	regKey = common.JOB_WORKER_DIR + register.WorkerId + "@" + register.WorkerIp

	// 只要当前服务一直在运行就应该保持重试
	for {

		// 申请租约
		if leaseGrantRes, err = register.Lease.Grant(context.TODO(), 3); err != nil {
			goto RETRY
		}
		leaseId = leaseGrantRes.ID

		// 启动自动续租
		ctx, cancelFunc = context.WithCancel(context.TODO())
		if keepAliveChan, err = register.Lease.KeepAlive(ctx, leaseId); err != nil {
			goto RETRY
		}

		// 写入workers
		if _, err = register.Kv.Put(context.TODO(), regKey, "", clientv3.WithLease(leaseId)); err != nil {
			goto RETRY
		}

		// 处理续租响应
		for {
			select {
			case leaseKeepAliveRes = <-keepAliveChan:
				if leaseKeepAliveRes == nil {
					goto RETRY
				}
			}
		}

	RETRY:
		time.Sleep(1 * time.Second)
		if cancelFunc != nil {
			cancelFunc()
		}
	}
}

func InitRegister() (err error) {

	var (
		client   *clientv3.Client
		kv       clientv3.KV
		lease    clientv3.Lease
		workerId string
		ip       string
	)

	workerId = guuid.New().String()

	if ip, err = workerIp(); err != nil {
		return
	}

	config := clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	if client, err = clientv3.New(config); err != nil {
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_register = &Register{
		WorkerId: workerId,
		WorkerIp: ip,
		Kv:       kv,
		Lease:    lease,
	}

	go G_register.keepAlive()

	return
}

func workerIp() (ipstr string, err error) {

	addr, err := net.InterfaceAddrs()
	if err != nil {
		return
	}

	for _, v := range addr {
		ip, isIpNet := v.(*net.IPNet)
		if isIpNet && !ip.IP.IsLoopback() {
			if ip.IP.To4() != nil {
				ipstr = ip.IP.String()
				return
			}
		}
	}
	return
}
