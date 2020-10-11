package worker

import (
	"fmt"
	"math/rand"
	"os/exec"
	"taskSchedule/prj_src/taskSchedule/common"
	"time"
)

type Executor struct {
}

var G_executor *Executor

func (executor *Executor) ExecuteJob(jobExeInfo *common.JobExecuteInfo) {

	go func() {

		fmt.Println("执行器收到任务", jobExeInfo.Job.Name)
		var (
			cmd              *exec.Cmd
			cmdOutPut        []byte
			err              error
			jobExecuteResult *common.JobExecuteResult
			jobLock          *JobLock
		)

		jobExecuteResult = jobExeInfo.BuildJobExecuteResult()
		jobExecuteResult.StartTime = time.Now()

		// 为这个任务创建锁
		jobLock = G_jobMgr.CreateJobLock(jobExeInfo.Job.Name)

		// 上锁
		// 随机睡眠
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

		err = jobLock.Lock()
		defer jobLock.UnLock()

		if err != nil {
			fmt.Println("锁被占用")

			jobExecuteResult.Err = err
			jobExecuteResult.EndTime = time.Now()

		} else {
			fmt.Println("抢锁成功")

			// 上锁成功后重置任务执行开始时间
			jobExecuteResult.StartTime = time.Now()

			cmd = exec.CommandContext(jobExeInfo.CancelCtx, "/bin/bash", "-c", jobExeInfo.Job.Command)
			cmdOutPut, err = cmd.CombinedOutput()

			jobExecuteResult.EndTime = time.Now()
			jobExecuteResult.Res = cmdOutPut
			jobExecuteResult.Err = err
		}

		// 把结果发送回Scheduler
		G_scheduler.PushJobResult(jobExecuteResult)
	}()
}

func InitExecutor() (err error) {

	G_executor = &Executor{}

	return
}
