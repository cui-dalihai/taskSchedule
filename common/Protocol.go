package common

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

type Job struct {
	Name     string `json:"name"`
	Command  string `json:"command"`
	CronExpr string `json:"cronExpr"`
}

func (job *Job) BuildJobSchedulePlan() (jsp *JobSchedulePlan, err error) {

	var (
		cronExpr *cronexpr.Expression
	)

	if cronExpr, err = cronexpr.Parse(job.CronExpr); err != nil {
		fmt.Printf("非法的cron表达式%s\n", job.CronExpr)
		return
	}

	jsp = &JobSchedulePlan{
		Job:      job,
		CronExpr: cronExpr,
		NextTime: cronExpr.Next(time.Now()),
	}

	return
}

type JobEvent struct {
	EventType int
	Job       *Job
}

type JobSchedulePlan struct {
	Job      *Job // 关联一个job
	CronExpr *cronexpr.Expression
	NextTime time.Time
}

func (plan *JobSchedulePlan) BuildJobExecuteInfo() (exeInfo *JobExecuteInfo) {
	exeInfo = &JobExecuteInfo{
		Job:        plan.Job,
		PlanTime:   plan.NextTime,
		ActualTime: time.Now(),
	}
	exeInfo.CancelCtx, exeInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}

type JobExecuteInfo struct {
	Job        *Job
	PlanTime   time.Time
	ActualTime time.Time
	CancelCtx  context.Context
	CancelFunc context.CancelFunc
}

func (exeInfo *JobExecuteInfo) BuildJobExecuteResult() *JobExecuteResult {
	return &JobExecuteResult{
		JobExecuteInfo: exeInfo,
	}
}

type JobExecuteResult struct {
	JobExecuteInfo *JobExecuteInfo
	Res            []byte
	Err            error
	StartTime      time.Time
	EndTime        time.Time
}

type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

func BuildResp(errno int, msg string, data interface{}) (resp []byte, err error) {
	var (
		response Response
	)

	response.Errno = errno
	response.Msg = msg
	response.Data = data

	resp, err = json.Marshal(response)
	return
}

func UnpackJob(val []byte) (ret *Job, err error) {

	// ret在函数内部申请了一个Job结构体的空间, 并取了这个空间的地址保存在ret中
	// 所以ret是一个指向Job类型的指针, 即*Job类型

	// 函数返回这个值给调用者使用时
	// p = UnpackJob(val)
	// 可以使用*p来修改这个Job类型的结构体
	// 可以使用p来继续引用这个Job类型的结构体
	// 另外, 每次调用都会申请新的地址空间, 都会返回新的地址, 也就是新的p

	ret = &Job{}
	if err = json.Unmarshal(val, ret); err != nil {
		return
	}
	return
}

func ExtractName(key string) (name string) {
	return strings.TrimPrefix(key, JOB_SAVE_DIR)
}

func ExtractKillName(key string) (name string) {
	return strings.TrimPrefix(key, JOB_KILLER_DIR)
}

func BuildJobEvent(eventType int, job *Job) *JobEvent {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}
