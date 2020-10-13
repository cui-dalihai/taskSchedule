package worker

import (
	"fmt"
	"taskSchedule/prj_src/taskSchedule/common"
	"time"
)

// Scheduler 调度器
type Scheduler struct {
	jobEventChan         chan *common.JobEvent
	jobPlanTable         map[string]*common.JobSchedulePlan
	jobExecuting         map[string]*common.JobExecuteInfo
	jobExecuteResultChan chan *common.JobExecuteResult
}

var (
	G_scheduler *Scheduler
)

// PushJob 向scheduler发送任务
func (scheduler *Scheduler) PushJob(e *common.JobEvent) {
	scheduler.jobEventChan <- e
}

// PushJobResult 向scheduler回传执行结果
func (scheduler *Scheduler) PushJobResult(r *common.JobExecuteResult) {
	scheduler.jobExecuteResultChan <- r
}

// StartJob 启动Job,并发时跳过本次执行
func (scheduler *Scheduler) StartJob(plan *common.JobSchedulePlan) {

	var (
		isExecuting bool
		exeInfo     *common.JobExecuteInfo
	)

	if exeInfo, isExecuting = scheduler.jobExecuting[plan.Job.Name]; isExecuting {
		fmt.Printf("%s正在执行, 跳过本次执行.\n", plan.Job.Name)
		return
	}

	exeInfo = plan.BuildJobExecuteInfo()

	scheduler.jobExecuting[plan.Job.Name] = exeInfo

	fmt.Printf("%s未在执行, 发送%s给执行器\n", plan.Job.Name, plan.Job.Name)
	G_executor.ExecuteJob(exeInfo)
}

// handleJobEvent 处理调度器通道中的事件，管理内存中的任务
func (scheduler *Scheduler) handleJobEvent(e *common.JobEvent) {

	var (
		plan        *common.JobSchedulePlan
		isExist     bool
		isExecuting bool
		exeInfo     *common.JobExecuteInfo
		err         error
	)

	switch e.EventType {

	case common.JOB_EVETN_SAVE:
		if plan, err = e.Job.BuildJobSchedulePlan(); err != nil {
			fmt.Println(err.Error())
			return
		}
		scheduler.jobPlanTable[e.Job.Name] = plan

	case common.JOB_EVENT_DELETE:
		if plan, isExist = scheduler.jobPlanTable[e.Job.Name]; isExist {
			delete(scheduler.jobPlanTable, e.Job.Name)
		}

	case common.JOB_EVENT_KILL:
		if exeInfo, isExecuting = scheduler.jobExecuting[e.Job.Name]; isExecuting {
			exeInfo.CancelFunc()
		}

	}
}

// handleJobResult 处理结果通道中的结果
func (scheduler *Scheduler) handleJobResult(jobExeRes *common.JobExecuteResult) {
	fmt.Println("收到执行结果", jobExeRes.JobExecuteInfo.Job.Name)
	fmt.Println("收到执行结果", jobExeRes.Err)
	fmt.Println("收到执行结果", string(jobExeRes.Res))
	delete(scheduler.jobExecuting, jobExeRes.JobExecuteInfo.Job.Name)
}

// Tick 扫描内存中的任务，处理到期任务，计算调度器下次Tick时间
func (scheduler *Scheduler) Tick() (nextTick time.Duration) {

	var (
		plan    *common.JobSchedulePlan
		now     time.Time
		minTime *time.Time
	)

	if len(scheduler.jobPlanTable) == 0 {
		nextTick = 1 * time.Second
		return
	}

	now = time.Now()
	for _, plan = range scheduler.jobPlanTable {

		if plan.NextTime.Before(now) || plan.NextTime.Equal(now) {
			scheduler.StartJob(plan)
			plan.NextTime = plan.CronExpr.Next(now)
		}

		if minTime == nil {
			minTime = &plan.NextTime
		} else if minTime.After(plan.NextTime) {
			minTime = &plan.NextTime
		}
	}
	nextTick = (*minTime).Sub(now)
	return
}

// Loop 主循环
func (scheduler *Scheduler) Loop() {

	var (
		event     *common.JobEvent
		result    *common.JobExecuteResult
		nextTick  time.Duration
		tickTimer *time.Timer
	)

	nextTick = scheduler.Tick()
	tickTimer = time.NewTimer(nextTick)

	for {
		// for的每轮循环select会被执行一次, 要么任务变更, 要么任务到期
		select {

		case event = <-scheduler.jobEventChan:
			scheduler.handleJobEvent(event)

		case <-tickTimer.C:

		case result = <-scheduler.jobExecuteResultChan:
			scheduler.handleJobResult(result)
		}

		nextTick = scheduler.Tick()
		tickTimer.Reset(nextTick)
	}
}

func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan:         make(chan *common.JobEvent, 1000),
		jobPlanTable:         make(map[string]*common.JobSchedulePlan),
		jobExecuting:         make(map[string]*common.JobExecuteInfo),
		jobExecuteResultChan: make(chan *common.JobExecuteResult, 1000),
	}

	// 启动调度器
	go G_scheduler.Loop()

	return
}
