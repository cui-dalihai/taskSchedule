package worker

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	_ "go.mongodb.org/mongo-driver/mongo/readpref"
	"taskSchedule/prj_src/taskSchedule/common"
	"time"
)

type JobLogger struct {
	Col *mongo.Collection

	LogsBuf               []interface{}
	LogsCommitTimeoutChan chan struct{}
	RecvChan              chan common.JobExecuteResult
}

var G_jobLogger *JobLogger

// SendLog 调度器向JobLogger发送执行日志
func (logger *JobLogger) SendLog(jobRes common.JobExecuteResult) {
	logger.RecvChan <- jobRes
}

func (logger *JobLogger) BatchWrite(logs []interface{}) {

	var (
		err           error
		insertManyRes *mongo.InsertManyResult
	)

	if insertManyRes, err = logger.Col.InsertMany(context.TODO(), logs); err != nil {
		fmt.Println("批量写入异常:", err.Error())
	}

	fmt.Println("批量写入成功:", insertManyRes.InsertedIDs)
}

// LogWatcher 监听队列, 一旦buf满足条件就写mongo
func (logger *JobLogger) LogWatcher() {

	var (
		log   common.JobExecuteResult
		timer *time.Timer
	)

	// 传给BatchWrite的&logs实际上是一个logs变量的别名, 两者都指向同一片地址, 但是每次新一轮for循环logs会被指向新的地址
	// 所以每个BatchWrite使用的是一个独立的地址, 下一轮for循环重写logs也不会影响到&logs在BatchWrite内部的使用
	for {
		select {
		case log = <-logger.RecvChan:

			// 第一条时启动定时器
			if len(logger.LogsBuf) == 0 {
				fmt.Println("首条时启动定时器")
				timer = time.AfterFunc(time.Duration(13)*time.Second,
					func() {
						logger.LogsCommitTimeoutChan <- struct{}{}
					})
			}

			logger.LogsBuf = append(logger.LogsBuf, bson.D{
				{"res", string(log.Res)},
				{"err", log.Err},
				{"startTime", log.StartTime},
				{"endTime", log.EndTime},
			})

			if len(logger.LogsBuf) >= 100 {

				// 提交, 清空, 停止定时器

				go logger.BatchWrite(logger.LogsBuf) // 值传递, goroutine会收到当前LogsBuf的一个拷贝, 所以后面对LogsBuf的修改
				logger.LogsBuf = logger.LogsBuf[:0]  // 不会影响goroutine
				timer.Stop()
			}

		case <-logger.LogsCommitTimeoutChan:

			go logger.BatchWrite(logger.LogsBuf)
			logger.LogsBuf = logger.LogsBuf[:0]
			timer.Stop()
		}
	}
}

func InitJobLogger() (err error) {

	var (
		col    *mongo.Collection
		client *mongo.Client
		ctx    context.Context
	)

	if client, err = mongo.NewClient(options.Client().ApplyURI(G_config.MongodbUri)); err != nil {
		return
	}

	if err = client.Connect(ctx); err != nil {
		return
	}

	if err = client.Ping(context.TODO(), readpref.Primary()); err != nil {
		fmt.Println("mongoDB 无法连接.")
		return
	}

	col = client.Database("JobSchedule").Collection("JobLogs")

	G_jobLogger = &JobLogger{
		Col:                   col,
		LogsBuf:               []interface{}{},
		RecvChan:              make(chan common.JobExecuteResult),
		LogsCommitTimeoutChan: make(chan struct{}),
	}

	go G_jobLogger.LogWatcher()

	return
}
