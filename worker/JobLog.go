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
)

type JobLogger struct {
	Col *mongo.Collection

	LogsBuf  []interface{}
	RecvChan chan common.JobExecuteResult
}

var G_jobLogger *JobLogger

// SendLog 调度器向JobLogger发送执行日志
func (logger *JobLogger) SendLog(jobRes common.JobExecuteResult) {
	logger.RecvChan <- jobRes
}

func (logger *JobLogger) BatchWrite(logs *[]interface{}) {

	var (
		err           error
		insertManyRes *mongo.InsertManyResult
	)

	if insertManyRes, err = logger.Col.InsertMany(context.TODO(), *logs); err != nil {
		fmt.Println("批量写入异常:", err.Error())
	}

	fmt.Println("批量写入成功:", insertManyRes.InsertedIDs)
}

// LogWatcher 监听队列, 一旦buf满足条件就写mongo
func (logger *JobLogger) LogWatcher() {

	var (
		log common.JobExecuteResult
	)

	for log = range logger.RecvChan {
		fmt.Println("收到执行结果.")
		var logs []interface{}
		logger.LogsBuf = append(logger.LogsBuf, bson.D{
			{"res", string(log.Res)},
			{"err", log.Err},
			{"startTime", log.StartTime},
			{"endTime", log.EndTime},
		})

		fmt.Println("写入logsBuf后:", len(logger.LogsBuf))
		if len(logger.LogsBuf) == 10 {

			for _, v := range logger.LogsBuf {
				logs = append(logs, v)
			}

			logger.LogsBuf = logger.LogsBuf[:0]
			fmt.Println("满足十条, 写入DB后清空logsBuf", len(logger.LogsBuf))
			go logger.BatchWrite(&logs)
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

	// if ctx, _ = context.WithTimeout(context.TODO(), time.Duration(G_config.MongodbConnectTimeout) * time.Millisecond); err != nil {
	// 	return
	// }

	col = client.Database("JobSchedule").Collection("JobLogs")

	G_jobLogger = &JobLogger{
		Col:      col,
		LogsBuf:  []interface{}{},
		RecvChan: make(chan common.JobExecuteResult),
	}

	go G_jobLogger.LogWatcher()

	return
}
