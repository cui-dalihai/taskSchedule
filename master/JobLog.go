package master

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type LogMgr struct {
	Col *mongo.Collection
}

var G_logMgr *LogMgr

func (logMgr *LogMgr) LogList(jobName string, skip int64, limit int) (res []interface{}, err error) {

	var (
		cursor  *mongo.Cursor
		filter  *options.FindOptions
		results []bson.M
	)

	filter = options.Find().SetSort(bson.D{{"startTime", -1}}).SetSkip(skip)

	if cursor, err = logMgr.Col.Find(context.TODO(), bson.D{{"name", jobName}}, filter); err != nil {
		return
	}
	defer cursor.Close(context.TODO())

	if err = cursor.All(context.TODO(), &results); err != nil {
		return
	}

	for _, result := range results {
		res = append(res, result)
	}

	return

	// if err != nil {
	// 	log.Fatal(err)
	// }
	//
	// get a list of all returned documents and print them out
	// see the mongo.Cursor documentation for more examples of using cursors
	// var results []bson.M
	// if err = cursor.All(context.TODO(), &results); err != nil {
	// 	log.Fatal(err)
	// }
	// for _, result := range results {
	// 	fmt.Println(result)
	// }

}

func InitLogMgr() (err error) {
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

	G_logMgr = &LogMgr{
		Col: col,
	}

	return
}
