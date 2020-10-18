package main

import (
	"flag"
	"fmt"
	"runtime"
	"taskSchedule/prj_src/taskSchedule/worker"
	"time"
)

var (
	confFile string
)

func initArgs() {
	flag.StringVar(&confFile, "config", "./worker.json", "请指定worker的配置文件")
	flag.Parse()
}

// 初始化线程数量，这步应该是没有必要的，go默认会使用cpu的核心数来使用OS线程
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {

	// todo 每个初始化文件改为init函数

	var (
		err error
	)

	// 初始化配置文件
	initArgs()

	// 初始化线程
	initEnv()

	// 加载配置文件
	if err = worker.InitConfig(confFile); err != nil {
		goto ERR
	}

	// 初始化mongo链接
	if err = worker.InitJobLogger(); err != nil {
		goto ERR
	}

	// 启动执行器
	if err = worker.InitExecutor(); err != nil {
		goto ERR
	}

	// 启动调度器
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}

	// 初始化任务管理器
	if err = worker.InitMgr(); err != nil {
		goto ERR
	}

	// 注册服务
	if err = worker.InitRegister(); err != nil {
		goto ERR
	}

	// 阻塞主goroutine
	for {
		time.Sleep(1 * time.Second)
	}

	return

ERR:
	fmt.Println(err)
}
