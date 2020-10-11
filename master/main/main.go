package main

import (
	"flag"
	"fmt"
	"runtime"
	"taskSchedule/prj_src/taskSchedule/master"
)

var (
	confFile string
)

func initArgs() {
	flag.StringVar(&confFile, "config", "./master.json", "请指定master的配置文件")
	flag.Parse()
}

// 初始化线程数量，这步应该是没有必要的，go默认会使用cpu的核心数来使用OS线程
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)

	// 初始化配置文件
	initArgs()

	// 初始化线程
	initEnv()

	// 加载配置文件
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	// 创建etcd的链接
	if err = master.InitMgr(); err != nil {
		goto ERR
	}

	// 向web提供api
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	// 阻塞主goroutine
	//for {
	//	time.Sleep(1 * time.Second)
	//}

ERR:
	fmt.Println(err)
}
