package master

import (
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"taskSchedule/prj_src/taskSchedule/common"
	"time"
)

var G_server *http.Server

func InitApiServer() (err error) {

	var (
		mux      *http.ServeMux
		listener net.Listener
	)

	// 创建路由器
	mux = http.NewServeMux()

	// 安装路由
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handleWorkerList)

	// 安装静态文件路由
	mux.Handle("/", http.StripPrefix("/", http.FileServer(http.Dir(G_config.WebRoot))))

	// 创建监听器
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort)); err != nil {
		return
	}

	// 创建server
	G_server = &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}

	// 启动服务器
	go G_server.Serve(listener)

	for {
		time.Sleep(1 * time.Second)
	}

	return
}

// 保存任务接口
func handleJobSave(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		bytes   []byte
		postJob string
		job     common.Job
		prevJob *common.Job
	)

	// 解析post表单
	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	// 提取提交的参数，反序列化到job
	postJob = r.PostForm.Get("job")
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}

	// 保存到etcd
	if prevJob, err = G_jobMgr.JobSave(&job); err != nil {
		goto ERR
	}

	// 成功响应
	if bytes, err = common.BuildResp(0, "success", prevJob); err == nil {
		w.Write(bytes)
	}
	return

	// 异常应答
ERR:
	if bytes, err = common.BuildResp(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

// 删除任务接口
func handleJobDelete(w http.ResponseWriter, r *http.Request) {

	var (
		err    error
		bytes  []byte
		p      string
		delJob *common.Job
	)

	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	p = r.Form.Get("name")

	if delJob, err = G_jobMgr.JobDelete(p); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResp(0, "success", delJob); err == nil {
		w.Write(bytes)
	}

	return

ERR:
	if bytes, err = common.BuildResp(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

// 任务列表接口
func handleJobList(w http.ResponseWriter, r *http.Request) {

	var (
		err     error
		bytes   []byte
		jobList []*common.Job
	)

	if jobList, err = G_jobMgr.JobList(); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResp(0, "success", jobList); err == nil {
		w.Write(bytes)
	}

	return

ERR:
	if bytes, err = common.BuildResp(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

// 杀死任务接口
func handleJobKill(w http.ResponseWriter, r *http.Request) {

	var (
		err   error
		bytes []byte
		name  string
	)

	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	name = r.Form.Get("name")

	if err = G_jobMgr.JobKill(name); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResp(0, "success", name); err == nil {
		w.Write(bytes)
	}

	return

ERR:
	if bytes, err = common.BuildResp(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

func handleJobLog(w http.ResponseWriter, r *http.Request) {

	var (
		err      error
		bytes    []byte
		logs     []interface{}
		skipstr  string
		skip     int
		name     string
		limitstr string
		limit    int
	)

	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	name = r.Form.Get("name")
	skipstr = r.Form.Get("skip")
	limitstr = r.Form.Get("limit")

	if skip, err = strconv.Atoi(skipstr); err != nil {
		skip = 0
	}

	if limit, err = strconv.Atoi(limitstr); err != nil {
		limit = 20
	}

	if logs, err = G_logMgr.LogList(name, int64(skip), limit); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResp(0, "success", logs); err == nil {
		w.Write(bytes)
	}
	return

ERR:
	if bytes, err = common.BuildResp(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

func handleWorkerList(w http.ResponseWriter, r *http.Request) {

	var (
		workerList []string
		err        error
		bytes      []byte
	)

	if workerList, err = G_workerMgr.WorkerList(); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResp(0, "success", workerList); err == nil {
		w.Write(bytes)
	}

	return
ERR:
	if bytes, err = common.BuildResp(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}
