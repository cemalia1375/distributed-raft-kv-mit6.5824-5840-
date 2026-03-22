package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 添加
// 添加

// 定义单个任务状态：
// 用 0: Idle (空闲), 1: In-progress (进行中), 2: Completed (已完成) 来表示。
type TaskStatus struct {
	// Your definitions here.
	state     int       // 0: Idle, 1: In-Progress, 2: Completed
	startTime time.Time // 用于记录开始时间，检查是否超时
}
type Master struct {
	files       []string
	nReduce     int
	phase       TaskType     // 当前处于 Map 阶段还是 Reduce 阶段
	mapTasks    []TaskStatus // Map 任务状态池
	reduceTasks []TaskStatus // Reduce 任务状态池
	mu          sync.Mutex   // 必须要锁，因为多个 Worker 会并发调用 RPC
}

// Your code here -- RPC handlers for the worker to call.
// GetTask 是 Worker 领任务的出口
func (m *Master) GetTask(args *ExampleArgs, reply *ExampleReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.phase == MapTask {
		// 寻找空闲的 Map 任务
		for i, status := range m.mapTasks {
			if status.state == 0 || (status.state == 1 && time.Since(status.startTime) > 10*time.Second) {
				reply.Type = MapTask
				reply.TaskId = i
				reply.FileName = m.files[i]
				reply.NReduce = m.nReduce
				reply.NFiles = len(m.files)

				// 更新状态
				m.mapTasks[i].state = 1
				m.mapTasks[i].startTime = time.Now()
				return nil
			}
		}
		// 如果 Map 还在跑但没全完，让 Worker 等一下
		reply.Type = WaitTask
	} else if m.phase == ReduceTask {
		// 寻找空闲的 Reduce 任务
		// 必须把 NFiles 发给 Worker，否则 Worker 循环次数为 0。
		for i, status := range m.reduceTasks {
			if status.state == 0 || (status.state == 1 && time.Since(status.startTime) > 10*time.Second) {
				reply.Type = ReduceTask
				reply.TaskId = i
				reply.NReduce = m.nReduce
				reply.NFiles = len(m.files) // 新增
				m.reduceTasks[i].state = 1
				m.reduceTasks[i].startTime = time.Now()
				return nil
			}
		}
		reply.Type = WaitTask
	} else {
		reply.Type = ExitTask
	}

	return nil
}

// 4. 实现汇报完成的 RPC 处理器
func (m *Master) TaskCompleted(args *ExampleReply, reply *ExampleArgs) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.Type == MapTask && m.phase == MapTask {
		m.mapTasks[args.TaskId].state = 2
	} else if args.Type == ReduceTask && m.phase == ReduceTask {
		m.reduceTasks[args.TaskId].state = 2
	}

	// 检查是否所有任务完成以切换阶段
	allDone := true
	if m.phase == MapTask {
		for _, t := range m.mapTasks {
			if t.state != 2 {
				allDone = false
				break
			}
		}
		if allDone {
			m.phase = ReduceTask
		}
	} else if m.phase == ReduceTask {
		for _, t := range m.reduceTasks {
			if t.state != 2 {
				allDone = false
				break
			}
		}
		if allDone {
			m.phase = ExitTask
		}
	}
	return nil
}

// 启动RPC服务
// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	//ret := false
	// Your code here.
	//return ret
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.phase == ExitTask
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:       files,
		nReduce:     nReduce,
		phase:       MapTask, // 初始阶段是 Map
		mapTasks:    make([]TaskStatus, len(files)),
		reduceTasks: make([]TaskStatus, nReduce),
	}
	// Your code here.

	m.server() //启动RPC服务
	return &m
}
