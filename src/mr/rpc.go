package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// 定义任务类型
type TaskType int
const (
    MapTask TaskType = iota
    ReduceTask
    WaitTask
    ExitTask
)
// Map, Reduce, Wait (等一会儿), Exit (干完了)

// Worker 向 Master 请求任务的参数（通常可以为空，因为 Worker 只是问“有活吗”）
type ExampleArgs struct {
	//X int
}

// Master 返回给 Worker 的任务信息
type ExampleReply struct {
		Type      TaskType // 任务类型：Map/Reduce/Wait/Exit
    TaskId    int      // 任务 ID
    FileName  string   // 如果是 Map，需要知道读哪个文件
    NReduce   int      // 传入 Reduce 任务的总数
    NFiles    int      // 传入 Map 任务的总数（即输入文件数）
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
