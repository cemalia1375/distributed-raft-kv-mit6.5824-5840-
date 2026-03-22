package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// ihash函数用于决定一个单词该去哪个Reduce任务
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(mapf func(string, string) []KeyValue, reply ExampleReply) {
	// 1. 读取输入文件内容
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()

	// 2. 调用外部传入的 mapf (比如 wc.so 里的 Map) 生成键值对
	kva := mapf(reply.FileName, string(content))

	// 3. 创建中间文件缓冲区：我们要把数据分给 nReduce 个文件
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % reply.NReduce
		intermediate[index] = append(intermediate[index], kv)
	}
	/*
		// 4. 将数据持久化到本地磁盘 (JSON 格式)
		for i := 0; i < reply.NReduce; i++ {
			oname := fmt.Sprintf("mr-%v-%v", reply.TaskId, i)
			ofile, _ := os.Create(oname)
			enc := json.NewEncoder(ofile)
			for _, kv := range intermediate[i] {
				enc.Encode(&kv) // 使用 JSON 格式存，方便 Reduce 阶段读
			}
			ofile.Close()
		}
	*/
	// --- 升级：原子性写入中间文件 ---
	for i := 0; i < reply.NReduce; i++ {
		// 1. 创建临时文件
		tempFile, err := ioutil.TempFile("", "mr-map-tmp-*")
		if err != nil {
			log.Fatal("无法创建 Map 临时文件", err)
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediate[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatal("JSON 编码失败", err)
			}
		}
		tempFile.Close()

		// 2. 原子重命名为最终的中间文件名 mr-TaskId-i
		finalName := fmt.Sprintf("mr-%v-%v", reply.TaskId, i)
		os.Rename(tempFile.Name(), finalName)
	}

	// 5. 汇报任务完成 (这一步需要调用一个 RPC 告诉 Master)
	CallTaskCompleted(reply)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//fmt.Println("Worker: 启动成功，准备领任务...") // 强制打印
	// Your worker implementation here.
	// 这是一个死循环，直到 Master 告诉我们退出
	for {
		args := ExampleArgs{}
		reply := ExampleReply{}

		// 1. 发送 RPC 请求领任务
		ok := call("Master.GetTask", &args, &reply)

		if !ok {
			//fmt.Println("Worker: RPC 调用 GetTask 失败，退出")
			// 如果打不通电话，说明 Master 可能已经退出了，我们也退出
			return
		}
		//fmt.Printf("Worker: 领到任务类型 %v, ID %v\n", reply.Type, reply.TaskId)
		// 2. 根据任务类型干活
		switch reply.Type {
		case MapTask:
			doMap(mapf, reply)
			//fmt.Println("Worker: Map 任务完成并汇报") // 加这一行
		case ReduceTask:
			doReduce(reducef, reply)
			//fmt.Println("Worker: Reduce 任务完成并汇报") // 加这一行
		case WaitTask:
			// 经理说活还没准备好，歇 1 秒再问
			time.Sleep(time.Second)
		case ExitTask:
			// 活干完了，回家休息
			return
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func CallTaskCompleted(task ExampleReply) {
	args := task           // 把拿到的任务信息传回去
	reply := ExampleArgs{} // 经理不需要回话，所以用空的
	call("Master.TaskCompleted", &args, &reply)
}

// 排序需要用到的辅助结构
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(reducef func(string, []string) string, reply ExampleReply) {
	intermediate := []KeyValue{}

	//fmt.Printf("DEBUG: NFiles is %v, TaskId is %v\n", reply.NFiles, reply.TaskId)
	// 添加这行调试：
	//fmt.Printf("Worker: 开始 Reduce 任务 %v, 预计读取 Map 产出文件数: %v\n", reply.TaskId, reply.NFiles)

	// 1. 读取所有该 Reduce 任务负责的中间文件 mr-X-Y
	// X 是 Map 任务数 (nFiles)，Y 是当前 Reduce 编号 (TaskId)
	for i := 0; i < reply.NFiles; i++ {
		oname := fmt.Sprintf("mr-%v-%v", i, reply.TaskId)
		file, err := os.Open(oname)
		if err != nil {
			continue // 如果文件不存在（可能该 Map 没产出对应数据），跳过
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// 2. 按 Key 排序，方便后续合并
	sort.Sort(ByKey(intermediate))

	/*
		// 3. 创建最终输出文件
		oname := fmt.Sprintf("mr-out-%v", reply.TaskId)
		ofile, _ := os.Create(oname)

		// 4. 遍历排序后的数据，对相同单词进行合并
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			// 调用用户的 reducef 函数 (比如 wc.go 里的 Reduce)
			output := reducef(intermediate[i].Key, values)

			// 写入最终结果
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		ofile.Close()
	*/

	// --- 升级：原子性写入最终结果 ---
	// 1. 创建临时文件
	tempFile, err := ioutil.TempFile("", "mr-res-tmp-*")
	if err != nil {
		log.Fatal("无法创建 Reduce 临时文件", err)
	}

	// 2. 遍历并写入临时文件
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// 写入临时文件而不是直接写 ofile
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()

	// 3. 原子重命名
	finalName := fmt.Sprintf("mr-out-%v", reply.TaskId)
	os.Rename(tempFile.Name(), finalName)

	// 5. 汇报完成
	CallTaskCompleted(reply)
}
