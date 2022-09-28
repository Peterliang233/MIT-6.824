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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		// 向coordinator获取任务进行执行
		getArgs := GetTaskArgs{}
		getReply := GetTaskReply{}
		call("Coordinator.GetTask", &getArgs, &getReply)
		switch getReply.Task.TaskType {
		case MapTask:
			fmt.Printf("Worker---------->Get Map Task: %v, %v\n",getReply.Task.TaskID, getReply.Task)
			doMapf(mapf, getReply.Task)
		case ReduceTask:
			fmt.Printf("Worker--------->Get Reduce Task: %v, %v\n",getReply.Task.TaskID, getReply.Task)
			doReducef(reducef, getReply.Task)
		case WaitTask:
			fmt.Println("Get Wait Task")
			time.Sleep(time.Second)
		case DoneTask:
			fmt.Println("All Task Done")
			return;
		default:
			os.Exit(1)
		}

		// 任务执行完了之后需要告诉coordinatior任务执行情况
		setTaskArgs := SetTaskDoneArgs{}
		setTaskArgs.Task = getReply.Task
		setTaskReply := SetTaskDoneReply{}
		call("Coordinator.SetTaskDone", &setTaskArgs,&setTaskReply)
	}
}

// map任务处理详情
// 将这个map任务进行处理，得到的key-val对划分到nReduce个桶里面
func doMapf(mapf func(string,string) []KeyValue, mapTask TaskInfo) {
	fmt.Printf("Map worker get taskID:%v\n",mapTask.TaskID)
	intermediateMaps := make([][]KeyValue,mapTask.NReduce)
	for i, _ := range intermediateMaps {
		intermediateMaps[i] = make([]KeyValue,0)
	}

	file, err := os.Open(mapTask.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", mapTask.FileName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapTask.FileName)
	}

	file.Close()

	kva := mapf(mapTask.FileName, string(content))


	// 将得到的kv键值对通过哈希分桶到不同的桶里面
	for _, kv := range kva {
		idx := ihash(kv.Key)%mapTask.NReduce
		intermediateMaps[idx] = append(intermediateMaps[idx], kv)
	}

	// 写入临时文件里面
	for i := 0; i < mapTask.NReduce; i++ {
		// if len(intermediateMaps[i]) == 0 {
		// 	continue
		// }

		oname := fmt.Sprintf("mr-%d-%d", mapTask.TaskID, i)
		
		// 先写入临时文件，然后
		ofile, _ := ioutil.TempFile("./", "tmp_")
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediateMaps[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("write file error %v", err)
			}
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}

}


// reduce任务处理详情
// 处理Map任务写入的那些临时文件的数据，将这些数据进行处理之后写入到最终的结果的文件里面
func doReducef(reducef func(string,[]string) string, reduceTask TaskInfo) {
	fmt.Printf("Reduce Work get task %d\n", reduceTask.TaskID)
	
	intermediate := make([]KeyValue, 0)

	for i := 0; i < reduceTask.NMap; i ++ {
		iname := fmt.Sprintf("mr-%d-%d", i, reduceTask.TaskID)
		file, err := os.Open(iname)
		if err != nil {
			log.Fatalf("Open file err: %v", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate =append(intermediate, kv)
		}
		file.Close()
	}


	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reduceTask.TaskID)
	ofile, _ := ioutil.TempFile("./", "tmp_")
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[j-1].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i=j
	}

	ofile.Close()
	os.Rename(ofile.Name(), oname)
	
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
