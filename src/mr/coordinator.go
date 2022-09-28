package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MapTask = 1
	ReduceTask = 2
	WaitTask = 3
	DoneTask = 4
)




// 任务主要分为两种任务，一种是Map任务，主要是将数据进行分桶处理，另一种是Reduce任务，是进行统一的求和计数的操作
type TaskInfo struct {
	FileName string  // 一个任务处理一个文件的数据
	TaskType int // 任务的类型， 1-Map任务，2-Recude任务 3-Wait任务 4-Done任务
	TaskID int // 任务的ID，用来唯一表示一个任务
	NReduce int // reduce任务的个数
	NMap int // map任务的个数，有几个需要读入的文件就有几个map任务
	TimeStamp int64 // 任务的执行时间
}


// master节点的主要属性
// 主要是如何处理一些任务的调度信息，这个部分主要处理的是一些任务调度，为一些worker节点提供任务
type Coordinator struct {
	// Your definitions here.
	Mutex sync.Mutex  // 用锁来控制不同的进程之间的并发，防止不同的进程之间互相干扰
	// Map任务调度
	MapTaskReady map[int]TaskInfo
	MapTaskInProgress map[int]TaskInfo

	// Reduce任务调度
	ReduceTaskReady map[int]TaskInfo
	ReduceTaskInProgress map[int]TaskInfo

	NReduce int // reduce任务的个数
	NMap int // map任务的个数
	TmpDir string // 中间临时目录
	StartReduce bool // 是否开始执行reduce任务
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


// 回收超时的任务,超时时间设置为10s
func (c *Coordinator) collectOutTimeTask() {

	curr := time.Now().Unix()

	for i, task := range c.MapTaskInProgress {
		if curr - task.TimeStamp > 10 {
			// 如果这个任务超时了，就需要将这个任务从正在执行的状态转换为待执行的状态
			c.MapTaskReady[i] = task
			fmt.Printf("collect map task %v\n", task)
			delete(c.MapTaskInProgress, i)
		}
	}


	for i, task := range c.ReduceTaskInProgress {
		if curr - task.TimeStamp > 10 {
			c.ReduceTaskReady[i] = task
			fmt.Printf("collect reduce task %v\n", task)

			delete(c.ReduceTaskInProgress, i)
		}
	}
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	c.collectOutTimeTask()
	// 先保证是Map任务都执行完成了
	if len(c.MapTaskReady) > 0 {
		for i,task := range c.MapTaskReady {
			task.TimeStamp = time.Now().Unix()
			// 拿出一个task
			reply.Task = task
			// 将这个task放到正在执行的队列里面
			c.MapTaskInProgress[i]=task
			// 在准备执行的队列里面删除这个任务
			delete(c.MapTaskReady, i)

			fmt.Printf("Master----------->Get Map Task,TaskID:%v, TaskInfo:%v\n", task.TaskID, task)
			return nil
		}

		return errors.New("Get Map Task error")
	}

	// 然后是看是否有map任务还在执行中,如果有的话返回一个等待执行的任务
	if len(c.MapTaskInProgress) > 0 {
		reply.Task.TaskType = WaitTask
		reply.Task.TimeStamp = time.Now().Unix()
		fmt.Printf("Get Map Task InProgress,these tasks is %v\n", c.MapTaskInProgress)
		return nil
	}

	// 如果Map任务已经执行完成了，就生成一些Reduce任务
	if !c.StartReduce {
		fmt.Println("All Map Task Done,Start to execute Reduce Task!")
		for i := 0; i < c.NReduce; i ++ {
			c.ReduceTaskReady[i] = TaskInfo{
				TaskType: ReduceTask,
				TaskID: i,
				TimeStamp: time.Now().Unix(),
				NReduce: c.NReduce,
				NMap: c.NMap,
			}
		}
		c.StartReduce = true
	}

	// 接下来才是返回Reduce的任务
	if len(c.ReduceTaskReady) > 0 {
		for i, task := range c.ReduceTaskReady {
			task.TimeStamp = time.Now().Unix()
			reply.Task = task
			c.ReduceTaskInProgress[i]=task
			delete(c.ReduceTaskReady, i)
			fmt.Printf("Master------->Reduce Task,taskID:%v, taskInfo:%v\n",task.TaskID, task)
			return nil
		}
		return errors.New("Get Reduce Task error")
	}

	// 看看有没有正在执行的Reduce任务
	if len(c.ReduceTaskInProgress) > 0 {
		reply.Task.TaskType = WaitTask
		reply.Task.TimeStamp = time.Now().Unix()
		fmt.Printf("Get Reduce Task InProgress,these tasks is %v\n", c.ReduceTaskInProgress)
		return nil
	}

	// 说明所有的任务都已经执行完成了
	reply.Task.TaskType = DoneTask
	return nil
}


func (c *Coordinator) SetTaskDone(args *SetTaskDoneArgs, reply *SetTaskDoneReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if args.Task.TaskType == MapTask {
		// 从MapTaskProgress删除这个任务
		delete(c.MapTaskInProgress, args.Task.TaskID)
		fmt.Printf("Map TaskID:%v finish, left task %v\n", args.Task.TaskID,len(c.MapTaskReady) + len(c.MapTaskInProgress))
		return nil
	}else if args.Task.TaskType == ReduceTask {
		// 从ReduceTaskProgress删除这个任务
		delete(c.ReduceTaskInProgress, args.Task.TaskID)
		fmt.Printf("Reduce TaskID:%v finish,left task %v\n", args.Task.TaskID, len(c.ReduceTaskInProgress) + len(c.ReduceTaskReady))
		return nil
	}

	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// 所有的任务执行完之后需要执行的操作
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	// Your code here.
	if len(c.MapTaskInProgress) == 0 && len(c.MapTaskReady) == 0 &&
		len(c.ReduceTaskInProgress) == 0 && len(c.ReduceTaskReady) == 0 && c.StartReduce {
			return true
		}

	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	numFile := len(files)
	c := Coordinator{
		NReduce: nReduce,
		NMap: numFile,
		StartReduce: false,
	}

	// Your code here.
	c.server()
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	c.MapTaskReady = make(map[int]TaskInfo)
	c.MapTaskInProgress = make(map[int]TaskInfo)
	c.ReduceTaskReady = make(map[int]TaskInfo)
	c.ReduceTaskInProgress = make(map[int]TaskInfo)

	// 先初始化Map任务
	for i, file := range files {
		c.MapTaskReady[i] = TaskInfo{
			FileName: file,
			TaskType: MapTask,
			TaskID: i,
			NReduce: nReduce,
			NMap: numFile,
			TimeStamp: time.Now().Unix(),
		}
	}

	return &c
}
