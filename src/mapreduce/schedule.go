package mapreduce

import (
    "fmt"
//    "net/rpc"
    "sync"
)

type ResultStruct struct {
    Srv string
    TaskNumberIndex int
    Reply bool
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//


    rpcSuccessCount := 0
    rpcname := "Worker.DoTask"

    var srv string
    var wg sync.WaitGroup
    var result ResultStruct

    srvCollection := make([]string, 0)
    availbSrvCollection := make([]string, 0)
    resultChan := make(chan ResultStruct)
    flagMapFiles := make([]bool, ntasks)

    fmt.Printf("flagMapFiles default value = %v\n", flagMapFiles)
    fmt.Printf("mapFiles = \n%v\n", mapFiles)
    fmt.Printf("n_other = %v\n", n_other)
    fmt.Printf("ntasks = %v\n", ntasks)

    args := DoTaskArgs{jobName, "", phase, 0, n_other}

Exit:
    for i := 0; ; i++ {

        if i >= ntasks {
            i = 0
        }
        select {
            case result = <-resultChan:
                if result.Reply == true {       //current task had finished
                    fmt.Printf("rpc call success, ntasks = %v, rpcSuccessCount = %v\n", ntasks, rpcSuccessCount)
                    fmt.Printf("server = %v\n", result.Srv)
                    fmt.Printf("task = %v has finished\n", mapFiles[result.TaskNumberIndex])

                    fmt.Printf("Before appending availbSrvCollection, value = %v\n", availbSrvCollection)
                    availbSrvCollection = append(availbSrvCollection, result.Srv)
                    fmt.Printf("After appending availbSrvCollection, value = %v\n", availbSrvCollection)
                    rpcSuccessCount++
                    if rpcSuccessCount >= ntasks {
                        fmt.Printf("rpcSuccessCount = %v, ntasks = %v\n", rpcSuccessCount, ntasks)
                        fmt.Printf("rpcSuccessCount > ntask - 1, break loop\n")
                        break Exit
                    }
                } else {                        //current task failure
                    fmt.Printf("rpc call failure, failure worker = %v\n", result.Srv)
                    fmt.Printf("Task number index = %v, flag = %v\n", result.TaskNumberIndex, flagMapFiles[result.TaskNumberIndex])
                    flagMapFiles[result.TaskNumberIndex] = false
                }
            case register := <-registerChan:
                fmt.Printf("Get a resigerchan, value = %v\n", register)
                srvCollection = append(srvCollection, register)
                fmt.Printf("Before appending availbSrvCollection, value = %v\n", availbSrvCollection)
                availbSrvCollection = append(availbSrvCollection, register)
                fmt.Printf("After appending availbSrvCollection, value = %v\n", availbSrvCollection)
                fmt.Printf("srvCollection = %v\n", srvCollection)
        }

        /*Preventing additional worker's invocations*/
        isHandOut := false
        for j := 0; j < ntasks; j++ {
            if flagMapFiles[j] == false {
                args.File = mapFiles[j]
                args.TaskNumber = j
                fmt.Printf("Selecting a map file = %v\n", args.File)
                flagMapFiles[j] = true
                break
            }
            if j == ntasks - 1 {
               isHandOut = true
            }
        }
        if isHandOut == true {
            fmt.Printf("All mapFiles has handed out, continue\n")
            continue
        }

        /*Selecting a worker to asign a task*/
        if len(availbSrvCollection) != 0 {
            srv = availbSrvCollection[0]
            fmt.Printf("Before deleting availbSrvCollection, value = %v\n", availbSrvCollection)
            availbSrvCollection = append(availbSrvCollection[:0], availbSrvCollection[1:]...)
            fmt.Printf("After deleting availbSrvCollection, value = %v\n", availbSrvCollection)
            fmt.Printf("Selecting srv = %v\n", srv)
        } else {
            fmt.Printf("There is no available worker, so discarding this task and waiting for available worker\n")
            fmt.Printf("Current task number index = %v\n", args.TaskNumber)
            fmt.Printf("Changing task number flag\n")
            fmt.Printf("Pre flag = %v\n", flagMapFiles[args.TaskNumber])
            flagMapFiles[args.TaskNumber] = false
            fmt.Printf("Now flag = %v\n", flagMapFiles[args.TaskNumber])
            continue
        }


        wg.Add(1)
        go func(srv string, rpcname string, args DoTaskArgs, resultChan chan ResultStruct) {

            fmt.Printf("[......rpc call start......]\n")
            fmt.Printf("[......svr = %v......]\n", srv)
            fmt.Printf("[......args.File = %v......]\n", args.File)
            err := call(srv, rpcname, args, nil)
            fmt.Printf("[......rpc call done......]\n")
            defer wg.Done()

            fmt.Printf("[......rpc call done, srv = %v, TaskNumber = %v, err = %v......]\n", srv, args.TaskNumber, err)
            resultChan <- ResultStruct{srv, args.TaskNumber, err}
        }(srv, rpcname, args, resultChan)
    }

    wg.Wait()
    fmt.Printf("Schedule: %v done\n", phase)
}

