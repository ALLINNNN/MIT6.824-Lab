package mapreduce

import (
    "fmt"
//    "net/rpc"
    "sync"
    "strings"
)

type ResultStruct struct {
    Srv string
//    Index int
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

    srvCollection := make([]string, 5)
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

                    srv = result.Srv            //Assign a task to the worker which had finished a task just now
                    rpcSuccessCount++
                    if rpcSuccessCount >= ntasks {
                        fmt.Printf("rpcSuccessCount = %v, ntasks = %v\n", rpcSuccessCount, ntasks)
                        fmt.Printf("rpcSuccessCount > ntask - 1, break loop\n")
                        break Exit
                    }
                } else {                        //current task failure
                    fmt.Printf("rpc call failure, failure worker = %v\n", result.Srv)
                    for j := 0; j < len(srvCollection); j++ {               //find another worker for this task
                        if strings.Compare(srvCollection[j], result.Srv) != 0 {
                            srv = srvCollection[j]
                            fmt.Printf("Select a new worker for task, new worker = %v\n", srv)
                            break
                        }
                    }
                    flagMapFiles[result.TaskNumberIndex] = false
                }
            case register := <-registerChan:
                srv = register
                srvCollection = append(srvCollection, register)
                fmt.Printf("Get a resigerchan, value = %v\n", srv)
                fmt.Printf("srvCollection = %v\n", srvCollection)
        }

        isHandOut := false
        for j := 0; j < ntasks; j++ {
            if flagMapFiles[j] == false {
                args.File = mapFiles[j]
                args.TaskNumber = j
                fmt.Printf("selec map file = %v\n", args.File)
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

