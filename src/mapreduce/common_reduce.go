package mapreduce

import (
    "fmt"
    "sort"
    "io"
    "io/ioutil"
    "log"
    "encoding/json"
    "strings"
    "os"
//    "strconv"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

    fmt.Printf("outFile = %v\n", outFile)
    var kvs []KeyValue
    for i := 0; i < nMap; i++ {

        filename := reduceName(jobName, i, reduceTask)    //This is curious, which previous is ok, but now it needs to -1
//        filename := reduceName(jobName, i, reduceTask -1 )
//        fmt.Printf("filename = %v\n", filename)
        content, err := ioutil.ReadFile(filename)
        if err != nil {
            fmt.Println("ReadFile error")
            log.Fatal(err)
        }
//        fmt.Printf("content:\n%v\n", string(content))
        dec := json.NewDecoder(strings.NewReader(string(content)))

        for {
            var kv KeyValue
            if err := dec.Decode(&kv); err == io.EOF {
                break
            } else if err != nil {
                log.Fatal(err)
            }
            kvs = append(kvs, kv)
        }

    }
    sort.Slice(kvs, func(i, j int) bool {

        return kvs[i].Key < kvs[j].Key
    })

//    fmt.Printf("kvs content:\n%v\n", kvs)
    fmt.Printf("sort kvs done!\n")

    index := 1
    kvsLen := len(kvs)
    var valueString []string
    for i := 0; i < kvsLen; {

//        fmt.Printf("i = %v, index = %v, len(kvs) = %v\n", i, index, kvsLen)
//        fmt.Printf("i = %v, kvs[i].Key = %v; index = %v, kvs[i+index].Key = %v\n", i, kvs[i].Key, index, kvs[i+index].Key)
        if (i + index < kvsLen) && (strings.Compare(kvs[i].Key, kvs[i+index].Key) == 0) {
            index++
            continue
        }else {

            for j := i; j < i + index; j ++ {
                valueString = append(valueString, kvs[j].Value)
            }
            file, err := os.OpenFile(outFile, os.O_RDWR | os.O_APPEND | os.O_CREATE, 0755)
            if err != nil {
                fmt.Println("OpenFile failure\n")
            }
            enc := json.NewEncoder(file)
            tempString := reduceF(kvs[i].Key, valueString)
//            fmt.Printf("reduceF return string = %v\n", tempString)
//            enc.Encode(KeyValue{kvs[i].Key, reduceF(kvs[i].Key, valueString)})
            enc.Encode(KeyValue{kvs[i].Key, tempString})
            file.Close()

            i += index
            index = 1
            valueString = valueString[:0]
        }
    }

//    content, _ := ioutil.ReadFile(outFile)
//    fmt.Printf("content:\n%v\n", string(content))

}





