package mapper

//package map não funcionou, verei depois o pq
import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"map-reduce/common"
	"os"
	"strings"
	"time"
	"unicode"
)

func mapF(document string, value string) (res []common.KeyValue) {
	words := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	for _, word := range words {
		res = append(res, common.KeyValue{Key: word, Value: "1"})
	}
	return res
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
func DoMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)

) {
	fmt.Println("Beginning of Mapper Phase")
	start := time.Now()
	content, err := ioutil.ReadFile(inFile)
	if err != nil {
		fmt.Println(err)
	}
	kvList := mapF(inFile, string(content)) //vai mudar aqui
	done := make(chan string)

	for r := 0; r < nReduce; r++ {
		go doMapLoop(jobName, mapTaskNumber, nReduce, done, kvList, r)

	} //recebendo o canal done antes de vir pra fazer reducer
	elapsed := time.Since(start)
	fmt.Println(<-done+", took:", elapsed)
	fmt.Printf("\n \n")
}
func doMapLoop(jobName string, mapTaskNumber int, nReduce int, done chan string, kvList []common.KeyValue, count int) {
	reduceFileName := common.ReduceName(jobName, mapTaskNumber, count)
	reduceFile, err := os.Create(reduceFileName)
	if err != nil {
		fmt.Println(err)
	}
	enc := json.NewEncoder(reduceFile)
	for _, kv := range kvList {
		if (int(ihash(kv.Key)) % nReduce) == count {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println(err)
			}

		}
	}
	reduceFile.Close()
	done <- "End of Mapper Phase"
}
