package reduce

import (
	"encoding/json"
	"fmt"
	"map-reduce/common"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

func reduceF(key string, values []string) string {
	total := 0
	for _, v := range values {
		val, _ := strconv.Atoi(v)
		total += val
	}
	return strconv.Itoa(total)
}
func DoReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int) {

	fmt.Println("Beginning of Reducer Phase")
	start := time.Now()
	mapKeyValue := make(map[string][]string)

	var wg sync.WaitGroup
	for m := 0; m < nMap; m++ {
		wg.Add(1)

		m := m
		go func() {
			defer wg.Done()
			doReducerLoop(jobName, m, reduceTaskNumber, mapKeyValue)
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Println("End of Reducer Phase, took:", elapsed)
} // the number of map tasks that were run ("M" in the paper)

func doReducerLoop(jobName string, count int, reduceTaskNumber int, mapKeyValue map[string][]string) {
	fileName := common.ReduceName(jobName, count, reduceTaskNumber)
	file, _ := os.Open(fileName)
	dec := json.NewDecoder(file)
	for {
		var kv common.KeyValue
		err := dec.Decode(&kv)
		if err != nil {
			break
		}
		_, ok := mapKeyValue[kv.Key]
		if !ok {
			mapKeyValue[kv.Key] = []string{}
		}
		mapKeyValue[kv.Key] = append(mapKeyValue[kv.Key], kv.Value)
	}
	file.Close()
	var keys []string
	for k := range mapKeyValue {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	merged := common.MergeName(jobName, reduceTaskNumber)
	file, _ = os.Create(merged)
	enc := json.NewEncoder(file)
	for _, k := range keys {
		enc.Encode(common.KeyValue{Key: k, Value: reduceF(k, mapKeyValue[k])})
	}
	file.Close()

}
