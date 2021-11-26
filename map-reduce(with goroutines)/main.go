package main

import (
	"fmt"
	"io/ioutil"
	"map-reduce/common"
	"map-reduce/mapper"
	"map-reduce/reduce"
)

func main() {
	var files []string
	var nReduce int
	var jobName string
	inFile := "pg-emma.txt"
	files = append(files, inFile)
	nReduce = 3
	jobName = "teste"

	//start := time.Now()
	for i, file := range files {
		mapper.DoMap(jobName, i, file, nReduce)
	}
	//elapsed := time.Since(start)
	//fmt.Println("Map phase took:", elapsed)

	//start = time.Now()
	for i := 0; i < nReduce; i++ {
		reduce.DoReduce(jobName, i, len(files))
	}
	//elapsed = time.Since(start)
	//fmt.Println("Reduce phase took:", elapsed)

	common.Merge(nReduce, jobName)
	result, err := ioutil.ReadFile("result.teste")
	if err != nil {
		fmt.Println(err)
	}
	content, err := ioutil.ReadFile("mrtmp.teste")
	if err != nil {
		fmt.Println(err)
	}
	if string(content) == string(result) {
		fmt.Println("funcionou")
	} else {
		fmt.Println("não funcionou")
	}

}
