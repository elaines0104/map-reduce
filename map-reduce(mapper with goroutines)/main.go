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

	for i, file := range files {
		mapper.DoMap(jobName, i, file, nReduce)
	}
	for i := 0; i < nReduce; i++ {
		reduce.DoReduce(jobName, i, len(files))
	}
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
