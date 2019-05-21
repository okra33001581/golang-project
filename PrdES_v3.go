package main

import (
	"./db"
	"./es"

	//"encoding/json"
	"fmt"
	"reflect"
	"time"
)

type PrdES struct {
	DB *prd.Mysql
	ES *es.Elastic
}

// func (this *PrdES) Handle(result []*prd.Series) {
//     // for _, value := range result {
//     //     this.DB.FormatData(value)
//     //     //json, _ := json.Marshal(value)
//     //     //fmt.Println(string(json))
//     // }
//     //写入ES,以多线程的方式执行，最多保持5个线程
//     this.ES.DoBulk(result)
// }
func (this *PrdES) Run() {
	count := 50
	offset := 0
	maxCount := 20
	//create channel
	chs := make([]chan []*prd.Series, maxCount)
	selectCase := make([]reflect.SelectCase, maxCount)
	for i := 0; i < maxCount; i++ {
		offset = count * i
		fmt.Println("offset:", offset)
		//init channel
		chs[i] = make(chan []*prd.Series)
		//set select case
		selectCase[i].Dir = reflect.SelectRecv
		selectCase[i].Chan = reflect.ValueOf(chs[i])
		//运行
		go this.DB.GetData(offset, count, chs[i])
	}
	var result []*prd.Series
	for {
		//wait data return
		chosen, recv, ok := reflect.Select(selectCase)
		if ok {
			fmt.Println("channel id:", chosen)
			result = recv.Interface().([]*prd.Series)

			//读取数据从mysql
			go this.DB.GetData(offset, count, chs[chosen])

			//写入ES,以多线程的方式执行，最多保持15个线程
			this.ES.DoBulk(result)
			//update offset
			offset = offset + len(result)
			//判断是否到达数据尾部，最后一次查询
			if len(result) < count {
				fmt.Println("read end of DB")
				//等所有的任务执行完毕
				this.ES.Over()
				fmt.Println("MySQL Total:", this.DB.GetTotal(), ",Elastic Total:", this.ES.GetTotal())
				return

			}
		}
	}

}

func main() {
	s := time.Now()
	fmt.Println("start")
	pe := new(PrdES)

	pe.DB = prd.NewDB()
	pe.ES = es.NewES()
	//fmt.Println("mysql info：")
	//fmt.Println("ES info：")
	pe.Run()

	fmt.Println("time out:", time.Since(s).Seconds(), "(s)")
	fmt.Println("Over!")

}
