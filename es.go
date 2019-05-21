package es

import (
	"db"
	//"encoding/json"
	"fmt"

	elastigo "github.com/mattbaird/elastigo/lib"

	//elastigo "github.com/Uncodin/elastigo/lib"
	//"github.com/Uncodin/elastigo/core"
	"time"
	//"bytes"
	"flag"
	"sync"
	//"github.com/fatih/structs"
)

var (
	//开发测试库
	//host = flag.String("host", "192.168.1.236", "Elasticsearch Host")
	//C平台线上
	host = flag.String("host", "192.168.100.23", "Elasticsearch Host")
	port = flag.String("port", "9200", "Elasticsearch port")
)

//indexor := core.NewBulkIndexorErrors(10, 60)
// func init() {
//     //connect to elasticsearch
//     fmt.Println("connecting  es")
//     //api.Domain = *host //"192.168.1.236"
//     //api.Port = "9300"

// }
//save thread count
var counter int

type Elastic struct {
	//Seq int64
	c         *elastigo.Conn
	lock      *sync.Mutex
	lockTotal *sync.Mutex
	wg        *sync.WaitGroup
	total     int64
}

func (this *Elastic) Conn() {
	this.c = elastigo.NewConn()
	this.c.Domain = *host
	this.c.Port = *port
	//NewClient(fmt.Sprintf("%s:%d", *host, *port))
}
func (this *Elastic) CreateLock() {
	this.lock = &sync.Mutex{}
	this.lockTotal = &sync.Mutex{}
	this.wg = &sync.WaitGroup{}
	counter = 0
	this.total = 0
}
func NewES() (es *Elastic) {
	//connect elastic
	es = new(Elastic)
	es.Conn()
	//create lock
	es.CreateLock()
	return es
}
func (this *Elastic) DoBulk(series []*prd.Series) {
	for true {
		this.lock.Lock()
		if counter < 25 {
			//跳出，执行任务
			break
		} else {
			this.lock.Unlock()
			//等待100毫秒
			//fmt.Println("wait counter less than 25, counter:", counter)
			time.Sleep(1e8)
		}
	}
	this.lock.Unlock()
	//执行任务
	go this.bulk(series, this.lock)
}
func (this *Elastic) Over() {
	this.wg.Wait()
	/*for {
	      this.lock.Lock()
	      if counter <= 0 {
	          this.lock.Unlock()
	          break
	      }
	      this.lock.Unlock()
	  }
	*/
}

func (this *Elastic) GetTotal() (t int64) {
	this.lockTotal.Lock()
	t = this.total
	this.lockTotal.Unlock()
	return t
}
func (this *Elastic) bulk(series []*prd.Series, lock *sync.Mutex) (succCount int64) {
	//增加计数器
	this.wg.Add(1)
	//减少计数器
	defer this.wg.Done()

	//加计数器
	lock.Lock()
	counter++
	fmt.Println("add task, coutner:", counter)
	lock.Unlock()

	//设置初始成功写入的数量
	succCount = 0

	for _, value := range series {
		//json, _ := json.Marshal(value)
		//fmt.Println(string(json))
		if value.ServiceGroup != nil {
			fmt.Println("series code:", value.Code, ",ServiceGroup:", value.ServiceGroup)

			resp, err := this.c.Index("guttv", "series", value.Code, nil, *value)

			if err != nil {
				panic(err)
			} else {
				//fmt.Println(value.Code + " write to ES succsessful!")
				fmt.Println(resp)
				succCount++
			}
		} else {
			fmt.Println("series code:", value.Code, "service group is null")
		}
	}

	//计数器减一
	lock.Lock()
	counter--
	fmt.Println("reduce task, coutner:", counter, ",success count:", succCount)
	lock.Unlock()

	this.lockTotal.Lock()
	this.total = this.total + succCount
	this.lockTotal.Unlock()
	return succCount
}
