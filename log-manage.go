package main

import (
	"fmt"

	"context"

	"gopkg.in/olivere/elastic.v6"

	"database/sql"

	"time"

	"os"

	_ "github.com/go-sql-driver/mysql"
)

type Tweet struct {
	Id            string    `json:"id`
	Sub_account   string    `json:"sub_account`
	Operate_name  string    `json:"operate_name`
	Log_content   string    `json:"log_content`
	Ip            string    `json:"ip`
	Cookies       string    `json:"cookies`
	Date          time.Time `json:"date`
	Merchant_id   string    `json:"merchant_id`
	Merchant_name string    `json:"merchant_name`
	Created_at    time.Time `json:"created_at`
	Origin        string    `json:"origin`
	Referer       string    `json:"referer`
	User_agent    string    `json:"user_agent`
	Type          string    `json:"type`
}

const (
	date        = "2006-01-02"
	shortdate   = "06-01-02"
	times       = "15:04:02"
	shorttime   = "15:04"
	datetime    = "2006-01-02 15:04:02"
	newdatetime = "2006/01/02 15~04~02"
	newtime     = "15~04~02"
)

func main() {
	client, err := elastic.NewClient(elastic.SetURL("http://192.168.36.147:9200"))
	if err != nil {
		fmt.Println("%v", err)
	}

	db, err := sql.Open("mysql", "root:123456@tcp(192.168.36.147:3306)/api?charset=utf8")

	checkErr(err)

	rows, err := db.Query("SELECT id, type, sub_account,operate_name,log_content,ip,cookies,date,merchant_id,merchant_name,created_at,origin,referer,user_agent FROM log_admin")
	checkErr(err)
	bulkRequest := client.Bulk()
	n := 0
	for rows.Next() {
		n++
		var id, type1, sub_account, operate_name, log_content, ip, cookies, date, merchant_id, merchant_name, created_at, origin, referer, user_agent string
		// thisdate := "2014-03-17 14:55:06"
		// fmt.Println(&date)

		if err := rows.Scan(&id, &type1, &sub_account, &operate_name, &log_content, &ip, &cookies, &date, &merchant_id, &merchant_name, &created_at, &origin, &referer, &user_agent); err == nil {
			fmt.Println(err)
		}
		// timeformatdate, _ := time.Parse(datetime, date)
		// fmt.Println(date)

		date_final, _ := time.Parse("2006-01-02 15:04:05", date)
		created_at_final, _ := time.Parse("2006-01-02 15:04:05", created_at)
		// fmt.Println(timeformatdate)

		tweet := Tweet{Id: id, Sub_account: sub_account, Operate_name: operate_name, Log_content: log_content, Ip: ip, Cookies: cookies, Date: date_final, Merchant_id: merchant_id, Merchant_name: merchant_name, Created_at: created_at_final, Origin: origin, Referer: referer, User_agent: user_agent, Type: type1}
		// fmt.Println(tweet)
		// os.Exit(3)
		req := elastic.NewBulkIndexRequest().Index("log_admin").Type("doc").Id(id).Doc(tweet)
		bulkRequest = bulkRequest.Add(req)

		if n%20000 == 0 {
			bulkResponse, err := bulkRequest.Do(context.TODO())
			if err != nil {
				fmt.Println(err)
			}
			if bulkResponse != nil {

			}
			n = 0
		}

	}

	bulkResponse, err := bulkRequest.Do(context.TODO())
	if err != nil {
		fmt.Println(err)
	}
	if bulkResponse != nil {

	}

	os.Exit(3)
	fmt.Println("sucess")
}
func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
