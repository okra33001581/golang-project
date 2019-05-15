package main

import (
	"fmt"

	"context"

	"gopkg.in/olivere/elastic.v6"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type Tweet struct {
	Id            string `json:"id`
	Sub_account   string `json:"sub_account`
	Operate_name  string `json:"operate_name`
	Log_content   string `json:"log_content`
	Ip            string `json:"ip`
	Cookies       string `json:"cookies`
	Date          string `json:"date`
	Merchant_id   string `json:"merchant_id`
	Merchant_name string `json:"merchant_name`
	Created_at    string `json:"created_at`
	Origin        string `json:"origin`
	Referer       string `json:"referer`
	User_agent    string `json:"user_agent`
	Type          string `json:"type`
}

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
	for rows.Next() {
		var id, type1, sub_account, operate_name, log_content, ip, cookies, date, merchant_id, merchant_name, created_at, origin, referer, user_agent string
		if err := rows.Scan(&id, &type1, &sub_account, &operate_name, &log_content, &ip, &cookies, &date, &merchant_id, &merchant_name, &created_at, &origin, &referer, &user_agent); err == nil {
			fmt.Println(err)
		}

		tweet := Tweet{Id: id, Sub_account: sub_account, Operate_name: operate_name, Log_content: log_content, Ip: ip, Cookies: cookies, Date: date, Merchant_id: merchant_id, Merchant_name: merchant_name, Created_at: created_at, Origin: origin, Referer: referer, User_agent: user_agent, Type: type1}
		// req := elastic.NewBulkIndexRequest().Index("log_admin").Type("log_admin").Id(id).Doc(tweet)
		req := elastic.NewBulkIndexRequest().Index("log_admin1").Type("log_admin1").Id(id).Doc(tweet)
		bulkRequest = bulkRequest.Add(req)

		// fmt.Println(type1)
		// fmt.Println(rows["id"])
		// fmt.Println(sub_account)
		// fmt.Println(operate_name)
	}

	bulkResponse, err := bulkRequest.Do(context.TODO())
	if err != nil {
		fmt.Println(err)
	}
	if bulkResponse != nil {

	}

	// n := 0
	// for i := 0; i < 1000; i++ {
	// 	bulkRequest := client.Bulk()
	// 	for j := 0; j < 10000; j++ {
	// 		n++
	// 		tweet := Tweet{User: "olivere", Message: "Package strconv implements conversions to and from string representations of basic data types. " + strconv.Itoa(n)}
	// 		req := elastic.NewBulkIndexRequest().Index("twitter").Type("tweet").Id(strconv.Itoa(n)).Doc(tweet)
	// 		bulkRequest = bulkRequest.Add(req)
	// 	}
	// 	bulkResponse, err := bulkRequest.Do(context.TODO())
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// 	if bulkResponse != nil {

	// 	}
	// 	fmt.Println(i)
	// }
}
func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
