package main

import (
	"fmt"

	"context"

	"gopkg.in/olivere/elastic.v6"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	"os"
	"time"
)

type Tweet struct {
	Id                   string    `json:"id`
	Date                 time.Time `json:"date`
	Ip_count             string    `json:"ip_count`
	Register_count       string    `json:"register_count`
	Active_count         string    `json:"active_count`
	First_deposit_count  string    `json:"first_deposit_count`
	First_deposit_amount string    `json:"first_deposit_amount`
	In_people_count      string    `json:"in_people_count`
	In_times             string    `json:"in_times`
	Out_times            string    `json:"out_times`
	Merchant_id          string    `json:"merchant_id`
	Merchant_name        string    `json:"merchant_name`
	Platform             string    `json:"platform`
	Model                string    `json:"model`
}

func main() {
	client, err := elastic.NewClient(elastic.SetURL("http://192.168.36.147:9200"))
	if err != nil {
		fmt.Println("%v", err)
	}

	db, err := sql.Open("mysql", "root:123456@tcp(192.168.36.147:3306)/api?charset=utf8")

	checkErr(err)

	rows, err := db.Query("SELECT id,date,ip_count,register_count,active_count,first_deposit_count,first_deposit_amount,in_people_count,in_times,out_times,merchant_id,merchant_name,platform,model FROM report_user")
	checkErr(err)
	bulkRequest := client.Bulk()
	n := 0
	for rows.Next() {
		var id, date, ip_count, register_count, active_count, first_deposit_count, first_deposit_amount, in_people_count, in_times, out_times, merchant_id, merchant_name, platform, model string
		if err := rows.Scan(&id, &date, &ip_count, &register_count, &active_count, &first_deposit_count, &first_deposit_amount, &in_people_count, &in_times, &out_times, &merchant_id, &merchant_name, &platform, &model); err == nil {
			fmt.Println(err)
		}

		date_final, _ := time.Parse("2006-01-02 15:04:05", date)

		tweet := Tweet{Id: id, Date: date_final, Ip_count: ip_count, Register_count: register_count, Active_count: active_count, First_deposit_count: first_deposit_count, First_deposit_amount: first_deposit_amount, In_people_count: in_people_count, In_times: in_times, Out_times: out_times, Merchant_id: merchant_id, Merchant_name: merchant_name, Platform: platform, Model: model}
		req := elastic.NewBulkIndexRequest().Index("report_user").Type("report_user").Id(id).Doc(tweet)
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
