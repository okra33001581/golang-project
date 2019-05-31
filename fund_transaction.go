package main

import (
	"fmt"

	"context"

	"gopkg.in/olivere/elastic.v6"

	"database/sql"
	"os"

	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Tweet struct {
	Id              string    `json: "id`
	Merchant_id     string    `json: "merchant_id`
	Merchant_name   string    `json: "merchant_name`
	Order_number    string    `json: "order_number`
	Date            time.Time `json: "date`
	User_id         string    `json: "user_id`
	Username        string    `json: "username`
	Account         string    `json: "account`
	Class_type      string    `json: "class_type`
	Platform        string    `json: "platform`
	Income          string    `json: "income`
	Outcome         string    `json: "outcome`
	Avaiable_amount string    `json: "avaiable_amount`
	Ip_address      string    `json: "ip_address`
	Message         string    `json: "message`
	Has_child       string    `json: "has_child`
}

func main() {
	client, err := elastic.NewClient(elastic.SetURL("http://192.168.36.147:9200"))
	if err != nil {
		fmt.Println("%v", err)
	}

	db, err := sql.Open("mysql", "root:123456@tcp(192.168.36.147:3306)/api?charset=utf8")

	checkErr(err)

	rows, err := db.Query("SELECT id,merchant_id,merchant_name,order_number,date,user_id,username,account,class_type,platform,income,outcome,avaiable_amount,ip_address,message,has_child FROM fund_transaction")
	checkErr(err)
	bulkRequest := client.Bulk()
	// n := 0
	for rows.Next() {
		var id, merchant_id, merchant_name, order_number, date, user_id, username, account, class_type, platform, income, outcome, avaiable_amount, ip_address, message, has_child string
		if err := rows.Scan(&id, &merchant_id, &merchant_name, &order_number, &date, &user_id, &username, &account, &class_type, &platform, &income, &outcome, &avaiable_amount, &ip_address, &message, &has_child); err == nil {
			fmt.Println(err)
		}

		date_final, _ := time.Parse("2006-01-02 15:04:05", date)

		tweet := Tweet{Id: id, Merchant_id: merchant_id, Merchant_name: merchant_name, Order_number: order_number, Date: date_final, User_id: user_id, Username: username, Account: account, Class_type: class_type, Platform: platform, Income: income, Outcome: outcome, Avaiable_amount: avaiable_amount, Ip_address: ip_address, Message: message, Has_child: has_child}
		req := elastic.NewBulkIndexRequest().Index("fund_transaction").Type("fund_transaction").Id(id).Doc(tweet)
		bulkRequest = bulkRequest.Add(req)

		// if n%20000 == 0 {
		// 	bulkResponse, err := bulkRequest.Do(context.TODO())
		// 	if err != nil {
		// 		fmt.Println(err)
		// 	}
		// 	if bulkResponse != nil {

		// 	}
		// 	n = 0
		// }
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
