package main

import (
	"fmt"

	"context"

	"gopkg.in/olivere/elastic.v6"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type Tweet struct {
	Id string`json:"id`
    Project string`json:"project`
    User_id string`json:"user_id`
    Uername string`json:"uername`
    Date string`json:"date`
    Lottery string`json:"lottery`
    Issue_count string`json:"issue_count`
    Prize_number string`json:"prize_number`
    Way string`json:"way`
    Dynamic_prize string`json:"dynamic_prize`
    Project_content string`json:"project_content`
    Multiple string`json:"multiple`
    Total_amount string`json:"total_amount`
    Mode string`json:"mode`
    Prize_amount string`json:"prize_amount`
    Prize_status string`json:"prize_status`
    Status string`json:"status`
    Rebate_amount string`json:"rebate_amount`
    Way_type string`json:"way_type`
    Merchant_id string`json:"merchant_id`
    Merchant_name string`json:"merchant_name`
}

func main() {
	client, err := elastic.NewClient(elastic.SetURL("http://192.168.36.147:9200"))
	if err != nil {
		fmt.Println("%v", err)
	}

	db, err := sql.Open("mysql", "root:123456@tcp(192.168.36.147:3306)/api?charset=utf8")

	checkErr(err)

	rows, err := db.Query("SELECT id,project,user_id,uername,date,lottery,issue_count,prize_number,way,dynamic_prize,project_content,multiple,total_amount,mode,prize_amount,prize_status,status,rebate_amount,way_type,merchant_id,merchant_name FROM log_admin")
	checkErr(err)
	bulkRequest := client.Bulk()
	for rows.Next() {
		var id,project,user_id,uername,date,lottery,issue_count,prize_number,way,dynamic_prize,project_content,multiple,total_amount,mode,prize_amount,prize_status,status,rebate_amount,way_type,merchant_id,merchant_name string
		if err := rows.Scan(&id,&project,&user_id,&uername,&date,&lottery,&issue_count,&prize_number,&way,&dynamic_prize,&project_content,&multiple,&total_amount,&mode,&prize_amount,&prize_status,&status,&rebate_amount,&way_type,&merchant_id,&merchant_name); err == nil {
			fmt.Println(err)
		}

		tweet := Tweet{id:id,project:project,user_id:user_id,uername:uername,date:date,lottery:lottery,issue_count:issue_count,prize_number:prize_number,way:way,dynamic_prize:dynamic_prize,project_content:project_content,multiple:multiple,total_amount:total_amount,mode:mode,prize_amount:prize_amount,prize_status:prize_status,status:status,rebate_amount:rebate_amount,way_type:way_type,merchant_id:merchant_id,merchant_name:merchant_name}
		// req := elastic.NewBulkIndexRequest().Index("log_admin").Type("log_admin").Id(id).Doc(tweet)
		req := elastic.NewBulkIndexRequest().Index("report_pgame_playlist").Type("report_pgame_playlist").Id(id).Doc(tweet)
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
