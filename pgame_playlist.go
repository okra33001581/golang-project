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
	Id              string    `json:"id`
	Project         string    `json:"project`
	User_id         string    `json:"user_id`
	Uername         string    `json:"uername`
	Date            time.Time `json:"date`
	Lottery         string    `json:"lottery`
	Issue_count     string    `json:"issue_count`
	Prize_number    string    `json:"prize_number`
	Way             string    `json:"way`
	Dynamic_prize   string    `json:"dynamic_prize`
	Project_content string    `json:"project_content`
	Multiple        string    `json:"multiple`
	Total_amount    string    `json:"total_amount`
	Mode            string    `json:"mode`
	Prize_amount    string    `json:"prize_amount`
	Prize_status    string    `json:"prize_status`
	Status          string    `json:"status`
	Rebate_amount   string    `json:"rebate_amount`
	Way_type        string    `json:"way_type`
	Merchant_id     string    `json:"merchant_id`
	Merchant_name   string    `json:"merchant_name`
}

func main() {
	client, err := elastic.NewClient(elastic.SetURL("http://192.168.36.147:9200"))
	if err != nil {
		fmt.Println("%v", err)
	}

	db, err := sql.Open("mysql", "root:123456@tcp(192.168.36.147:3306)/api?charset=utf8")

	checkErr(err)

	rows, err := db.Query("SELECT id,project,user_id,uername,date,lottery,issue_count,prize_number,way,dynamic_prize,project_content,multiple,total_amount,mode,prize_amount,prize_status,status,rebate_amount,way_type,merchant_id,merchant_name FROM report_pgame_playlist")
	checkErr(err)
	bulkRequest := client.Bulk()
	n := 0
	for rows.Next() {
		var id, project, user_id, uername, date, lottery, issue_count, prize_number, way, dynamic_prize, project_content, multiple, total_amount, mode, prize_amount, prize_status, status, rebate_amount, way_type, merchant_id, merchant_name string
		if err := rows.Scan(&id, &project, &user_id, &uername, &date, &lottery, &issue_count, &prize_number, &way, &dynamic_prize, &project_content, &multiple, &total_amount, &mode, &prize_amount, &prize_status, &status, &rebate_amount, &way_type, &merchant_id, &merchant_name); err == nil {
			fmt.Println(err)
		}

		date_final, _ := time.Parse("2006-01-02 15:04:05", date)

		tweet := Tweet{Id: id, Project: project, User_id: user_id, Uername: uername, Date: date_final, Lottery: lottery, Issue_count: issue_count, Prize_number: prize_number, Way: way, Dynamic_prize: dynamic_prize, Project_content: project_content, Multiple: multiple, Total_amount: total_amount, Mode: mode, Prize_amount: prize_amount, Prize_status: prize_status, Status: status, Rebate_amount: rebate_amount, Way_type: way_type, Merchant_id: merchant_id, Merchant_name: merchant_name}
		req := elastic.NewBulkIndexRequest().Index("report_pgame_playlist").Type("report_pgame_playlist").Id(id).Doc(tweet)
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
