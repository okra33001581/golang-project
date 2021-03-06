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
	Id                 string    `json:"id`
	Merchant_id        string    `json:"merchant_id`
	Merchant_name      string    `json:"merchant_name`
	User_id            string    `json:"user_id`
	Username           string    `json:"username`
	Group_tmp          string    `json:"group_tmp`
	Total_project      string    `json:"total_project`
	Valid_project      string    `json:"valid_project`
	Prize_total_amount string    `json:"prize_total_amount`
	Rebate_amount      string    `json:"rebate_amount`
	Game_profit_loss   string    `json:"game_profit_loss`
	Profit_ratio       string    `json:"profit_ratio`
	Project_count      string    `json:"project_count`
	Active_count       string    `json:"active_count`
	Date               time.Time `json:"date`
}

func main() {
	client, err := elastic.NewClient(elastic.SetURL("http://192.168.36.147:9200"))
	if err != nil {
		fmt.Println("%v", err)
	}

	db, err := sql.Open("mysql", "root:123456@tcp(192.168.36.147:3306)/api?charset=utf8")

	checkErr(err)

	rows, err := db.Query("SELECT id,merchant_id,merchant_name,user_id,username,group_tmp,total_project,valid_project,prize_total_amount,rebate_amount,game_profit_loss,profit_ratio,project_count,active_count,date FROM report_platform")
	checkErr(err)
	bulkRequest := client.Bulk()
	n := 0
	for rows.Next() {
		var id, merchant_id, merchant_name, user_id, username, group_tmp, total_project, valid_project, prize_total_amount, rebate_amount, game_profit_loss, profit_ratio, project_count, active_count, date string
		if err := rows.Scan(&id, &merchant_id, &merchant_name, &user_id, &username, &group_tmp, &total_project, &valid_project, &prize_total_amount, &rebate_amount, &game_profit_loss, &profit_ratio, &project_count, &active_count, &date); err == nil {
			fmt.Println(err)
		}

		date_final, _ := time.Parse("2006-01-02 15:04:05", date)

		tweet := Tweet{Id: id, Merchant_id: merchant_id, Merchant_name: merchant_name, User_id: user_id, Username: username, Group_tmp: group_tmp, Total_project: total_project, Valid_project: valid_project, Prize_total_amount: prize_total_amount, Rebate_amount: rebate_amount, Game_profit_loss: game_profit_loss, Profit_ratio: profit_ratio, Project_count: project_count, Active_count: active_count, Date: date_final}
		req := elastic.NewBulkIndexRequest().Index("report_platform").Type("report_platform").Id(id).Doc(tweet)
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
