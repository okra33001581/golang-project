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
	Id                  string    `json: "id`
	Merchant_id         string    `json: "merchant_id`
	Merchant_name       string    `json: "merchant_name`
	Date                time.Time `json: "date`
	Game_type           string    `json: "game_type`
	Lottery_id          string    `json: "lottery_id`
	User_id             string    `json: "user_id`
	Account_id          string    `json: "account_id`
	Username            string    `json: "username`
	Is_tester           string    `json: "is_tester`
	User_forefather_ids string    `json: "user_forefather_ids`
	Rate                string    `json: "rate`
	Turnover            string    `json: "turnover`
	Amount              string    `json: "amount`
	Status              string    `json: "status`
	Locked              string    `json: "locked`
	Sent_at             string    `json: "sent_at`
	Created_at          time.Time `json: "created_at`
	Updated_at          time.Time `json: "updated_at`
	Source_username     string    `json: "source_username`
	Source_id           string    `json: "source_id`
}

func main() {
	client, err := elastic.NewClient(elastic.SetURL("http://192.168.36.147:9200"))
	if err != nil {
		fmt.Println("%v", err)
	}

	db, err := sql.Open("mysql", "root:123456@tcp(192.168.36.147:3306)/api?charset=utf8")

	checkErr(err)

	rows, err := db.Query("SELECT id,merchant_id,merchant_name,date,game_type,lottery_id,user_id,account_id,username,is_tester,user_forefather_ids,rate,turnover,amount,status,locked,sent_at,created_at,updated_at,source_username,source_id FROM report_commissions_statistics")
	checkErr(err)
	bulkRequest := client.Bulk()
	// n := 0
	for rows.Next() {
		var id, merchant_id, merchant_name, date, game_type, lottery_id, user_id, account_id, username, is_tester, user_forefather_ids, rate, turnover, amount, status, locked, sent_at, created_at, updated_at, source_username, source_id string
		if err := rows.Scan(&id, &merchant_id, &merchant_name, &date, &game_type, &lottery_id, &user_id, &account_id, &username, &is_tester, &user_forefather_ids, &rate, &turnover, &amount, &status, &locked, &sent_at, &created_at, &updated_at, &source_username, &source_id); err == nil {
			fmt.Println(err)
		}

		date_final, _ := time.Parse("2006-01-02 15:04:05", date)
		created_at_final, _ := time.Parse("2006-01-02 15:04:05", created_at)
		updated_at_final, _ := time.Parse("2006-01-02 15:04:05", updated_at)

		tweet := Tweet{Id: id, Merchant_id: merchant_id, Merchant_name: merchant_name, Date: date_final, Game_type: game_type, Lottery_id: lottery_id, User_id: user_id, Account_id: account_id, Username: username, Is_tester: is_tester, User_forefather_ids: user_forefather_ids, Rate: rate, Turnover: turnover, Amount: amount, Status: status, Locked: locked, Sent_at: sent_at, Created_at: created_at_final, Updated_at: updated_at_final, Source_username: source_username, Source_id: source_id}
		req := elastic.NewBulkIndexRequest().Index("report_commissions_statistics").Type("report_commissions_statistics").Id(id).Doc(tweet)
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
