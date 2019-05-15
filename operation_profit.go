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
            Merchant_id string`json:"merchant_id`
            Merchant_name string`json:"merchant_name`
            User_id string`json:"user_id`
            Username string`json:"username`
            Group string`json:"group`
            In_total_amount string`json:"in_total_amount`
            Total_out_amount string`json:"total_out_amount`
            Valid_profit string`json:"valid_profit`
            Sum_turnover string`json:"sum_turnover`
            Prize_amount string`json:"prize_amount`
            Rebate_amount string`json:"rebate_amount`
            Game_profit_loss string`json:"game_profit_loss`
            Benefit_amount string`json:"benefit_amount`
            Day_salary string`json:"day_salary`
            System_subtraction string`json:"system_subtraction`
            Final_amount string`json:"final_amount`
            Date string`json:"date`
            Platform string`json:"platform`
            Model string`json:"model`
}

id
merchant_id
merchant_name
user_id
username
group
in_total_amount
total_out_amount
valid_profit
sum_turnover
prize_amount
rebate_amount
game_profit_loss
benefit_amount
day_salary
system_subtraction
final_amount
date
platform
model





func main() {
	client, err := elastic.NewClient(elastic.SetURL("http://192.168.36.147:9200"))
	if err != nil {
		fmt.Println("%v", err)
	}

	db, err := sql.Open("mysql", "root:123456@tcp(192.168.36.147:3306)/api?charset=utf8")

	checkErr(err)

	rows, err := db.Query("SELECT id,merchant_id,merchant_name,user_id,username,group,in_total_amount,total_out_amount,valid_profit,sum_turnover,prize_amount,rebate_amount,game_profit_loss,benefit_amount,day_salary,system_subtraction,final_amount,date,platform,model FROM log_admin")
	checkErr(err)
	bulkRequest := client.Bulk()
	for rows.Next() {
		var id,merchant_id,merchant_name,user_id,username,group,in_total_amount,total_out_amount,valid_profit,sum_turnover,prize_amount,rebate_amount,game_profit_loss,benefit_amount,day_salary,system_subtraction,final_amount,date,platform,model string
		if err := rows.Scan(&id,&merchant_id,&merchant_name,&user_id,&username,&group,&in_total_amount,&total_out_amount,&valid_profit,&sum_turnover,&prize_amount,&rebate_amount,&game_profit_loss,&benefit_amount,&day_salary,&system_subtraction,&final_amount,&date,&platform,&model); err == nil {
			fmt.Println(err)
		}

		tweet := Tweet{Id:id,Merchant_id:merchant_id,Merchant_name:merchant_name,User_id:user_id,Username:username,Group:group,In_total_amount:in_total_amount,Total_out_amount:total_out_amount,Valid_profit:valid_profit,Sum_turnover:sum_turnover,Prize_amount:prize_amount,Rebate_amount:rebate_amount,Game_profit_loss:game_profit_loss,Benefit_amount:benefit_amount,Day_salary:day_salary,System_subtraction:system_subtraction,Final_amount:final_amount,Date:date,Platform:platform,Model:model,}
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
