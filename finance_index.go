package main

import (
	"fmt"

	"context"

	"gopkg.in/olivere/elastic.v6"

	"database/sql"
	"os"

	_ "github.com/go-sql-driver/mysql"

	"time"
)

type Tweet struct {
	Id                 string    `json:"id`
	Date               time.Time `json:"date`
	Company_in         string    `json:"company_in`
	Third_in           string    `json:"third_in`
	Deposit            string    `json:"deposit`
	Common_deposit     string    `json:"common_deposit`
	Benefit            string    `json:"benefit`
	Total_rebate       string    `json:"total_rebate`
	Day_salary         string    `json:"day_salary`
	Bankcard_out       string    `json:"bankcard_out`
	Third_out          string    `json:"third_out`
	User_subtraction   string    `json:"user_subtraction`
	Artifical_withdraw string    `json:"artifical_withdraw`
	Total              string    `json:"total`
	Merchant_id        string    `json:"merchant_id`
	Merchant_name      string    `json:"merchant_name`
}

func main() {

	t1 := time.Now() // get current time

	client, err := elastic.NewClient(elastic.SetURL("http://192.168.36.147:9200"))
	if err != nil {
		fmt.Println("%v", err)
	}

	db, err := sql.Open("mysql", "root:123456@tcp(192.168.36.147:3306)/api?charset=utf8")

	// checkErr(err)

	rows, err := db.Query("SELECT id,date,company_in,third_in,deposit,common_deposit,benefit,total_rebate,day_salary,bankcard_out,third_out,user_subtraction,artifical_withdraw,total,merchant_id,merchant_name FROM report_finance limit 100000")
	// checkErr(err)
	bulkRequest := client.Bulk()
	// n := 0
	for rows.Next() {
		var id, date, company_in, third_in, deposit, common_deposit, benefit, total_rebate, day_salary, bankcard_out, third_out, user_subtraction, artifical_withdraw, total, merchant_id, merchant_name string
		if err := rows.Scan(&id, &date, &company_in, &third_in, &deposit, &common_deposit, &benefit, &total_rebate, &day_salary, &bankcard_out, &third_out, &user_subtraction, &artifical_withdraw, &total, &merchant_id, &merchant_name); err == nil {
			fmt.Println(err)
		}

		date_final, _ := time.Parse("2006-01-02 15:04:05", date)
		// fmt.Println(timeformatdate)

		tweet := Tweet{Id: id, Date: date_final, Company_in: company_in, Third_in: third_in, Deposit: deposit, Common_deposit: common_deposit, Benefit: benefit, Total_rebate: total_rebate, Day_salary: day_salary, Bankcard_out: bankcard_out, Third_out: third_out, User_subtraction: user_subtraction, Artifical_withdraw: artifical_withdraw, Total: total, Merchant_id: merchant_id, Merchant_name: merchant_name}
		req := elastic.NewBulkIndexRequest().Index("report_finance").Type("doc").Id(id).Doc(tweet)
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
		// fmt.Println(err)
	}
	if bulkResponse != nil {

	}

	fmt.Println("sucess")

	elapsed := time.Since(t1)
	fmt.Println("App elapsed: ", elapsed)

	os.Exit(3)

}
func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
