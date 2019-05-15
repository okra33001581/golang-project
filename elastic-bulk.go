package main

import (
	"fmt"
	"strconv"

	"context"

	"gopkg.in/olivere/elastic.v6"
)

type Tweet struct {
	User    string `json:"user"`
	Message string `json:"message"`
}

func main() {
	client, err := elastic.NewClient(elastic.SetURL("http://192.168.36.147:9200"))
	if err != nil {
		fmt.Println("%v", err)
	}
	fmt.Println("abcdeeee")

	n := 0
	for i := 0; i < 1000; i++ {
		bulkRequest := client.Bulk()
		for j := 0; j < 10000; j++ {
			n++
			tweet := Tweet{User: "olivere", Message: "Package strconv implements conversions to and from string representations of basic data types. " + strconv.Itoa(n)}
			req := elastic.NewBulkIndexRequest().Index("twitter").Type("tweet").Id(strconv.Itoa(n)).Doc(tweet)
			bulkRequest = bulkRequest.Add(req)
		}
		bulkResponse, err := bulkRequest.Do(context.TODO())
		if err != nil {
			fmt.Println(err)
		}
		if bulkResponse != nil {

		}
		fmt.Println(i)
	}
}
