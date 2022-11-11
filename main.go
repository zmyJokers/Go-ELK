package main

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"logQueue/lib"
	"time"
)

func main() {

	queueName := "log_oms5_request"
	exchangeName := "log_exchange_request"
	routingKey := "log_oms5_route_request"
	callBack := Consumer
	// 队列消费
	lib.MQConsume("db", queueName, exchangeName, routingKey, callBack)
}

type data struct {
	Authorization string `json:"authorization"`
	ClientId      string `json:"client_Id"`
	Method        string `json:"method"`
	Path          string `json:"path"`
	Request       string `json:"request"`
	Response      string `json:"response"`
	SystemName    string `json:"systemName"`
	Route         string `json:"route"`
	ServerIp      string `json:"server_ip"`
	Time          string `json:"time"`
	Url           string `json:"url"`
}

func Consumer(message <-chan amqp.Delivery, index string) {

	timeTmp := "2006-01-02 15:04:05"

	for res := range message {
		fmt.Println("Waiting ...")
		// 连接es
		es, _ := lib.Conn()

		// 解析数据
		var d data
		_ = json.Unmarshal(res.Body, &d)

		// ES 时区问题 UTC+8 所以这里要重新计算日期(-8 hour)
		datetime, _ := time.ParseInLocation(timeTmp, d.Time, time.Local)
		d.Time = time.Unix(datetime.Unix()-28800, 0).Format(timeTmp)

		// 日志插入
		arr, _ := json.Marshal(d)
		r, _ := lib.CreateDoc(es, index, string(arr))

		if r.StatusCode != 201 {
			res.Nack(false, false)
			fmt.Println("consumer error！")
		} else {
			// 确认消费
			res.Ack(false)
			fmt.Println("consumer success！")
		}
	}
}
