package lib

import (
	"github.com/streadway/amqp"
	"logQueue/config"
)

// RabbitMQ 用于管理和维护rabbitmq的对象
type RabbitMQ struct {
	channel *amqp.Channel
	mqConn  *amqp.Connection
}

//连接mq
func (mq *RabbitMQ) connMq(url string, config *amqp.Config) (err error) {
	mq.mqConn, err = amqp.DialConfig(url, *config)
	if err != nil {
		return err
	}
	mq.channel, err = mq.mqConn.Channel()
	if err != nil {
		return err
	}
	return nil
}

/*
	func PrepareQueue 持久化设置
*/
func (mq *RabbitMQ) PrepareQueue(queueName string) (queue amqp.Queue, err error) {
	queue, err = mq.channel.QueueDeclare(
		queueName, //name
		true,      //durable，是否持久化，默认持久需要根据情况选择
		false,     //delete when unused
		false,     //exclusive
		false,     //no-wait
		nil,       //arguments
	)
	return
}

/*
	func PrepareExchange 准备rabbitmq的Exchange
*/
func (mq *RabbitMQ) PrepareExchange(exchangeName, exchangeType string) error {
	err := mq.channel.ExchangeDeclare(
		exchangeName, // exchange
		exchangeType, // type
		true,         // durable 是否持久化，默认持久需要根据情况选择
		false,        // autoDelete
		false,        // internal
		false,        // noWait
		nil,          // args
	)
	if nil != err {
		return err
	}

	return nil
}

/*
	func ExchangeSend 通过exchange发送消息
*/
func (mq *RabbitMQ) ExchangeSend(exchangeName, routingKey string, publishing amqp.Publishing) error {

	return mq.channel.Publish(
		exchangeName, //exchangeName
		routingKey,   //routing key
		false,        //mandatory
		false,        //immediate
		publishing,
	)
}

/*
	func QueueSend 通过队列发送消息
*/
func (mq *RabbitMQ) QueueSend(queueName string, publishing amqp.Publishing) error {

	return mq.channel.Publish(
		"",        //exchangeName
		queueName, //queue name
		false,     //mandatory
		false,     //immediate
		publishing,
	)
}

/*
	func QueueBindExchange 队列绑定exchange
*/
func (mq *RabbitMQ) QueueBindExchange(queueName, routingKey, exchangeName string) error {
	return mq.channel.QueueBind(queueName, routingKey, exchangeName, false, nil)
}

/*
	func Close 关闭连接
*/

func (mq *RabbitMQ) Close() {
	_ = mq.mqConn.Close()
}

func (mq *RabbitMQ) ChannelClose() {
	_ = mq.channel.Close()
}

/*
	func InitRabbitMq 初始化连接
*/
func (mq *RabbitMQ) InitRabbitMq(name string) (t *RabbitMQ, err error) {
	// 获取配置信息
	var conf amqp.Config

	confInit := config.RabbitMQConf[name]
	host := confInit.HOST
	port := confInit.PORT
	user := confInit.USER
	password := confInit.PASSWORD

	// 连接地址
	dsn := "amqp://" + user + ":" + password + "@" + host + ":" + port

	conf.Vhost = confInit.VHOST

	mq.mqConn, err = amqp.DialConfig(dsn, conf)
	if err != nil {
		return mq, err
	}

	mq.channel, err = mq.mqConn.Channel()
	if err != nil {
		return mq, err
	}

	return mq, nil
}

/*
	RabbitMqSend 发送消息
	queueName string
*/
func MQSend(conn string, queueName string, exchangeName string, routingKey string, data []byte) error {
	// 连接mq
	var R RabbitMQ
	mq, err := R.InitRabbitMq(conn)
	if err != nil {
		return err
	}

	defer mq.Close()
	defer mq.ChannelClose()

	// 可以定义基本的exchange类型，topic(模糊匹配),direct
	err = mq.PrepareExchange(exchangeName, "direct")
	if err != nil {
		return err
	}

	// 持久化设置
	_, err = mq.PrepareQueue(queueName)
	if err != nil {
		return err
	}

	// 绑定交换机
	if err = mq.QueueBindExchange(queueName, routingKey, exchangeName); err != nil {
		return err
	}
	// 发送消息
	err = mq.ExchangeSend(exchangeName, routingKey, amqp.Publishing{
		ContentType:  "text/plain",
		Body:         data,
		DeliveryMode: amqp.Persistent,
	})
	if err != nil {
		return err
	}
	return nil
}

/*
	RabbitMqSend 发送消息
	queueName string
*/
func MQConsume(conn string, queueName string, exchangeName string, routingKey string, callBach func(<-chan amqp.Delivery, string)) {
	// 连接mq
	var R RabbitMQ
	mq, err := R.InitRabbitMq(conn)
	if err != nil {
		panic(err)
	}

	defer mq.Close()
	defer mq.ChannelClose()

	// 可以定义基本的exchange类型，topic(模糊匹配),direct
	err = mq.PrepareExchange(exchangeName, "direct")
	if err != nil {
		panic(err)
	}

	// 持久化设置
	_, err = mq.PrepareQueue(queueName)
	if err != nil {
		panic(err)
	}

	// 绑定交换机
	if err = mq.QueueBindExchange(queueName, routingKey, exchangeName); err != nil {
		panic(err)
	}
	// 2.从队列获取消息（消费者只关注队列）consume方式会不断的从队列中获取消息
	message, err := mq.channel.Consume(
		queueName, // 队列名
		"",        // 消费者名，用来区分多个消费者，以实现公平分发或均等分发策略
		false,     // 是否自动应答
		false,     // 是否排他
		false,     // 是否接收只同一个连接中的消息，若为true，则只能接收别的conn中发送的消息
		false,     // 队列消费是否阻塞
		nil,       // 额外属性
	)
	if err != nil {
		panic(err)
	}
	forever := make(chan bool)

	// 申明一个goroutine,程序始终监听
	for i := 0; i <= 5; i++ {
		go callBach(message, queueName)
	}

	<-forever
}
