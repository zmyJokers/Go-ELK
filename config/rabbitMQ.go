package config

type RabbitMQConfig struct {
	HOST     string
	PORT     string
	VHOST    string
	USER     string
	PASSWORD string
}

var RabbitMQConf = map[string]*RabbitMQConfig{
	"db1": {
		HOST:     "127.0.0.1",
		PORT:     "5672",
		VHOST:    "V-logs",
		USER:     "guest",
		PASSWORD: "Password01!",
	},
	"db": {
		HOST:     "127.0.0.1",
		PORT:     "5672",
		VHOST:    "/",
		USER:     "guest",
		PASSWORD: "guest",
	},
}
