package lib

import (
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"strings"
)

func Conn() (E *elasticsearch.Client, err error) {
	conf := elasticsearch.Config{
		Addresses: []string{"http://127.0.0.1:9200"},
		Username:  "elastic",
		Password:  "Password01!",
	}
	Conn, err := elasticsearch.NewClient(conf)
	if err != nil {
		panic("elasticsearch conn error!")
	}
	return Conn, err
}

// 创建索引
func CreateDoc(client *elasticsearch.Client, index string, doc string) (*esapi.Response, error) {
	r, _ := client.API.Indices.Exists([]string{index})

	if r.StatusCode != 200 {
		CreateIndex(client, index)
	}

	res, err := client.API.Index(index, strings.NewReader(doc))
	return res, err
}

// 创建索引
func CreateIndex(client *elasticsearch.Client, index string) {
	mapping := `{
			"settings": {
				"number_of_shards": 5,
				"number_of_replicas": 0
			},
			"mappings": {
				"_source" : {"enabled" : true},
				"properties": {
					"time": {
						"type": "date",
						"format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
				  	}
				}
			}
		}`
	_, err := client.API.Indices.Create(index, func(request *esapi.IndicesCreateRequest) {
		request.Index = index
		request.Body = strings.NewReader(mapping)
	})
	if err != nil {
		panic("elasticsearch create index error!")
	}
}
