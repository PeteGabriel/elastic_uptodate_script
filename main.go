package main

import (
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/pkg/errors"
	"log"
)

/*
Connect to elastic environment.
Retrieve data from indices.
Import that data into local elastic environment.
*/
func main() {
	envSettings, err := New("debug.env")
	if err != nil {
		log.Fatalln(err)
	}
	es, err := newElasticClient(envSettings)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println(es.Info())
}

func newElasticClient(env *Environment) (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{env.InternalURI},
		Username:  env.InternalUsername,
		Password:  env.InternalPassword,
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "error creating new client")
	}
	return es, nil
}
