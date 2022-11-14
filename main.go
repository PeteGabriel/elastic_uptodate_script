package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/pkg/errors"
	"log"
	"sync"
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

	//will contain all the indices names for a cluster
	idxNames, err := getIndicesNamesFromCluster(err, es)
	if err != nil {
		log.Fatalln(err)
	}

	var wg sync.WaitGroup

	//for each name get the documents and insert into local storage
	for _, name := range idxNames {
		docs, err := getDocumentsForIndice(es, name)
		if err != nil {
			log.Fatalln(err)
		}
		wg.Add(1)
		name := name //for goroutine to grab the right value
		go func() {
			err := insertDocsIntoLocalStorage(es, name, docs, &wg)
			if err != nil {
				log.Println(err)
			}
		}()

	}
	wg.Wait()
}

func getIndicesNamesFromCluster(err error, ec *elasticsearch.Client) ([]string, error) {
	names := make([]string, 0)

	catIdxReq := esapi.CatIndicesRequest{
		Pretty: true,
		Format: "JSON",
	}
	res, err := catIdxReq.Do(context.Background(), ec)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, errors.Wrap(err, fmt.Sprintf("[%s]", res.Status()))
	} else {
		// Deserialize the response into a map.
		var r []map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return nil, errors.Wrap(err, "error parsing the response body for indices names")
		} else {
			//traverse and append indices name to slice
			for _, idx := range r {
				names = append(names, idx["index"].(string))
			}
		}
	}
	return names, nil
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

func getDocumentsForIndice(ec *elasticsearch.Client, idx string) ([]interface{}, error) {
	var (
		buf bytes.Buffer
		r   map[string]interface{}
	)
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"query_string": map[string]interface{}{
				"query": "*",
			},
		},
		"size": 10000,
		"from": 0,
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, errors.Wrap(err, "error encoding query")
	}

	res, err := ec.Search(
		ec.Search.WithContext(context.Background()),
		ec.Search.WithIndex(idx),
		ec.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, errors.Wrap(err, "error performing query")
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return nil, errors.Wrap(err, "error parsing the response body for getting documents from indices")
		} else {
			// Print the response status and error information.
			return nil, errors.New(fmt.Sprintf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			))
		}
	}

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, errors.Wrap(err, "error parsing the response body")
	}
	// Print the response status, number of results, and request duration.
	log.Printf(
		"[%s] %d hits; took: %dms\n",
		res.Status(),
		int(r["hits"].(map[string]interface{})["total"].(float64)),
		int(r["took"].(float64)),
	)

	return r["hits"].(map[string]interface{})["hits"].([]interface{}), nil
}

func insertDocsIntoLocalStorage(ec *elasticsearch.Client, idx string, docs []interface{}, wg *sync.WaitGroup) error {
	fmt.Println(fmt.Sprintf("inserting into %s total amount of %d docs", idx, len(docs)))
	wg.Done()
	return nil
}
