package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

func fatalf(msg string, args ...interface{}) {
	fmt.Printf(msg, args...)
	os.Exit(2)
}

func main() {
	host := "localhost:9200"
	indexPrefix := "logstash-"
	msgField := "@message"
	timeField := "@timestamp"
	exclude := ""
	size := 1000

	flag.StringVar(&host, "host", host, "host and port of elasticsearch")
	flag.StringVar(&indexPrefix, "prefix", indexPrefix, "prefix of log indexes")
	flag.StringVar(&msgField, "message", msgField, "message field to display")
	flag.StringVar(&timeField, "timestamp", timeField, "timestap field to sort by")
	flag.StringVar(&exclude, "exclude", exclude, "comma separated list of field:value pairs to exclude")
	flag.IntVar(&size, "size", size, "number of docs to return per polling interval")

	flag.Parse()

	exFilter := map[string]interface{}{}
	if len(exclude) > 0 {
		exkv := map[string]string{}
		for _, pair := range strings.Split(exclude, ",") {
			kv := strings.Split(pair, ":")
			exkv[kv[0]] = kv[1]
		}
		terms := []map[string]interface{}{}
		for k, v := range exkv {
			terms = append(terms, map[string]interface{}{"terms": map[string]interface{}{k: []string{v}}})
		}
		exFilter["not"] = map[string]interface{}{"or": terms}
	} else {
		exFilter["match_all"] = map[string]interface{}{}
	}

	lastTime := time.Now()

	for {
		resp, err := http.Get(fmt.Sprintf("http://%s/_status", host))
		if err != nil {
			fatalf("Error contacting Elasticsearch %s: %v", host, err)
		}
		status := map[string]interface{}{}
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			fatalf("Error decoding _status response from Elasticsearch: %v", err)
		}
		resp.Body.Close()

		indices := []string{}
		for k, _ := range status["indices"].(map[string]interface{}) {
			if strings.HasPrefix(k, indexPrefix) {
				indices = append(indices, k)
			}
		}
		if len(indices) == 0 {
			fatalf("No indexes found with the prefix '%s'", indexPrefix)
		}
		sort.Strings(indices)
		index := indices[len(indices)-1]

		url := fmt.Sprintf("http://%s/%s/_search", host, index)
		req, err := json.Marshal(map[string]interface{}{
			"filter": map[string]interface{}{
				"and": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							timeField: map[string]interface{}{
								"gt": lastTime.Format(time.RFC3339Nano),
							},
						},
					},
					exFilter,
				},
			},
			"size":   size,
			"fields": []string{msgField, timeField},
		})
		if err != nil {
			fatalf("Error creating search body: %v", err)
		}
		resp, err = http.Post(url, "application/json", bytes.NewReader(req))
		if err != nil {
			fatalf("Error searching Elasticsearch: %v", err)
		}
		if resp.StatusCode != 200 {
			body, _ := ioutil.ReadAll(resp.Body)
			fatalf("Elasticsearch failed: %s\n%s", resp.Status, string(body))
		}
		results := map[string]interface{}{}
		if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
			fatalf("Error reading search results: %v", err)
		}
		resp.Body.Close()

		hits := results["hits"].(map[string]interface{})["hits"].([]interface{})
		for _, lineI := range hits {
			line := lineI.(map[string]interface{})["fields"].(map[string]interface{})
			ts := line[timeField].([]interface{})[0].(string)

			fmt.Printf("%s %v\n", ts, line[msgField].([]interface{})[0])

			lastTime, err = time.Parse(time.RFC3339Nano, ts)
			if err != nil {
				fatalf("Error decoding timestamp: %v", err)
			}
		}

		time.Sleep(1 * time.Second)
	}
}
