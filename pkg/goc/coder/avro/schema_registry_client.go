package avro

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/amient/avro"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

//schemaRegistryClient is not concurrent so it's not exported

type schemaRegistryClient struct {
	url    string
	cache1 map[uint32]avro.Schema
	cache2 map[string]map[fingerprint]uint32
}

type schemaResponse struct {
	Schema string
}

func (c *schemaRegistryClient) get(schemaId uint32) avro.Schema {
	if c.cache1 == nil {
		c.cache1 = make(map[uint32]avro.Schema)
	}
	result := c.cache1[schemaId]
	if result == nil {
		var url = c.url + "/schemas/ids/" + strconv.Itoa(int(schemaId))
		resp, err := http.Get(url)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		if body, err := ioutil.ReadAll(resp.Body); err != nil {
			panic(err)
		} else {
			response := new(schemaResponse)
			json.Unmarshal(body, response)
			if result, err = avro.ParseSchema(response.Schema); err != nil {
				panic(err)
			} else {
				c.cache1[schemaId] = result
			}
		}

	}
	return result
}
func (c *schemaRegistryClient) getSchemaId(schema avro.Schema, subject string) uint32 {
	if c.cache2 == nil {
		c.cache2 = make(map[string]map[fingerprint]uint32)
	}
	var s map[fingerprint]uint32
	if s = c.cache2[subject]; s == nil {
		s = make(map[fingerprint]uint32)
		c.cache2[subject] = s
	}

	f := schema.Fingerprint()
	result, ok := s[f]
	if !ok {
		request := make(map[string]string)
		request["schema"] = schema.String()
		if schemaJson, err := json.Marshal(request); err != nil {
			panic(err)
		} else {
			log.Printf("Registering schema for subject %q schema: %v", subject, schema.GetName())
			var url= c.url + "/subjects/" + subject + "/versions"
			j := make(map[string]uint32)
			if resp, err := http.Post(url, "application/json", bytes.NewReader(schemaJson)); err != nil {
				panic(err)
			} else if resp.StatusCode != 200 {
				panic(fmt.Errorf(resp.Status))
			} else if data, err := ioutil.ReadAll(resp.Body); err != nil {
				panic(err)
			} else if err := json.Unmarshal(data, &j); err != nil {
				panic(err)
			} else {
				result = j["id"]
				log.Printf("Got Schema ID: %v", result)
				s[f] = result
			}
		}
	}
	return result
}

