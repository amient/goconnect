package avro

import (
	"encoding/json"
	"github.com/amient/avro"
	"io/ioutil"
	"net/http"
	"strconv"
)

//this class is not concurrent

type schemaRegistryClient struct {
	url   string
	cache map[uint32]avro.Schema
}

type schemaResponse struct {
	Schema string
}

func (c *schemaRegistryClient) get(schemaId uint32) avro.Schema {
	if c.cache == nil {
		c.cache = make(map[uint32]avro.Schema)
	}
	result := c.cache[schemaId]
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
				c.cache[schemaId] = result
			}
		}

	}
	return result
}

