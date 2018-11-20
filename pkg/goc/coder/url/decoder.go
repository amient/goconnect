/*
 * Copyright 2018 Amient Ltd, London
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package url

import (
	"bytes"
	"encoding/binary"
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/util"
	"reflect"
)

type Decoder struct{}

func (d *Decoder) InType() reflect.Type {
	return goc.BinaryType
}

func (d *Decoder) OutType() reflect.Type {
	return UrlType
}

func (d *Decoder)  Materialize() func(input interface{}) interface{} {
	return func(input interface{}) interface{} {
		reader := bytes.NewReader(input.([]byte))
		var err error
		proto, err := util.ReadString(reader)
		path, err := util.ReadString(reader)
		name, err := util.ReadString(reader)
		mod := int64(0)
		err = binary.Read(reader, binary.LittleEndian, &mod)
		if err != nil {
			panic(err)
		}
		return &Url{
			Proto: proto,
			Path:  path,
			Name:  name,
			Mod:   mod,
		}
	}
}
