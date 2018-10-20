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

type Encoder struct{}

func (e *Encoder) InType() reflect.Type {
	return UrlType
}

func (e *Encoder) OutType() reflect.Type {
	return goc.BinaryType
}

func (e *Encoder) Process(input interface{}) interface{} {
	url := input.(*Url)
	result := new(bytes.Buffer)
	util.WriteString(url.Proto, result)
	util.WriteString(url.Path, result)
	util.WriteString(url.Name, result)
	binary.Write(result, binary.LittleEndian, url.Mod)
	return result.Bytes()
}

