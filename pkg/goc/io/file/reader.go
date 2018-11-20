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

package file

import (

	"github.com/amient/goconnect/pkg/goc/coder/url"
	"io"
	"os"
	"reflect"
)

type Reader struct{}

var ByteStreamType = reflect.TypeOf((*ByteStream)(nil)).Elem()

type ByteStream interface {
	Read(bytes *[]byte) int // 0 means eof, -1 means error
}

func (r *Reader) InType() reflect.Type {
	return url.UrlType
}

func (r *Reader) OutType() reflect.Type {
	return ByteStreamType
}

func (r *Reader)  Materialize() func(input interface{}) interface{} {
	return func(input interface{}) interface{} {
		url := input.(*url.Url)
		//TODO based on the url.Proto chose the appropriate reader type and convert into a
		if file, err := os.Open(url.AbsolutePath()); err != nil {
			panic(err)
		} else {
			return &LocalFileByteStream{
				file: file,
			}
		}
	}
}

type LocalFileByteStream struct {
	file *os.File
}

func (bs *LocalFileByteStream) Read(bytes *[]byte) int {
	*bytes = (*bytes)[:cap(*bytes)]
	if n, err := bs.file.Read(*bytes); err == io.EOF {
		return 0
	} else if err != nil {
		panic(err)
	} else {
		*bytes = (*bytes)[:n]
		return n
	}
}
