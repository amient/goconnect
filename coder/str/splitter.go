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

package str

import (
	"github.com/amient/goconnect"
	"reflect"
	"strings"
)

func Split(separator string) goc.Processor {
	return &Splitter{separator: separator}
}

type Splitter struct {
	separator string
}

func (s *Splitter) InType() reflect.Type {
	return goc.StringType
}

func (s *Splitter) OutType() reflect.Type {
	return goc.StringType
}

func (s *Splitter) Materialize() func(input *goc.Element, ctx goc.PContext) {
	return func(input *goc.Element, ctx goc.PContext) {
		for _, s := range strings.Split(input.Value.(string), s.separator) {
			ctx.Emit(&goc.Element{Value: s})
		}
	}
}