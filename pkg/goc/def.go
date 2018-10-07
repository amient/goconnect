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

package goc

import (
	"reflect"
	"time"
)

type Def struct {
	Type                   reflect.Type
	Fn                     Fn
	Id                     int
	Up                     *Def
	pipeline               *Pipeline
	maxVerticalParallelism int
	triggerEach            int
	triggerEvery           time.Duration
}

func (def *Def) Apply(f Fn) *Def {
	return def.pipeline.Apply(def, f)
}

func (def *Def) Map(f interface{}) *Def {
	return def.pipeline.Map(def, UserMapFn(f))
}

func (def *Def) FlatMap(f interface{}) *Def {
	return def.pipeline.ElementWise(def, UserFlatMapFn(f))
}

func (def *Def) Filter(f interface{}) *Def {
	return def.pipeline.Filter(def, UserFilterFn(f))
}

func (def *Def) Fold(init interface{}, acc interface{}) *Def {
	return def.pipeline.Fold(def, UserFoldFn(init, acc))
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

func (def *Def) Par(i int) *Def {
	def.maxVerticalParallelism = i
	return def
}

func (def *Def) TriggerEach(i int) *Def {
	def.triggerEach = i
	return def
}

func (def *Def) TriggerEvery(i time.Duration) *Def {
	def.triggerEvery = i
	return def
}
