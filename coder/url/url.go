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
	"reflect"
	"time"
)

var UrlType = reflect.TypeOf(&Url{})

type Url struct {
	Proto string
	Path  string
	Name  string
	Mod   int64
}

func (u *Url) String() string {
	return time.Unix(u.Mod, 0).String() + " " + u.Proto + "://" + u.Path + "/" + u.Name
}
func (u *Url) AbsolutePath() string {
	return u.Path + "/" + u.Name
}
