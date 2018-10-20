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
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/coder/url"
	"io/ioutil"
	"reflect"
)

type Source struct {
	Path string
	//TODO regex filter
}

func (s *Source) OutType() reflect.Type {
	return url.UrlType
}

func (s *Source) Run(ctx *goc.Context) {
	//TODO select one node on each host, not one node out of all nodes
	if ctx.GetNodeID() == 1 {
		if files, err := ioutil.ReadDir(s.Path); err != nil {
			panic(err)
		} else {
			//TODO termination handling
			for i, f := range files {
				//TODO generate local dir complex checkpoint (timestamp + []incomplete)
				//TODO general representation of a file (not io.FileInfo or sftp.Url but goc.Url)
				ctx.Emit(&goc.Element{
					Checkpoint: goc.Checkpoint{0, i},
					Value: &url.Url{
						Proto: "file",
						Path:  s.Path,
						Name:  f.Name(),
						Mod:   f.ModTime().Unix(),
					},
				})
			}
		}
	}
}

func (s *Source) Commit(goc.Watermark, *goc.Context) error {
	return nil
}
