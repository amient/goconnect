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
	"fmt"
)

type Stamp struct {
	Unix  int64
	Lo    uint64
	Hi    uint64
	Trace []uint16
}

func (s *Stamp) Valid() bool {
	return s.Lo != 0 && s.Hi != 0 && s.Lo <= s.Hi
}

func (s *Stamp) Merge(other Stamp) Stamp {
	if !s.Valid() {
		s.Lo = other.Lo
		s.Hi = other.Hi
		s.Unix = other.Unix
	} else {
		if other.Lo < s.Lo {
			s.Lo = other.Lo
		}
		if other.Hi > s.Hi {
			s.Hi = other.Hi
		}
		if other.Unix > s.Unix {
			s.Unix = other.Unix
		}
	}
	return *s
}

func (s *Stamp) String() string {
	if s.Valid() {
		return fmt.Sprintf("{%d:%d %v}", s.Lo, s.Hi, s.Trace)
	} else {
		return "{-}"
	}
}

func NewTrace(minNumTraceSteps uint16) []uint16 {
	return make([]uint16, 0, (minNumTraceSteps/16+1)*16)
}
func (s *Stamp) AddTrace(id uint16) {
	s.Trace = append(s.Trace, id)
}

func (s *Stamp) TraceLen() uint16 {
	return uint16(len(s.Trace))
}
