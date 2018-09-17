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
	"time"
)

/**
	Checkpoint is a map of int identifiers and values. The identifiers are specific to each transform, some
	may have only one identifier, e.g. AMQP Source, others may have multiple, e.g. Kafka Source
 */

type Checkpoint struct {
	Part int
	Data interface{}
	acked bool
}

type Stamp struct {
	Time time.Time
	Lo   uint64
	Hi   uint64
}

func (s *Stamp) valid() bool {
	return s.Lo != 0 && s.Hi != 0 && s.Lo <= s.Hi
}

func (s *Stamp) merge(other Stamp) Stamp {
	if !s.valid() {
		s.Lo = other.Lo
		s.Hi = other.Hi
		s.Time = other.Time
	} else {
		if other.Lo < s.Lo {
			s.Lo = other.Lo
		}
		if other.Hi > s.Hi {
			s.Hi = other.Hi
		}
		if other.Time.After(s.Time) {
			s.Time = other.Time
		}
	}
	return *s
}

func (s *Stamp) String() string {
	if s.valid() {
		return fmt.Sprintf("{%d:%d}", s.Lo, s.Hi)
	} else {
		return "{-}"
	}
}