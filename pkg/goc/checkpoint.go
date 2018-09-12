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

/**
	Checkpoint is a map of int identifiers and values. The identifiers are specific to each transform, some
	may have only one identifier, e.g. AMQP Source, others may have multiple, e.g. Kafka Source
 */

type Checkpoint map[int]interface{}

func (checkpoint Checkpoint) merge(with Checkpoint) {
	if with != nil {
		for k, v := range with {
			checkpoint[k] = v
		}
	}
}
