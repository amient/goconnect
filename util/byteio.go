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

package util

import (
	"encoding/binary"
	"fmt"
	"io"
)

func ReadString(reader io.Reader) (string, error) {
	var len uint32
	if err := binary.Read(reader, binary.LittleEndian, &len); err != nil {
		panic(err)
		return "", err
	}
	result := make([]byte, len)
	if n, err := reader.Read(result); err != nil {
		panic(err)
		return "", err
	} else if uint32(n) != len {
		panic(fmt.Errorf(" %d <> %d", n, len))
		return "", fmt.Errorf("!")
	}
	return string(result), nil
}

func WriteString(value string, writer io.Writer) error {
	var len = uint32(len(value))
	if err := binary.Write(writer, binary.LittleEndian, len); err != nil {
		return err
	}
	if n, err := writer.Write([]byte(value)); err != nil {
		return err
	} else if uint32(n) != len {
		return fmt.Errorf("%d <> %d", n, len)
	}
	return nil
}
