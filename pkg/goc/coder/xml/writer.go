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

package xml

import (
	"fmt"
	"github.com/amient/goconnect/pkg/goc/util"
	"io"
)

var openBeginTag = []byte("<")
var openEndTag = []byte("</")
var closeTag = []byte(">")
var whitespace = []byte(" ")
var colon = []byte(":")
var valueOpen = []byte("=\"")
var valueClose = []byte("\"")

func WriteNodeAsString(n Node) (string, error) {
	w := util.NewStringWriter()
	if _, err := WriteNode(w, n); err != nil {
		return "", err
	}
	return w.String(), nil
}

func WriteNode(w io.Writer, n Node) (int, error) {

	var written int
	var err error
	switch n.Type() {
	case Root:
		return writeNodeChildren(w, n)
	case Tag:
		var wr int
		wr, err = w.Write(openBeginTag)
		if written += wr; err != nil {
			return written, err
		}
		wr, err = w.Write([]byte(n.TagName()))
		if written += wr; err != nil {
			return written, err
		}
		for _, a := range n.Attr() {
			wr, err = w.Write(whitespace)
			if written += wr; err != nil {
				return written, err
			}
			if a.Name.Space != "" {
				wr, err = w.Write([]byte(a.Name.Space))
				if written += wr; err != nil {
					return written, err
				}
				wr, err = w.Write(colon)
				if written += wr; err != nil {
					return written, err
				}
			}
			wr, err = w.Write([]byte(a.Name.Local))
			if written += wr; err != nil {
				return written, err
			}
			wr, err = w.Write(valueOpen)
			if written += wr; err != nil {
				return written, err
			}
			wr, err = w.Write([]byte(a.Value))
			if written += wr; err != nil {
				return written, err
			}
			wr, err = w.Write(valueClose)
			if written += wr; err != nil {
				return written, err
			}
		}
		wr, err = w.Write(closeTag)
		if written += wr; err != nil {
			return written, err
		}

		wr, err = writeNodeChildren(w, n)
		if written += wr; err != nil {
			return written, err
		}
		wr, err = w.Write(openEndTag)
		if written += wr; err != nil {
			return written, err
		}
		wr, err = w.Write([]byte(n.TagName()))
		if written += wr; err != nil {
			return written, err
		}
		wr, err = w.Write(closeTag)
		if written += wr; err != nil {
			return written, err
		}
		return written, nil

	case Text:
		return w.Write([]byte(n.Text()))

	case Comment:
		return w.Write([]byte("<!--" + n.Comment() + "-->"))
	case ProcInst:
		return w.Write([]byte("<?" + n.Target() + " " + n.Inst() + "?>\n"))

	case Directive:
		return w.Write([]byte("<!" + n.Directive() + "!>\n"))

	default:
		return written, fmt.Errorf("Unknown node type %d", n.Type())
	}

	return written, nil
}

func writeNodeChildren(w io.Writer, node Node) (int, error) {
	written := 0
	for _, n := range node.Children() {
		_written, err := WriteNode(w, n)
		written += _written
		if err != nil {
			return written, err
		}
	}

	return written, nil
}
