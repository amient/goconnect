package std

import (
	"bufio"
	"fmt"
	"github.com/amient/goconnect"
	"github.com/amient/goconnect/io/file"
)

func process(stdout *bufio.Writer, element interface{}) {
	switch e := element.(type) {
	case []byte:
		stdout.Write(e)
	case *goc.KVBinary:
		stdout.Write(e.Key)
		stdout.WriteString(" -> ")
		stdout.Write(e.Value)
	case file.ByteStream:
		buf := make([]byte, 0, 100)
		for e.Read(&buf) > 0 {
			stdout.Write(buf)
		}
	default:
		fmt.Fprint(stdout, element)
	}
	stdout.WriteByte('\n')

}

