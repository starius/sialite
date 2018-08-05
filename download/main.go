package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"

	"github.com/starius/sialite/netlib"
	"github.com/xtaci/smux"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/fastrand"
)

var (
	source = flag.String("source", "", "Source of data (siad node)")
)

type tee struct {
	impl io.ReadWriter
	sink io.Writer
}

func (t *tee) Read(b []byte) (int, error) {
	n, err := t.impl.Read(b)
	if err != nil {
		return n, err
	}
	n1, err := t.sink.Write(b[:n])
	if err != nil {
		return n, err
	} else if n1 != n {
		log.Println(n1, n)
		return n, io.ErrShortWrite
	}
	return n, nil
}

func (t *tee) Write(b []byte) (int, error) {
	return t.impl.Write(b)
}

func main() {
	flag.Parse()
	ctx := context.Background()
	node := *source
	if node == "" {
		i := fastrand.Intn(len(modules.BootstrapPeers))
		node = string(modules.BootstrapPeers[i])
	}
	conn, err := netlib.Connect(ctx, node)
	if err != nil {
		panic(err)
	}
	sess, err := smux.Client(conn, nil)
	if err != nil {
		panic(err)
	}
	bchan := make(chan *types.Block)
	go func() {
		for range bchan {
		}
	}()
	f := func() (io.ReadWriter, error) {
		rw, err := sess.OpenStream()
		if err != nil {
			return nil, err
		}
		return &tee{
			impl: rw,
			sink: os.Stdout,
		}, nil
	}
	if err := netlib.DownloadAllBlocks(ctx, bchan, f); err != nil {
		panic(err)
	}
}
