package netlib

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/xtaci/smux"
	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/encoding"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/modules/consensus"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/fastrand"
)

type sessionHeader struct {
	GenesisID  types.BlockID
	UniqueID   [8]byte
	NetAddress modules.NetAddress
}

func Connect(ctx context.Context, node string) (net.Conn, error) {
	log.Println("Using node: ", node)
	conn, err := net.Dial("tcp", node)
	if err != nil {
		return nil, err
	}
	version := build.Version
	if err := encoding.WriteObject(conn, version); err != nil {
		return nil, err
	}
	if err := encoding.ReadObject(conn, &version, uint64(100)); err != nil {
		return nil, err
	}
	log.Println(version)
	sh := sessionHeader{
		GenesisID:  types.GenesisID,
		UniqueID:   [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		NetAddress: modules.NetAddress("example.com:1111"),
	}
	if err := encoding.WriteObject(conn, sh); err != nil {
		return nil, err
	}
	var response string
	if err := encoding.ReadObject(conn, &response, 100); err != nil {
		return nil, fmt.Errorf("failed to read header acceptance: %v", err)
	} else if response == modules.StopResponse {
		return nil, fmt.Errorf("peer did not want a connection")
	} else if response != modules.AcceptResponse {
		return nil, fmt.Errorf("peer rejected our header: %v", response)
	}
	if err := encoding.ReadObject(conn, &sh, uint64(100)); err != nil {
		return nil, err
	}
	if err := encoding.WriteObject(conn, modules.AcceptResponse); err != nil {
		return nil, err
	}
	return conn, nil
}

func DownloadBlocks(ctx context.Context, bchan chan *types.Block, conn io.ReadWriter, prevBlockID types.BlockID) (types.BlockID, error) {
	var prevBlock *types.Block
	var err error
	var rpcName [8]byte
	copy(rpcName[:], "SendBlocks")
	if err = encoding.WriteObject(conn, rpcName); err != nil {
		return prevBlockID, err
	}
	var history [32]types.BlockID
	history[31] = types.GenesisID
	moreAvailable := true
	// Send the block ids.
	history[0] = prevBlockID
	if err = encoding.WriteObject(conn, history); err != nil {
		goto exit
	}
	for moreAvailable {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			goto exit
		default:
		}
		// Read a slice of blocks from the wire.
		var newBlocks []types.Block
		if err = encoding.ReadObject(conn, &newBlocks, uint64(consensus.MaxCatchUpBlocks)*types.BlockSizeLimit); err != nil {
			goto exit
		}
		if err = encoding.ReadObject(conn, &moreAvailable, 1); err != nil {
			goto exit
		}
		for i := range newBlocks {
			b := &newBlocks[i]
			bchan <- b
			prevBlock = b
		}
	}
exit:
	if prevBlock != nil {
		prevBlockID = prevBlock.ID()
	}
	return prevBlockID, nil
}

func DownloadAllBlocks(ctx context.Context, bchan chan *types.Block, sess func() (io.ReadWriter, error)) error {
	prevBlockID := types.GenesisID
	for {
		stream, err := sess()
		if err != nil {
			return err
		}
		newPrevBlockID, err := DownloadBlocks(ctx, bchan, stream, prevBlockID)
		hadBlocks := newPrevBlockID != prevBlockID
		log.Printf("DownloadBlocks returned %v, %v.", hadBlocks, err)
		if err == nil || newPrevBlockID == prevBlockID {
			log.Printf("No error, all blocks were downloaded. Stopping.")
			break
		}
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			return err
		}
		prevBlockID = newPrevBlockID
	}
	return nil
}

type blockchainReader struct {
	impl io.Reader
}

func (t *blockchainReader) Read(b []byte) (int, error) {
	return t.impl.Read(b)
}

func (t *blockchainReader) Write(b []byte) (int, error) {
	// No operation.
	return len(b), nil
}

func OpenOrConnect(ctx context.Context, file, node string) (*smux.Session, func() (io.ReadWriter, error), error) {
	if file != "" {
		bc, err := os.Open(file)
		if err != nil {
			return nil, nil, err
		}
		f := func() (io.ReadWriter, error) {
			return &blockchainReader{impl: bc}, nil
		}
		return nil, f, nil
	}
	if node == "" {
		i := fastrand.Intn(len(modules.BootstrapPeers))
		node = string(modules.BootstrapPeers[i])
	}
	conn, err := Connect(ctx, node)
	if err != nil {
		return nil, nil, err
	}
	sess, err := smux.Client(conn, nil)
	if err != nil {
		return nil, nil, err
	}
	f := func() (io.ReadWriter, error) {
		return sess.OpenStream()
	}
	return sess, f, nil
}
