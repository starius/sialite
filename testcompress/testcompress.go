package testcompress

import (
	"bytes"
	"compress/flate"
	"compress/lzw"
	"io"
	"time"

	"github.com/DataDog/zstd"
	"github.com/dsnet/compress/bzip2"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/fse"
	"github.com/ulikunitz/xz/lzma"
)

var (
	dst, dst2 bytes.Buffer
	dstBytes  []byte
	dstBytes2 []byte
	scratch   fse.Scratch
	scratch2  fse.Scratch
)

func makeFlate(level int) func([]byte) (int, time.Duration, time.Duration) {
	return func(input []byte) (int, time.Duration, time.Duration) {
		src := bytes.NewBuffer(input)
		dst.Reset()
		dst2.Reset()
		w, err := flate.NewWriter(&dst, level)
		if err != nil {
			panic(err)
		}
		t1 := time.Now()
		if _, err := io.Copy(w, src); err != nil {
			panic(err)
		}
		if err := w.Close(); err != nil {
			panic(err)
		}
		l := dst.Len()
		t2 := time.Now()
		r := flate.NewReader(&dst)
		if _, err := io.Copy(&dst2, r); err != nil {
			panic(err)
		}
		if err := r.Close(); err != nil {
			panic(err)
		}
		t3 := time.Now()
		if !bytes.Equal(dst2.Bytes(), input) {
			panic("mismatch")
		}
		return l, t2.Sub(t1), t3.Sub(t2)
	}
}

func makeLzw(order lzw.Order) func([]byte) (int, time.Duration, time.Duration) {
	return func(input []byte) (int, time.Duration, time.Duration) {
		src := bytes.NewBuffer(input)
		dst.Reset()
		dst2.Reset()
		w := lzw.NewWriter(&dst, order, 8)
		t1 := time.Now()
		if _, err := io.Copy(w, src); err != nil {
			panic(err)
		}
		if err := w.Close(); err != nil {
			panic(err)
		}
		l := dst.Len()
		t2 := time.Now()
		r := lzw.NewReader(&dst, order, 8)
		if _, err := io.Copy(&dst2, r); err != nil {
			panic(err)
		}
		if err := r.Close(); err != nil {
			panic(err)
		}
		t3 := time.Now()
		if !bytes.Equal(dst2.Bytes(), input) {
			panic("mismatch")
		}
		return l, t2.Sub(t1), t3.Sub(t2)
	}
}

func snappyCompress(input []byte) (int, time.Duration, time.Duration) {
	t1 := time.Now()
	dstBytes = snappy.Encode(dstBytes, input)
	l := len(dstBytes)
	t2 := time.Now()
	var err error
	dstBytes2, err = snappy.Decode(dstBytes2, dstBytes)
	if err != nil {
		panic(err)
	}
	t3 := time.Now()
	if !bytes.Equal(dstBytes2, input) {
		panic("mismatch")
	}
	return l, t2.Sub(t1), t3.Sub(t2)
}

func lzmaCompress(input []byte) (int, time.Duration, time.Duration) {
	src := bytes.NewBuffer(input)
	dst.Reset()
	dst2.Reset()
	w, err := lzma.NewWriter(&dst)
	if err != nil {
		panic(err)
	}
	t1 := time.Now()
	if _, err := io.Copy(w, src); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
	l := dst.Len()
	t2 := time.Now()
	r, err := lzma.NewReader(&dst)
	if err != nil {
		panic(err)
	}
	if _, err := io.Copy(&dst2, r); err != nil {
		panic(err)
	}
	t3 := time.Now()
	if !bytes.Equal(dst2.Bytes(), input) {
		panic("mismatch")
	}
	return l, t2.Sub(t1), t3.Sub(t2)
}

func lzma2Compress(input []byte) (int, time.Duration, time.Duration) {
	src := bytes.NewBuffer(input)
	dst.Reset()
	dst2.Reset()
	w, err := lzma.NewWriter2(&dst)
	if err != nil {
		panic(err)
	}
	t1 := time.Now()
	if _, err := io.Copy(w, src); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
	l := dst.Len()
	t2 := time.Now()
	r, err := lzma.NewReader2(&dst)
	if err != nil {
		panic(err)
	}
	if _, err := io.Copy(&dst2, r); err != nil {
		panic(err)
	}
	t3 := time.Now()
	if !bytes.Equal(dst2.Bytes(), input) {
		panic("mismatch")
	}
	return l, t2.Sub(t1), t3.Sub(t2)
}

func fseCompress(input []byte) (int, time.Duration, time.Duration) {
	t1 := time.Now()
	compressed, err := fse.Compress(input, &scratch)
	t2 := time.Now()
	if err == fse.ErrIncompressible || err == fse.ErrUseRLE {
		return len(input), t2.Sub(t1), 0
	} else if err != nil {
		panic(err)
	}
	l := len(compressed)
	decompressed, err := fse.Decompress(compressed, &scratch2)
	if err != nil {
		panic(err)
	}
	t3 := time.Now()
	if !bytes.Equal(decompressed, input) {
		panic("mismatch")
	}
	return l, t2.Sub(t1), t3.Sub(t2)
}

func makeBzip2(level int) func([]byte) (int, time.Duration, time.Duration) {
	config := &bzip2.WriterConfig{
		Level: level,
	}
	return func(input []byte) (int, time.Duration, time.Duration) {
		src := bytes.NewBuffer(input)
		dst.Reset()
		dst2.Reset()
		w, err := bzip2.NewWriter(&dst, config)
		if err != nil {
			panic(err)
		}
		t1 := time.Now()
		if _, err := io.Copy(w, src); err != nil {
			panic(err)
		}
		if err := w.Close(); err != nil {
			panic(err)
		}
		l := dst.Len()
		t2 := time.Now()
		r, err := bzip2.NewReader(&dst, nil)
		if err != nil {
			panic(err)
		}
		if _, err := io.Copy(&dst2, r); err != nil {
			panic(err)
		}
		if err := r.Close(); err != nil {
			panic(err)
		}
		t3 := time.Now()
		if !bytes.Equal(dst2.Bytes(), input) {
			panic("mismatch")
		}
		return l, t2.Sub(t1), t3.Sub(t2)
	}
}

func makeZstd(level int) func([]byte) (int, time.Duration, time.Duration) {
	return func(input []byte) (int, time.Duration, time.Duration) {
		var err error
		t1 := time.Now()
		dstBytes, err = zstd.CompressLevel(dstBytes, input, level)
		if err != nil {
			panic(err)
		}
		l := len(dstBytes)
		t2 := time.Now()
		dstBytes2, err = zstd.Decompress(dstBytes2, dstBytes)
		if err != nil {
			panic(err)
		}
		t3 := time.Now()
		if !bytes.Equal(dstBytes2, input) {
			panic("mismatch")
		}
		return l, t2.Sub(t1), t3.Sub(t2)
	}
}

var (
	Algos = map[string]func([]byte) (int, time.Duration, time.Duration){
		"lzw_LSB":       makeLzw(lzw.LSB),
		"lzw_MSB":       makeLzw(lzw.MSB),
		"flate_minus_2": makeFlate(-2),
		"flate_minus_1": makeFlate(-1),
		"flate_1":       makeFlate(1),
		"flate_4":       makeFlate(4),
		"flate_9":       makeFlate(9),
		"snappy":        snappyCompress,
		"lzma":          lzmaCompress,
		"lzma2":         lzma2Compress,
		"fse":           fseCompress,
		"bzip2_speed":   makeBzip2(bzip2.BestSpeed),
		"bzip2_default": makeBzip2(bzip2.DefaultCompression),
		"bzip2_comp":    makeBzip2(bzip2.BestCompression),
		"zstd_1":        makeZstd(1),
		"zstd_3":        makeZstd(3),
		"zstd_10":       makeZstd(10),
		"zstd_22":       makeZstd(22),
	}
)
