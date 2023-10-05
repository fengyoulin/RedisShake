package types

import (
	"RedisShake/internal/rdb/structure"
	"io"
)

// CuckooObject for MBbloomCF
type CuckooObject struct {
	encver int
}

type cuckooFilter struct {
	numBuckets    uint64
	numItems      uint64
	numDeletes    uint64
	numFilters    uint16
	bucketSize    uint16
	maxIterations uint16
	expansion     uint16
	filters       []subCF
}

type subCF struct {
	numBucketsAndSize uint64
	data              string
}

func (s *subCF) SetNumBuckets(n uint64) {
	s.numBucketsAndSize |= n & 0xffffffffffffff
}

func (s *subCF) SetBucketSize(n uint64) {
	s.numBucketsAndSize |= (n & 0xff) << 56
}

func (s subCF) NumBuckets() uint64 {
	return s.numBucketsAndSize & 0xffffffffffffff
}

func (s subCF) BucketSize() uint64 {
	return (s.numBucketsAndSize >> 56) & 0xff
}

const CF_MIN_EXPANSION_VERSION = 4

const (
	CF_DEFAULT_BUCKETSIZE = 2
	CF_MAX_ITERATIONS     = 20
	CF_DEFAULT_EXPANSION  = 1
)

func (o *CuckooObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	var cf cuckooFilter
	cf.numFilters = uint16(readUnsigned(rd))
	cf.numBuckets = readUnsigned(rd)
	cf.numItems = readUnsigned(rd)
	if o.encver < CF_MIN_EXPANSION_VERSION {
		cf.numDeletes = 0
		cf.bucketSize = CF_DEFAULT_BUCKETSIZE
		cf.maxIterations = CF_MAX_ITERATIONS
		cf.expansion = CF_DEFAULT_EXPANSION
	} else {
		cf.numDeletes = readUnsigned(rd)
		cf.bucketSize = uint16(readUnsigned(rd))
		cf.maxIterations = uint16(readUnsigned(rd))
		cf.expansion = uint16(readUnsigned(rd))
	}
	cf.filters = make([]subCF, cf.numFilters)
	exp := uint64(1)
	for i := uint16(0); i < cf.numFilters; i++ {
		cf.filters[i].SetBucketSize(uint64(cf.bucketSize))
		if o.encver < CF_MIN_EXPANSION_VERSION {
			cf.filters[i].SetNumBuckets(cf.numBuckets)
		} else {
			cf.filters[i].SetNumBuckets(readUnsigned(rd))
		}
		cf.filters[i].data = structure.ReadModuleString(rd)
		if f := &cf.filters[i]; f.data == "" || uint64(len(f.data)) != f.NumBuckets()*f.BucketSize() {
			panic("assert failed")
		}
		exp *= uint64(cf.expansion)
	}
	return
}

func (o *CuckooObject) Rewrite() []RedisCmd {
	return nil
}
