package lane

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const (
	blockPoolMagic       = uint32(0x4C414E45) // "LANE"
	blockPoolVersion     = uint16(1)
	blockPoolHeaderSize  = 64
	blockPoolDefaultSize = uint64(4096)
	blockPoolDefaultGrow = uint64(256)
)

const (
	blockFlagFree    = uint16(1 << 0)
	blockFlagWritten = uint16(1 << 1)
)

var (
	// ErrBlockPoolClosed is returned when an operation is attempted on a
	// closed block pool.
	ErrBlockPoolClosed = errors.New("lane: block pool is closed")

	// ErrBlockPoolCorrupt is returned when the block pool file cannot be
	// decoded or has an unsupported layout.
	ErrBlockPoolCorrupt = errors.New("lane: block pool is corrupt")

	// ErrBlockPoolFull is returned when no free blocks are available and the
	// pool cannot grow.
	ErrBlockPoolFull = errors.New("lane: block pool is full")

	// ErrInvalidBlock is returned for a nil or otherwise invalid block handle.
	ErrInvalidBlock = errors.New("lane: invalid block")

	// ErrInvalidQueueID is returned when queue id zero is used. Zero is
	// reserved to mean "no owner" in an on-disk block header.
	ErrInvalidQueueID = errors.New("lane: invalid queue id")

	// ErrBlockTooLarge is returned when the payload does not fit in a block.
	ErrBlockTooLarge = errors.New("lane: block payload is too large")

	// ErrBlockUnwritten is returned when a block is read after allocation but
	// before its first successful write.
	ErrBlockUnwritten = errors.New("lane: block has not been written")

	// ErrBlockReleased is returned when a released block handle is used.
	ErrBlockReleased = errors.New("lane: block has been released")

	// ErrBlockOwnerMismatch is returned when the owner information stored in
	// the block header does not match the handle used to read it.
	ErrBlockOwnerMismatch = errors.New("lane: block owner does not match")

	// ErrBlockCorrupt is returned when a block header or payload checksum does
	// not match its contents.
	ErrBlockCorrupt = errors.New("lane: block is corrupt")
)

// QueueID identifies the queue that owns a block.
type QueueID uint64

// BlockID identifies a block inside a BlockPool.
type BlockID uint64

// BlockPoolOptions configures a BlockPool.
//
// Zero values select the defaults for newly created pools. When an existing
// pool is opened, BlockSize must either be zero or match the size stored in
// the file.
type BlockPoolOptions struct {
	BlockSize   uint64
	GrowBy      uint64
	SyncOnWrite bool
}

// BlockPoolStats is a snapshot of block pool activity.
type BlockPoolStats struct {
	Allocations  uint64
	Releases     uint64
	Reads        uint64
	Writes       uint64
	Grows        uint64
	BytesRead    uint64
	BytesWritten uint64
}

// Block is a handle for a block owned by one queue at one logical offset.
//
// A Block is only valid for the pool that created it. Handles become stale
// after Release; a stale handle can never read a block that has since been
// assigned to another queue.
type Block struct {
	pool *BlockPool

	ID         BlockID
	QueueID    QueueID
	Offset     uint64
	Generation uint64
}

type blockState uint8

const (
	blockAllocated blockState = iota
	blockWritten
)

type blockMeta struct {
	state      blockState
	generation uint64
	queueID    QueueID
	offset     uint64
	length     uint32
}

// BlockPool stores fixed-size blocks in a single file. Each block starts with
// a header containing its current queue, logical offset, generation, length,
// and checksums.
//
// Allocation is an in-memory operation. The new header is written together
// with the first payload write, so assigning a recycled block does not add a
// separate disk write. The old header is therefore overwritten before the new
// payload becomes readable.
type BlockPool struct {
	mu sync.Mutex

	file       *os.File
	blockSize  uint64
	growBy     uint64
	blockCount uint64

	syncOnWrite bool
	closed      bool

	free           []BlockID
	blocks         map[BlockID]*blockMeta
	nextGeneration uint64
	stats          BlockPoolStats
}

// OpenBlockPool opens or creates a block pool at path.
func OpenBlockPool(path string, opts BlockPoolOptions) (*BlockPool, error) {
	if path == "" {
		return nil, errors.New("lane: empty block pool path")
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	pool := &BlockPool{
		file:   file,
		blocks: make(map[BlockID]*blockMeta),
	}

	if err := pool.initialize(opts); err != nil {
		_ = file.Close()
		return nil, err
	}

	return pool, nil
}

// NewBlockPool is an alias for OpenBlockPool.
func NewBlockPool(path string, opts BlockPoolOptions) (*BlockPool, error) {
	return OpenBlockPool(path, opts)
}

func (p *BlockPool) initialize(opts BlockPoolOptions) error {
	info, err := p.file.Stat()
	if err != nil {
		return err
	}

	p.syncOnWrite = opts.SyncOnWrite
	p.nextGeneration = 1

	if info.Size() == 0 {
		p.blockSize = opts.BlockSize
		if p.blockSize == 0 {
			p.blockSize = blockPoolDefaultSize
		}
		if p.blockSize <= blockPoolHeaderSize {
			return fmt.Errorf("%w: block size is too small", ErrBlockPoolCorrupt)
		}

		p.growBy = opts.GrowBy
		if p.growBy == 0 {
			p.growBy = blockPoolDefaultGrow
		}

		return p.growLocked()
	}

	super, err := p.readSuperblock()
	if err != nil {
		return err
	}
	if opts.BlockSize != 0 && opts.BlockSize != super.blockSize {
		return fmt.Errorf("%w: block size does not match file", ErrBlockPoolCorrupt)
	}

	p.blockSize = super.blockSize
	p.blockCount = super.blockCount
	p.growBy = super.growBy
	if opts.GrowBy != 0 {
		p.growBy = opts.GrowBy
	}
	if !poolFileContainsBlocks(info.Size(), p.blockSize, p.blockCount) {
		return fmt.Errorf("%w: file is shorter than its block table", ErrBlockPoolCorrupt)
	}

	if err := p.scanBlocks(); err != nil {
		return err
	}
	if len(p.free) == 0 {
		return p.growLocked()
	}

	return nil
}

// Close closes the underlying file. The BlockPool cannot be used after Close.
func (p *BlockPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	if p.syncOnWrite {
		if err := p.file.Sync(); err != nil {
			_ = p.file.Close()
			p.closed = true
			return err
		}
	}

	p.closed = true
	return p.file.Close()
}

// Allocate claims a free block for queueID at offset.
//
// Allocate performs no disk I/O in the normal case. The header on disk is
// replaced with the new ownership data by Write, in the same system call that
// writes the payload.
func (p *BlockPool) Allocate(queueID QueueID, offset uint64) (*Block, error) {
	if queueID == 0 {
		return nil, ErrInvalidQueueID
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrBlockPoolClosed
	}
	if len(p.free) == 0 {
		if err := p.growLocked(); err != nil {
			return nil, err
		}
	}

	index := len(p.free) - 1
	id := p.free[index]
	p.free = p.free[:index]

	generation := p.nextGeneration
	p.nextGeneration++

	p.blocks[id] = &blockMeta{
		state:      blockAllocated,
		generation: generation,
		queueID:    queueID,
		offset:     offset,
	}
	p.stats.Allocations++

	return &Block{
		pool:       p,
		ID:         id,
		QueueID:    queueID,
		Offset:     offset,
		Generation: generation,
	}, nil
}

// WriteBlock allocates a block and writes data to it. When the write fails,
// the allocation is discarded before the error is returned.
func (p *BlockPool) WriteBlock(queueID QueueID, offset uint64, data []byte) (*Block, error) {
	block, err := p.Allocate(queueID, offset)
	if err != nil {
		return nil, err
	}
	if err := p.Write(block, data); err != nil {
		p.discard(block)
		return nil, err
	}

	return block, nil
}

// Write stores data in block, replacing any previous ownership header and
// payload. Header and payload are written together in one WriteAt call.
func (p *BlockPool) Write(block *Block, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrBlockPoolClosed
	}
	if err := p.validateHandleLocked(block); err != nil {
		return err
	}
	if uint64(len(data)) > p.blockSize-blockPoolHeaderSize {
		return ErrBlockTooLarge
	}

	meta := p.blocks[block.ID]
	header := encodedBlockHeader{
		blockID:    block.ID,
		generation: meta.generation,
		queueID:    meta.queueID,
		offset:     meta.offset,
		length:     uint32(len(data)),
		flags:      blockFlagWritten,
		dataCRC:    crc32.ChecksumIEEE(data),
	}
	buf := make([]byte, blockPoolHeaderSize+len(data))
	encodeBlockHeader(buf[:blockPoolHeaderSize], header)
	copy(buf[blockPoolHeaderSize:], data)

	if _, err := p.file.WriteAt(buf, p.blockOffset(block.ID)); err != nil {
		return err
	}

	meta.state = blockWritten
	meta.length = uint32(len(data))
	p.stats.Writes++
	p.stats.BytesWritten += uint64(len(data))

	return p.syncIfNeeded()
}

// Read returns the payload stored in block after validating the on-disk
// ownership header. A handle from an older allocation can never read a block
// that has been reassigned.
func (p *BlockPool) Read(block *Block) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrBlockPoolClosed
	}
	if err := p.validateHandleLocked(block); err != nil {
		return nil, err
	}

	meta := p.blocks[block.ID]
	if meta.state != blockWritten {
		return nil, ErrBlockUnwritten
	}

	buf := make([]byte, p.blockSize)
	if _, err := p.file.ReadAt(buf, p.blockOffset(block.ID)); err != nil {
		return nil, err
	}

	header, err := decodeBlockHeader(buf[:blockPoolHeaderSize])
	if err != nil || header.blockID != block.ID || header.flags&blockFlagWritten == 0 {
		return nil, ErrBlockCorrupt
	}
	if header.queueID != block.QueueID ||
		header.offset != block.Offset ||
		header.generation != block.Generation {
		return nil, ErrBlockOwnerMismatch
	}
	if header.length > uint32(p.blockSize-blockPoolHeaderSize) {
		return nil, ErrBlockCorrupt
	}

	payload := buf[blockPoolHeaderSize : blockPoolHeaderSize+uint64(header.length)]
	if crc32.ChecksumIEEE(payload) != header.dataCRC {
		return nil, ErrBlockCorrupt
	}

	p.stats.Reads++
	p.stats.BytesRead += uint64(header.length)

	result := make([]byte, len(payload))
	copy(result, payload)

	return result, nil
}

// Release returns block to the free list and clears its on-disk owner header.
// The block payload is intentionally left in place; the free header makes it
// unreachable until the next Write replaces both the header and the payload.
func (p *BlockPool) Release(block *Block) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrBlockPoolClosed
	}
	if err := p.validateHandleLocked(block); err != nil {
		return err
	}

	generation := p.nextGeneration
	p.nextGeneration++
	header := encodedBlockHeader{
		blockID:    block.ID,
		generation: generation,
		flags:      blockFlagFree,
	}
	buf := make([]byte, blockPoolHeaderSize)
	encodeBlockHeader(buf, header)

	if _, err := p.file.WriteAt(buf, p.blockOffset(block.ID)); err != nil {
		return err
	}

	delete(p.blocks, block.ID)
	p.free = append(p.free, block.ID)
	p.stats.Releases++
	p.stats.Writes++

	return p.syncIfNeeded()
}

// Lookup returns the current handle for an allocated block.
func (p *BlockPool) Lookup(id BlockID) (*Block, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrBlockPoolClosed
	}

	meta, ok := p.blocks[id]
	if !ok {
		return nil, ErrBlockReleased
	}

	return &Block{
		pool:       p,
		ID:         id,
		QueueID:    meta.queueID,
		Offset:     meta.offset,
		Generation: meta.generation,
	}, nil
}

// Stats returns a snapshot of the pool's activity counters.
func (p *BlockPool) Stats() BlockPoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.stats
}

func (p *BlockPool) discard(block *Block) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed || p.blocks[block.ID] == nil {
		return
	}

	delete(p.blocks, block.ID)
	p.free = append(p.free, block.ID)
}

func (p *BlockPool) validateHandleLocked(block *Block) error {
	if block == nil || block.pool != p || block.ID == 0 {
		return ErrInvalidBlock
	}

	meta, ok := p.blocks[block.ID]
	if !ok {
		return ErrBlockReleased
	}
	if meta.generation != block.Generation ||
		meta.queueID != block.QueueID ||
		meta.offset != block.Offset {
		return ErrBlockOwnerMismatch
	}

	return nil
}

func (p *BlockPool) syncIfNeeded() error {
	if !p.syncOnWrite {
		return nil
	}

	return p.file.Sync()
}

func (p *BlockPool) blockOffset(id BlockID) int64 {
	return int64(blockPoolHeaderSize) + int64(id-1)*int64(p.blockSize)
}

func (p *BlockPool) growLocked() error {
	if p.growBy == 0 {
		return ErrBlockPoolFull
	}

	newCount := p.blockCount + p.growBy
	if newCount < p.blockCount {
		return ErrBlockPoolFull
	}

	const maxInt64 = uint64(1<<63 - 1)
	if newCount > (maxInt64-blockPoolHeaderSize)/p.blockSize {
		return ErrBlockPoolFull
	}
	size := uint64(blockPoolHeaderSize) + newCount*p.blockSize

	if err := p.file.Truncate(int64(size)); err != nil {
		return err
	}

	for id := p.blockCount + 1; id <= newCount; id++ {
		p.free = append(p.free, BlockID(id))
	}
	p.blockCount = newCount
	p.stats.Grows++

	return p.writeSuperblockLocked()
}

func (p *BlockPool) writeSuperblockLocked() error {
	buf := make([]byte, blockPoolHeaderSize)
	writeUint32(buf[0:4], 0)
	writeUint32(buf[4:8], blockPoolMagic)
	writeUint16(buf[8:10], blockPoolVersion)
	writeUint16(buf[10:12], blockPoolHeaderSize)
	writeUint64(buf[16:24], p.blockSize)
	writeUint64(buf[24:32], p.blockCount)
	writeUint64(buf[32:40], p.growBy)
	writeUint32(buf[0:4], crc32.ChecksumIEEE(buf))

	if _, err := p.file.WriteAt(buf, 0); err != nil {
		return err
	}
	p.stats.Writes++

	return p.syncIfNeeded()
}

func (p *BlockPool) readSuperblock() (superblock, error) {
	buf := make([]byte, blockPoolHeaderSize)
	if err := readFullAt(p.file, buf, 0); err != nil {
		return superblock{}, fmt.Errorf("%w: %v", ErrBlockPoolCorrupt, err)
	}

	if headerChecksum(buf) != binary.LittleEndian.Uint32(buf[0:4]) ||
		binary.LittleEndian.Uint32(buf[4:8]) != blockPoolMagic ||
		binary.LittleEndian.Uint16(buf[8:10]) != blockPoolVersion ||
		binary.LittleEndian.Uint16(buf[10:12]) != blockPoolHeaderSize {
		return superblock{}, ErrBlockPoolCorrupt
	}

	super := superblock{
		blockSize:  binary.LittleEndian.Uint64(buf[16:24]),
		blockCount: binary.LittleEndian.Uint64(buf[24:32]),
		growBy:     binary.LittleEndian.Uint64(buf[32:40]),
	}
	if super.blockSize <= blockPoolHeaderSize {
		return superblock{}, ErrBlockPoolCorrupt
	}

	return super, nil
}

func (p *BlockPool) scanBlocks() error {
	header := make([]byte, blockPoolHeaderSize)
	for id := uint64(1); id <= p.blockCount; id++ {
		blockID := BlockID(id)
		offset := p.blockOffset(blockID)
		if err := readFullAt(p.file, header, offset); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				p.free = append(p.free, blockID)
				continue
			}
			return err
		}

		decoded, err := decodeBlockHeader(header)
		if err == nil && decoded.generation >= p.nextGeneration {
			p.nextGeneration = decoded.generation + 1
		}
		if err != nil || decoded.blockID != blockID || decoded.flags&blockFlagFree != 0 {
			p.free = append(p.free, blockID)
			continue
		}
		if decoded.flags&blockFlagWritten == 0 {
			p.free = append(p.free, blockID)
			continue
		}

		p.blocks[blockID] = &blockMeta{
			state:      blockWritten,
			generation: decoded.generation,
			queueID:    decoded.queueID,
			offset:     decoded.offset,
			length:     decoded.length,
		}
	}

	return nil
}

type superblock struct {
	blockSize  uint64
	blockCount uint64
	growBy     uint64
}

type encodedBlockHeader struct {
	blockID    BlockID
	generation uint64
	queueID    QueueID
	offset     uint64
	length     uint32
	flags      uint16
	dataCRC    uint32
}

func encodeBlockHeader(buf []byte, header encodedBlockHeader) {
	writeUint32(buf[0:4], 0)
	writeUint32(buf[4:8], blockPoolMagic)
	writeUint16(buf[8:10], blockPoolVersion)
	writeUint16(buf[10:12], blockPoolHeaderSize)
	writeUint16(buf[12:14], header.flags)
	writeUint64(buf[16:24], uint64(header.blockID))
	writeUint64(buf[24:32], header.generation)
	writeUint64(buf[32:40], uint64(header.queueID))
	writeUint64(buf[40:48], header.offset)
	writeUint32(buf[48:52], header.length)
	writeUint32(buf[52:56], header.dataCRC)
	writeUint32(buf[0:4], crc32.ChecksumIEEE(buf))
}

func decodeBlockHeader(buf []byte) (encodedBlockHeader, error) {
	if len(buf) < blockPoolHeaderSize {
		return encodedBlockHeader{}, ErrBlockPoolCorrupt
	}

	if headerChecksum(buf) != binary.LittleEndian.Uint32(buf[0:4]) ||
		binary.LittleEndian.Uint32(buf[4:8]) != blockPoolMagic ||
		binary.LittleEndian.Uint16(buf[8:10]) != blockPoolVersion ||
		binary.LittleEndian.Uint16(buf[10:12]) != blockPoolHeaderSize {
		return encodedBlockHeader{}, ErrBlockPoolCorrupt
	}

	return encodedBlockHeader{
		blockID:    BlockID(binary.LittleEndian.Uint64(buf[16:24])),
		generation: binary.LittleEndian.Uint64(buf[24:32]),
		queueID:    QueueID(binary.LittleEndian.Uint64(buf[32:40])),
		offset:     binary.LittleEndian.Uint64(buf[40:48]),
		length:     binary.LittleEndian.Uint32(buf[48:52]),
		flags:      binary.LittleEndian.Uint16(buf[12:14]),
		dataCRC:    binary.LittleEndian.Uint32(buf[52:56]),
	}, nil
}

func readFullAt(file *os.File, buf []byte, offset int64) error {
	n, err := file.ReadAt(buf, offset)
	if err != nil {
		if errors.Is(err, io.EOF) && n == len(buf) {
			return nil
		}
		return err
	}
	if n != len(buf) {
		return io.ErrUnexpectedEOF
	}

	return nil
}

func headerChecksum(buf []byte) uint32 {
	checksum := crc32.NewIEEE()
	_, _ = checksum.Write([]byte{0, 0, 0, 0})
	_, _ = checksum.Write(buf[4:])

	return checksum.Sum32()
}

func poolFileContainsBlocks(fileSize int64, blockSize, blockCount uint64) bool {
	if fileSize < 0 {
		return false
	}

	const maxInt64 = uint64(1<<63 - 1)
	if blockCount > (maxInt64-blockPoolHeaderSize)/blockSize {
		return false
	}

	return uint64(fileSize) >= uint64(blockPoolHeaderSize)+blockCount*blockSize
}

func writeUint16(buf []byte, value uint16) {
	binary.LittleEndian.PutUint16(buf, value)
}

func writeUint32(buf []byte, value uint32) {
	binary.LittleEndian.PutUint32(buf, value)
}

func writeUint64(buf []byte, value uint64) {
	binary.LittleEndian.PutUint64(buf, value)
}
