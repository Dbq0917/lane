package lane

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testBlockSize = uint64(256)

func openTestBlockPool(t *testing.T) *BlockPool {
	t.Helper()

	pool, err := OpenBlockPool(
		filepath.Join(t.TempDir(), "blocks.db"),
		BlockPoolOptions{BlockSize: testBlockSize, GrowBy: 2},
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = pool.Close()
	})

	return pool
}

func TestBlockPoolReusedBlockBelongsToNewQueue(t *testing.T) {
	t.Parallel()

	pool := openTestBlockPool(t)

	oldBlock, err := pool.WriteBlock(1, 10, []byte("old queue payload"))
	require.NoError(t, err)
	require.NoError(t, pool.Release(oldBlock))

	releasedHeader := readTestBlockHeader(t, pool, oldBlock.ID)
	assert.Equal(t, blockFlagFree, releasedHeader.flags)
	assert.Equal(t, QueueID(0), releasedHeader.queueID)

	writesBeforeAllocate := pool.Stats().Writes
	newBlock, err := pool.Allocate(2, 99)
	require.NoError(t, err)
	assert.Equal(t, oldBlock.ID, newBlock.ID)
	assert.Equal(t, writesBeforeAllocate, pool.Stats().Writes)

	_, err = pool.Read(newBlock)
	assert.ErrorIs(t, err, ErrBlockUnwritten)

	require.NoError(t, pool.Write(newBlock, []byte("new queue payload")))

	payload, err := pool.Read(newBlock)
	require.NoError(t, err)
	assert.Equal(t, []byte("new queue payload"), payload)

	header := readTestBlockHeader(t, pool, newBlock.ID)
	assert.Equal(t, QueueID(2), header.queueID)
	assert.Equal(t, uint64(99), header.offset)
	assert.Equal(t, newBlock.Generation, header.generation)
	assert.Equal(t, blockFlagWritten, header.flags)

	_, err = pool.Read(oldBlock)
	assert.ErrorIs(t, err, ErrBlockOwnerMismatch)
}

func TestBlockPoolAllocateDoesNotWriteToDisk(t *testing.T) {
	t.Parallel()

	pool := openTestBlockPool(t)

	before := pool.Stats()
	block, err := pool.Allocate(1, 1)
	require.NoError(t, err)
	afterAllocate := pool.Stats()
	assert.Equal(t, before.Writes, afterAllocate.Writes)

	require.NoError(t, pool.Write(block, []byte("payload")))
	afterWrite := pool.Stats()
	assert.Equal(t, afterAllocate.Writes+1, afterWrite.Writes)
}

func TestBlockPoolReopenPreservesOwnershipAndFreeBlocks(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "blocks.db")
	pool, err := OpenBlockPool(path, BlockPoolOptions{BlockSize: testBlockSize, GrowBy: 2})
	require.NoError(t, err)

	live, err := pool.WriteBlock(7, 1, []byte("still live"))
	require.NoError(t, err)
	released, err := pool.WriteBlock(8, 2, []byte("will be released"))
	require.NoError(t, err)
	require.NoError(t, pool.Release(released))
	require.NoError(t, pool.Close())

	reopened, err := OpenBlockPool(path, BlockPoolOptions{GrowBy: 2})
	require.NoError(t, err)
	defer reopened.Close()

	liveHandle, err := reopened.Lookup(live.ID)
	require.NoError(t, err)
	assert.Equal(t, live.QueueID, liveHandle.QueueID)
	assert.Equal(t, live.Offset, liveHandle.Offset)

	payload, err := reopened.Read(liveHandle)
	require.NoError(t, err)
	assert.Equal(t, []byte("still live"), payload)

	_, err = reopened.Lookup(released.ID)
	assert.ErrorIs(t, err, ErrBlockReleased)

	newBlock, err := reopened.Allocate(9, 3)
	require.NoError(t, err)
	assert.Equal(t, released.ID, newBlock.ID)
	require.NoError(t, reopened.Write(newBlock, []byte("reused after reopen")))

	payload, err = reopened.Read(newBlock)
	require.NoError(t, err)
	assert.Equal(t, []byte("reused after reopen"), payload)
}

func TestBlockPoolRejectsOversizedPayload(t *testing.T) {
	t.Parallel()

	pool := openTestBlockPool(t)

	block, err := pool.Allocate(1, 1)
	require.NoError(t, err)
	payload := make([]byte, testBlockSize-blockPoolHeaderSize+1)
	assert.ErrorIs(t, pool.Write(block, payload), ErrBlockTooLarge)
}

func TestBlockPoolDetectsHeaderOwnerMismatch(t *testing.T) {
	t.Parallel()

	pool := openTestBlockPool(t)

	block, err := pool.WriteBlock(1, 1, []byte("payload"))
	require.NoError(t, err)

	header := readTestBlockHeader(t, pool, block.ID)
	header.queueID = 2
	buf := make([]byte, blockPoolHeaderSize)
	encodeBlockHeader(buf, header)
	_, err = pool.file.WriteAt(buf, pool.blockOffset(block.ID))
	require.NoError(t, err)

	_, err = pool.Read(block)
	assert.ErrorIs(t, err, ErrBlockOwnerMismatch)
}

func readTestBlockHeader(t *testing.T, pool *BlockPool, id BlockID) encodedBlockHeader {
	t.Helper()

	buf := make([]byte, blockPoolHeaderSize)
	_, err := pool.file.ReadAt(buf, pool.blockOffset(id))
	require.NoError(t, err)

	header, err := decodeBlockHeader(buf)
	require.NoError(t, err)

	return header
}
