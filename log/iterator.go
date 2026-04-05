package log

import (
	"fmt"

	"github.com/stanj98/simpleDB/file"
)

type iterator struct {
	fm           *file.FileMgr
	currentBlock *file.BlockID
	currentPos   int
	page         *file.Page
}

func newIterator(fm *file.FileMgr, blockID file.BlockID) (*iterator, error) {
	page := file.NewPage(fm.Blocksize)

	_, err := fm.Read(&blockID, page)
	if err != nil {
		return nil, fmt.Errorf("Failed to read block: %w", err)
	}

	currentPosBytes := make([]byte, intBytesSize)
	page.Read(0, currentPosBytes)
	currentPos := int(endian.Uint32(currentPosBytes))

	return &iterator{
		fm:           fm,
		currentBlock: &blockID,
		currentPos:   currentPos,
		page:         page,
	}, nil
}

func (i *iterator) HasNext() bool {
	return i.currentPos < i.fm.Blocksize || i.currentBlock.Number > 0
}

func (i *iterator) Next() (*Record, error) {
	if i.currentPos == i.fm.Blocksize {
		//move to the next block in reverse
		i.currentBlock.Number--

		_, err := i.fm.Read(i.currentBlock, i.page)
		if err != nil {
			return nil, fmt.Errorf("Failed to read next block: %w", err)
		}

		currentPostBytes := make([]byte, intBytesSize)
		i.page.Read(0, currentPostBytes)
		i.currentPos = int(endian.Uint32(currentPostBytes))
	}

	lengthBytes := make([]byte, intBytesSize)
	i.page.Read(i.currentPos, lengthBytes)
	length := endian.Uint32(lengthBytes)

	data := make([]byte, length)
	i.page.Read(i.currentPos+intBytesSize, data)

	rec := &Record{
		Length: int(length),
		Data:   data,
	}

	i.currentPos = i.currentPos + rec.totalLength()

	return rec, nil
}
