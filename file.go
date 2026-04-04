package file

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type FileMgr struct {
	blocksize   int
	dataDir     string
	openedFiles map[string]*os.File
	mu          sync.Mutex //when reading and writing to files, ensure 1 goroutine is used at a time
}

func NewFileMgr(dataDir string, blockSize int) *FileMgr {
	return &FileMgr{
		blocksize:   blockSize,
		dataDir:     dataDir,
		openedFiles: make(map[string]*os.File),
	}
}

// Read reads the contents of the specified block into the provided page
func (fm *FileMgr) Read(blockID *BlockID, p *Page) (int, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(blockID.Filename)
	if err != nil {
		return 0, err
	}

	n, err := f.ReadAt(p.Bytes(), int64(blockID.Number*fm.blocksize))
	if err != nil && err.Error() != "EOF" {
		return 0, fmt.Errorf("Failed to read file: %v", err)
	}
	//return number of bytes in page i.e. block size
	return n, nil
}

// Write writes the contents of the provided page to the specified block
func (fm *FileMgr) Write(blockID *BlockID, p *Page) (int, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(blockID.Filename)
	if err != nil {
		return 0, err
	}

	n, err := f.WriteAt(p.Bytes(), int64(blockID.Number*fm.blocksize))
	if err != nil && err.Error() != "EOF" {
		return 0, fmt.Errorf("failed to read file: %v", err)
	}
	//return number of written bytes
	return n, nil
}

// Close closes all opened files
func (fm *FileMgr) Close() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	for _, f := range fm.openedFiles {
		err := f.Close()
		if err != nil {
			return fmt.Errorf("failed to close file: %v", err)
		}
	}
	return nil
}

// getFile returns the file with the specified filename, creating it if it does not exist
func (fm *FileMgr) getFile(filename string) (*os.File, error) {
	f, ok := fm.openedFiles[filename]
	var err error
	if !ok {
		//This opens the file at the specified path with read and write permissions (os.O_RDWR),
		//creates the file if it does not exist (os.O_CREATE),
		///and ensures that writes are synchronized to stable storage (os.O_SYNC) i.e. write it immediately to the disk
		//0666 sets the file permissions to be made readable and writeable by all users
		f, err = os.OpenFile(filepath.Join(fm.dataDir, filename), os.O_RDWR|os.O_CREATE|os.O_SYNC, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %v", err)
		}
		fm.openedFiles[filename] = f
	}
	return f, nil
}
