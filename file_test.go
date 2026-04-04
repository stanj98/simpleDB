package file

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileMgrWriteRead(t *testing.T) {
	blockSize := 16
	dataDir := "testdata"
	testFile := "writetestfile"

	mgr := NewFileMgr(dataDir, blockSize)

	t.Cleanup(func() {
		mgr.Close()
		os.Remove(filepath.Join(dataDir, testFile))
	})

	blockZero := &BlockID{
		Filename: testFile,
		Number:   0,
	}

	data := "aaaaaaaaaaaaaaaa"
	checkWrite(t, mgr, blockZero, data)
	checkRead(t, mgr, blockZero, data)
	checkFileContent(t, filepath.Join(dataDir, testFile), "aaaaaaaaaaaaaaaa")

	//Write to block 1
	blockOne := &BlockID{
		Filename: testFile,
		Number:   1,
	}

	data = "bbbbbbbbbbbbbb"
	checkWrite(t, mgr, blockOne, data)
	checkRead(t, mgr, blockOne, data)
	checkFileContent(t, filepath.Join(dataDir, testFile), "aaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb")

	//Rewrite to block 0
	data = "ccccccccccccccc"
	checkWrite(t, mgr, blockZero, data)
	checkRead(t, mgr, blockZero, data)
	checkFileContent(t, filepath.Join(dataDir, testFile), "cccccccccccccccccbbbbbbbbbbbbbbbb")

}

func checkWrite(t *testing.T, mgr *FileMgr, blockID *BlockID, data string) {
	page := NewPage(mgr.blocksize)
	page.Write(0, []byte(data))

	n, err := mgr.Write(blockID, page)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != mgr.blocksize {
		t.Fatalf("Write returned %d, want %d", n, mgr.blocksize)
	}
}

func checkRead(t *testing.T, mgr *FileMgr, blockID *BlockID, want string) {
	page := NewPage(mgr.blocksize)
	n, err := mgr.Read(blockID, page)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != mgr.blocksize {
		t.Fatalf("Read returned %d, want %d", n, mgr.blocksize)
	}
	if string(page.Bytes()) != want {
		t.Fatalf("Read returned %q, want %q", page.Bytes(), want)
	}
}

func checkFileContent(t *testing.T, filename, want string) {
	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	got := make([]byte, len(want))
	_, err = f.Read([]byte(got))
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("Failed to read file: %v", err)
	}
	defer f.Close()

	if string(got) != want {
		t.Fatalf("File content is %q, want %q", got, want)
	}
}
