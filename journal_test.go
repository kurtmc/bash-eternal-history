package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJournalLoadsLinesOldestFirst(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "journal")
	j, err := openJournal(dir)
	require.NoError(t, err)

	lines := []AppendHistoryMessage{
		{Content: "#1700000000\necho ten\n", Timestamp: 10, Nonce: 1},
		{Content: "echo nine b\n", Timestamp: 9, Nonce: 2},
		{Content: "echo nine a\n", Timestamp: 9, Nonce: 1},
	}
	for _, m := range lines {
		require.NoError(t, j.add(m))
	}

	got, err := j.load()
	require.NoError(t, err)
	assert.Equal(t, []AppendHistoryMessage{lines[2], lines[1], lines[0]}, got)

	info, err := os.Stat(dir)
	require.NoError(t, err)
	assert.Equal(t, fs.FileMode(0o700), info.Mode().Perm())
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		info, err := e.Info()
		require.NoError(t, err)
		assert.Equal(t, fs.FileMode(0o600), info.Mode().Perm(), "journaled history must be private to the user")
	}
}

func TestJournalRemove(t *testing.T) {
	j, err := openJournal(t.TempDir())
	require.NoError(t, err)
	m := AppendHistoryMessage{Content: "echo hi\n", Timestamp: 1, Nonce: 2}
	require.NoError(t, j.add(m))

	require.NoError(t, j.remove(m))
	require.NoError(t, j.remove(m), "removing a line that is already gone is not an error")

	got, err := j.load()
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestJournalIgnoresFilesItDidNotWrite(t *testing.T) {
	dir := t.TempDir()
	j, err := openJournal(dir)
	require.NoError(t, err)
	m := AppendHistoryMessage{Content: "echo hi\n", Timestamp: 1, Nonce: 2}
	require.NoError(t, j.add(m))
	temp := filepath.Join(dir, journalTempPrefix+"123")
	stray := filepath.Join(dir, "notes.txt")
	require.NoError(t, os.WriteFile(temp, []byte("half a line"), 0o600))
	require.NoError(t, os.WriteFile(stray, []byte("mine"), 0o600))

	got, err := j.load()
	require.NoError(t, err)
	assert.Equal(t, []AppendHistoryMessage{m}, got)
	assert.FileExists(t, stray)

	_, err = openJournal(dir)
	require.NoError(t, err)
	assert.NoFileExists(t, temp, "a temp file left by a crash is removed when the journal is opened")
	assert.FileExists(t, stray)
}
