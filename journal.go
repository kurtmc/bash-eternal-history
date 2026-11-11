package main

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

const journalTempPrefix = ".tmp-"

// journal keeps every history line not yet stored in DynamoDB as one file per
// line, so lines survive the daemon stopping before they are written.
type journal struct {
	dir string
}

func openJournal(dir string) (*journal, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), journalTempPrefix) {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, err
			}
		}
	}
	return &journal{dir: dir}, nil
}

func journalName(m AppendHistoryMessage) string {
	return fmt.Sprintf("%019d-%019d", m.Timestamp, m.Nonce)
}

func (j *journal) add(m AppendHistoryMessage) error {
	tmp, err := os.CreateTemp(j.dir, journalTempPrefix+"*")
	if err != nil {
		return err
	}
	_, err = tmp.WriteString(m.Content)
	err = errors.Join(err, tmp.Close())
	if err == nil {
		err = os.Rename(tmp.Name(), filepath.Join(j.dir, journalName(m)))
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
	}
	return err
}

func (j *journal) remove(m AppendHistoryMessage) error {
	err := os.Remove(filepath.Join(j.dir, journalName(m)))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (j *journal) load() ([]AppendHistoryMessage, error) {
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return nil, err
	}
	var lines []AppendHistoryMessage
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, journalTempPrefix) {
			continue
		}
		var m AppendHistoryMessage
		if _, err := fmt.Sscanf(name, "%d-%d", &m.Timestamp, &m.Nonce); err != nil || journalName(m) != name {
			slog.Warn("skipping unrecognised file in the write journal", slog.String("path", filepath.Join(j.dir, name)))
			continue
		}
		content, err := os.ReadFile(filepath.Join(j.dir, name))
		if err != nil {
			return nil, err
		}
		m.Content = string(content)
		lines = append(lines, m)
	}
	return lines, nil
}
