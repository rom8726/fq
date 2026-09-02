package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const segmentMetadataSuffix = ".meta"
const lastFlushDBLSNFileName = "last_flushdb_lsn.meta"

type segmentMetadata struct {
	MaxLSN uint64
}

func isWALSegmentFile(name string) bool {
	return strings.HasPrefix(name, "wal_") && strings.HasSuffix(name, ".log")
}

func isWALSegmentMetadataFile(name string) bool {
	return strings.HasPrefix(name, "wal_") && strings.HasSuffix(name, ".log"+segmentMetadataSuffix)
}

func segmentMetadataPath(segmentPath string) string {
	return segmentPath + segmentMetadataSuffix
}

func writeSegmentMetadata(segmentPath string, meta segmentMetadata) error {
	metadataPath := segmentMetadataPath(segmentPath)
	tmpPath := fmt.Sprintf("%s.tmp.%d", metadataPath, os.Getpid())

	if err := os.WriteFile(tmpPath, encodeLSNFile(meta.MaxLSN), 0o600); err != nil {
		return fmt.Errorf("write segment metadata: %w", err)
	}

	if err := os.Rename(tmpPath, metadataPath); err != nil {
		_ = os.Remove(tmpPath)

		return fmt.Errorf("replace segment metadata: %w", err)
	}

	return nil
}

func readSegmentMetadata(segmentPath string) (segmentMetadata, error) {
	metadataPath := segmentMetadataPath(segmentPath)

	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return segmentMetadata{}, err
	}

	maxLSN, err := decodeLSNFile(data)
	if err != nil {
		return segmentMetadata{}, fmt.Errorf("parse segment metadata %s: %w", metadataPath, err)
	}

	return segmentMetadata{MaxLSN: maxLSN}, nil
}

func writeLastFlushDBLSN(directory string, lsn uint64) error {
	if err := ensureWALDirectory(directory); err != nil {
		return err
	}

	path := filepath.Join(directory, lastFlushDBLSNFileName)
	tmpPath := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())

	if err := os.WriteFile(tmpPath, encodeLSNFile(lsn), 0o600); err != nil {
		return fmt.Errorf("write last FLUSHDB LSN: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)

		return fmt.Errorf("replace last FLUSHDB LSN: %w", err)
	}

	return syncDirectory(directory)
}

func ensureWALDirectory(directory string) error {
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create WAL directory: %w", err)
	}

	return nil
}

func readLastFlushDBLSN(directory string) (uint64, error) {
	path := filepath.Join(directory, lastFlushDBLSNFileName)

	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	lsn, err := decodeLSNFile(data)
	if err != nil {
		return 0, fmt.Errorf("parse last FLUSHDB LSN %s: %w", path, err)
	}

	return lsn, nil
}

func syncDirectory(directory string) error {
	dir, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open directory for sync: %w", err)
	}

	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return fmt.Errorf("sync directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close directory after sync: %w", closeErr)
	}

	return nil
}

func removeSegmentAndMetadata(segmentPath string) error {
	if err := os.Remove(segmentPath); err != nil {
		return err
	}

	metadataPath := segmentMetadataPath(segmentPath)
	if err := os.Remove(metadataPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove segment metadata %s: %w", metadataPath, err)
	}

	return nil
}

func walSegmentNames(directory string) ([]string, error) {
	if err := ensureWALDirectory(directory); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to scan WAL directory: %w", err)
	}

	filenames := make([]string, 0, len(files))
	for _, file := range files {
		if file.IsDir() || !isWALSegmentFile(file.Name()) {
			continue
		}

		filenames = append(filenames, file.Name())
	}

	return filenames, nil
}

func walSegmentPaths(directory string) ([]string, error) {
	filenames, err := walSegmentNames(directory)
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(filenames))
	for _, filename := range filenames {
		paths = append(paths, filepath.Join(directory, filename))
	}

	return paths, nil
}
