package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const segmentMetadataSuffix = ".meta"

type segmentMetadata struct {
	MaxLSN uint64
}

func isWALSegmentFile(name string) bool {
	return strings.HasPrefix(name, "wal_") && strings.HasSuffix(name, ".log")
}

func segmentMetadataPath(segmentPath string) string {
	return segmentPath + segmentMetadataSuffix
}

func writeSegmentMetadata(segmentPath string, meta segmentMetadata) error {
	metadataPath := segmentMetadataPath(segmentPath)
	tmpPath := fmt.Sprintf("%s.tmp.%d", metadataPath, os.Getpid())
	data := []byte(strconv.FormatUint(meta.MaxLSN, 10) + "\n")

	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return fmt.Errorf("write segment metadata: %w", err)
	}

	if err := os.Rename(tmpPath, metadataPath); err != nil {
		_ = os.Remove(tmpPath)

		return fmt.Errorf("replace segment metadata: %w", err)
	}

	return nil
}

func readSegmentMetadata(segmentPath string) (segmentMetadata, error) {
	data, err := os.ReadFile(segmentMetadataPath(segmentPath))
	if err != nil {
		return segmentMetadata{}, err
	}

	maxLSN, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return segmentMetadata{}, fmt.Errorf("parse segment metadata: %w", err)
	}

	return segmentMetadata{MaxLSN: maxLSN}, nil
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
