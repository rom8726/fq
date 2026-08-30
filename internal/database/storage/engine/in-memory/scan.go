package inmemory

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/fq-db/fq/internal/database"
)

const scanCursorInitial = "0"

type scanCursor struct {
	partition int
	after     hashTableKey
}

func (e *Engine) Scan(prefix, cursor string, count uint32) (database.ScanResult, error) {
	if !e.scanIndexEnabled {
		return database.ScanResult{}, database.ErrScanIndexDisabled
	}

	cur, err := e.decodeScanCursor(cursor)
	if err != nil {
		return database.ScanResult{}, err
	}

	keys := make([]database.BatchKey, 0, count)
	var lastKey database.BatchKey
	var lastPartition int
	for partitionIdx := cur.partition; partitionIdx < len(e.partitions); partitionIdx++ {
		after := hashTableKey{}
		if partitionIdx == cur.partition {
			after = cur.after
		}

		partitionKeys := e.partitions[partitionIdx].Scan(prefix, after, count-uint32(len(keys)))
		keys = append(keys, partitionKeys...)
		if len(partitionKeys) > 0 {
			lastKey = partitionKeys[len(partitionKeys)-1]
			lastPartition = partitionIdx
		}
		if len(keys) >= int(count) {
			break
		}
	}

	nextCursor := scanCursorInitial
	if len(keys) >= int(count) {
		nextCursor = encodeScanCursor(scanCursor{
			partition: lastPartition,
			after: hashTableKey{
				key:       lastKey.Key,
				batchSize: lastKey.BatchSize,
			},
		})
	}

	return database.ScanResult{
		NextCursor: nextCursor,
		Keys:       keys,
	}, nil
}

func (e *Engine) decodeScanCursor(cursor string) (scanCursor, error) {
	if cursor == scanCursorInitial {
		return scanCursor{}, nil
	}

	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return scanCursor{}, database.ErrInvalidScanCursor
	}

	parts := strings.Split(string(data), "\n")
	if len(parts) != 3 {
		return scanCursor{}, database.ErrInvalidScanCursor
	}

	partition, err := strconv.Atoi(parts[0])
	if err != nil || partition < 0 || partition >= len(e.partitions) {
		return scanCursor{}, database.ErrInvalidScanCursor
	}
	batchSize, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return scanCursor{}, database.ErrInvalidScanCursor
	}

	return scanCursor{
		partition: partition,
		after: hashTableKey{
			key:       parts[1],
			batchSize: uint32(batchSize),
		},
	}, nil
}

func encodeScanCursor(cursor scanCursor) string {
	data := fmt.Sprintf("%d\n%s\n%d", cursor.partition, cursor.after.key, cursor.after.batchSize)

	return base64.RawURLEncoding.EncodeToString([]byte(data))
}
