package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type ContentRepository struct {
	svc         dynamodb.ScanAPIClient
	tableName   string
	readTimeout time.Duration
}

func NewContentRepository(svc dynamodb.ScanAPIClient, tableName string, readTimeout time.Duration) *ContentRepository {
	return &ContentRepository{
		svc:         svc,
		tableName:   tableName,
		readTimeout: readTimeout,
	}
}

type historyEntry struct {
	timestamp int64
	content   string
}

func (c *ContentRepository) readContent(ctx context.Context) (string, error) {
	paginator := dynamodb.NewScanPaginator(c.svc, &dynamodb.ScanInput{
		TableName: &c.tableName,
		// A strongly consistent read makes a line that was just flushed by the
		// writer visible to this scan immediately. Without it an eventually
		// consistent replica can omit a just-written line, and because the scan
		// result replaces the in-memory buffer wholesale, that line would
		// vanish from the local view until a later refresh — hiding a command
		// the user just ran. The Pending gate guarantees the flush completed,
		// not that the replica it lands on has caught up.
		ConsistentRead: aws.Bool(true),
	})

	var entries []historyEntry
	for paginator.HasMorePages() {
		// Bound each page rather than the whole scan. A single deadline over
		// the entire paginated scan would make every load fail outright once
		// the eternal history grew past what fits in readTimeout; per-page
		// deadlines let an arbitrarily large table load page by page.
		pageCtx, cancel := context.WithTimeout(ctx, c.readTimeout)
		output, err := paginator.NextPage(pageCtx)
		cancel()
		if err != nil {
			return "", err
		}
		for _, item := range output.Items {
			entry, ok := parseItem(item)
			if !ok {
				// Avoid logging the raw item: it can contain history content.
				log.Printf("WARN: skipping malformed history item")
				continue
			}
			if entry.content == "" {
				continue
			}
			entries = append(entries, entry)
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].timestamp < entries[j].timestamp
	})

	var b strings.Builder
	for _, entry := range entries {
		b.WriteString(entry.content)
		b.WriteByte('\n')
	}
	return b.String(), nil
}

// parseItem extracts a history entry, rejecting items that do not have the
// expected attribute types so a malformed row cannot panic the daemon.
func parseItem(item map[string]types.AttributeValue) (historyEntry, bool) {
	tsAttr, ok := item["timestamp"].(*types.AttributeValueMemberN)
	if !ok {
		return historyEntry{}, false
	}
	timestamp, err := strconv.ParseInt(tsAttr.Value, 10, 64)
	if err != nil {
		return historyEntry{}, false
	}
	contentAttr, ok := item["content"].(*types.AttributeValueMemberS)
	if !ok {
		return historyEntry{}, false
	}
	// bash appends lines including their trailing newline; strip it so that
	// joining entries does not introduce blank lines between them.
	content := strings.TrimRight(contentAttr.Value, "\n")
	return historyEntry{timestamp: timestamp, content: content}, true
}

func (c *ContentRepository) Get(ctx context.Context) (string, error) {
	content, err := c.readContent(ctx)
	if err != nil {
		return "", fmt.Errorf("could not retrieve data from dynamodb: %w", err)
	}
	return content, nil
}
