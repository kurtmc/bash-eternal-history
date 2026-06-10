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
	ctx, cancel := context.WithTimeout(ctx, c.readTimeout)
	defer cancel()

	paginator := dynamodb.NewScanPaginator(c.svc, &dynamodb.ScanInput{
		TableName: &c.tableName,
		// A consistent read makes lines that were just flushed by the
		// writer visible immediately, so a refresh never serves a view that
		// is missing this session's history.
		ConsistentRead: aws.Bool(true),
	})

	var entries []historyEntry
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return "", err
		}
		for _, item := range output.Items {
			entry, ok := parseItem(item)
			if !ok {
				log.Printf("WARN: skipping malformed history item: %v", item)
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
