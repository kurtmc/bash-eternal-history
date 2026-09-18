package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type ContentRepository struct {
	svc         dynamodb.ScanAPIClient
	tableName   string
	readTimeout time.Duration
	segments    int32
}

func NewContentRepository(svc dynamodb.ScanAPIClient, tableName string, readTimeout time.Duration, segments int32) *ContentRepository {
	if segments < 1 {
		segments = 1
	}
	return &ContentRepository{
		svc:         svc,
		tableName:   tableName,
		readTimeout: readTimeout,
		segments:    segments,
	}
}

type historyEntry struct {
	timestamp int64
	content   string
}

func (c *ContentRepository) Get(ctx context.Context) ([]historyEntry, error) {
	entries, err := c.readEntries(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve data from dynamodb: %w", err)
	}
	return entries, nil
}

func (c *ContentRepository) readEntries(ctx context.Context) ([]historyEntry, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([][]historyEntry, c.segments)
	errs := make([]error, c.segments)
	var wg sync.WaitGroup
	for i := range c.segments {
		wg.Go(func() {
			results[i], errs[i] = c.readSegment(ctx, i)
			if errs[i] != nil {
				cancel()
			}
		})
	}
	wg.Wait()

	if err := firstCause(errs); err != nil {
		return nil, err
	}

	total := 0
	for _, r := range results {
		total += len(r)
	}
	entries := make([]historyEntry, 0, total)
	for _, r := range results {
		entries = append(entries, r...)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].timestamp < entries[j].timestamp
	})
	return entries, nil
}

func firstCause(errs []error) error {
	var canceled error
	for _, err := range errs {
		if err == nil {
			continue
		}
		if !errors.Is(err, context.Canceled) {
			return err
		}
		canceled = err
	}
	return canceled
}

func (c *ContentRepository) readSegment(ctx context.Context, segment int32) ([]historyEntry, error) {
	input := &dynamodb.ScanInput{
		TableName: &c.tableName,
		// Strongly consistent so a line the writer just flushed is never missing from the scan that replaces the local view.
		ConsistentRead: aws.Bool(true),
	}
	if c.segments > 1 {
		input.Segment = aws.Int32(segment)
		input.TotalSegments = aws.Int32(c.segments)
	}
	paginator := dynamodb.NewScanPaginator(c.svc, input)

	var entries []historyEntry
	for paginator.HasMorePages() {
		pageCtx, cancel := context.WithTimeout(ctx, c.readTimeout)
		output, err := paginator.NextPage(pageCtx)
		cancel()
		if err != nil {
			return nil, err
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
	return entries, nil
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

func renderEntries(entries []historyEntry) []byte {
	size := 0
	for _, entry := range entries {
		size += len(entry.content) + 1
	}
	data := make([]byte, 0, size)
	for _, entry := range entries {
		data = append(data, entry.content...)
		data = append(data, '\n')
	}
	return data
}
