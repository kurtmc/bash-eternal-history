package main

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type ContentRepository struct {
	Content     string
	svc         *dynamodb.Client
	tableName   string
	lastUpdated time.Time
}

func NewContentRepository(svc *dynamodb.Client, tableName string) *ContentRepository {
	return &ContentRepository{
		svc:       svc,
		tableName: tableName,
	}
}

func (c *ContentRepository) readContent(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	paginator := dynamodb.NewScanPaginator(c.svc, &dynamodb.ScanInput{
		TableName: &c.tableName,
	})

	var items []map[string]types.AttributeValue
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return "", err
		}
		items = append(items, output.Items...)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i]["timestamp"].(*types.AttributeValueMemberN).Value < items[j]["timestamp"].(*types.AttributeValueMemberN).Value
	})

	var lines []string
	for _, item := range items {
		line := item["content"].(*types.AttributeValueMemberS).Value
		lines = append(lines, line)
	}

	content := strings.Join(lines, "\n") + "\n"

	return content, nil
}

func (c *ContentRepository) Get(ctx context.Context) string {
	if time.Since(c.lastUpdated) < (100 * time.Millisecond) {
		return c.Content
	}

	content, err := c.readContent(ctx)
	if err != nil {
		// TODO: store a copy of content on disk, so that when
		// an error occurs we can load the last valid value of
		// content
		return c.Content
	}
	c.lastUpdated = time.Now()
	c.Content = content
	return content
}
