package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type ContentRepository struct {
	Content   string
	svc       dynamodb.ScanAPIClient
	tableName string
}

func NewContentRepository(svc dynamodb.ScanAPIClient, tableName string) *ContentRepository {
	return &ContentRepository{
		svc:       svc,
		tableName: tableName,
	}
}

func (c *ContentRepository) readContent(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, appConig.ReadContentTimeout)
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

func (c *ContentRepository) Get(ctx context.Context) (string, error) {
	content, err := c.readContent(ctx)
	if err != nil {
		return "", fmt.Errorf("could not retrive data from dyanmodb: %v", err)
	}
	return content, nil
}
