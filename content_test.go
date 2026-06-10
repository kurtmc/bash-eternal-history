package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scanPage struct {
	out *dynamodb.ScanOutput
	err error
}

type fakeScanClient struct {
	pages  []scanPage
	inputs []*dynamodb.ScanInput
}

func (f *fakeScanClient) Scan(ctx context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	call := len(f.inputs)
	f.inputs = append(f.inputs, in)
	if call >= len(f.pages) {
		return &dynamodb.ScanOutput{}, nil
	}
	return f.pages[call].out, f.pages[call].err
}

func historyItem(timestamp, content string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"timestamp": &types.AttributeValueMemberN{Value: timestamp},
		"content":   &types.AttributeValueMemberS{Value: content},
	}
}

func newTestRepository(pages ...scanPage) (*ContentRepository, *fakeScanClient) {
	client := &fakeScanClient{pages: pages}
	return NewContentRepository(client, "test", time.Second), client
}

func singlePage(items ...map[string]types.AttributeValue) scanPage {
	return scanPage{out: &dynamodb.ScanOutput{Items: items}}
}

func TestGetSortsByTimestamp(t *testing.T) {
	repo, _ := newTestRepository(singlePage(
		historyItem("1713416083", "abc"),
		historyItem("1713416082", "def"),
		historyItem("1713416081", "123"),
		historyItem("1713416085", "afsd"),
	))

	actual, err := repo.Get(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "123\ndef\nabc\nafsd\n", actual)
}

func TestGetSortsNumericallyNotLexically(t *testing.T) {
	// A lexical sort would order these "10", "100", "9".
	repo, _ := newTestRepository(singlePage(
		historyItem("100", "third"),
		historyItem("9", "first"),
		historyItem("10", "second"),
	))

	actual, err := repo.Get(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "first\nsecond\nthird\n", actual)
}

func TestGetHandlesTimestampsBeyond32Bits(t *testing.T) {
	repo, _ := newTestRepository(singlePage(
		historyItem("9223372036854775807", "max int64"),
		historyItem("1713416083000000000", "nanosecond precision"),
	))

	actual, err := repo.Get(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "nanosecond precision\nmax int64\n", actual)
}

func TestGetSkipsMalformedItems(t *testing.T) {
	repo, _ := newTestRepository(singlePage(
		historyItem("1", "ok"),
		// missing content attribute
		map[string]types.AttributeValue{
			"timestamp": &types.AttributeValueMemberN{Value: "2"},
		},
		// content has the wrong type
		map[string]types.AttributeValue{
			"timestamp": &types.AttributeValueMemberN{Value: "3"},
			"content":   &types.AttributeValueMemberN{Value: "42"},
		},
		// missing timestamp attribute
		map[string]types.AttributeValue{
			"content": &types.AttributeValueMemberS{Value: "no timestamp"},
		},
		// timestamp has the wrong type
		map[string]types.AttributeValue{
			"timestamp": &types.AttributeValueMemberS{Value: "5"},
			"content":   &types.AttributeValueMemberS{Value: "string timestamp"},
		},
		// timestamp is not a number
		map[string]types.AttributeValue{
			"timestamp": &types.AttributeValueMemberN{Value: "not-a-number"},
			"content":   &types.AttributeValueMemberS{Value: "bad number"},
		},
	))

	actual, err := repo.Get(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "ok\n", actual)
}

func TestGetStripsTrailingNewlines(t *testing.T) {
	// bash appends entries with a trailing newline; multi-line entries (e.g.
	// with HISTTIMEFORMAT comment lines) must keep their inner newlines.
	repo, _ := newTestRepository(singlePage(
		historyItem("1", "ls -la\n"),
		historyItem("2", "#1713416083\necho hi\n"),
	))

	actual, err := repo.Get(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "ls -la\n#1713416083\necho hi\n", actual)
}

func TestGetSkipsEmptyEntries(t *testing.T) {
	repo, _ := newTestRepository(singlePage(
		historyItem("1", "\n"),
		historyItem("2", "real"),
	))

	actual, err := repo.Get(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "real\n", actual)
}

func TestGetEmptyTable(t *testing.T) {
	repo, _ := newTestRepository(singlePage())

	actual, err := repo.Get(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "", actual)
}

func TestGetPaginates(t *testing.T) {
	lastKey := map[string]types.AttributeValue{
		"timestamp": &types.AttributeValueMemberN{Value: "2"},
	}
	repo, client := newTestRepository(
		scanPage{out: &dynamodb.ScanOutput{
			Items:            []map[string]types.AttributeValue{historyItem("2", "b")},
			LastEvaluatedKey: lastKey,
		}},
		singlePage(historyItem("1", "a")),
	)

	actual, err := repo.Get(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "a\nb\n", actual)
	require.Len(t, client.inputs, 2)
	assert.Equal(t, lastKey, client.inputs[1].ExclusiveStartKey)
}

func TestGetReturnsScanError(t *testing.T) {
	repo, _ := newTestRepository(scanPage{err: errors.New("boom")})

	actual, err := repo.Get(context.Background())

	assert.ErrorContains(t, err, "boom")
	assert.Equal(t, "", actual)
}

func TestGetUsesConsistentRead(t *testing.T) {
	repo, client := newTestRepository(singlePage())

	_, err := repo.Get(context.Background())

	require.NoError(t, err)
	require.Len(t, client.inputs, 1)
	require.NotNil(t, client.inputs[0].ConsistentRead)
	assert.True(t, *client.inputs[0].ConsistentRead)
}
