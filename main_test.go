package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeTableClient struct {
	mu sync.Mutex
	// describeErrs[i] is returned for describe call i; a nil or missing
	// entry yields an ACTIVE table.
	describeErrs  []error
	describeCalls int
	createErr     error
	createCalls   int
	createInput   *dynamodb.CreateTableInput
}

func (f *fakeTableClient) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := f.describeCalls
	f.describeCalls++
	if call < len(f.describeErrs) && f.describeErrs[call] != nil {
		return nil, f.describeErrs[call]
	}
	return &dynamodb.DescribeTableOutput{
		Table: &types.TableDescription{TableStatus: types.TableStatusActive},
	}, nil
}

func (f *fakeTableClient) CreateTable(ctx context.Context, in *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	f.createInput = in
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &dynamodb.CreateTableOutput{}, nil
}

func fastWaiter(o *dynamodb.TableExistsWaiterOptions) {
	o.MinDelay = time.Millisecond
	o.MaxDelay = 2 * time.Millisecond
}

func TestEnsureTableAlreadyExists(t *testing.T) {
	client := &fakeTableClient{}

	err := ensureTable(context.Background(), client, "history", fastWaiter)

	require.NoError(t, err)
	assert.Equal(t, 0, client.createCalls)
	assert.Equal(t, 1, client.describeCalls)
}

func TestEnsureTableCreatesMissingTableAndWaits(t *testing.T) {
	client := &fakeTableClient{describeErrs: []error{
		&types.ResourceNotFoundException{},
		// Simulate the table still being created on the waiter's first poll.
		&types.ResourceNotFoundException{},
	}}

	err := ensureTable(context.Background(), client, "history", fastWaiter)

	require.NoError(t, err)
	require.Equal(t, 1, client.createCalls)
	// One describe before create plus at least two waiter polls.
	assert.GreaterOrEqual(t, client.describeCalls, 3)

	require.NotNil(t, client.createInput)
	assert.Equal(t, "history", *client.createInput.TableName)
	require.Len(t, client.createInput.KeySchema, 2)
	assert.Equal(t, "timestamp", *client.createInput.KeySchema[0].AttributeName)
	assert.Equal(t, types.KeyTypeHash, client.createInput.KeySchema[0].KeyType)
	assert.Equal(t, "timestamp_2", *client.createInput.KeySchema[1].AttributeName)
	assert.Equal(t, types.KeyTypeRange, client.createInput.KeySchema[1].KeyType)
}

func TestEnsureTableToleratesConcurrentCreate(t *testing.T) {
	client := &fakeTableClient{
		describeErrs: []error{&types.ResourceNotFoundException{}},
		createErr:    &types.ResourceInUseException{},
	}

	err := ensureTable(context.Background(), client, "history", fastWaiter)

	require.NoError(t, err)
	assert.Equal(t, 1, client.createCalls)
}

func TestEnsureTablePropagatesDescribeError(t *testing.T) {
	client := &fakeTableClient{describeErrs: []error{errors.New("access denied")}}

	err := ensureTable(context.Background(), client, "history", fastWaiter)

	assert.ErrorContains(t, err, "access denied")
	assert.Equal(t, 0, client.createCalls)
}

func TestEnsureTableWithRetryRecoversFromNetworkErrors(t *testing.T) {
	// An offline start must not be fatal: the check retries until the
	// network returns.
	client := &fakeTableClient{describeErrs: []error{
		errors.New("dial tcp: network is unreachable"),
		errors.New("dial tcp: network is unreachable"),
	}}

	ensureTableWithRetry(context.Background(), client, "history", time.Millisecond, fastWaiter)

	assert.Equal(t, 3, client.describeCalls)
}

func TestEnsureTablePropagatesCreateError(t *testing.T) {
	client := &fakeTableClient{
		describeErrs: []error{&types.ResourceNotFoundException{}},
		createErr:    errors.New("limit exceeded"),
	}

	err := ensureTable(context.Background(), client, "history", fastWaiter)

	assert.ErrorContains(t, err, "limit exceeded")
}
