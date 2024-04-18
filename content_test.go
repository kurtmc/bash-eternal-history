package main

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MyMockedObject struct {
	mock.Mock
}

func (m *MyMockedObject) Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return &dynamodb.ScanOutput{
		Count: 1,
		Items: []map[string]types.AttributeValue{
			{
				"timestamp": &types.AttributeValueMemberN{
					Value: "1713416083",
				},
				"content": &types.AttributeValueMemberS{
					Value: "abc",
				},
			},
			{
				"timestamp": &types.AttributeValueMemberN{
					Value: "1713416082",
				},
				"content": &types.AttributeValueMemberS{
					Value: "def",
				},
			},
			{
				"timestamp": &types.AttributeValueMemberN{
					Value: "1713416081",
				},
				"content": &types.AttributeValueMemberS{
					Value: "123",
				},
			},
			{
				"timestamp": &types.AttributeValueMemberN{
					Value: "1713416085",
				},
				"content": &types.AttributeValueMemberS{
					Value: "afsd",
				},
			},
		},
	}, nil
}

func TestGet(t *testing.T) {

	expected := "123\ndef\nabc\nafsd\n"

	m := new(MyMockedObject)

	contentRepository := NewContentRepository(m, "test")

	actual, err := contentRepository.Get(context.TODO())

	if assert.NoError(t, err) {
		assert.Equal(t, expected, actual)
	}
}
