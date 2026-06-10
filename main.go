package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/kelseyhightower/envconfig"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

type Config struct {
	ReadContentTimeout time.Duration `envconfig:"READ_CONTENT_TIMEOUT" default:"15s"`
	TableName          string        `envconfig:"DYNAMODB_TABLE_NAME" default:"bash-eternal-history"`
	ContentCacheTTL    time.Duration `envconfig:"CONTENT_CACHE_TTL" default:"5m"`
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	var appConfig Config
	if err := envconfig.Process("", &appConfig); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	svc := dynamodb.NewFromConfig(cfg)

	if err := ensureTable(ctx, svc, appConfig.TableName); err != nil {
		log.Fatalf("unable to ensure dynamodb table exists: %v", err)
	}

	writer := NewHistoryWriter(svc, appConfig.TableName)
	go writer.Run()

	repo := NewContentRepository(svc, appConfig.TableName, appConfig.ReadContentTimeout)
	file := NewFile(repo, writer, appConfig.ContentCacheTTL)

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("basheternalhistory"),
		fuse.Subtype("basheternalhistoryfs"),
		fuse.AllowNonEmptyMount(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = fs.Serve(c, FS{file: file})
	if err != nil {
		log.Fatal(err)
	}
}

// TableClient is the subset of the DynamoDB API needed to ensure the history
// table exists.
type TableClient interface {
	dynamodb.DescribeTableAPIClient
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
}

// ensureTable creates the history table if it does not exist and waits until
// it is active, so the first scan does not hit a table that is still being
// created.
func ensureTable(ctx context.Context, client TableClient, tableName string, waiterOpts ...func(*dynamodb.TableExistsWaiterOptions)) error {
	_, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		return nil
	}
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		return err
	}

	log.Printf("table %q not found, creating it", tableName)
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("timestamp"),
				AttributeType: types.ScalarAttributeTypeN,
			},
			{
				AttributeName: aws.String("timestamp_2"),
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("timestamp"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("timestamp_2"),
				KeyType:       types.KeyTypeRange,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		// Another machine may have created the table between our describe
		// and create; that is fine, we still wait for it to become active.
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			return err
		}
	}

	waiter := dynamodb.NewTableExistsWaiter(client, waiterOpts...)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 2*time.Minute); err != nil {
		return fmt.Errorf("waiting for table %q to become active: %w", tableName, err)
	}
	log.Printf("table %q is active", tableName)
	return nil
}

// FS implements the bash eternal history file system.
type FS struct {
	file *File
}

func (f FS) Root() (fs.Node, error) {
	return Dir{file: f.file}, nil
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	file *File
}

func (Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0o555
	return nil
}

func (d Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if name == ".bash_eternal_history" {
		return d.file, nil
	}
	return nil, syscall.ENOENT
}

var dirDirs = []fuse.Dirent{
	{Inode: 2, Name: ".bash_eternal_history", Type: fuse.DT_File},
}

func (Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return dirDirs, nil
}
