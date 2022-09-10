package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"bazil.org/fuse/fuseutil"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

var svc *dynamodb.Client

type AppendHistoryMessage struct {
	Content   string
	Timestamp int
}

var ch chan AppendHistoryMessage

var dynmodbTableName string = "bash-eternal-history"

func init() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRetryer(func() aws.Retryer {
		return aws.NopRetryer{}
	}))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	svc = dynamodb.NewFromConfig(cfg)

	ch = make(chan AppendHistoryMessage, 100)

	go func() {
		ctx := context.TODO()
		for {
			m := <-ch
			for {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				_, err := svc.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: &dynmodbTableName,
					Item: map[string]types.AttributeValue{
						"timestamp":   &types.AttributeValueMemberN{Value: strconv.Itoa(m.Timestamp)},
						"timestamp_2": &types.AttributeValueMemberN{Value: strconv.Itoa(m.Timestamp)},
						"content":     &types.AttributeValueMemberS{Value: m.Content},
					},
				})
				if err == nil {
					break
				}
				log.Printf("unable to write to dynamodb, trying again: %v", err)
				time.Sleep(5 * time.Second)
			}
		}
	}()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	// create table if it does not exist
	_, err := svc.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: &dynmodbTableName,
	})
	var notFoundException *types.ResourceNotFoundException
	if errors.As(err, &notFoundException) {
		input := &dynamodb.CreateTableInput{
			TableName: &dynmodbTableName,
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
		}
		_, err := svc.CreateTable(context.TODO(), input)
		if err != nil {
			panic(err)
		}
		log.Println("error:", notFoundException)
	} else {
		panic(err)
	}

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("basheternalhistory"),
		fuse.Subtype("basheternalhistoryfs"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = fs.Serve(c, FS{})
	if err != nil {
		log.Fatal(err)
	}
}

// FS implements the hello world file system.
type FS struct{}

func (FS) Root() (fs.Node, error) {
	return Dir{}, nil
}

// Dir implements both Node and Handle for the root directory.
type Dir struct{}

func (Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0o555
	return nil
}

func (Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if name == ".bash_eternal_history" {
		return NewFile("bash-eternal-history"), nil
	}
	return nil, syscall.ENOENT
}

var dirDirs = []fuse.Dirent{
	{Inode: 2, Name: ".bash_eternal_history", Type: fuse.DT_File},
}

func (Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return dirDirs, nil
}

// File implements both Node and Handle for the hello file.
type File struct {
	DynamodbTableName string
	ContentCache      string
}

func NewFile(tableName string) *File {
	return &File{
		DynamodbTableName: tableName,
	}
}

func (f *File) getContent(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := svc.Scan(ctx, &dynamodb.ScanInput{
		TableName: &f.DynamodbTableName,
	})
	if err != nil {
		log.Printf("could not read content: %v", err)
		return f.ContentCache, nil
	}

	log.Printf("count: %v", out.Count)
	var lines []string
	for _, item := range out.Items {
		line := item["content"].(*types.AttributeValueMemberS).Value
		lines = append(lines, line)
	}

	for i, j := 0, len(lines)-1; i < j; i, j = i+1, j-1 {
		lines[i], lines[j] = lines[j], lines[i]
	}

	content := strings.Join(lines, "\n") + "\n"

	// The initial cache should be stored on the machine as a file, that
	// way when you boot up and have no history, this file can be used.
	f.ContentCache = content

	return content, nil
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 2
	a.Mode = 0o444

	content, err := f.getContent(ctx)
	if err != nil {
		return err
	}

	a.Size = uint64(len(content))

	// TODO: I don't know how to set this correctly
	a.Uid = 1000
	a.Gid = 1000

	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	content, err := f.getContent(ctx)
	if err != nil {
		return err
	}
	fuseutil.HandleRead(req, resp, []byte(content))
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	ch <- AppendHistoryMessage{
		Timestamp: int(time.Now().UnixNano()),
		Content:   string(req.Data),
	}
	resp.Size = len(req.Data)
	return nil
}
