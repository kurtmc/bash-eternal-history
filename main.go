package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
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

var content *ContentRepository = nil

var existingDataLoaded bool = false
var data []byte = make([]byte, 0)

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

	content = NewContentRepository(svc, dynmodbTableName)

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
	} else if err != nil {
		panic(err)
	}

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

type ContentCache struct {
	Content     string
	LastUpdated time.Time
}

func NewFile(tableName string) *File {
	return &File{
		DynamodbTableName: tableName,
	}
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	if !existingDataLoaded {
		log.Printf("DEBUG: loading existing data")
		c, err := content.Get(ctx)
		if err != nil {
			log.Printf("DEBUG: WARN: could not get content: %v", err)
		} else {
			existingDataLoaded = true
		}
		data = []byte(c)
	}

	a.Inode = 2
	a.Mode = 0o444

	a.Size = uint64(len(data))

	// TODO: I don't know how to set this correctly
	a.Uid = 1000
	a.Gid = 1000

	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	log.Printf("DEBUG: Read()")
	if !existingDataLoaded {
		log.Printf("DEBUG: loading existing data")
		c, err := content.Get(ctx)
		if err != nil {
			log.Printf("DEBUG: WARN: could not get content: %v", err)
		} else {
			existingDataLoaded = true
		}
		data = []byte(c)
	}

	fuseutil.HandleRead(req, resp, data)

	return nil
}

func HandleWrite(req *fuse.WriteRequest, resp *fuse.WriteResponse, data *[]byte) {
	size := len(req.Data)

	if int(req.Offset)+size > int(len(*data)) {
		newData := make([]byte, int(req.Offset)+size)
		copy(newData, *data)
		*data = newData
	}
	n := copy((*data)[req.Offset:int(req.Offset)+size], req.Data)
	resp.Size = n
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	defer duration(track("Write()"))
	ch <- AppendHistoryMessage{
		Timestamp: int(time.Now().UnixNano()),
		Content:   string(req.Data),
	}
	HandleWrite(req, resp, &data)
	return nil
}

func track(msg string) (string, time.Time) {
	return msg, time.Now()
}

func duration(msg string, start time.Time) {
	log.Printf("DEBUG: %v: %v\n", msg, time.Since(start))
}
