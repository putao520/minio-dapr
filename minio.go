package minio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"strconv"
	"time"
)

const (
	Endpoint = "endpoint"
	AccessKey = "accessKey"
	SecretAccessKey = "secretKey"
	SSLKey = "ssl"
	BucketKey = "bucket"
	RegionKey = "region"

	PresignedGetOperation bindings.OperationKind = "presignedGet"
	ReadBufferMax = 0x40000
)

type Minio struct {
	minioClient	*minio.Client
	logger 		logger.Logger
	Bucket		string
	Region		string
}

var _ = bindings.OutputBinding(&Minio{})

func NewMinio(logger logger.Logger) *Minio{
	return &Minio{logger: logger}
}

func (m *Minio) Init(metadata bindings.Metadata) error {
	m.logger.Debug("Initializing Minio binding")
	p := metadata.Properties
	endpoint, ok := p[Endpoint]
	if !ok || endpoint == "" {
		return errors.Errorf("missing Minio endpoint string")
	}
	accessKey, ok := p[AccessKey]
	if !ok || accessKey == "" {
		return errors.Errorf("missing Minio accessKey string")
	}
	secretKey, ok := p[SecretAccessKey]
	if !ok || secretKey == "" {
		return errors.Errorf("missing Minio secretKey string")
	}
	bucket, ok := p[BucketKey]
	if !ok || bucket == "" {
		return errors.Errorf("missing Minio bucket string")
	}
	region, ok := p[RegionKey]
	if !ok {
		region = ""
	}
	secure := propertyToBool(p, SSLKey)

	client, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return err
	}
	m.minioClient = client
	m.Bucket = bucket
	m.Region = region

	ctx := context.Background()

	exists, err := client.BucketExists(ctx, bucket)
	if err != nil  {
		return errors.Errorf("error Minio bucket %s error:%s", bucket, err.Error())
	}
	if !exists {
		err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region})
		if err != nil {
			return errors.Errorf("make Minio bucket %s error", bucket)
		}
	}
	return nil
}

func (m *Minio) Close() error {
	return nil
}

func (m *Minio) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
		PresignedGetOperation,
	}
}

type createResponse struct {
	Location  string  `json:"location"`
	VersionID string `json:"versionID"`
	Key string `json:"key"`
}
func (m *Minio) create(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	ctx := context.Background()

	d, err := strconv.Unquote(string(req.Data))
	if err == nil {
		req.Data = []byte(d)
	}

	r := bytes.NewReader(req.Data)

	p := req.Metadata

	objectName, ok := p["objectName"]
	if !ok || objectName == "" {
		return nil, errors.Errorf("missing name field")
	}

	resultUpload, err := m.minioClient.PutObject(ctx, m.Bucket, objectName, r, r.Size(), minio.PutObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("minio binding error. Uploading: %w", err)
	}
	jsonResponse, err := json.Marshal(createResponse{
		Location:  resultUpload.Location,
		VersionID: resultUpload.VersionID,
		Key: resultUpload.Key,
	})

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (m *Minio) get(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	ctx := context.Background()

	p := req.Metadata

	objectName, ok := p["objectName"]
	if !ok || objectName== "" {
		return nil, errors.Errorf("missing name field")
	}

	reader, err := m.minioClient.GetObject(ctx, m.Bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get object error: %w", err)
	}

	defer reader.Close()

	stat, err := reader.Stat()
	if err != nil {
		return nil, fmt.Errorf("io streaming stat is error: %w", err)
	}

	resultData := readByBuffer(reader, stat.Size)
	if resultData == nil {
		return nil, errors.Errorf("read io buffer error")
	}

	info := map[string]string{
		"size":      strconv.FormatInt(stat.Size, 10),
		"versionID": stat.VersionID,
		"key":       stat.Key,
	}
	return &bindings.InvokeResponse{
		Data: resultData,
		Metadata: info,
	}, nil
}

func (m *Minio) delete(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	ctx := context.Background()

	p := req.Metadata

	objectName, ok := p["objectName"]
	if !ok || objectName== "" {
		return nil, errors.Errorf("missing name field")
	}

	err := m.minioClient.RemoveObject(ctx, m.Bucket, objectName, minio.RemoveObjectOptions{GovernanceBypass: true})
	if err != nil {
		return nil, fmt.Errorf("minio binding error. remove: %w", err)
	}

	return nil, err
}

type fileInfoResponse struct {
	Size  string  `json:"size"`
	VersionID string `json:"versionID"`
	Key string `json:"key"`
}
func (m *Minio) list(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var resultList []fileInfoResponse
	for object := range m.minioClient.ListObjects(context.Background(), m.Bucket, minio.ListObjectsOptions{
		UseV1:     true,
		Recursive: true,
	}) {
		if object.Err != nil {
			fmt.Println(object.Err)
			continue
		}
		resultList = append(resultList, fileInfoResponse{
			Size:      strconv.FormatInt(object.Size, 10),
			VersionID: object.VersionID,
			Key:       object.Key,
		})
	}

	jsonResponse, err := json.Marshal(resultList)
	if err != nil {
		return nil, fmt.Errorf("minio binding error. list operation. cannot marshal blobs to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (m *Minio) presignedGet(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	ctx := context.Background()

	p := req.Metadata

	objectName, ok := p["objectName"]
	if !ok || objectName== "" {
		return nil, errors.Errorf("missing name field")
	}
	// expires time.Duration
	duration, ok := p["expires"]
	if !ok || objectName== "" {
		return nil, errors.Errorf("missing duration field")
	}
	expires, err := time.ParseDuration(duration)
	if err != nil  {
		return nil, errors.Errorf("expires %s is invalid", duration)
	}

	// reqParams := make(url.Values)
	// reqParams.Set("response-content-disposition", "attachment; filename=\"" + "" + "\"")
	result, err := m.minioClient.PresignedGetObject(ctx, m.Bucket, objectName, expires, nil)
	if err != nil {
		return nil, fmt.Errorf("presigned object error: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: []byte(result.String()),
		Metadata: nil,
	}, nil
}


func (m *Minio) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if req == nil {
		return nil, errors.Errorf("invoke request required")
	}
	switch req.Operation {
	case PresignedGetOperation:
		return m.presignedGet(req)
	case bindings.CreateOperation:
		return m.create(req)
	case bindings.GetOperation:
		return m.get(req)
	case bindings.DeleteOperation:
		return m.delete(req)
	case bindings.ListOperation:
		return m.list(req)
	default:
		return nil, errors.Errorf("minio binding error. unsupported operation %s", req.Operation)
	}
}

func readByBuffer(reader *minio.Object, size int64) []byte{
	totalSize := size
	resultData := make([]byte, totalSize)
	i := int64(0)
	l := ReadBufferMax
	for i < totalSize{
		_i := i + ReadBufferMax
		if _i > totalSize {
			l = int(ReadBufferMax - (_i - totalSize))
		}
		n, err := reader.ReadAt(resultData[i:l], i)
		if n != l && err != nil {
			_ = fmt.Errorf("readat error: %w size: %d/%d", err, n, l)
			return nil
		}
		i = _i
	}
	return resultData
}

func propertyToBool(props map[string]string, key string) bool {
	if v, ok := props[key]; ok {
		if i, err := strconv.ParseBool(v); err == nil {
			return i
		} else {
			return false
		}
	}
	return false
}