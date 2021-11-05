package minio

import (
	"encoding/json"
	"fmt"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

// put
func TestGetMinio(t *testing.T){
	minio := NewMinio(logger.NewLogger("minio"))
	meta :=bindings.Metadata{Properties: map[string]string{
		Endpoint: "192.168.75.80:9000",
		AccessKey: "minio",
		SecretAccessKey: "putao520",
		SSLKey: "false",
		BucketKey: "fos",
		RegionKey: "lb-1",
	}}
	if err := minio.Init(meta); err != nil {
		t.Fatal(err)
	}
	defer minio.Close()
	// read file
	f, err := ioutil.ReadFile("./test.txt")
	if err != nil{
		t.Fatal("test file open fatal")
	}
	// put object
	t.Run("return err if is error", func(t *testing.T){
		inputCreate := map[string]string{}
		inputCreate["objectName"] = "test_file"
		r1 := bindings.InvokeRequest{
			Data:      f,
			Metadata:  inputCreate,
			Operation: bindings.CreateOperation,
		}
		result1, err := minio.create(&r1)
		assert.Nil(t, err)
		data := createResponse{}
		err = json.Unmarshal(result1.Data, &data)
		if err != nil {
			t.Fatal(err)
		}
		objectName := data.Key

		inputGet := map[string]string{}
		inputGet["objectName"] = objectName
		r2 := bindings.InvokeRequest{
			Data:      nil,
			Metadata:  inputGet,
			Operation: bindings.GetOperation,
		}
		result2, err := minio.get(&r2)
		assert.Nil(t, err)
		assert.Equal(t, string(result2.Data), "test content")

		presignedGet := map[string]string{}
		presignedGet["objectName"] = objectName
		presignedGet["expires"] = "60s"
		r5 := bindings.InvokeRequest{
			Data:      nil,
			Metadata:  presignedGet,
			Operation: PresignedGetOperation,
		}
		result5, err := minio.presignedGet(&r5)
		assert.Nil(t, err)
		fmt.Println(string(result5.Data))

		r3 := bindings.InvokeRequest{
			Data:      nil,
			Metadata:  nil,
			Operation: bindings.ListOperation,
		}
		result3, err := minio.list(&r3)
		assert.Nil(t, err)
		fmt.Println(string(result3.Data))

		inputRemove := map[string]string{}
		inputRemove["objectName"] = objectName
		r4 := bindings.InvokeRequest{
			Data:      nil,
			Metadata:  inputRemove,
			Operation: bindings.DeleteOperation,
		}
		_, err = minio.delete(&r4)
		assert.Nil(t, err)
	})
}
