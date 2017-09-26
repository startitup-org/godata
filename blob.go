package godata

import (
	"io"
	"log"
	"mime/multipart"
	"net/http"

	"github.com/startitup-org/azure-sdk-for-go/storage"
)

type BlobClient struct {
	storage.BlobStorageClient
	container string
}

func NewBlobClient(acc, key, cn string) (BlobClient, error) {
	c, err := storage.NewBasicClient(acc, key)
	if err != nil {
		log.Println("storage.NewBasicClient", err)
		return BlobClient{}, err
	}
	//c.UseSharedKeyLite = true
	bc := BlobClient{
		c.GetBlobService(),
		cn,
	}
	//log.Println("storage.NewBasicClient", bc, c, err)

	return bc, nil
}

func (bc *BlobClient) Upload(bn string, f multipart.File) (*storage.Blob, error) {
	c := bc.GetContainerReference(bc.container)
	created, err := c.CreateIfNotExists(nil)
	if err != nil {
		log.Println("c.CreateIfNotExists", created, err, c)
		return nil, err
	}
	b := c.GetBlobReference(bn)
	sz, err := f.Seek(0, 2)
	if err != nil {
		log.Println("f.Seek(0, 2)", sz, err)
		return nil, err
	}
	sz2, err := f.Seek(0, 0)
	if err != nil {
		log.Println("f.Seek(0, 0)", sz2, err)
		return nil, err
	}
	b.Properties.ContentLength = sz
	//b.Properties.ContentType = "image/jpeg"
	//op := &storage.PutBlobOptions{
	//	LeaseID: base64.StdEncoding.EncodeToString([]byte(n)),
	//	Timeout: 60,
	//}
	//log.Println("c.GetBlobReference", sz, sz2, b.Properties)
	// Only the first 512 bytes are used to sniff the content type.
	buffer := make([]byte, 512)
	n, err := f.Read(buffer)
	if err != nil && err != io.EOF {
		log.Println("f.Read(buffer)", n, err)
		return nil, err
	}
	b.Properties.ContentType = http.DetectContentType(buffer[:n])
	f.Seek(0, 0)

	err = b.CreateBlockBlobFromReader(f, nil)
	if err != nil {
		log.Println("b.CreateBlockBlobFromReader", b.Name, b.Properties, err)
		return nil, err
	}
	return b, err
}
