package godata

import (
	"log"
	"mime/multipart"

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

func (bc *BlobClient) Upload(n string, f multipart.File) (string, error) {
	c := bc.GetContainerReference(bc.container)
	created, err := c.CreateIfNotExists(nil)
	if err != nil {
		log.Println("c.CreateIfNotExists", created, err, c)
		return "", err
	}
	b := c.GetBlobReference(n)
	sz, err := f.Seek(0, 2)
	if err != nil {
		log.Println("f.Seek(0, 2)", sz, err)
		return "", err
	}
	sz2, err := f.Seek(0, 0)
	if err != nil {
		log.Println("f.Seek(0, 0)", sz2, err)
		return "", err
	}
	b.Properties.ContentLength = sz
	//b.Properties.ContentType = "image/jpeg"
	//op := &storage.PutBlobOptions{
	//	LeaseID: base64.StdEncoding.EncodeToString([]byte(n)),
	//	Timeout: 60,
	//}
	//log.Println("c.GetBlobReference", sz, sz2, b.Properties)

	err = b.CreateBlockBlobFromReader(f, nil)
	if err != nil {
		log.Println("b.CreateBlockBlobFromReader", b.Name, b.Properties, err)
		return "", err
	}
	return b.Name, err
}
