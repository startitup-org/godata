package godata

import (
	"io"
	"log"
	"mime/multipart"
	"net/http"

	"github.com/startitup-org/azure-sdk-for-go/storage"
)

type BlobClient struct {
	*storage.BlobStorageClient
	Container *storage.Container
}

func NewBlobClient(acc, key, cn string) (*BlobClient, error) {
	c, err := storage.NewBasicClient(acc, key)
	if err != nil {
		log.Println("storage.NewBasicClient", err)
		return &BlobClient{}, err
	}

	bs := c.GetBlobService()
	bc := bs.GetContainerReference(cn)
	created, err := bc.CreateIfNotExists(nil)
	if err != nil {
		log.Println("c.CreateIfNotExists", created, err, bc)
		return nil, err
	}
	bc.SetPermissions(storage.ContainerPermissions{AccessType: storage.ContainerAccessTypeBlob}, nil)

	return &BlobClient{
		&bs,
		bc,
	}, nil
}

func (bc *BlobClient) Upload(bn string, f multipart.File) (*storage.Blob, error) {
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

	// Only the first 512 bytes are used to sniff the content type.
	buffer := make([]byte, 512)
	n, err := f.Read(buffer)
	if err != nil && err != io.EOF {
		log.Println("f.Read(buffer)", n, err)
		return nil, err
	}
	f.Seek(0, 0)

	b := bc.Container.GetBlobReference(bn)
	b.Properties.ContentType = http.DetectContentType(buffer[:n])
	b.Properties.ContentLength = sz
	//log.Println("c.GetBlobReference", sz, sz2, b.Properties)

	err = b.CreateBlockBlobFromReader(f, nil)
	if err != nil {
		log.Println("b.CreateBlockBlobFromReader", b.Name, b.Properties, err)
		return nil, err
	}
	return b, err
}
