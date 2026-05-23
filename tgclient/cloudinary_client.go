package tgclient

import (
	"context"
	"telecloud/config"

	"github.com/cloudinary/cloudinary-go/v2"
	"github.com/cloudinary/cloudinary-go/v2/api"
	"github.com/cloudinary/cloudinary-go/v2/api/uploader"
)

var cld *cloudinary.Cloudinary
var cfg *config.Config

func init() {
	var err error
	cfg, err = config.Load()
	cld, err = cloudinary.New()

	if err != nil {
		panic(err)
	}
}

func UploadImage(localThumb *string) string {
	if cfg.ThumbBackupType == "" {
		return ""
	}
	var ctx = context.Background()
	resp, err := cld.Upload.Upload(ctx, *localThumb, uploader.UploadParams{
		ResourceType:    "image",
		// PublicID:        *localThumb,
		Folder: 		 "telecloud",
		Overwrite:       api.Bool(true)})

	if err != nil {
		return ""
	}

	return resp.URL
}