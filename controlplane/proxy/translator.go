package main

import (
	"net/http"
	"strings"

	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	pmLib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	log "github.com/sirupsen/logrus"
)

func GetEncodingType(r *http.Request) pmLib.Format {
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		ct = r.Header.Get("Accept")
	}
	ct = strings.ToLower(ct)
	res := strings.Split(ct, "/")
	return pmLib.Format(res[1])
}

func GetReqStruct(name string) any {
	switch name {
	case cpLib.GET_RACK:
		return &cpLib.GetReq{}
	case cpLib.PUT_RACK:
		return &cpLib.Rack{}
	}
	return nil
}

func GetRespStruct(name string) any {
	switch name {
	case cpLib.GET_RACK:
		return &[]cpLib.Rack{}
	case cpLib.PUT_RACK:
		return &cpLib.ResponseXML{}
	}
	return nil
}

func DecodeRequest(enctype pmLib.Format, name string, req []byte) (any, error) {
	res := GetReqStruct(name)
	err := pmLib.Decoder(enctype, req, res)
	if err != nil {
		log.Error("%v: failed to decode request: ", enctype, err)
		return nil, err
	}
	return res, nil
}

func EncodeResponse(enctype pmLib.Format, name string, resp *[]byte) error {
	res := GetRespStruct(name)
	err := pmLib.Decoder(pmLib.GOB, *resp, res)
	if err != nil {
		log.Error("gob: failed to decode response: ", err)
		return err
	}
	log.Info("encoding res: ", res)
	*resp, err = pmLib.Encoder(enctype, res)
	if err != nil {
		log.Error("%v: failed to encode response: ", enctype, err)
		return err
	}
	return nil
}
