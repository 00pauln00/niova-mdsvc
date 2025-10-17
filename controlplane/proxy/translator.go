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
	case cpLib.GET_RACK, cpLib.GET_NISD, cpLib.GET_DEVICE, cpLib.GET_PDU, cpLib.GET_HYPERVISOR, cpLib.GET_PARTITION, cpLib.GET_VDEV:
		return &cpLib.GetReq{}
	case cpLib.PUT_RACK:
		return &cpLib.Rack{}
	case cpLib.PUT_DEVICE:
		return &cpLib.Device{}
	case cpLib.PUT_HYPERVISOR:
		return &cpLib.Hypervisor{}
	case cpLib.PUT_NISD:
		return &cpLib.Nisd{}
	case cpLib.PUT_PDU:
		return &cpLib.PDU{}
	case cpLib.PUT_PARTITION:
		return &cpLib.DevicePartition{}
	case cpLib.CREATE_SNAP, cpLib.READ_SNAP_NAME, cpLib.READ_SNAP_VDEV:
		return &cpLib.SnapXML{}
	case cpLib.CREATE_VDEV:
		var size int64
		return &size
	}
	return nil
}

func GetRespStruct(name string) any {
	switch name {
	case cpLib.GET_RACK:
		return &[]cpLib.Rack{}
	case cpLib.GET_DEVICE:
		return &[]cpLib.Device{}
	case cpLib.GET_HYPERVISOR:
		return &[]cpLib.Hypervisor{}
	case cpLib.GET_NISD:
		return &[]cpLib.Nisd{}
	case cpLib.GET_PDU:
		return &[]cpLib.PDU{}
	case cpLib.GET_PARTITION:
		return &[]cpLib.DevicePartition{}
	case cpLib.CREATE_VDEV:
		return &cpLib.Vdev{}
	case cpLib.GET_VDEV:
		return &[]*cpLib.Vdev{}
	case cpLib.READ_SNAP_NAME, cpLib.READ_SNAP_VDEV:
		return &cpLib.SnapXML{}
	case cpLib.CREATE_SNAP:
		return &cpLib.SnapResponseXML{}
	case cpLib.PUT_RACK, cpLib.PUT_DEVICE, cpLib.PUT_HYPERVISOR, cpLib.PUT_NISD, cpLib.PUT_PDU, cpLib.PUT_PARTITION:
		return &cpLib.ResponseXML{}
	}
	return nil
}

func DecodeRequest(enctype pmLib.Format, name string, req []byte) (any, error) {
	res := GetReqStruct(name)
	log.Tracef("decoding request type %s, with %v decoder", name, enctype)
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
	log.Tracef("encoding response type %s, with %v encoder", name, enctype)
	*resp, err = pmLib.Encoder(enctype, res)
	if err != nil {
		log.Error("%v: failed to encode response: ", enctype, err)
		return err
	}
	return nil
}
