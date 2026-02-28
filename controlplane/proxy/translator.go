package main

import (
	"net/http"
	"strings"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"
	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	userlib "github.com/00pauln00/niova-mdsvc/controlplane/user/lib"
	pmLib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
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
	case cpLib.GET_RACK, cpLib.GET_NISD, cpLib.GET_DEVICE, cpLib.GET_PDU, cpLib.GET_HYPERVISOR, cpLib.GET_PARTITION, cpLib.GET_VDEV_CHUNK_INFO, cpLib.GET_VDEV, cpLib.GET_CHUNK_NISD, cpLib.GET_VDEV_INFO, userlib.LoginAPI:
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
	case cpLib.PUT_NISD_ARGS:
		return &cpLib.NisdArgs{}
	case cpLib.CREATE_SNAP, cpLib.READ_SNAP_NAME, cpLib.READ_SNAP_VDEV:
		return &cpLib.SnapXML{}
	case cpLib.CREATE_VDEV:
		return &cpLib.VdevReq{}
	case userlib.PutUserAPI, userlib.AdminUserAPI:
		return &userlib.UserReq{}
	case userlib.GetUserAPI:
		return &userlib.GetReq{}
	case cpLib.DELETE_VDEV:
		return &cpLib.DeleteVdevReq{}
	default:
		return &cpLib.GetReq{}
	}
}

func GetRespStruct(name string) any {
	switch name {
	case cpLib.GET_RACK:
		return &[]cpLib.Rack{}
	case cpLib.GET_DEVICE:
		return &[]cpLib.Device{}
	case cpLib.GET_CHUNK_NISD:
		return &cpLib.ChunkNisd{}
	case cpLib.GET_VDEV_INFO:
		return &cpLib.VdevCfg{}
	case cpLib.GET_ALL_VDEV:
		return &[]cpLib.VdevCfg{}
	case cpLib.GET_HYPERVISOR:
		return &[]cpLib.Hypervisor{}
	case cpLib.GET_NISD_LIST:
		return &[]cpLib.Nisd{}
	case cpLib.GET_NISD:
		return &cpLib.Nisd{}
	case cpLib.GET_PDU:
		return &[]cpLib.PDU{}
	case cpLib.GET_PARTITION:
		return &[]cpLib.DevicePartition{}
	case cpLib.GET_VDEV_CHUNK_INFO, cpLib.GET_VDEV:
		return &[]*cpLib.Vdev{}
	case cpLib.READ_SNAP_NAME, cpLib.READ_SNAP_VDEV:
		return &cpLib.SnapXML{}
	case cpLib.CREATE_SNAP:
		return &cpLib.SnapResponseXML{}
	case cpLib.GET_NISD_ARGS:
		return &cpLib.NisdArgs{}
	case cpLib.PUT_RACK, cpLib.PUT_DEVICE, cpLib.PUT_HYPERVISOR, cpLib.PUT_NISD, cpLib.PUT_PDU, cpLib.PUT_PARTITION, cpLib.PUT_NISD_ARGS, cpLib.CREATE_VDEV:
		return &cpLib.ResponseXML{}
	case userlib.PutUserAPI, userlib.AdminUserAPI:
		return &userlib.UserResp{}
	case userlib.GetUserAPI:
		return &[]userlib.UserResp{}
	case userlib.LoginAPI:
		return &userlib.LoginResp{}
	case cpLib.DELETE_VDEV:
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

// EncodeErrorResponse constructs a GOB-encoded error response matching the
// expected response type for the given operation. Returns nil if the operation
// does not have a known error-capable response type.
func EncodeErrorResponse(name string, errMsg string) []byte {
	switch name {
	case cpLib.PUT_RACK, cpLib.PUT_DEVICE, cpLib.PUT_HYPERVISOR, cpLib.PUT_NISD, cpLib.PUT_PDU, cpLib.PUT_PARTITION, cpLib.PUT_NISD_ARGS, cpLib.CREATE_VDEV:
		return encode(&cpLib.ResponseXML{Success: false, Error: errMsg})
	case userlib.PutUserAPI, userlib.AdminUserAPI:
		return encode(&userlib.UserResp{Success: false, Error: errMsg})
	default:
		return nil
	}
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
