package srvctlplanefuncs

import (
	"C"
	"bytes"
	"encoding/gob"
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	PumiceDBServer "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
	log "github.com/sirupsen/logrus"
)

var colmfamily string

const (
	NISD_ID        = "n"
	SERIAL_NUM     = "sn"
	STATUS         = "s"
	HV_ID          = "hv"
	FAILURE_DOMAIN = "fd"
	CLIENT_PORT    = "cp"
	PEER_PORT      = "pp"
	IP_ADDR        = "ip"
)

func decode(payload []byte, s interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(payload))
	return dec.Decode(s)
}

func encode(s interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(s)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func SetClmFamily(cf string) {
	colmfamily = cf
}

func ReadSnapByName(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)

	var Snap ctlplfl.SnapXML
	// Decode the input buffer into structure format
	err := xml.Unmarshal(args[1].([]byte), &Snap)
	if err != nil {
		return nil, err
	}

	//FIX: Arbitrary read size
	key := fmt.Sprintf("snap/%s", Snap.SnapName)
	log.Info("Key to be read : ", key)
	readResult, err := PumiceDBServer.PmdbReadKV(cbArgs.UserID, key, int64(len(key)), colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}

	return readResult, nil
}

func ReadSnapForVdev(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)

	var Snap ctlplfl.SnapXML
	// Decode the input buffer into structure format
	err := xml.Unmarshal(args[1].([]byte), &Snap)
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("%s/snap", Snap.Vdev)
	readResult, _, _, _, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}

	log.Info("Read result by Vdev", readResult)
	for key, _ := range readResult {
		c := strings.Split(key, "/")

		idx, err := strconv.ParseUint(c[len(c)-2], 10, 32)
		seq, err := strconv.ParseUint(c[len(c)-1], 10, 64)
		if err != nil {
			return nil, err
		}

		Snap.Chunks = append(Snap.Chunks, ctlplfl.ChunkXML{
			Idx: uint32(idx),
			Seq: seq,
		})
	}

	//Need to figure out what if rsb size blows up more than 4MB?
	rsb, err := xml.Marshal(Snap)
	if err != nil {
		return nil, err
	}
	return rsb, nil
}

func WritePrepCreateSnap(args ...interface{}) (interface{}, error) {

	var Snap ctlplfl.SnapXML

	// Decode the input buffer into structure format
	err := xml.Unmarshal(args[0].([]byte), &Snap)
	if err != nil {
		return nil, err
	}

	commitChgs := make([]funclib.CommitChg, 0)
	for _, chunk := range Snap.Chunks {
		// Schema: {vdev}/snap/{chunk}/{Seq} : {Ref count}
		// TODO: Change the dummy ref count
		chg := funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/snap/%d/%d", Snap.Vdev, chunk.Idx, chunk.Seq)),
			Value: []byte{uint8(1)},
		}
		commitChgs = append(commitChgs, chg)
	}

	// Schema: snap/{name}:{blob}
	chg := funclib.CommitChg{
		Key:   []byte(fmt.Sprintf("snap/%s", Snap.SnapName)),
		Value: args[0].([]byte),
	}

	commitChgs = append(commitChgs, chg)

	//Fill the response structure
	snapResponse := ctlplfl.SnapResponseXML{
		SnapName: ctlplfl.SnapName{
			Name:    Snap.SnapName,
			Success: true,
		},
	}

	r, err := xml.Marshal(snapResponse)
	if err != nil {
		log.Error("Failed to marshal snapshot response: ", err)
		return nil, fmt.Errorf("failed to marshal snapshot response: %v", err)
	}

	//Fill in FuncIntrm structure
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}
	return encode(funcIntrm)
}

func ApplyFunc(args ...interface{}) (interface{}, error) {
	cbargs := args[0].(*PumiceDBServer.PmdbCbArgs)

	var intrm funclib.FuncIntrm
	buf := C.GoBytes(cbargs.AppData, C.int(cbargs.AppDataSize))
	err := decode(buf, &intrm)
	if err != nil {
		log.Error("Failed to decode the apply changes: ", err)
		return nil, fmt.Errorf("failed to decode apply changes: %v", err)
	}

	Chgs := intrm.Changes
	for _, Chg := range Chgs {
		log.Info(string(Chg.Key), " : ", string(Chg.Value))

		rc := PumiceDBServer.PmdbWriteKV(cbargs.UserID, cbargs.PmdbHandler,
			string(Chg.Key),
			int64(len(Chg.Key)), string(Chg.Value),
			int64(len(Chg.Value)), colmfamily)

		if rc < 0 {
			//Should we revert the changes?
			log.Error("Failed to apply changes for key: ", Chg.Key)
			return nil, fmt.Errorf("failed to apply changes for key: %s", Chg.Key)
		}
	}
	//Empty return for the interface as we are filling the reply buf in the function
	return intrm.Response, nil
}

func RdNisdCfg(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)

	var nisd ctlplfl.Nisd
	// Decode the input buffer into structure format
	err := ctlplfl.XMLDecode(args[1].([]byte), &nisd)
	if err != nil {
		log.Errorf("failed xml decode:", err)
		return nil, err
	}

	key := nisd.GetConfKey()
	readResult, _, _, _, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}

	for _, field := range []string{CLIENT_PORT, PEER_PORT, HV_ID, FAILURE_DOMAIN, IP_ADDR} {
		k := fmt.Sprintf("%s/%s_", key, field)
		if val, ok := readResult[k]; ok {
			switch field {
			case CLIENT_PORT:
				p, _ := strconv.Atoi(string(val))
				nisd.ClientPort = uint16(p)
			case PEER_PORT:
				p, _ := strconv.Atoi(string(val))
				nisd.PeerPort = uint16(p)
			case HV_ID:
				nisd.HyperVisorID = string(val)
			case FAILURE_DOMAIN:
				nisd.FailureDomain = string(val)
			case IP_ADDR:
				nisd.IPAddr = string(val)
			}
		}
	}

	log.Debug("Read nisd config: ", nisd)
	response, err := ctlplfl.XMLEncode(nisd)
	if err != nil {
		log.Error("failed to encode nisd config:", err)
		return nil, fmt.Errorf("failed to encode nisd config: %v", err)
	}

	log.Debug("range read nisd config result: ", response)
	return response, nil
}

func WPNisdCfg(args ...interface{}) (interface{}, error) {

	var nisd ctlplfl.Nisd

	err := ctlplfl.XMLDecode(args[0].([]byte), &nisd)
	if err != nil {
		return nil, err
	}

	commitChgs := make([]funclib.CommitChg, 0)
	key := nisd.GetConfKey()
	// Schema: /n/{nisdID}/cfg/{field} : {value}
	for _, field := range []string{CLIENT_PORT, PEER_PORT, HV_ID, FAILURE_DOMAIN, IP_ADDR} {
		var value string
		switch field {
		case CLIENT_PORT:
			value = strconv.Itoa(int(nisd.ClientPort))
		case PEER_PORT:
			value = strconv.Itoa(int(nisd.PeerPort))
		case HV_ID:
			value = nisd.HyperVisorID
		case FAILURE_DOMAIN:
			value = nisd.FailureDomain
		case IP_ADDR:
			value = nisd.IPAddr
		default:
			continue
		}
		commitChgs = append(commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s_", key, field)),
			Value: []byte(value),
		})
	}
	//Fill the response structure
	nisdResponse := ctlplfl.ResponseXML{
		Name:    nisd.DevID,
		Success: true,
	}

	r, err := ctlplfl.XMLEncode(nisdResponse)
	if err != nil {
		log.Error("Failed to marshal nisd response: ", err)
		return nil, fmt.Errorf("failed to marshal nisd response: %v", err)
	}

	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}
	return encode(funcIntrm)
}

func RdDeviceCfg(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)

	var dev ctlplfl.DeviceInfo
	// Decode the input buffer into structure format
	err := ctlplfl.XMLDecode(args[1].([]byte), &dev)
	if err != nil {
		return nil, err
	}

	key := dev.GetConfKey()
	readResult, _, _, _, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}

	for _, field := range []string{NISD_ID, SERIAL_NUM, STATUS, HV_ID, FAILURE_DOMAIN} {
		k := fmt.Sprintf("%s/%s_", key, field)
		if val, ok := readResult[k]; ok {
			log.Info("Value for key ", k, " : ", string(val))
			switch field {
			case NISD_ID:
				dev.NisdID = string(val)
			case SERIAL_NUM:
				dev.SerialNumber = string(val)
			case STATUS:
				status, _ := strconv.Atoi(string(val))
				dev.Status = uint16(status)
			case HV_ID:
				dev.HyperVisorID = string(val)
			case FAILURE_DOMAIN:
				dev.FailureDomain = string(val)
			}
		}
	}

	response, err := ctlplfl.XMLEncode(dev)
	if err != nil {
		log.Error("failed to encode device config:", err)
		return nil, fmt.Errorf("failed to encode device config: %v", err)
	}

	return response, nil
}

func WPDeviceCfg(args ...interface{}) (interface{}, error) {

	var dev ctlplfl.DeviceInfo

	err := ctlplfl.XMLDecode(args[0].([]byte), &dev)
	if err != nil {
		return nil, err
	}

	k := dev.GetConfKey()
	//Schema : /d/{devID}/cfg/{field} : {value}
	commitChgs := make([]funclib.CommitChg, 0)
	for _, field := range []string{NISD_ID, SERIAL_NUM, STATUS, HV_ID, FAILURE_DOMAIN} {
		var value string
		switch field {
		case NISD_ID:
			value = dev.NisdID
		case SERIAL_NUM:
			value = dev.SerialNumber
		case STATUS:
			value = strconv.Itoa(int(dev.Status))
		case HV_ID:
			value = dev.HyperVisorID
		case FAILURE_DOMAIN:
			value = dev.FailureDomain
		default:
			continue
		}
		commitChgs = append(commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s_", k, field)),
			Value: []byte(value),
		})
	}

	// TODO: use a common response struct for all the read functions
	nisdResponse := ctlplfl.ResponseXML{
		Name:    dev.DevID,
		Success: true,
	}

	r, err := ctlplfl.XMLEncode(nisdResponse)
	if err != nil {
		log.Error("Failed to marshal nisd response: ", err)
		return nil, fmt.Errorf("failed to marshal nisd response: %v", err)
	}

	//Fill in FuncIntrm structure
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}
	return encode(funcIntrm)
}
