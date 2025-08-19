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

	"github.com/google/uuid"
)

var colmfamily string

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

	key := fmt.Sprintf("/n/%s/cfg", nisd.NisdID)
	readResult, _, _, _, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}

	for _, field := range []string{"ClientPort", "PeerPort", "HyperVisorID", "FailureDomain", "IPAddr"} {
		k := fmt.Sprintf("/n/%s/cfg_%s", nisd.NisdID, field)
		if val, ok := readResult[k]; ok {
			switch field {
			case "ClientPort":
				p, _ := strconv.Atoi(string(val))
				nisd.ClientPort = uint16(p)
			case "PeerPort":
				p, _ := strconv.Atoi(string(val))
				nisd.PeerPort = uint16(p)
			case "HyperVisorID":
				nisd.HyperVisorID = string(val)
			case "FailureDomain":
				nisd.FailureDomain = string(val)
			case "IPAddr":
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
	// Schema: /n/{nisdID}/cfg_{field} : {value}
	for _, field := range []string{"ClientPort", "PeerPort", "HyperVisorID", "FailureDomain", "IPAddr"} {
		var value string
		switch field {
		case "ClientPort":
			value = strconv.Itoa(int(nisd.ClientPort))
		case "PeerPort":
			value = strconv.Itoa(int(nisd.PeerPort))
		case "HyperVisorID":
			value = nisd.HyperVisorID
		case "FailureDomain":
			value = nisd.FailureDomain
		case "IPAddr":
			value = nisd.IPAddr
		default:
			continue
		}
		commitChgs = append(commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("/n/%s/cfg_%s", nisd.NisdID, field)),
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

	log.Debug("Read nisd uuid for block device: ", dev)
	
	key := fmt.Sprintf("/d/%s/cfg", dev)
	readResult, _, _, _, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}

	for _, field := range []string{"NisdID", "SerialNumber", "Status", "HyperVisorID", "FailureDomain"} {
		k := fmt.Sprintf("/d/%s/cfg_%s", dev, field)
		if val, ok := readResult[k]; ok {
			switch field {
			case "NisdID":
				dev.NisdID, _ = uuid.Parse(string(val))
			case "SerialNumber":
				dev.SerialNumber = string(val)
			case "Status":
				status, _ := strconv.Atoi(string(val))
				dev.Status = uint16(status)
			case "HyperVisorID":
				dev.HyperVisorID = string(val)
			case "FailureDomain":
				dev.FailureDomain = string(val)
			}
		}
	}

	log.Debug("Read device config: ", dev)
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

	k := fmt.Sprintf("/d/%v/cfg", dev.DevID.String())
	//Schema : /d/{devID}/cfg_{field} : {value}
	commitChgs := make([]funclib.CommitChg, 0)
	for _, field := range []string{"NisdID", "SerialNumber", "Status", "HyperVisorID", "FailureDomain"} {
		var value string
		switch field {
		case "NisdID":
			value = dev.NisdID.String()
		case "SerialNumber":
			value = dev.SerialNumber
		case "Status":
			value = strconv.Itoa(int(dev.Status))
		case "HyperVisorID":
			value = dev.HyperVisorID
		case "FailureDomain":
			value = dev.FailureDomain
		default:
			continue
		}
		commitChgs = append(commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/cfg_%s", k, field)),
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
