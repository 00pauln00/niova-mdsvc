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

func ReadNisdConfig(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)

	var nisd ctlplfl.Nisd
	// Decode the input buffer into structure format
	err := xml.Unmarshal(args[1].([]byte), &nisd)
	if err != nil {
		return nil, err
	}

	log.Info("Key to be read : ", nisd.DeviceID)
	readResult, err := PumiceDBServer.PmdbReadKV(cbArgs.UserID, nisd.DeviceID, int64(len(nisd.DeviceID)), colmfamily)
	if err != nil {
		log.Error("read failure ", err)
		return nil, err
	}

	return readResult, nil
}

func RangeReadNisdConfig(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)

	var key string
	// Decode the input buffer into structure format
	err := xml.Unmarshal(args[1].([]byte), &key)
	if err != nil {
		return nil, err
	}
	readResult, _, _, _, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("failed to read nisd config:", err)
		return nil, err
	}

	encRes, err := encode(readResult)
	if err != nil {
		return nil, err
	}
	log.Debug("Range read nisd config result: ", readResult)
	return encRes, nil
}

func WritePrepNisd(args ...interface{}) (interface{}, error) {

	var nisd ctlplfl.Nisd

	err := xml.Unmarshal(args[0].([]byte), &nisd)
	if err != nil {
		return nil, err
	}

	commitChgs := make([]funclib.CommitChg, 0)
	data := map[string]interface{}{
		"conf_d":  nisd.DeviceID,
		"conf_cp": nisd.ClientPort,
		"conf_pp": nisd.PeerPort,
	}

	baseKey := fmt.Sprintf("/n/%v/", nisd.DeviceUUID)
	for prefix, val := range data {
		commitChgs = append(commitChgs, funclib.CommitChg{
			Key:   []byte(baseKey + prefix),
			Value: []byte(fmt.Sprintf("%v", val)),
		})
	}

	//Fill the response structure
	nisdResponse := ctlplfl.NisdResponseXML{
		DeviceID: nisd.DeviceID,
		Success:  true,
	}

	r, err := xml.Marshal(nisdResponse)
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

func ReadDeviceUUID(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)

	var dev string
	// Decode the input buffer into structure format
	err := xml.Unmarshal(args[1].([]byte), &dev)
	if err != nil {
		return nil, err
	}

	log.Info("Key to be read : ", dev)
	readResult, err := PumiceDBServer.PmdbReadKV(cbArgs.UserID, dev, int64(len(dev)), colmfamily)
	if err != nil {
		log.Error("read failure ", err)
		return nil, err
	}

	return readResult, nil
}

func WriteDeviceInfo(args ...interface{}) (interface{}, error) {

	var dev ctlplfl.DeviceInfo

	err := xml.Unmarshal(args[0].([]byte), &dev)
	if err != nil {
		return nil, err
	}

	commitChgs := make([]funclib.CommitChg, 0)
	data := map[string]interface{}{
		"n_": dev.UniqID,
		"S_": dev.SerialNumber,
		"s_": dev.Status,
	}

	baseKey := fmt.Sprintf("/d/%v/", dev.ID)
	for prefix, val := range data {
		commitChgs = append(commitChgs, funclib.CommitChg{
			Key:   []byte(baseKey + prefix),
			Value: []byte(fmt.Sprintf("%v", val)),
		})
	}

	// TODO: use a common response struct for all the read functions
	nisdResponse := ctlplfl.NisdResponseXML{
		DeviceID: dev.ID,
		Success:  true,
	}

	r, err := xml.Marshal(nisdResponse)
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
