package srvctlplanefuncs

import (
	"C"
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
	DEVICE_NAME    = "d"
	NISD_ID        = "n"
	SERIAL_NUM     = "sn"
	STATUS         = "s"
	FAILURE_DOMAIN = "fd"
	CLIENT_PORT    = "cp"
	PEER_PORT      = "pp"
	IP_ADDR        = "ip"
	TOTAL_SPACE    = "ts"
	AVAIL_SPACE    = "as"
	SIZE           = "sz"
	NUM_CHUNKS     = "nc"
	NUM_REPLICAS   = "nr"
	ERASURE_CODE   = "e"

	nisdCfgKey   = "n_cfg"
	vdevCfgKey   = "v_cfg"
	deviceCfgKey = "d_cfg"
	sdeviceKey   = "sd"
	parentInfo   = "pi"
	pduKey       = "p"
	rackKey      = "r"
	nisdKey      = "n"
	vdevKey      = "v"
	chunkKey     = "c"
	hvKey        = "hv"
)

func SetClmFamily(cf string) {
	colmfamily = cf
}

func getConfKey(cfgType, id string) string {
	return fmt.Sprintf("%s/%s", cfgType, id)
}

func getVdevChunkKey(vdevID string) string {
	return fmt.Sprintf("%s/%s/%s", vdevKey, vdevID, chunkKey)
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
	readResult := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if readResult.Error != nil {
		log.Error("Range read failure ", readResult.Error)
		return nil, readResult.Error
	}

	log.Info("Read result by Vdev", readResult)
	for key, _ := range readResult.ResultMap {
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

	var req ctlplfl.GetReq
	err := ctlplfl.XMLDecode(args[1].([]byte), &req)
	if err != nil {
		return nil, err
	}

	key := getConfKey(nisdCfgKey, req.ID)
	readResult := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if readResult.Error != nil {
		log.Error("Range read failure ", readResult.Error)
		return nil, readResult.Error
	}

	nisdList := ParseEntities[ctlplfl.Nisd](readResult.ResultMap, nisdParser{})

	response, err := ctlplfl.XMLEncode(nisdList)
	if err != nil {
		log.Error("failed to encode nisd config:", err)
		return nil, fmt.Errorf("failed to encode nisd config: %v", err)
	}

	log.Debug("range read nisd config result: ", response)
	return response, nil
}

func getNisdList(cbArgs *PumiceDBServer.PmdbCbArgs) ([]ctlplfl.Nisd, error) {
	readResult := PumiceDBServer.RangeReadKV(cbArgs.UserID, nisdCfgKey, int64(len(nisdCfgKey)), nisdCfgKey, cbArgs.ReplySize, false, 0, colmfamily)
	if readResult.Error != nil {
		log.Error("Range read failure ", readResult.Error)
		return nil, readResult.Error
	}
	nisdList := ParseEntities[ctlplfl.Nisd](readResult.ResultMap, nisdParser{})
	log.Info("fetching nisd list: ", nisdList)
	return nisdList, nil
}

func WPNisdCfg(args ...interface{}) (interface{}, error) {

	var nisd ctlplfl.Nisd

	err := ctlplfl.XMLDecode(args[0].([]byte), &nisd)
	if err != nil {
		return nil, err
	}
	commitChgs := PopulateEntities[*ctlplfl.Nisd](&nisd, nisdPopulator{})

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

func RdDeviceInfo(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)

	var req ctlplfl.GetReq
	// Decode the input buffer into structure format
	err := ctlplfl.XMLDecode(args[1].([]byte), &req)
	if err != nil {
		return nil, err
	}

	dev := ctlplfl.Device{
		DevID: req.ID,
	}
	key := getConfKey(deviceCfgKey, dev.DevID)
	readResult := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if readResult.Error != nil {
		log.Error("Range read failure ", readResult.Error)
		return nil, readResult.Error
	}

	devices := ParseEntities[ctlplfl.Device](readResult.ResultMap, deviceParser{})
	response, err := ctlplfl.XMLEncode(devices)
	if err != nil {
		log.Error("failed to encode device config:", err)
		return nil, fmt.Errorf("failed to encode device config: %v", err)
	}

	return response, nil
}

func WPDeviceInfo(args ...interface{}) (interface{}, error) {
	// Limited to update only
	// DevID, SerialNumber, Status
	// HypervisorID, FailureDomain (Parent Info)
	var dev ctlplfl.Device

	err := ctlplfl.XMLDecode(args[0].([]byte), &dev)
	if err != nil {
		return nil, err
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
	commitChgs := PopulateEntities(&dev, devicePopulator{})

	//Fill in FuncIntrm structure
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}
	return encode(funcIntrm)
}

// Allocates Nisd to the Requested VDEV
func allocateNisd(vdev *ctlplfl.Vdev, nisds []ctlplfl.Nisd) []*ctlplfl.Nisd {
	allocatedNisd := make([]*ctlplfl.Nisd, 0)
	remainingVdevSize := vdev.Size
	vdev.NisdToChkMap = make([]ctlplfl.NisdChunk, 0)
	for _, nisd := range nisds {
		if (nisd.AvailableSize > int64(vdev.Size)) && remainingVdevSize > 0 {
			nisdChunk := ctlplfl.NisdChunk{
				Nisd:  &nisd,
				Chunk: make([]int, vdev.NumChunks),
			}
			allocatedNisd = append(allocatedNisd, &nisd)
			for i := 0; i < int(vdev.NumChunks); i++ {
				nisdChunk.Chunk[i] = i
				remainingVdevSize -= ctlplfl.CHUNK_SIZE
				nisd.AvailableSize -= ctlplfl.CHUNK_SIZE
			}
			vdev.NisdToChkMap = append(vdev.NisdToChkMap, nisdChunk)
		}
	}
	return allocatedNisd
}

// Generates all the Keys and Values that needs to be inserted into VDEV key space on vdev generation
func genVdevKV(vdev *ctlplfl.Vdev, nisdList []*ctlplfl.Nisd, commitChgs *[]funclib.CommitChg) {
	key := getConfKey(vdevCfgKey, vdev.VdevID)
	for _, field := range []string{SIZE, NUM_CHUNKS, NUM_REPLICAS} {
		var value string
		switch field {
		case SIZE:
			value = strconv.Itoa(int(vdev.Size))
		case NUM_CHUNKS:
			value = strconv.Itoa(int(vdev.NumChunks))
		case NUM_REPLICAS:
			value = strconv.Itoa(int(vdev.NumReplica))
		default:
			continue
		}
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", key, field)),
			Value: []byte(value),
		})
	}
	vcKey := getVdevChunkKey(vdev.VdevID)
	for _, nisd := range nisdList {
		for i := 0; i < int(vdev.NumChunks); i++ {
			*commitChgs = append(*commitChgs, funclib.CommitChg{
				Key:   []byte(fmt.Sprintf("%s/%d", vcKey, i)),
				Value: []byte(nisd.NisdID),
			})
		}

	}
}

// Generates all the Keys and Values that needs to be inserted into NISD key space on vdev generation
func genNisdKV(vdev *ctlplfl.Vdev, nisdList []*ctlplfl.Nisd, commitChgs *[]funclib.CommitChg) {
	for _, nisd := range nisdList {
		key := fmt.Sprintf("%s/%s/%s", nisdKey, nisd.NisdID, vdev.VdevID)
		for i := 0; i < int(vdev.NumChunks); i++ {
			*commitChgs = append(*commitChgs, funclib.CommitChg{
				Key:   []byte(key),
				Value: []byte(fmt.Sprintf("R.0.%d", i)),
			})
		}
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", getConfKey(nisdCfgKey, nisd.NisdID), AVAIL_SPACE)),
			Value: []byte(strconv.Itoa(int(nisd.AvailableSize))),
		})

	}

}

// Creates a VDEV, allocates the NISD and updates the PMDB with new data
func CreateVdev(args ...interface{}) (interface{}, error) {
	var vdev ctlplfl.Vdev
	commitChgs := make([]funclib.CommitChg, 0)
	// Decode the input buffer into structure format
	err := xml.Unmarshal(args[0].([]byte), &vdev.Size)
	if err != nil {
		log.Error("failed to decode vdev size: ", err)
		return nil, err
	}
	cbArgs := args[1].(*PumiceDBServer.PmdbCbArgs)
	log.Info("allocating vdev with size: ", vdev.Size)
	vdev.Init()
	nisdList, err := getNisdList(cbArgs)
	if err != nil {
		log.Error("failed to get nisd list:", err)
		return nil, err
	}
	// allocate nisd chunks to vdev
	allocNisds := allocateNisd(&vdev, nisdList)
	if len(allocNisds) == 0 {
		log.Error("failed to allocate nisd")
		return nil, fmt.Errorf("failed to allocate nisd: not enough space avialable")
	}

	genVdevKV(&vdev, allocNisds, &commitChgs)
	genNisdKV(&vdev, allocNisds, &commitChgs)

	r, err := ctlplfl.XMLEncode(vdev)
	if err != nil {
		log.Error("Failed to marshal vdev response: ", err)
		return nil, fmt.Errorf("failed to marshal nisd response: %v", err)
	}

	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}
	return encode(funcIntrm)
}

func WPPDUCfg(args ...interface{}) (interface{}, error) {
	var pdu ctlplfl.PDU
	// Decode the input buffer into structure format
	err := xml.Unmarshal(args[0].([]byte), &pdu)
	if err != nil {
		return nil, err
	}
	resp := &ctlplfl.ResponseXML{
		Name:    pdu.ID,
		Success: true,
	}
	r, err := ctlplfl.XMLEncode(resp)
	if err != nil {
		log.Error("Failed to marshal vdev response: ", err)
		return nil, fmt.Errorf("failed to marshal nisd response: %v", err)
	}
	commitChgs := PopulateEntities[*ctlplfl.PDU](&pdu, pduPopulator{})
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}

	return encode(funcIntrm)
}

func ReadPDUCfg(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)

	var req ctlplfl.GetReq
	// Decode the input buffer into structure format
	err := ctlplfl.XMLDecode(args[1].([]byte), &req)
	if err != nil {
		return nil, err
	}

	key := pduKey
	if !req.GetAll {
		key = getConfKey(pduKey, req.ID)
	}

	readResult := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if readResult.Error != nil {
		log.Error("Range read failure ", readResult.Error)
		return nil, readResult.Error
	}
	pduList := ParseEntities[ctlplfl.PDU](readResult.ResultMap, pduParser{})

	response, err := ctlplfl.XMLEncode(pduList)
	if err != nil {
		log.Error("failed to encode device config:", err)
		return nil, fmt.Errorf("failed to encode device config: %v", err)
	}

	return response, nil

}

func WPRackCfg(args ...interface{}) (interface{}, error) {
	var rack ctlplfl.Rack
	// Decode the input buffer into structure format
	err := xml.Unmarshal(args[0].([]byte), &rack)
	if err != nil {
		return nil, err
	}
	resp := &ctlplfl.ResponseXML{
		Name:    rack.ID,
		Success: true,
	}
	r, err := ctlplfl.XMLEncode(resp)
	if err != nil {
		log.Error("Failed to marshal vdev response: ", err)
		return nil, fmt.Errorf("failed to marshal nisd response: %v", err)
	}

	commitChgs := PopulateEntities[*ctlplfl.Rack](&rack, rackPopulator{})
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}

	return encode(funcIntrm)
}

func ReadRackCfg(args ...interface{}) (interface{}, error) {

	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)

	var req ctlplfl.GetReq
	// Decode the input buffer into structure format
	err := ctlplfl.XMLDecode(args[1].([]byte), &req)
	if err != nil {
		return nil, err
	}

	key := rackKey
	if !req.GetAll {
		key = getConfKey(rackKey, req.ID)
	}
	readResult := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if readResult.Error != nil {
		log.Error("Range read failure ", readResult.Error)
		return nil, readResult.Error
	}

	rackList := ParseEntities[ctlplfl.Rack](readResult.ResultMap, rackParser{})
	response, err := ctlplfl.XMLEncode(rackList)
	if err != nil {
		log.Error("failed to encode rack info:", err)
		return nil, fmt.Errorf("failed to encode rack info: %v", err)
	}

	return response, nil
}

func WPHyperVisorCfg(args ...interface{}) (interface{}, error) {
	var hv ctlplfl.Hypervisor
	// Decode the input buffer into structure format
	err := xml.Unmarshal(args[0].([]byte), &hv)
	if err != nil {
		return nil, err
	}
	resp := &ctlplfl.ResponseXML{
		Name:    hv.ID,
		Success: true,
	}
	r, err := ctlplfl.XMLEncode(resp)
	if err != nil {
		log.Error("Failed to marshal vdev response: ", err)
		return nil, fmt.Errorf("failed to marshal nisd response: %v", err)
	}
	commitChgs := PopulateEntities[*ctlplfl.Hypervisor](&hv, hvPopulator{})
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}

	return encode(funcIntrm)

}

func ReadHyperVisorCfg(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)

	var req ctlplfl.GetReq
	// Decode the input buffer into structure format
	err := ctlplfl.XMLDecode(args[1].([]byte), &req)
	if err != nil {
		return nil, err
	}

	key := hvKey
	if !req.GetAll {
		key = getConfKey(hvKey, req.ID)
	}

	readResult := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if readResult.Error != nil {
		log.Error("Range read failure ", readResult.Error)
		return nil, readResult.Error
	}

	hvList := ParseEntities[ctlplfl.Hypervisor](readResult.ResultMap, hvParser{})
	response, err := ctlplfl.XMLEncode(hvList)
	if err != nil {
		log.Error("failed to encode hypervisor info:", err)
		return nil, fmt.Errorf("failed to encode hypervisor info: %v", err)
	}

	return response, nil
}
