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
	DEVICE_NAME    = "d"
	NISD_ID        = "n"
	SERIAL_NUM     = "sn"
	STATUS         = "s"
	HV_ID          = "hv"
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

	UUID_PREFIX     = 3
	CONF_PREFIX     = 4
	NISD_PREFIX_LEN = 5

	nisdCfgKey   = "/n/cfg"
	vdevCfgKey   = "/v/cfg"
	deviceCfgKey = "/d/cfg"
	parentInfo   = "pi"
	pduKey       = "p"
	rackKey      = "r"
	nisdKey      = "n"
	vdevKey      = "v"
	chunkKey     = "c"
	hvKey        = "hv"

	PDU_UUID_PREFIX = 1
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

func getConfKey(cfgType, id string) string {
	return fmt.Sprintf("%s/%s", cfgType, id)
}

func getVdevChunkKey(vdevID string) string {
	return fmt.Sprintf("/%s/%s/%s", vdevKey, vdevID, chunkKey)
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

	key := getConfKey(nisdCfgKey, nisd.NisdID)
	readResult, _, _, _, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}

	for _, field := range []string{CLIENT_PORT, PEER_PORT, HV_ID, FAILURE_DOMAIN, IP_ADDR, TOTAL_SPACE, AVAIL_SPACE} {
		k := fmt.Sprintf("%s/%s", key, field)
		if val, ok := readResult[k]; ok {
			populateNisd(&nisd, field, string(val))
		}
	}

	response, err := ctlplfl.XMLEncode(nisd)
	if err != nil {
		log.Error("failed to encode nisd config:", err)
		return nil, fmt.Errorf("failed to encode nisd config: %v", err)
	}

	log.Debug("range read nisd config result: ", response)
	return response, nil
}

func populateNisd(nisd *ctlplfl.Nisd, opt, val string) {
	switch opt {

	case DEVICE_NAME:
		nisd.DevID = val
	case CLIENT_PORT:
		p, _ := strconv.Atoi(val)
		nisd.ClientPort = uint16(p)
	case PEER_PORT:
		p, _ := strconv.Atoi(val)
		nisd.PeerPort = uint16(p)
	case HV_ID:
		nisd.HyperVisorID = val
	case FAILURE_DOMAIN:
		nisd.FailureDomain = val
	case IP_ADDR:
		nisd.IPAddr = val
	case TOTAL_SPACE:
		ts, _ := strconv.Atoi(val)
		nisd.TotalSize = int64(ts)
	case AVAIL_SPACE:
		as, _ := strconv.Atoi(val)
		nisd.AvailableSize = int64(as)

	}

}

func getNisdList(cbArgs *PumiceDBServer.PmdbCbArgs) (map[string]*ctlplfl.Nisd, error) {
	readResult, _, _, _, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, nisdCfgKey, int64(len(nisdCfgKey)), nisdCfgKey, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	nisdMap := make(map[string]*ctlplfl.Nisd)
	for key, val := range readResult {
		res := strings.Split(key, "/")
		if len(res) < NISD_PREFIX_LEN {
			continue
		}
		if nisd, ok := nisdMap[res[UUID_PREFIX]]; !ok {
			nisd := &ctlplfl.Nisd{
				NisdID: res[UUID_PREFIX],
			}
			populateNisd(nisd, res[CONF_PREFIX], string(val))
			nisdMap[res[UUID_PREFIX]] = nisd
		} else {
			populateNisd(nisd, res[CONF_PREFIX], string(val))
		}

	}
	return nisdMap, nil
}

func WPNisdCfg(args ...interface{}) (interface{}, error) {

	var nisd ctlplfl.Nisd

	err := ctlplfl.XMLDecode(args[0].([]byte), &nisd)
	if err != nil {
		return nil, err
	}

	commitChgs := make([]funclib.CommitChg, 0)
	key := getConfKey(nisdCfgKey, nisd.NisdID)

	// Schema: /n/cfg/{nisdID}/{field} : {value}
	for _, field := range []string{DEVICE_NAME, CLIENT_PORT, PEER_PORT, HV_ID, FAILURE_DOMAIN, IP_ADDR, TOTAL_SPACE, AVAIL_SPACE} {
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
		case TOTAL_SPACE:
			value = strconv.Itoa(int(nisd.TotalSize))
		case AVAIL_SPACE:
			value = strconv.Itoa(int(nisd.AvailableSize))
		case DEVICE_NAME:
			value = nisd.DevID
		default:
			continue
		}
		commitChgs = append(commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", key, field)),
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

	key := getConfKey(deviceCfgKey, dev.DevID)
	readResult, _, _, _, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}

	for _, field := range []string{NISD_ID, SERIAL_NUM, STATUS, HV_ID, FAILURE_DOMAIN} {
		k := fmt.Sprintf("%s/%s", key, field)
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

	k := getConfKey(deviceCfgKey, dev.DevID)
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
			Key:   []byte(fmt.Sprintf("%s/%s", k, field)),
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

// Allocates Nisd to the Requested VDEV
func allocateNisd(vdev *ctlplfl.Vdev, nisds map[string]*ctlplfl.Nisd) []*ctlplfl.Nisd {
	allocatedNisd := make([]*ctlplfl.Nisd, 0)
	remainingVdevSize := vdev.Size
	vdev.NisdToChkMap = make([]ctlplfl.NisdChunk, 0)
	for _, nisd := range nisds {
		if (nisd.AvailableSize > int64(vdev.Size)) && remainingVdevSize > 0 {
			nisdChunk := ctlplfl.NisdChunk{
				Nisd:  nisd,
				Chunk: make([]int, vdev.NumChunks),
			}
			allocatedNisd = append(allocatedNisd, nisd)
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
		key := fmt.Sprintf("/%s/%s/%s", nisdKey, nisd.NisdID, vdev.VdevID)
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
	err := ctlplfl.XMLDecode(args[0].([]byte), &vdev.Size)
	if err != nil {
		return nil, err
	}
	cbArgs := args[1].(*PumiceDBServer.PmdbCbArgs)
	log.Info("allocating vdev with size: ", vdev.Size)
	vdev.Init()
	nisdMap, err := getNisdList(cbArgs)
	if err != nil {
		log.Error("failed to get nisd list:", err)
		return nil, err
	}
	// allocate nisd chunks to vdev
	nisdList := allocateNisd(&vdev, nisdMap)
	if len(nisdList) == 0 {
		log.Error("failed to allocate nisd")
		return nil, fmt.Errorf("failed to allocate nisd: not enough space avialable")
	}

	genVdevKV(&vdev, nisdList, &commitChgs)
	genNisdKV(&vdev, nisdList, &commitChgs)

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
	commitChgs := make([]funclib.CommitChg, 0)

	commitChgs = append(commitChgs, funclib.CommitChg{
		Key: []byte(getConfKey(pduKey, pdu.ID)),
	})
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

	readResult, _, _, _, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	pduList := make([]ctlplfl.PDU, 0)
	for k, _ := range readResult {
		pduKey := strings.Split(k, "/")
		pduList = append(pduList, ctlplfl.PDU{
			ID: pduKey[PDU_UUID_PREFIX],
		})
	}

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
	commitChgs := make([]funclib.CommitChg, 0)

	commitChgs = append(commitChgs, funclib.CommitChg{
		Key: []byte(getConfKey(rackKey, rack.ID)),
	},
		funclib.CommitChg{
			Key: []byte(fmt.Sprintf("%s/%s/%s/%s", getConfKey(rackKey, rack.ID), parentInfo, pduKey, rack.PDUId)),
		})
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}

	return encode(funcIntrm)
}

func parseRackList(readResult map[string][]byte) []ctlplfl.Rack {
	rackMap := make(map[string]*ctlplfl.Rack)

	for k := range readResult {
		parts := strings.Split(strings.Trim(k, "/"), "/")
		if len(parts) < 2 || parts[0] != rackKey {
			continue
		}
		rackId := parts[PDU_UUID_PREFIX]
		rack, ok := rackMap[rackId]
		if !ok {
			rack = &ctlplfl.Rack{ID: rackId}
			rackMap[rackId] = rack
		}

		if len(parts) == 5 && parts[2] == parentInfo && parts[3] == pduKey {
			rack.PDUId = parts[4]
		}
	}

	rackList := make([]ctlplfl.Rack, 0, len(rackMap))
	for _, r := range rackMap {
		rackList = append(rackList, *r)
	}
	return rackList
}

// TODO: Merge with parseRackList
func parseHyperVisorList(readResult map[string][]byte) []ctlplfl.Hypervisor {
	hvMap := make(map[string]*ctlplfl.Hypervisor)

	for k := range readResult {
		parts := strings.Split(strings.Trim(k, "/"), "/")
		if len(parts) < 2 || parts[0] != hvKey {
			continue
		}
		hvID := parts[PDU_UUID_PREFIX]
		hv, ok := hvMap[hvID]
		if !ok {
			hv = &ctlplfl.Hypervisor{ID: hvID}
			hvMap[hvID] = hv
		}

		if len(parts) == 5 && parts[2] == parentInfo && parts[3] == pduKey {
			hv.RackID = parts[4]
		} else if len(parts) == 4 && parts[2] == IP_ADDR {
			hv.IPAddress = parts[3]
		}
	}

	hvList := make([]ctlplfl.Hypervisor, 0, len(hvMap))
	for _, hv := range hvMap {
		hvList = append(hvList, *hv)
	}
	return hvList
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
	readResult, _, _, _, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	rackList := parseRackList(readResult)

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
	commitChgs := make([]funclib.CommitChg, 0)

	commitChgs = append(commitChgs, funclib.CommitChg{
		Key: []byte(getConfKey(hvKey, hv.ID)),
	},
		funclib.CommitChg{
			Key: []byte(fmt.Sprintf("%s/%s/%s/%s", getConfKey(hvKey, hv.ID), parentInfo, rackKey, hv.RackID)),
		}, funclib.CommitChg{
			Key: []byte(fmt.Sprintf("%s/%s/%s", getConfKey(hvKey, hv.ID), IP_ADDR, hv.IPAddress)),
		},
	)
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

	readResult, _, _, _, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	hvList := parseHyperVisorList(readResult)

	response, err := ctlplfl.XMLEncode(hvList)
	if err != nil {
		log.Error("failed to encode hypervisor info:", err)
		return nil, fmt.Errorf("failed to encode hypervisor info: %v", err)
	}

	return response, nil
}
