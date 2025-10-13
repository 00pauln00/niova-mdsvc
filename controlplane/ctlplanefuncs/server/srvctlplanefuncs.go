package srvctlplanefuncs

import (
	"C"
	"fmt"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	PumiceDBServer "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
	log "github.com/sirupsen/logrus"
)
import (
	"strconv"
	"strings"
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
	ptKey        = "pt"
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

	Snap := args[1].(ctlplfl.SnapXML)

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

	Snap := args[1].(ctlplfl.SnapXML)

	key := fmt.Sprintf("%s/snap", Snap.Vdev)
	readResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
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

	return pmCmn.Encoder(pmCmn.GOB, Snap)
}

func WritePrepCreateSnap(args ...interface{}) (interface{}, error) {

	Snap := args[0].(ctlplfl.SnapXML)

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

	r, err := pmCmn.Encoder(pmCmn.GOB, snapResponse)
	if err != nil {
		log.Error("Failed to marshal snapshot response: ", err)
		return nil, fmt.Errorf("failed to marshal snapshot response: %v", err)
	}

	//Fill in FuncIntrm structure
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}
	return pmCmn.Encoder(pmCmn.GOB, funcIntrm)
}

func applyKV(chgs []funclib.CommitChg, cbargs *PumiceDBServer.PmdbCbArgs) error {
	for _, chg := range chgs {
		log.Trace("Applying change: ", string(chg.Key), " -> ", string(chg.Value))
		rc := PumiceDBServer.PmdbWriteKV(cbargs.UserID, cbargs.PmdbHandler,
			string(chg.Key),
			int64(len(chg.Key)), string(chg.Value),
			int64(len(chg.Value)), colmfamily)

		if rc < 0 {
			log.Fatal("Failed to apply changes for key: ", string(chg.Key))
			return fmt.Errorf("failed to apply changes for key: %s", string(chg.Key))
		}
	}
	return nil
}

func ApplyFunc(args ...interface{}) (interface{}, error) {
	cbargs := args[0].(*PumiceDBServer.PmdbCbArgs)

	var intrm funclib.FuncIntrm
	buf := C.GoBytes(cbargs.AppData, C.int(cbargs.AppDataSize))
	err := pmCmn.Decoder(pmCmn.GOB, buf, &intrm)
	if err != nil {
		log.Error("Failed to decode the apply changes: ", err)
		return nil, fmt.Errorf("failed to decode apply changes: %v", err)
	}

	applyKV(intrm.Changes, cbargs)

	return intrm.Response, nil
}

func RdNisdCfg(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)

	req := args[1].(ctlplfl.GetReq)

	key := getConfKey(nisdCfgKey, req.ID)
	log.Trace("fetching nisd details for key : ", key)
	readResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	nisdList := ParseEntities[ctlplfl.Nisd](readResult.ResultMap, nisdParser{})
	return pmCmn.Encoder(pmCmn.GOB, nisdList)
}

func getNisdList(cbArgs *PumiceDBServer.PmdbCbArgs) ([]ctlplfl.Nisd, error) {
	readResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, nisdCfgKey, int64(len(nisdCfgKey)), nisdCfgKey, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	nisdList := ParseEntities[ctlplfl.Nisd](readResult.ResultMap, nisdParser{})
	return nisdList, nil
}

func WPNisdCfg(args ...interface{}) (interface{}, error) {
	nisd := args[0].(ctlplfl.Nisd)
	commitChgs := PopulateEntities[*ctlplfl.Nisd](&nisd, nisdPopulator{})

	nisdResponse := ctlplfl.ResponseXML{
		Name:    nisd.ID,
		Success: true,
	}
	r, err := pmCmn.Encoder(pmCmn.GOB, nisdResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to encode nisd response: %v", err)
	}
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}
	return pmCmn.Encoder(pmCmn.GOB, funcIntrm)
}

func RdDeviceInfo(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)
	req := args[1].(ctlplfl.GetReq)
	key := getConfKey(deviceCfgKey, req.ID)
	readResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	deviceList := ParseEntities[ctlplfl.Device](readResult.ResultMap, deviceParser{})
	return pmCmn.Encoder(pmCmn.GOB, deviceList)
}

func WPDeviceInfo(args ...interface{}) (interface{}, error) {
	// Limited to update only
	// DevID, SerialNumber, Status
	// HypervisorID, FailureDomain (Parent Info)
	dev := args[0].(ctlplfl.Device)

	// TODO: use a common response struct for all the read functions
	nisdResponse := ctlplfl.ResponseXML{
		Name:    dev.ID,
		Success: true,
	}

	r, err := pmCmn.Encoder(pmCmn.GOB, nisdResponse)
	if err != nil {
		log.Error("Failed to marshal nisd response: ", err)
		return nil, fmt.Errorf("failed to marshal nisd response: %v", err)
	}
	commitChgs := PopulateEntities[*ctlplfl.Device](&dev, devicePopulator{})

	//Fill in FuncIntrm structure
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}
	return pmCmn.Encoder(pmCmn.GOB, funcIntrm)
}

// Allocates Nisd to the Requested VDEV
func allocateNisd(vdev *ctlplfl.Vdev, nisds []ctlplfl.Nisd) []*ctlplfl.Nisd {
	allocatedNisd := make([]*ctlplfl.Nisd, 0)
	remainingVdevSize := vdev.Size
	vdev.NisdToChkMap = make([]ctlplfl.NisdChunk, 0)
	for _, nisd := range nisds {
		if (nisd.AvailableSize > int64(vdev.Size)) && remainingVdevSize > 0 {
			nisdChunk := ctlplfl.NisdChunk{
				Nisd:  nisd,
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
				Value: []byte(nisd.ID),
			})
		}

	}
}

// Generates all the Keys and Values that needs to be inserted into NISD key space on vdev generation
func genNisdKV(vdev *ctlplfl.Vdev, nisdList []*ctlplfl.Nisd, commitChgs *[]funclib.CommitChg) {
	for _, nisd := range nisdList {
		key := fmt.Sprintf("%s/%s/%s", nisdKey, nisd.ID, vdev.VdevID)
		for i := 0; i < int(vdev.NumChunks); i++ {
			*commitChgs = append(*commitChgs, funclib.CommitChg{
				Key:   []byte(key),
				Value: []byte(fmt.Sprintf("R.0.%d", i)),
			})
		}
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", getConfKey(nisdCfgKey, nisd.ID), AVAIL_SPACE)),
			Value: []byte(strconv.Itoa(int(nisd.AvailableSize))),
		})

	}

}

// Creates a VDEV, allocates the NISD and updates the PMDB with new data
func APCreateVdev(args ...interface{}) (interface{}, error) {
	var vdev ctlplfl.Vdev
	commitChgs := make([]funclib.CommitChg, 0)
	// Decode the input buffer into structure format
	vdev.Size = args[0].(int64)
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

	r, err := pmCmn.Encoder(pmCmn.GOB, vdev)
	if err != nil {
		log.Error("Failed to marshal vdev response: ", err)
		return nil, fmt.Errorf("failed to marshal nisd response: %v", err)
	}

	applyKV(commitChgs, cbArgs)

	return r, nil
}

func WPCreatePartition(args ...interface{}) (interface{}, error) {
	pt := args[0].(ctlplfl.DevicePartition)
	resp := &ctlplfl.ResponseXML{
		Name:    pt.PartitionUUID,
		Success: true,
	}
	r, err := pmCmn.Encoder(pmCmn.GOB, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to encode pt response: %v", err)
	}
	commitChgs := PopulateEntities[*ctlplfl.DevicePartition](&pt, partitionPopulator{})
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}
	return pmCmn.Encoder(pmCmn.GOB, funcIntrm)
}

func ReadPartition(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)
	req := args[1].(ctlplfl.GetReq)
	key := ptKey
	if !req.GetAll {
		key = getConfKey(ptKey, req.ID)
	}
	readResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	pt := ParseEntities[ctlplfl.DevicePartition](readResult.ResultMap, ptParser{})
	return pmCmn.Encoder(pmCmn.GOB, pt)
}

func WPPDUCfg(args ...interface{}) (interface{}, error) {
	pdu := args[0].(ctlplfl.PDU)
	resp := &ctlplfl.ResponseXML{
		Name:    pdu.ID,
		Success: true,
	}
	r, err := pmCmn.Encoder(pmCmn.GOB, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to  encode pdu response: %v", err)
	}
	commitChgs := PopulateEntities[*ctlplfl.PDU](&pdu, pduPopulator{})
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}

	return pmCmn.Encoder(pmCmn.GOB, funcIntrm)
}

func ReadPDUCfg(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)
	req := args[1].(ctlplfl.GetReq)
	key := pduKey
	if !req.GetAll {
		key = getConfKey(pduKey, req.ID)
	}

	readResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	pduList := ParseEntities[ctlplfl.PDU](readResult.ResultMap, pduParser{})

	return pmCmn.Encoder(pmCmn.GOB, pduList)

}

func WPRackCfg(args ...interface{}) (interface{}, error) {

	rack := args[0].(ctlplfl.Rack)
	resp := &ctlplfl.ResponseXML{
		Name:    rack.ID,
		Success: true,
	}
	commitChgs := PopulateEntities[*ctlplfl.Rack](&rack, rackPopulator{})
	r, err := pmCmn.Encoder(pmCmn.GOB, resp)
	if err != nil {
		return nil, err
	}
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}

	return pmCmn.Encoder(pmCmn.GOB, funcIntrm)
}

func ReadRackCfg(args ...interface{}) (interface{}, error) {

	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)
	req := args[1].(ctlplfl.GetReq)
	key := rackKey
	if !req.GetAll {
		key = getConfKey(rackKey, req.ID)
	}
	readResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	rackList := ParseEntities[ctlplfl.Rack](readResult.ResultMap, rackParser{})
	response, err := pmCmn.Encoder(pmCmn.GOB, rackList)
	if err != nil {
		log.Error("failed to encode rack info:", err)
		return nil, fmt.Errorf("failed to encode rack info: %v", err)
	}
	return response, nil
}

func WPHyperVisorCfg(args ...interface{}) (interface{}, error) {
	// Decode the input buffer into structure format
	hv := args[0].(ctlplfl.Hypervisor)
	resp := &ctlplfl.ResponseXML{
		Name:    hv.ID,
		Success: true,
	}
	r, err := pmCmn.Encoder(pmCmn.GOB, resp)
	if err != nil {
		log.Error("Failed to marshal vdev response: ", err)
		return nil, fmt.Errorf("failed to marshal nisd response: %v", err)
	}
	commitChgs := PopulateEntities[*ctlplfl.Hypervisor](&hv, hvPopulator{})
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}

	return pmCmn.Encoder(pmCmn.GOB, funcIntrm)

}

func ReadHyperVisorCfg(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)
	req := args[1].(ctlplfl.GetReq)

	key := hvKey
	if !req.GetAll {
		key = getConfKey(hvKey, req.ID)
	}

	readResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}

	hvList := ParseEntities[ctlplfl.Hypervisor](readResult.ResultMap, hvParser{})

	return pmCmn.Encoder(pmCmn.GOB, hvList)
}
