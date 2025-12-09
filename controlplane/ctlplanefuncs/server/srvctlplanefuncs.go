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
	"encoding/binary"
	"path"
	"strconv"
	"strings"
	"unsafe"
)

var colmfamily string

const (
	DEVICE_ID      = "d"
	NISD_ID        = "n"
	SERIAL_NUM     = "sn"
	STATE          = "s"
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
	LOCATION       = "l"
	POWER_CAP      = "pw"
	SPEC           = "sp"
	NAME           = "nm"
	PORT_RANGE     = "pr"
	SSH_PORT       = "ssh"
	DEVICE_PATH    = "dp"
	PARTITION_PATH = "ptp"

	DEFRAG                  = "dfg"
	MBCCnt                  = "mbc"
	MergeHCnt               = "mhc"
	MCIReadCache            = "mrc"
	S3                      = "s3"
	DSYNC                   = "ds"
	ALLOW_DEFRAG_MCIB_CACHE = "admc"

	cfgkey       = "cfg"
	nisdCfgKey   = "n_cfg"
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
	argsKey      = "na"

	ENC_TYPE = pmCmn.GOB
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
		log.Info("Applying change: ", string(chg.Key), " -> ", string(chg.Value))
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

func ApplyNisd(args ...interface{}) (interface{}, error) {
	cbargs := args[1].(*PumiceDBServer.PmdbCbArgs)
	nisd := args[0].(ctlplfl.Nisd)
	var intrm funclib.FuncIntrm
	buf := C.GoBytes(cbargs.AppData, C.int(cbargs.AppDataSize))
	err := pmCmn.Decoder(pmCmn.GOB, buf, &intrm)
	if err != nil {
		log.Error("Failed to decode the apply changes: ", err)
		return nil, fmt.Errorf("failed to decode apply changes: %v", err)
	}

	applyKV(intrm.Changes, cbargs)

	HR.AddNisd(&nisd)

	return intrm.Response, nil
}

// TODO: This method needs to be tested
func ReadAllNisdConfigs(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)
	log.Trace("fetching nisd details for key : ", nisdCfgKey)
	readResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, nisdCfgKey, int64(len(nisdCfgKey)), nisdCfgKey, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	nisdList := ParseEntities[ctlplfl.Nisd](readResult.ResultMap, NisdParser{})
	return pmCmn.Encoder(pmCmn.GOB, nisdList)
}

func ReadNisdConfig(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)

	req := args[1].(ctlplfl.GetReq)
	err := req.ValidateRequest()
	if err != nil {
		return nil, err
	}
	key := getConfKey(nisdCfgKey, req.ID)
	log.Trace("fetching nisd details for key : ", key)
	readResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	nisdList := ParseEntities[ctlplfl.Nisd](readResult.ResultMap, NisdParser{})
	return pmCmn.Encoder(pmCmn.GOB, nisdList[0])
}

func getNisdList(cbArgs *PumiceDBServer.PmdbCbArgs) ([]ctlplfl.Nisd, error) {
	readResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, nisdCfgKey, int64(len(nisdCfgKey)), nisdCfgKey, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	nisdList := ParseEntities[ctlplfl.Nisd](readResult.ResultMap, NisdParser{})
	return nisdList, nil
}

func WPNisdCfg(args ...interface{}) (interface{}, error) {
	nisd := args[0].(ctlplfl.Nisd)
	err := nisd.Validate()
	if err != nil {
		log.Error("failed to validate nisd: ", err)
		return nil, err
	}
	commitChgs := PopulateEntities[*ctlplfl.Nisd](&nisd, nisdPopulator{}, nisdCfgKey)

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
	deviceList := ParseEntities[ctlplfl.Device](readResult.ResultMap, deviceWithPartitionParser{})
	return pmCmn.Encoder(pmCmn.GOB, deviceList)
}

func WPDeviceInfo(args ...interface{}) (interface{}, error) {
	dev := args[0].(ctlplfl.Device)

	nisdResponse := ctlplfl.ResponseXML{
		Name:    dev.ID,
		Success: true,
	}

	r, err := pmCmn.Encoder(pmCmn.GOB, nisdResponse)
	if err != nil {
		log.Error("Failed to marshal nisd response: ", err)
		return nil, fmt.Errorf("failed to marshal nisd response: %v", err)
	}
	commitChgs := PopulateEntities[*ctlplfl.Device](&dev, devicePopulator{}, deviceCfgKey)
	for _, pt := range dev.Partitions {
		ptCommits := PopulateEntities[*ctlplfl.DevicePartition](&pt, partitionPopulator{}, fmt.Sprintf("%s/%s/%s", deviceCfgKey, dev.ID, ptKey))
		commitChgs = append(commitChgs, ptCommits...)
	}

	//Fill in FuncIntrm structure
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}
	return pmCmn.Encoder(pmCmn.GOB, funcIntrm)
}

// Generates all the Keys and Values that needs to be inserted into VDEV key space on vdev generation
func genAllocationKV(ID, chunk string, nisd *ctlplfl.Nisd, i int, commitChgs *[]funclib.CommitChg) {
	vcKey := getVdevChunkKey(ID)
	nKey := fmt.Sprintf("%s/%s/%s", nisdKey, nisd.ID, ID)

	// TODO: handle EC blocks
	*commitChgs = append(*commitChgs, funclib.CommitChg{
		Key:   []byte(fmt.Sprintf("%s/%s/R.%d", vcKey, chunk, i)),
		Value: []byte(nisd.ID),
	})
	// TODO: how do we update the replication details
	*commitChgs = append(*commitChgs, funclib.CommitChg{
		Key:   []byte(nKey),
		Value: []byte(fmt.Sprintf("R.%d.%s", i, chunk)),
	})
	*commitChgs = append(*commitChgs, funclib.CommitChg{
		Key:   []byte(fmt.Sprintf("%s/%s", getConfKey(nisdCfgKey, nisd.ID), AVAIL_SPACE)),
		Value: []byte(strconv.Itoa(int(nisd.AvailableSize))),
	})

}

// Initialize the VDEV during the write preparation stage.
// Since the VDEV ID is derived from a randomly generated UUID, It needs to be generated within the Write Prep Phase.
func WPCreateVdev(args ...interface{}) (interface{}, error) {
	var vdev ctlplfl.Vdev
	commitChgs := make([]funclib.CommitChg, 0)
	// Decode the input buffer into structure format
	vdev = args[0].(ctlplfl.Vdev)
	vdev.Init()
	log.Debug("Initializing vdev with size: ", vdev)
	key := getConfKey(vdevKey, vdev.Cfg.ID)
	for _, field := range []string{SIZE, NUM_CHUNKS, NUM_REPLICAS} {
		var value string
		switch field {
		case SIZE:
			value = strconv.Itoa(int(vdev.Cfg.Size))
		case NUM_CHUNKS:
			value = strconv.Itoa(int(vdev.Cfg.NumChunks))
		case NUM_REPLICAS:
			value = strconv.Itoa(int(vdev.Cfg.NumReplica))
		default:
			continue
		}
		commitChgs = append(commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s/%s", key, cfgkey, field)),
			Value: []byte(value),
		})
	}
	r, err := pmCmn.Encoder(pmCmn.GOB, vdev)
	if err != nil {
		log.Error("Failed to marshal vdev response: ", err)
		return nil, fmt.Errorf("failed to marshal nisd response: %v", err)
	}
	//Fill in FuncIntrm structure
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}
	return pmCmn.Encoder(pmCmn.GOB, funcIntrm)
}

func allocateNisdPerChunk(vdev *ctlplfl.VdevCfg, fd int, chunk string, commitChgs *[]funclib.CommitChg) error {
	hash := ctlplfl.Hash64([]byte(vdev.ID + chunk))

	log.Debugf("hash generated for chunk: %d", hash)
	// select entity by index
	// TODO: handle scenario when there is only one element
	entityIDX, err := GetIndex(hash, HR.FD[fd].Tree.Len())
	if err != nil {
		return err
	}
	log.Debugf("selecting from entity: %d from %d", entityIDX, HR.FD[fd].Tree.Len())
	for i := 0; i <= int(vdev.NumReplica); i++ {
		if entityIDX >= HR.FD[fd].Tree.Len() {
			entityIDX = 0
		}
		nisd, err := HR.PickNISD(fd, entityIDX, hash)
		if err != nil {
			return err
		}
		log.Infof("picked nisd: %+v", nisd.ID)
		nisd.AvailableSize -= ctlplfl.CHUNK_SIZE
		if nisd.AvailableSize < ctlplfl.CHUNK_SIZE {
			HR.DeleteNisd(nisd)
			log.Debug("Deleting NISD: ", nisd.ID)
		}

		// TODO: Don't upate the same nisd-space multiple times
		genAllocationKV(vdev.ID, chunk, nisd, i, commitChgs)
		// update hash
		hashByte := make([]byte, 8)
		binary.BigEndian.PutUint64(hashByte, hash)
		hash = ctlplfl.Hash64(hashByte)
		entityIDX += 1
	}
	return nil
}

func allocateNisdPerVdev(vdev *ctlplfl.VdevCfg, commitCh *[]funclib.CommitChg) error {
	fd := HR.GetFDLevel(int(vdev.NumReplica))
	for i := 0; i <= int(vdev.NumChunks); i++ {
		log.Trace("allocating nisd for chunk: ", i)
		err := allocateNisdPerChunk(vdev, fd, strconv.Itoa(i), commitCh)
		if err != nil {
			log.Error("failed to allocate nisd:", err)
			return err
		}
	}
	return nil
}

// Creates a VDEV, allocates the NISD and updates the PMDB with new data
func APCreateVdev(args ...interface{}) (interface{}, error) {
	var vdev ctlplfl.Vdev
	var funcIntrm funclib.FuncIntrm
	cbArgs := args[1].(*PumiceDBServer.PmdbCbArgs)
	fnI := unsafe.Slice((*byte)(cbArgs.AppData), int(cbArgs.AppDataSize))
	pmCmn.Decoder(pmCmn.GOB, fnI, &funcIntrm)
	pmCmn.Decoder(pmCmn.GOB, funcIntrm.Response, &vdev)
	log.Debug("Allocating vdev for ID: ", vdev.Cfg.ID)
	// allocate nisd chunks to vdev
	err := allocateNisdPerVdev(&vdev.Cfg, &funcIntrm.Changes)
	if err != nil {
		log.Error("Failed to Allocate NISD: ", err)
		return nil, err
	}

	r, err := pmCmn.Encoder(pmCmn.GOB, vdev)
	if err != nil {
		log.Error("Failed to marshal vdev response: ", err)
		return nil, fmt.Errorf("failed to marshal nisd response: %v", err)
	}

	applyKV(funcIntrm.Changes, cbArgs)

	return r, nil
}

func WPCreatePartition(args ...interface{}) (interface{}, error) {
	pt := args[0].(ctlplfl.DevicePartition)
	resp := &ctlplfl.ResponseXML{
		Name:    pt.PartitionID,
		Success: true,
	}
	r, err := pmCmn.Encoder(pmCmn.GOB, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to encode pt response: %v", err)
	}
	commitChgs := PopulateEntities[*ctlplfl.DevicePartition](&pt, partitionPopulator{}, ptKey)
	devPTCommits := PopulateEntities[*ctlplfl.DevicePartition](&pt, partitionPopulator{}, fmt.Sprintf("%s/%s/%s", deviceCfgKey, pt.DevID, ptKey))
	commitChgs = append(commitChgs, devPTCommits...)
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
	commitChgs := PopulateEntities[*ctlplfl.PDU](&pdu, pduPopulator{}, pduKey)
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
		log.Error("Range read failure: ", err)
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
	commitChgs := PopulateEntities[*ctlplfl.Rack](&rack, rackPopulator{}, rackKey)
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
	commitChgs := PopulateEntities[*ctlplfl.Hypervisor](&hv, hvPopulator{}, hvKey)
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

func ReadVdevsInfoWithChunkMapping(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)
	req := args[1].(ctlplfl.GetReq)

	key := vdevKey
	if !req.GetAll {
		key = getConfKey(vdevKey, req.ID)
	}

	nisdResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, nisdCfgKey, int64(len(nisdCfgKey)), nisdCfgKey, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure: ", err)
		return nil, err
	}
	// ParseEntitiesMap now returns map[string]Entity
	nisdEntityMap := ParseEntitiesMap(nisdResult.ResultMap, NisdParser{})

	readResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, key, int64(len(key)), key, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure: ", err)
		return nil, err
	}

	vdevMap := make(map[string]*ctlplfl.Vdev)
	// top-level map: vdevID -> (nisdID -> *NisdChunk)
	vdevNisdChunkMap := make(map[string]map[string]*ctlplfl.NisdChunk)

	for k, value := range readResult.ResultMap {
		parts := strings.Split(strings.Trim(k, "/"), "/")
		vdevID := parts[BASE_UUID_PREFIX]
		// expect something like: /<root>/<vdevID>/c/<chunkIndex> -> <nisdID>
		if _, ok := vdevMap[vdevID]; !ok {
			vdevMap[vdevID] = &ctlplfl.Vdev{Cfg: ctlplfl.VdevCfg{
				ID: vdevID}}
		}
		vdev := vdevMap[vdevID]
		if parts[VDEV_CFG_C_KEY] == cfgkey {
			switch parts[VDEV_ELEMENT_KEY] {
			case SIZE:
				if sz, err := strconv.ParseInt(string(value), 10, 64); err == nil {
					vdev.Cfg.Size = sz
				}
			case NUM_CHUNKS:
				if nc, err := strconv.ParseUint(string(value), 10, 32); err == nil {
					vdev.Cfg.NumChunks = uint32(nc)
				}
			case NUM_REPLICAS:
				if nr, err := strconv.ParseUint(string(value), 10, 8); err == nil {
					vdev.Cfg.NumReplica = uint8(nr)
				}

			}

		} else if parts[VDEV_CFG_C_KEY] == chunkKey {

			nisdID := string(value)

			// ensure per-vdev map exists
			if _, ok := vdevNisdChunkMap[vdevID]; !ok {
				vdevNisdChunkMap[vdevID] = make(map[string]*ctlplfl.NisdChunk)
			}
			perVdevMap := vdevNisdChunkMap[vdevID]

			// if chunk index is stored in parts[3] (original code used parts[3])
			chunkIdx := -1
			if idx, err := strconv.Atoi(parts[3]); err == nil {
				chunkIdx = idx
			}

			// create nisd chunk entry for this vdev if not present
			if _, ok := perVdevMap[nisdID]; !ok {
				// lookup nisd entity from parsed nisd map
				if ent, ok := nisdEntityMap[nisdID]; ok {
					// ent is Entity (interface) created by nisdParser (pointer to ctlplfl.Nisd)
					if nisdPtr, ok := ent.(*ctlplfl.Nisd); ok {
						perVdevMap[nisdID] = &ctlplfl.NisdChunk{
							Nisd:  *nisdPtr,
							Chunk: make([]int, 0),
						}
					} else {
						// fallback: create minimal Nisd with ID only
						perVdevMap[nisdID] = &ctlplfl.NisdChunk{
							Nisd:  ctlplfl.Nisd{ID: nisdID},
							Chunk: make([]int, 0),
						}
					}
				} else {
					// fallback: create minimal Nisd with ID only
					perVdevMap[nisdID] = &ctlplfl.NisdChunk{
						Nisd:  ctlplfl.Nisd{ID: nisdID},
						Chunk: make([]int, 0),
					}
				}
			}
			if chunkIdx >= 0 {
				perVdevMap[nisdID].Chunk = append(perVdevMap[nisdID].Chunk, chunkIdx)
			}
		}
	}

	vdevList := make([]*ctlplfl.Vdev, 0, len(vdevMap))
	// attach only the nisd chunks that belong to each vdev
	for vid, v := range vdevMap {
		if perVdevMap, ok := vdevNisdChunkMap[vid]; ok {
			for _, nc := range perVdevMap {
				v.NisdToChkMap = append(v.NisdToChkMap, *nc)
			}
		}
		vdevList = append(vdevList, v)
	}

	return pmCmn.Encoder(ENC_TYPE, vdevList)
}

func ReadVdevInfo(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)
	req := args[1].(ctlplfl.GetReq)

	err := req.ValidateRequest()
	if err != nil {
		log.Error("failed to validate request:", err)
		return nil, err
	}
	vKey := getConfKey(vdevKey, req.ID)
	rqResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, vKey, int64(len(vKey)), vKey, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("RangeReadKV failure: ", err)
		return nil, err
	}
	vdevInfo := ctlplfl.VdevCfg{
		ID: req.ID,
	}

	// TODO: move this to parsing file
	for k, v := range rqResult.ResultMap {
		parts := strings.Split(strings.Trim(k, "/"), "/")
		if parts[BASE_UUID_PREFIX] != vdevInfo.ID {
			continue
		}
		switch parts[VDEV_CFG_C_KEY] {
		case cfgkey:
			switch parts[VDEV_ELEMENT_KEY] {
			case SIZE:
				if sz, err := strconv.ParseInt(string(v), 10, 64); err == nil {
					vdevInfo.Size = sz
				}
			case NUM_CHUNKS:
				if nc, err := strconv.ParseUint(string(v), 10, 32); err == nil {
					vdevInfo.NumChunks = uint32(nc)
				}
			case NUM_REPLICAS:
				if nr, err := strconv.ParseUint(string(v), 10, 8); err == nil {
					vdevInfo.NumReplica = uint8(nr)
				}
			}
		}

	}
	return pmCmn.Encoder(ENC_TYPE, vdevInfo)
}

func ReadChunkNisd(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)
	req := args[1].(ctlplfl.GetReq)

	err := req.ValidateRequest()
	if err != nil {
		log.Error("failed to validate request:", err)
		return nil, err
	}
	keys := strings.Split(strings.Trim(req.ID, "/"), "/")
	vdevID, chunk := keys[0], keys[1]

	vcKey := path.Clean(getConfKey(vdevKey, path.Join(vdevID, chunkKey, chunk))) + "/"

	log.Info("searching for key:", vcKey)

	rqResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, vcKey, int64(len(vcKey)), vcKey, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("RangeReadKV failure: ", err)
		return nil, err
	}

	var ids []string
	for _, v := range rqResult.ResultMap {
		ids = append(ids, string(v))
	}
	chunkInfo := ctlplfl.ChunkNisd{
		NisdUUIDs:   strings.Join(ids, ","),
		NumReplicas: uint8(len(rqResult.ResultMap)) - 1,
	}

	return pmCmn.Encoder(ENC_TYPE, chunkInfo)

}

func WPNisdArgs(args ...interface{}) (interface{}, error) {
	nArgs := args[0].(ctlplfl.NisdArgs)
	resp := &ctlplfl.ResponseXML{
		Name:    "nisd-args",
		Success: true,
	}
	r, err := pmCmn.Encoder(pmCmn.GOB, resp)
	if err != nil {
		log.Error("Failed to marshal nisd args response: ", err)
		return nil, fmt.Errorf("failed to marshal nisd args response: %v", err)
	}
	commitChgs := PopulateEntities[*ctlplfl.NisdArgs](&nArgs, nisdArgsPopulator{}, argsKey)
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: r,
	}

	return pmCmn.Encoder(pmCmn.GOB, funcIntrm)
}

func RdNisdArgs(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)
	readResult, err := PumiceDBServer.RangeReadKV(cbArgs.UserID, argsKey, int64(len(argsKey)), argsKey, cbArgs.ReplySize, false, 0, colmfamily)
	if err != nil {
		log.Error("Range read failure: ", err)
		return nil, err
	}
	var nisdArgs ctlplfl.NisdArgs
	for k, v := range readResult.ResultMap {
		parts := strings.Split(strings.Trim(k, "/"), "/")
		if len(parts) < 2 {
			continue
		}
		switch parts[BASE_UUID_PREFIX] {
		case DEFRAG:
			nisdArgs.Defrag, _ = strconv.ParseBool(string(v))
		case MBCCnt:
			nisdArgs.MBCCnt, _ = strconv.Atoi(string(v))
		case MergeHCnt:
			nisdArgs.MergeHCnt, _ = strconv.Atoi(string(v))
		case MCIReadCache:
			nisdArgs.MCIBReadCache, _ = strconv.Atoi(string(v))
		case DSYNC:
			nisdArgs.DSync = string(v)
		case S3:
			nisdArgs.S3 = string(v)
		case ALLOW_DEFRAG_MCIB_CACHE:
			nisdArgs.AllowDefragMCIBCache, _ = strconv.ParseBool(string(v))
		}
	}
	return pmCmn.Encoder(ENC_TYPE, nisdArgs)

}
