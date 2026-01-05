package srvctlplanefuncs

import (
	"C"
	"encoding/binary"
	"fmt"
	"path"
	"strconv"
	"strings"
	"unsafe"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	PumiceDBServer "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
	"github.com/tidwall/btree"
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
	PORT           = "prt"
	DEVICE_PATH    = "dp"
	PARTITION_PATH = "ptp"
	NETWORK_INFO   = "ni"
	SOCKET_PATH    = "sck"
	ENABLE_RDMA    = "rdma"

	DEFRAG                  = "dfg"
	MBCCnt                  = "mbc"
	MergeHCnt               = "mhc"
	MCIReadCache            = "mrc"
	S3                      = "s3"
	DSYNC                   = "ds"
	ALLOW_DEFRAG_MCIB_CACHE = "admc"

	cfgkey       = "cfg"
	NisdCfgKey   = "n_cfg"
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

	cbargs := args[0].(*PumiceDBServer.PmdbCbArgs)
	Snap := args[1].(ctlplfl.SnapXML)

	//FIX: Arbitrary read size
	key := fmt.Sprintf("snap/%s", Snap.SnapName)
	log.Info("Key to be read : ", key)
	readResult, err := cbargs.PmdbReadKV(colmfamily, key)
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
	rrargs := PumiceDBServer.RangeReadArgs{
		ColFamily:  colmfamily,
		Key:        key,
		BufSize:    cbArgs.ReplySize,
		Consistent: false,
		Prefix:     key,
	}
	readResult, err := cbArgs.PmdbRangeRead(rrargs)
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
		rc := cbargs.PmdbWriteKV(colmfamily, string(chg.Key), string(chg.Value))
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

	err = applyKV(intrm.Changes, cbargs)
	if err != nil {
		log.Error("applyKV(): ", err)
		return nil, err
	}

	return intrm.Response, nil
}

func ApplyNisd(args ...interface{}) (interface{}, error) {
	cbargs, ok := args[1].(*PumiceDBServer.PmdbCbArgs)
	if !ok {
		err := fmt.Errorf("invalid argument: expecting type PmdbCbArgs")
		return nil, err
	}
	nisd, ok := args[0].(ctlplfl.Nisd)
	if !ok {
		err := fmt.Errorf("invalid argument: expecting type Nisd")
		return nil, err
	}
	var intrm funclib.FuncIntrm
	buf := C.GoBytes(cbargs.AppData, C.int(cbargs.AppDataSize))
	err := pmCmn.Decoder(pmCmn.GOB, buf, &intrm)
	if err != nil {
		log.Error("Failed to decode the apply changes: ", err)
		return nil, fmt.Errorf("failed to decode apply changes: %v", err)
	}

	err = applyKV(intrm.Changes, cbargs)
	if err != nil {
		log.Error("applyKV(): ", err)
		return nil, err
	}

	err = HR.AddNisd(&nisd)
	if err != nil {
		log.Error("AddNisd()", err)
	}

	return intrm.Response, nil
}

// TODO: This method needs to be tested
func ReadAllNisdConfigs(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)
	log.Trace("fetching nisd details for key : ", NisdCfgKey)
	rrargs := PumiceDBServer.RangeReadArgs{
		ColFamily:  colmfamily,
		Key:        NisdCfgKey,
		BufSize:    cbArgs.ReplySize,
		Consistent: false,
		Prefix:     NisdCfgKey,
	}
	readResult, err := cbArgs.PmdbRangeRead(rrargs)
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
	key := getConfKey(NisdCfgKey, req.ID)
	log.Trace("fetching nisd details for key : ", key)
	rrargs := PumiceDBServer.RangeReadArgs{
		ColFamily:  colmfamily,
		Key:        key,
		BufSize:    cbArgs.ReplySize,
		Consistent: false,
		Prefix:     key,
	}
	readResult, err := cbArgs.PmdbRangeRead(rrargs)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	nisdList := ParseEntities[ctlplfl.Nisd](readResult.ResultMap, NisdParser{})
	return pmCmn.Encoder(pmCmn.GOB, nisdList[0])
}

func getNisdList(cbArgs *PumiceDBServer.PmdbCbArgs) ([]ctlplfl.Nisd, error) {
	readResult, err := cbArgs.PmdbRangeRead(PumiceDBServer.RangeReadArgs{
		ColFamily:  colmfamily,
		Key:        NisdCfgKey,
		BufSize:    cbArgs.ReplySize,
		Consistent: false,
		Prefix:     NisdCfgKey,
	})
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
	commitChgs := PopulateEntities[*ctlplfl.Nisd](&nisd, nisdPopulator{}, NisdCfgKey)

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
	readResult, err := cbArgs.PmdbRangeRead(PumiceDBServer.RangeReadArgs{
		ColFamily:  colmfamily,
		Key:        key,
		BufSize:    cbArgs.ReplySize,
		Consistent: false,
		Prefix:     key,
	})
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
func genAllocationKV(ID, chunk string, nisd *ctlplfl.NisdVdevAlloc, i int, commitChgs *[]funclib.CommitChg) {
	vcKey := getVdevChunkKey(ID)
	nKey := fmt.Sprintf("%s/%s/%s", nisdKey, nisd.Ptr.ID, ID)

	// TODO: handle EC blocks
	*commitChgs = append(*commitChgs, funclib.CommitChg{
		Key:   []byte(fmt.Sprintf("%s/%s/R.%d", vcKey, chunk, i)),
		Value: []byte(nisd.Ptr.ID),
	})
	*commitChgs = append(*commitChgs, funclib.CommitChg{
		Key:   []byte(nKey),
		Value: []byte(fmt.Sprintf("R.%d.%s", i, chunk)),
	})
	log.Debugf("generated kv updates for chunk %s/R.%d", chunk, i)
}

// Initialize the VDEV during the write preparation stage.
// Since the VDEV ID is derived from a randomly generated UUID,
// It needs to be generated within the Write Prep Phase.
func WPCreateVdev(args ...interface{}) (interface{}, error) {
	commitChgs := make([]funclib.CommitChg, 0)
	// Decode the input buffer into structure format
	vdev, ok := args[0].(ctlplfl.Vdev)
	if !ok {
		err := fmt.Errorf("invalid argument: expecting type vdev")
		return nil, err
	}
	err := vdev.Init()
	if err != nil {
		log.Errorf("failed to initialize vdev: %v", err)
		return nil, err
	}
	log.Infof("initializing vdev: %+v", vdev)
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

func allocateNisdPerChunk(vdev *ctlplfl.VdevCfg, fd int, chunk string, commitChgs *[]funclib.CommitChg,
	nisdMap *btree.Map[string, *ctlplfl.NisdVdevAlloc]) error {

	if int(fd) >= ctlplfl.FD_MAX {
		err := fmt.Errorf("invalid failure domain: %d", fd)
		log.Error(err)
		return err
	}

	treeLen := HR.FD[fd].Tree.Len()
	if treeLen == 0 {
		return fmt.Errorf("no entities available in failure domain %s", fd)
	}

	hash := ctlplfl.NisdAllocHash([]byte(vdev.ID + chunk))

	log.Debugf("hash generated %d for chunk: %s, fd: %d", hash, chunk, fd)

	// select entity by index
	entityIDX, err := GetIdxForNisdAlloc(hash, treeLen)
	if err != nil {
		log.Error("failed to get entity: ", err)
		return err
	}

	log.Debugf("selecting nisd from chunk %s, fd:%d, entity: %d/%d", chunk, fd, entityIDX, treeLen)

	// track NISDs already selected for this allocation
	picked := make(map[string]struct{})

	// chunk's replica's are stored in different NISD's from different entities
	for i := 0; i < int(vdev.NumReplica); i++ {
		var (
			nisd *ctlplfl.NisdVdevAlloc
			err  error
		)

		attempts := 0

		for attempts < treeLen {
			nisd, err = HR.PickNISD(fd, entityIDX, hash, picked, nisdMap)
			if err == nil {
				break
			}
			log.Warnf(
				"pick failed for chunk=%s replica=%d entityIDX=%d attempt=%d/%d err=%v", chunk,
				i, entityIDX, attempts, treeLen, err,
			)
			entityIDX = (entityIDX + 1) % treeLen
			attempts++

		}

		if err != nil {
			return fmt.Errorf(
				"failed to allocate replica %d after trying %d entities",
				i, treeLen,
			)
		}

		log.Debugf("picked nisd: %s, for chunk %s/R.%d", nisd.Ptr.ID, chunk, i)
		genAllocationKV(vdev.ID, chunk, nisd, i, commitChgs)

		// advance base index only after success
		entityIDX = (entityIDX + 1) % treeLen

		// decorrelate next replica
		var buf [ctlplfl.HASH_SIZE]byte
		binary.BigEndian.PutUint64(buf[:], hash)
		hash = ctlplfl.NisdAllocHash(buf[:])
	}
	return nil
}

func allocateNisdPerVdev(vdev *ctlplfl.VdevCfg, fd int, nisdMap *btree.Map[string, *ctlplfl.NisdVdevAlloc]) ([]funclib.CommitChg, error) {
	commitCh := make([]funclib.CommitChg, 0)
	for i := 0; i < int(vdev.NumChunks); i++ {
		log.Debugf("allocating nisd for chunk: %d, from fd: %d ", i, fd)
		err := allocateNisdPerChunk(vdev, fd, strconv.Itoa(i), &commitCh, nisdMap)
		if err != nil {
			err = fmt.Errorf("failed to allocate nisd from fd: %d, %v", fd, err)
			log.Error(err)
			return nil, err
		}
	}
	return commitCh, nil
}

// Creates a VDEV, allocates the NISD and updates the PMDB with new data
func APCreateVdev(args ...interface{}) (interface{}, error) {
	var vdev ctlplfl.Vdev
	var funcIntrm funclib.FuncIntrm
	resp := ctlplfl.ResponseXML{
		Name:    "vdev",
		Success: false,
	}
	cbArgs, ok := args[1].(*PumiceDBServer.PmdbCbArgs)
	if !ok {
		err := fmt.Errorf("invalid argument: expecting type PmdbCbArgs")
		resp.Error = err.Error()
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}
	fnI := unsafe.Slice((*byte)(cbArgs.AppData), int(cbArgs.AppDataSize))
	pmCmn.Decoder(pmCmn.GOB, fnI, &funcIntrm)
	pmCmn.Decoder(pmCmn.GOB, funcIntrm.Response, &vdev)
	log.Infof("allocating vdev for ID: %s", vdev.Cfg.ID)
	nisdMap := btree.NewMap[string, *ctlplfl.NisdVdevAlloc](32)
	HR.Dump()
	// allocate nisd chunks to vdev
	fd, err := HR.GetFDLevel(int(vdev.Cfg.NumReplica))
	if err != nil {
		log.Error("failed to get fd:", err)
		resp.Error = fmt.Sprintf("failed to get fd:", err)
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	for i := fd; i < ctlplfl.FD_MAX; i++ {
		log.Debugf("selected fd %d, for vdev ID: %s & ft: %d.", i, vdev.Cfg.ID, vdev.Cfg.NumReplica)
		commitCh, err := allocateNisdPerVdev(&vdev.Cfg, i, nisdMap)
		if err != nil {
			log.Error("allocateNisdPerVdev():failed to allocate nisd -> inc fd in next itr: ", err)
			resp.Error = fmt.Sprintf("failed to allocate nisd: %v", err)
			nisdMap.Clear()
			continue
		}
		resp.Success = true
		funcIntrm.Changes = append(funcIntrm.Changes, commitCh...)
		break

	}

	if resp.Success {
		nisdMap.Ascend("", func(k string, v *ctlplfl.NisdVdevAlloc) bool {
			funcIntrm.Changes = append(funcIntrm.Changes, funclib.CommitChg{
				Key:   []byte(fmt.Sprintf("%s/%s", getConfKey(NisdCfgKey, v.Ptr.ID), AVAIL_SPACE)),
				Value: []byte(strconv.Itoa(int(v.AvailableSize))),
			})
			return true
		})
		err = applyKV(funcIntrm.Changes, cbArgs)
		if err != nil {
			log.Error("applyKV(): ", err)
			return nil, err
		}
		nisdMap.Ascend("", func(k string, v *ctlplfl.NisdVdevAlloc) bool {
			log.Debugf("updating nisd %s available space %d from map -> tree", v.Ptr.ID, v.AvailableSize)
			v.Ptr.AvailableSize = v.AvailableSize
			if v.Ptr.AvailableSize < ctlplfl.CHUNK_SIZE {
				log.Debugf("deleting nisd %s from tree, as space %f GB is less than 8GB", v.Ptr.ID, BytesToGB(v.AvailableSize))
				HR.DeleteNisd(v.Ptr)
			}
			return true
		})
		log.Infof("vdev %s, request successfully processed", vdev.Cfg.ID)
	} else {
		log.Infof("vdev %s, creation failed", vdev.Cfg.ID)
	}
	HR.Dump()
	nisdMap.Clear()
	return pmCmn.Encoder(pmCmn.GOB, resp)
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
	readResult, err := cbArgs.PmdbRangeRead(PumiceDBServer.RangeReadArgs{
		ColFamily: colmfamily,
		Key:       key,
		BufSize:   cbArgs.ReplySize,
		Prefix:    key,
	})
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
	readResult, err := cbArgs.PmdbRangeRead(PumiceDBServer.RangeReadArgs{
		ColFamily: colmfamily,
		Key:       key,
		BufSize:   cbArgs.ReplySize,
		Prefix:    key,
	})
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
	readResult, err := cbArgs.PmdbRangeRead(PumiceDBServer.RangeReadArgs{
		ColFamily: colmfamily,
		Key:       key,
		BufSize:   cbArgs.ReplySize,
		Prefix:    key,
	})
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

	readResult, err := cbArgs.PmdbRangeRead(PumiceDBServer.RangeReadArgs{
		ColFamily: colmfamily,
		Key:       key,
		BufSize:   cbArgs.ReplySize,
		Prefix:    key,
	})
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

	nisdResult, err := cbArgs.PmdbRangeRead(PumiceDBServer.RangeReadArgs{
		ColFamily: colmfamily,
		Key:       NisdCfgKey,
		BufSize:   cbArgs.ReplySize,
		Prefix:    NisdCfgKey,
	})
	if err != nil {
		log.Error("Range read failure: ", err)
		return nil, err
	}
	// ParseEntitiesMap now returns map[string]Entity
	nisdEntityMap := ParseEntitiesMap(nisdResult.ResultMap, NisdParser{})

	readResult, err := cbArgs.PmdbRangeRead(PumiceDBServer.RangeReadArgs{
		ColFamily: colmfamily,
		Key:       key,
		BufSize:   cbArgs.ReplySize,
		Prefix:    key,
	})
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
	rqResult, err := cbArgs.PmdbRangeRead(PumiceDBServer.RangeReadArgs{
		ColFamily: colmfamily,
		Key:       vKey,
		BufSize:   cbArgs.ReplySize,
		Prefix:    vKey,
	})
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
	cbargs := args[0].(*PumiceDBServer.PmdbCbArgs)
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

	rqResult, err := cbargs.PmdbRangeRead(PumiceDBServer.RangeReadArgs{
		ColFamily: colmfamily,
		Key:       vcKey,
		BufSize:   cbargs.ReplySize,
		Prefix:    vcKey,
	})
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
		NumReplicas: uint8(len(rqResult.ResultMap)),
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
	readResult, err := cbArgs.PmdbRangeRead(PumiceDBServer.RangeReadArgs{
		ColFamily: colmfamily,
		Key:       argsKey,
		BufSize:   cbArgs.ReplySize,
		Prefix:    argsKey,
	})
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
