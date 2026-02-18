package srvctlplanefuncs

import (
	"C"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"
	"unsafe"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"
	auth "github.com/00pauln00/niova-mdsvc/controlplane/auth/jwt"
	authz "github.com/00pauln00/niova-mdsvc/controlplane/authorizer"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	PumiceDBServer "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
	storageiface "github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/interface"
	"github.com/tidwall/btree"
)

var colmfamily string

var (
	authorizer *authz.Authorizer
)

// InitAuthorizer initializes the global authorizer instance with the config file
func InitAuthorizer(configPath string) error {
	authorizer = &authz.Authorizer{
		Config: make(authz.Config),
	}
	return authorizer.LoadConfig(configPath)
}

// GetAuthorizer returns the global authorizer instance
func GetAuthorizer() *authz.Authorizer {
	return authorizer
}

const (
	DEVICE_ID        = "d"
	NISD_ID          = "n"
	SERIAL_NUM       = "sn"
	STATE            = "s"
	FAILURE_DOMAIN   = "fd"
	CLIENT_PORT      = "cp"
	PEER_PORT        = "pp"
	IP_ADDR          = "ip"
	TOTAL_SPACE      = "ts"
	AVAIL_SPACE      = "as"
	SIZE             = "sz"
	NUM_CHUNKS       = "nc"
	NUM_REPLICAS     = "nr"
	ERASURE_CODE     = "e"
	LOCATION         = "l"
	POWER_CAP        = "pw"
	SPEC             = "sp"
	NAME             = "nm"
	PORT_RANGE       = "pr"
	SSH_PORT         = "ssh"
	PORT             = "prt"
	DEVICE_PATH      = "dp"
	PARTITION_PATH   = "ptp"
	NETWORK_INFO     = "ni"
	NETWORK_INFO_CNT = "nic"
	SOCKET_PATH      = "sck"
	ENABLE_RDMA      = "rdma"

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

// TokenClaims holds user identity extracted from a verified JWT.
type TokenClaims struct {
	UserID  string
	Role    string
	IsAdmin bool
}

// ValidateToken verifies the JWT token using CP_SECRET and extracts user claims.
// Returns an error if the token is empty, invalid, or missing required fields.
func ValidateToken(token string) (*TokenClaims, error) {
	if token == "" {
		return nil, fmt.Errorf("user token is required")
	}
	tc := &auth.Token{
		Secret: []byte(ctlplfl.CP_SECRET),
	}
	claims, err := tc.VerifyToken(token)
	if err != nil {
		log.Errorf("token verification failed: %v", err)
		return nil, fmt.Errorf("authentication failed: %v", err)
	}
	userID, ok := claims["userID"].(string)
	if !ok || userID == "" {
		log.Error("userID not found in token")
		return nil, fmt.Errorf("invalid token: missing userID")
	}
	userRole, ok := claims["role"].(string)
	if !ok || userRole == "" {
		log.Error("role not found in token")
		return nil, fmt.Errorf("invalid token: missing role")
	}
	isAdmin, _ := claims["isAdmin"].(bool)
	tokenClaim := &TokenClaims{
		UserID:  userID,
		Role:    userRole,
		IsAdmin: isAdmin,
	}
	return tokenClaim, nil
}

// validateAndAuthorizeRBAC verifies the token and checks RBAC-only permissions.
// For operations that also require ABAC (ownership checks)
func validateAndAuthorizeRBAC(token, operation string) (*TokenClaims, error) {
	tc, err := ValidateToken(token)
	if err != nil {
		return nil, err
	}
	if authorizer != nil {
		if !authorizer.CheckRBAC(operation, []string{tc.Role}) {
			log.Errorf("user %s with role %s not authorized for %s", tc.UserID, tc.Role, operation)
			return nil, fmt.Errorf("authorization failed: insufficient permissions")
		}
	}
	log.Infof("user %s authorized for %s", tc.UserID, operation)
	return tc, nil
}

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
	readResult, err := cbargs.Store.Read(key, colmfamily)
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
	rrargs := storageiface.RangeReadArgs{
		Selector:   colmfamily,
		Key:        key,
		BufSize:    cbArgs.ReplySize,
		Consistent: false,
		Prefix:     key,
	}
	readResult, err := cbArgs.Store.RangeRead(rrargs)
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

func applyKV(chgs []funclib.CommitChg, ds storageiface.DataStore) error {
	for _, chg := range chgs {
		var err error
		switch chg.Op {
		case funclib.OpApply:
			log.Info("Applying change: ", string(chg.Key), " -> ", string(chg.Value))
			err = ds.Write(string(chg.Key), string(chg.Value), colmfamily)
		case funclib.OpDelete:
			log.Info("Deleting key: ", string(chg.Key))
			err = ds.Delete(string(chg.Key), colmfamily)
		default:
			log.Error("Unknown operation type: ", chg.Op, " for key: ", string(chg.Key))
			return fmt.Errorf("unknown operation type %d for key %s", chg.Op, string(chg.Key))
		}
		if err != nil {
			log.Error("Failed to apply changes for key: ", string(chg.Key), " err: ", err)
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

	err = applyKV(intrm.Changes, cbargs.Store)
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
	if _, err := validateAndAuthorizeRBAC(nisd.UserToken, "ApplyNisd"); err != nil {
		return nil, err
	}
	var intrm funclib.FuncIntrm
	buf := C.GoBytes(cbargs.AppData, C.int(cbargs.AppDataSize))
	err := pmCmn.Decoder(pmCmn.GOB, buf, &intrm)
	if err != nil {
		log.Error("Failed to decode the apply changes: ", err)
		return nil, fmt.Errorf("failed to decode apply changes: %v", err)
	}

	err = applyKV(intrm.Changes, cbargs.Store)
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
	req := args[1].(ctlplfl.GetReq)

	if _, err := validateAndAuthorizeRBAC(req.UserToken, "ReadAllNisdConfigs"); err != nil {
		return nil, err
	}
	log.Trace("fetching nisd details for key : ", NisdCfgKey)
	rrargs := storageiface.RangeReadArgs{
		Selector:   colmfamily,
		Key:        NisdCfgKey,
		BufSize:    cbArgs.ReplySize,
		Consistent: false,
		Prefix:     NisdCfgKey,
	}
	readResult, err := cbArgs.Store.RangeRead(rrargs)
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
	if err := req.ValidateRequest(); err != nil {
		return nil, err
	}
	if _, err := validateAndAuthorizeRBAC(req.UserToken, "ReadNisdConfig"); err != nil {
		return nil, err
	}
	key := getConfKey(NisdCfgKey, req.ID)
	log.Trace("fetching nisd details for key : ", key)
	rrargs := storageiface.RangeReadArgs{
		Selector:   colmfamily,
		Key:        key,
		BufSize:    cbArgs.ReplySize,
		Consistent: false,
		Prefix:     key,
	}
	readResult, err := cbArgs.Store.RangeRead(rrargs)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}
	nisdList := ParseEntities[ctlplfl.Nisd](readResult.ResultMap, NisdParser{})
	return pmCmn.Encoder(pmCmn.GOB, nisdList[0])
}

func getNisdList(cbArgs *PumiceDBServer.PmdbCbArgs) ([]ctlplfl.Nisd, error) {
	readResult, err := cbArgs.Store.RangeRead(storageiface.RangeReadArgs{
		Selector:   colmfamily,
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
	if err := nisd.Validate(); err != nil {
		log.Error("failed to validate nisd: ", err)
		return nil, err
	}
	if _, err := validateAndAuthorizeRBAC(nisd.UserToken, "WPNisdCfg"); err != nil {
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
	if _, err := validateAndAuthorizeRBAC(req.UserToken, "RdDeviceInfo"); err != nil {
		return nil, err
	}
	key := getConfKey(deviceCfgKey, req.ID)
	readResult, err := cbArgs.Store.RangeRead(storageiface.RangeReadArgs{
		Selector:   colmfamily,
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
	if _, err := validateAndAuthorizeRBAC(dev.UserToken, "WPDeviceInfo"); err != nil {
		return nil, err
	}
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
	req, ok := args[0].(ctlplfl.VdevReq)
	if !ok {
		err := fmt.Errorf("invalid argument: expecting type vdev")
		return nil, err
	}
	tc, err := validateAndAuthorizeRBAC(req.UserToken, "WPCreateVdev")
	if err != nil {
		return nil, err
	}

	err = req.Vdev.Init()
	if err != nil {
		log.Errorf("failed to initialize vdev: %v", err)
		return nil, err
	}
	log.Infof("initializing vdev: %+v for user: %s", req.Vdev, tc.UserID)
	key := getConfKey(vdevKey, req.Vdev.ID)
	for _, field := range []string{SIZE, NUM_CHUNKS, NUM_REPLICAS} {
		var value string
		switch field {
		case SIZE:
			value = strconv.Itoa(int(req.Vdev.Size))
		case NUM_CHUNKS:
			value = strconv.Itoa(int(req.Vdev.NumChunks))
		case NUM_REPLICAS:
			value = strconv.Itoa(int(req.Vdev.NumReplica))
		default:
			continue
		}
		commitChgs = append(commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s/%s", key, cfgkey, field)),
			Value: []byte(value),
		})
	}

	// Add ownership key to mark user as owner of this vdev
	ownershipKey := fmt.Sprintf("/u/%s/v/%s", tc.UserID, req.Vdev.ID)
	commitChgs = append(commitChgs, funclib.CommitChg{
		Key:   []byte(ownershipKey),
		Value: []byte("1"),
	})
	log.Infof("added ownership key: %s for vdev: %s", ownershipKey, req.Vdev.ID)

	r, err := pmCmn.Encoder(pmCmn.GOB, req)
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

func allocateNisdPerChunk(req *ctlplfl.VdevReq, fd int, chunk string,
	commitChgs *[]funclib.CommitChg,
	nisdMap *btree.Map[string, *ctlplfl.NisdVdevAlloc]) error {

	if fd < 0 || fd >= ctlplfl.FD_MAX {
		return fmt.Errorf("invalid failure domain: %d", fd)
	}

	pickedNISD := make(map[string]struct{})
	pickedEntity := make(map[int]struct{})

	tree := HR.FD[fd].Tree
	treeLen := tree.Len()
	if treeLen == 0 {
		return fmt.Errorf("no entities available in failure domain %d", fd)
	}

	// Filtered allocation path
	if req.Filter.ID != "" {
		en, err := GetEntityByID(req.Filter)
		if err != nil {
			return err
		}

		for r := 0; r < int(req.Vdev.NumReplica); r++ {
			nisd, err := HR.PickNISD(en, pickedNISD, nisdMap)
			if err != nil {
				log.Error("PickNISD():failed to pick NISD: ", err)
				return err
			}
			genAllocationKV(req.Vdev.ID, chunk, nisd, r, commitChgs)
		}
		return nil
	}

	// Deterministic entity start index
	hash := ctlplfl.NisdAllocHash([]byte(req.Vdev.ID + chunk))
	startIdx, err := GetIdxForNisdAlloc(hash, treeLen)
	if err != nil {
		return err
	}

	entityIdx := startIdx

	for r := 0; r < int(req.Vdev.NumReplica); r++ {
		var (
			nisd    *ctlplfl.NisdVdevAlloc
			picked        = -1
			lastErr error = fmt.Errorf("no entity attempted")
			tried         = 0
		)

		for tried < treeLen {
			curIdx := entityIdx
			entityIdx = (entityIdx + 1) % treeLen

			// do not count skipped entities as attempts
			if _, used := pickedEntity[curIdx]; used {
				continue
			}

			tried++

			ent, ok := tree.GetAt(curIdx)
			if !ok {
				lastErr = fmt.Errorf("failed to fetch entity idx=%d fd=%d", curIdx, fd)
				continue
			}

			nisd, lastErr = HR.PickNISD(ent, pickedNISD, nisdMap)
			if lastErr == nil {
				picked = curIdx
				break
			}
		}

		if picked == -1 {
			return fmt.Errorf(
				"failed to allocate replica %d for chunk=%s fd=%d after %d entities (startIdx=%d): lastErr=%v",
				r, chunk, fd, treeLen, startIdx, lastErr,
			)
		}

		genAllocationKV(req.Vdev.ID, chunk, nisd, r, commitChgs)
		pickedEntity[picked] = struct{}{}
	}

	return nil
}

func allocateNisdPerVdev(req *ctlplfl.VdevReq, fd int, nisdMap *btree.Map[string, *ctlplfl.NisdVdevAlloc]) ([]funclib.CommitChg, error) {
	commitCh := make([]funclib.CommitChg, 0)
	for i := 0; i < int(req.Vdev.NumChunks); i++ {
		log.Debugf("allocating nisd for chunk: %d, from fd: %d ", i, fd)
		err := allocateNisdPerChunk(req, fd, strconv.Itoa(i), &commitCh, nisdMap)
		if err != nil {
			err = fmt.Errorf("failed to allocate nisd from fd: %d, %v", fd, err)
			log.Error(err)
			return nil, err
		}
	}
	return commitCh, nil
}

func AllocNISDs(
	req *ctlplfl.VdevReq,
	allocMap *btree.Map[string, *ctlplfl.NisdVdevAlloc],
	txn *funclib.FuncIntrm,
) error {

	if req.Filter.Type != ctlplfl.FD_ANY {
		return allocateNisdsAtFailureDomain(req, ctlplfl.GetFDIdx(req.Filter.Type), allocMap, txn)
	}

	startFD, err := HR.GetFDLevel(int(req.Vdev.NumReplica))
	if err != nil {
		return fmt.Errorf("failed to resolve failure domain: %w", err)
	}

	for fd := startFD; fd < ctlplfl.FD_MAX; fd++ {
		if err := allocateNisdsAtFailureDomain(req, fd, allocMap, txn); err != nil {
			log.Error("allocation failed, retrying next failure domain: ", err)
			allocMap.Clear()
			continue
		}
		return nil
	}

	return fmt.Errorf("unable to allocate NISDs for vdev %s", req.Vdev.ID)
}

func allocateNisdsAtFailureDomain(
	req *ctlplfl.VdevReq,
	fd int,
	allocMap *btree.Map[string, *ctlplfl.NisdVdevAlloc],
	txn *funclib.FuncIntrm,
) error {

	log.Debugf("selected fd %d for vdev ID: %s", fd, req.Vdev.ID)

	changes, err := allocateNisdPerVdev(req, fd, allocMap)
	if err != nil {
		return err
	}

	txn.Changes = append(txn.Changes, changes...)
	return nil
}

func commitAllocChgs(
	txn *funclib.FuncIntrm,
	allocMap *btree.Map[string, *ctlplfl.NisdVdevAlloc],
	cbArgs *PumiceDBServer.PmdbCbArgs,
) error {

	allocMap.Ascend("", func(_ string, alloc *ctlplfl.NisdVdevAlloc) bool {
		txn.Changes = append(txn.Changes, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", getConfKey(NisdCfgKey, alloc.Ptr.ID), AVAIL_SPACE)),
			Value: []byte(strconv.Itoa(int(alloc.AvailableSize))),
		})
		return true
	})

	return applyKV(txn.Changes, cbArgs.Store)
}

func applyNISDAlloc(
	allocMap *btree.Map[string, *ctlplfl.NisdVdevAlloc],
) {
	allocMap.Ascend("", func(_ string, alloc *ctlplfl.NisdVdevAlloc) bool {
		alloc.Ptr.AvailableSize = alloc.AvailableSize
		if alloc.Ptr.AvailableSize < ctlplfl.CHUNK_SIZE {
			log.Debugf(
				"removing nisd %s, available space %f GB below chunk size",
				alloc.Ptr.ID,
				BytesToGB(alloc.AvailableSize),
			)
			HR.DeleteNisd(alloc.Ptr)
		}
		return true
	})
}

// Creates a VDEV, allocates the NISD and updates the PMDB with new data
func APCreateVdev(args ...interface{}) (interface{}, error) {
	var req ctlplfl.VdevReq
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
	pmCmn.Decoder(pmCmn.GOB, funcIntrm.Response, &req)
	log.Infof("allocating vdev for ID: %s", req.Vdev.ID)
	resp.ID = req.Vdev.ID
	allocMap := btree.NewMap[string, *ctlplfl.NisdVdevAlloc](32)
	defer allocMap.Clear()
	HR.Dump()
	// allocate nisd chunks to vdev
	if err := AllocNISDs(&req, allocMap, &funcIntrm); err != nil {
		resp.Error = err.Error()
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	resp.Success = true

	if err := commitAllocChgs(&funcIntrm, allocMap, cbArgs); err != nil {
		return nil, err
	}

	applyNISDAlloc(allocMap)

	log.Infof("vdev %s, request successfully processed", req.Vdev.ID)
	HR.Dump()

	return pmCmn.Encoder(pmCmn.GOB, resp)
}

func WPCreatePartition(args ...interface{}) (interface{}, error) {
	pt := args[0].(ctlplfl.DevicePartition)
	if _, err := validateAndAuthorizeRBAC(pt.UserToken, "WPCreatePartition"); err != nil {
		return nil, err
	}
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
	if _, err := validateAndAuthorizeRBAC(req.UserToken, "ReadPartition"); err != nil {
		return nil, err
	}
	readResult, err := cbArgs.Store.RangeRead(storageiface.RangeReadArgs{
		Selector: colmfamily,
		Key:      key,
		BufSize:  cbArgs.ReplySize,
		Prefix:   key,
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
	if _, err := validateAndAuthorizeRBAC(pdu.UserToken, "WPPDUCfg"); err != nil {
		return nil, err
	}
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
	if _, err := validateAndAuthorizeRBAC(req.UserToken, "ReadPDUCfg"); err != nil {
		return nil, err
	}
	readResult, err := cbArgs.Store.RangeRead(storageiface.RangeReadArgs{
		Selector: colmfamily,
		Key:      key,
		BufSize:  cbArgs.ReplySize,
		Prefix:   key,
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
	if _, err := validateAndAuthorizeRBAC(rack.UserToken, "WPRackCfg"); err != nil {
		return nil, err
	}
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
	if _, err := validateAndAuthorizeRBAC(req.UserToken, "ReadRackCfg"); err != nil {
		return nil, err
	}
	readResult, err := cbArgs.Store.RangeRead(storageiface.RangeReadArgs{
		Selector: colmfamily,
		Key:      key,
		BufSize:  cbArgs.ReplySize,
		Prefix:   key,
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
	hv := args[0].(ctlplfl.Hypervisor)
	if _, err := validateAndAuthorizeRBAC(hv.UserToken, "WPHyperVisorCfg"); err != nil {
		return nil, err
	}
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
	if _, err := validateAndAuthorizeRBAC(req.UserToken, "ReadHyperVisorCfg"); err != nil {
		return nil, err
	}
	readResult, err := cbArgs.Store.RangeRead(storageiface.RangeReadArgs{
		Selector: colmfamily,
		Key:      key,
		BufSize:  cbArgs.ReplySize,
		Prefix:   key,
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
	tc, err := ValidateToken(req.UserToken)
	if err != nil {
		return nil, err
	}
	if authorizer != nil {
		if req.GetAll {
			// Listing all vdevs with chunk info: admin only, same restriction as ReadAllVdevInfo
			if !authorizer.Authorize("ReadAllVdevInfo", tc.UserID, []string{tc.Role}, map[string]string{}, nil, "") {
				log.Errorf("user %s with role %s not authorized to list all vdevs with chunk info", tc.UserID, tc.Role)
				return nil, fmt.Errorf("authorization failed: admin required to list all vdevs")
			}
		} else {
			// Specific vdev: verify RBAC + ABAC ownership
			attributes := map[string]string{"vdev": req.ID}
			if !authorizer.Authorize("ReadVdevsInfoWithChunkMapping", tc.UserID, []string{tc.Role}, attributes, cbArgs.Store, colmfamily) {
				log.Errorf("user %s with role %s not authorized to read vdev %s with chunk info", tc.UserID, tc.Role, req.ID)
				return nil, fmt.Errorf("authorization failed")
			}
		}
	}

	key := vdevKey
	if !req.GetAll {
		key = getConfKey(vdevKey, req.ID)
	}

	nisdResult, err := cbArgs.Store.RangeRead(storageiface.RangeReadArgs{
		Selector: colmfamily,
		Key:      NisdCfgKey,
		BufSize:  cbArgs.ReplySize,
		Prefix:   NisdCfgKey,
	})
	if err != nil {
		log.Error("Range read failure: ", err)
		return nil, err
	}
	// ParseEntitiesMap now returns map[string]Entity
	nisdEntityMap := ParseEntitiesMap(nisdResult.ResultMap, NisdParser{})

	readResult, err := cbArgs.Store.RangeRead(storageiface.RangeReadArgs{
		Selector: colmfamily,
		Key:      key,
		BufSize:  cbArgs.ReplySize,
		Prefix:   key,
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

	tc, err := ValidateToken(req.UserToken)
	if err != nil {
		return nil, err
	}
	if authorizer != nil {
		attributes := map[string]string{"vdev": req.ID}
		if !authorizer.Authorize("ReadVdevInfo", tc.UserID, []string{tc.Role}, attributes, cbArgs.Store, colmfamily) {
			log.Errorf("user %s with role %s not authorized to read vdev %s", tc.UserID, tc.Role, req.ID)
			return nil, fmt.Errorf("authorization failed")
		}
	}
	log.Infof("user %s authorized to read vdev %s", tc.UserID, req.ID)
	vKey := getConfKey(vdevKey, req.ID)
	var rqResult *storageiface.RangeReadResult
	rqResult, err = cbArgs.Store.RangeRead(storageiface.RangeReadArgs{
		Selector: colmfamily,
		Key:      vKey,
		BufSize:  cbArgs.ReplySize,
		Prefix:   vKey,
	})
	if err != nil {
		log.Error("RangeReadKV failure: ", err)
		return nil, err
	}

	authtc := &auth.Token{
		Secret: []byte(ctlplfl.NISD_SECRET),
		TTL:    time.Minute,
	}

	claims := map[string]any{
		"vdevID": req.ID,
	}

	authtoken, err := authtc.CreateToken(claims)
	if err != nil {
		log.Error("Token Creation failed with: ", err)
		return nil, err
	}
	log.Trace("Created AuthToken ", authtoken, " for vdev ", req.ID)

	vdevInfo := ctlplfl.VdevCfg{
		ID:        req.ID,
		AuthToken: authtoken,
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

func ReadAllVdevInfo(args ...interface{}) (interface{}, error) {
	cbArgs := args[0].(*PumiceDBServer.PmdbCbArgs)
	rqResult, err := cbArgs.Store.RangeRead(storageiface.RangeReadArgs{
		Selector: colmfamily,
		Key:      vdevKey,
		BufSize:  cbArgs.ReplySize,
		Prefix:   vdevKey,
	})
	if err != nil {
		log.Error("RangeReadKV failure: ", err)
		return nil, err
	}

	// TODO: move this to parsing file
	vdevList := ParseEntities[ctlplfl.VdevCfg](rqResult.ResultMap, vdevParser{})
	return pmCmn.Encoder(ENC_TYPE, vdevList)
}

func ReadChunkNisd(args ...interface{}) (interface{}, error) {
	cbargs := args[0].(*PumiceDBServer.PmdbCbArgs)
	req := args[1].(ctlplfl.GetReq)

	if err := req.ValidateRequest(); err != nil {
		log.Error("failed to validate request:", err)
		return nil, err
	}
	tc, err := ValidateToken(req.UserToken)
	if err != nil {
		return nil, err
	}
	keys := strings.Split(strings.Trim(req.ID, "/"), "/")
	if len(keys) < 2 {
		log.Errorf("invalid request ID format %q: expected vdevID/chunkIndex", req.ID)
		return nil, fmt.Errorf("invalid request ID format: expected vdevID/chunkIndex")
	}
	vdevID, chunk := keys[0], keys[1]

	// Check authorization: ownership of the vdev implies access to its chunks
	if authorizer != nil {
		attributes := map[string]string{"vdev": vdevID}
		if !authorizer.Authorize("ReadChunkNisd", tc.UserID, []string{tc.Role}, attributes, cbargs.Store, colmfamily) {
			log.Errorf("user %s with role %s not authorized to read chunk nisd for vdev %s", tc.UserID, tc.Role, vdevID)
			return nil, fmt.Errorf("authorization failed")
		}
	}

	vcKey := path.Clean(getConfKey(vdevKey, path.Join(vdevID, chunkKey, chunk))) + "/"

	log.Info("searching for key:", vcKey)

	rqResult, err := cbargs.Store.RangeRead(storageiface.RangeReadArgs{
		Selector: colmfamily,
		Key:      vcKey,
		BufSize:  cbargs.ReplySize,
		Prefix:   vcKey,
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
	if _, err := validateAndAuthorizeRBAC(nArgs.UserToken, "WPNisdArgs"); err != nil {
		return nil, err
	}
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
	req := args[1].(ctlplfl.GetReq)
	if _, err := validateAndAuthorizeRBAC(req.UserToken, "RdNisdArgs"); err != nil {
		return nil, err
	}
	readResult, err := cbArgs.Store.RangeRead(storageiface.RangeReadArgs{
		Selector: colmfamily,
		Key:      argsKey,
		BufSize:  cbArgs.ReplySize,
		Prefix:   argsKey,
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
