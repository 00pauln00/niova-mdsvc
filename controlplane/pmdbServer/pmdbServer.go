package main

import (
	leaseServerLib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicelease/server"
	"bufio"
	"bytes"
	httpClient "github.com/00pauln00/niova-pumicedb/go/pkg/utils/httpclient"
	lookout "github.com/00pauln00/niova-pumicedb/go/pkg/utils/ctlmonitor"
	"github.com/00pauln00/niova-mdsvc/controlplane/requestResponseLib"
	serfAgent "github.com/00pauln00/niova-pumicedb/go/pkg/utils/serfagent"
	compressionLib "github.com/00pauln00/niova-pumicedb/go/pkg/utils/compressor"
	PumiceDBFunc "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/server"
	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	defaultLogger "log"
	"net"
	PumiceDBCommon "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	PumiceDBServer "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

var seqno = 0

var encodingOverhead = 256

var RecvdPort int

// Use the default column family
var colmfamily = "PMDBTS_CF"

type pmdbServerHandler struct {
	raftUUID          uuid.UUID
	peerUUID          uuid.UUID
	logDir            string
	logLevel          string
	gossipClusterFile string
	servicePortRangeS uint16
	servicePortRangeE uint16
	hport             uint16
	prometheus        bool
	nodeAddr          net.IP
	addrList          []net.IP
	GossipData        map[string]string
	ConfigString      string
	ConfigData        []PumiceDBCommon.PeerConfigData
	lookoutInstance   lookout.EPContainer
	serfAgentHandler  serfAgent.SerfAgentHandler
	portRange         []uint16
}

func main() {
	serverHandler := pmdbServerHandler{}
	nso, pErr := serverHandler.parseArgs()
	if pErr != nil {
		log.Println(pErr)
		return
	}

	switch serverHandler.logLevel {
	case "Info":
		log.SetLevel(log.InfoLevel)
	case "Trace":
		log.SetLevel(log.TraceLevel)
	}

	log.Info("Log Dir - ", serverHandler.logDir)

	//Create log file
	err := PumiceDBCommon.InitLogger(serverHandler.logDir)
	if err != nil {
		log.Error("Error while initating logger ", err)
		os.Exit(1)
	}

	err = serverHandler.startSerfAgent()
	if err != nil {
		log.Fatal("Error while initializing serf agent ", err)
	}

	log.Info("Raft and Peer UUID: ", nso.raftUuid, " ", nso.peerUuid)
	/*
	   Initialize the internal pmdb-server-object pointer.
	   Assign the Directionary object to PmdbAPI so the apply and
	   read callback functions can be called through pmdb common library
	   functions.
	*/

	portAddr := &RecvdPort

	//Start lookout monitoring
	ctl_path := os.Getenv("NIOVA_INOTIFY_BASE_PATH")
	if len(ctl_path) == 0 {
		ctl_path = "/ctl-interface/"
	}

	serverHandler.lookoutInstance = lookout.EPContainer{
		MonitorUUID:      nso.peerUuid.String(),
		AppType:          "PMDB",
		PortRange:        serverHandler.portRange,
		CTLPath:          ctl_path,
		SerfMembershipCB: serverHandler.SerfMembership,
		EnableHttp:       serverHandler.prometheus,
		RetPort:          portAddr,
	}
	go serverHandler.lookoutInstance.Start()

	//Wait till HTTP Server has started
	funcAPI := PumiceDBFunc.NewFuncServer()
	funcAPI.RegisterWritePrepFunc("CreateSnap", WritePrepCreateSnap)
	funcAPI.RegisterApplyFunc("*", ApplyFunc)

	nso.pso = &PumiceDBServer.PmdbServerObject{
		RaftUuid:       nso.raftUuid.String(),
		PeerUuid:       nso.peerUuid.String(),
		PmdbAPI:        nso,
		FuncAPI:	  funcAPI,
		SyncWrites:     false,
		CoalescedWrite: true,
		LeaseEnabled:   true,
	}

	nso.leaseObj = leaseServerLib.LeaseServerObject{}
	//Initalise leaseObj
	nso.leaseObj.InitLeaseObject(nso.pso)
	// Separate column families for application requests and lease
	nso.pso.ColumnFamilies = []string{colmfamily, nso.leaseObj.LeaseColmFam}

	// Start the pmdb server
	//TODO Check error
	go nso.pso.Run()

	serverHandler.checkPMDBLiveness()
	serverHandler.exportTags()
}

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

func WritePrepCreateSnap(args ...interface{}) (interface{}, error) {
	//Define Snapshot XML structure
	type SnapName struct {
		Name string `xml:"Name,attr"`
		Sucess bool `xml:"Success"`
	}
	
	type SnapResponseXML struct {
		SnapName  SnapName `xml:"Snap"`
	}

	type ChunkXML struct {
		Seq  int64  `xml:"Seq,attr"`
	}
	
	type VdevXML struct {
		VdevName string `xml:"Name,attr"`
		Chunks   []ChunkXML `xml:"Chunk"`
	}

	type SnapXML struct {
		SnapName string `xml:"Name,attr"` 
		Vdevs    []VdevXML `xml:"Vdev"`
	}

	var Snap SnapXML

	// Decode the input buffer into structure format
	err := xml.Unmarshal(args[0].([]byte), &Snap)
	if err != nil {
		return nil, err
	}

	commitChgs := make([]funclib.CommitChg, 0)
	for _, vdev := range Snap.Vdevs {
		for index, chunk := range vdev.Chunks {
			// Create a CommitChg for each chunk
			chg := funclib.CommitChg{
				Key:   []byte(fmt.Sprintf("%s/%s/%d", Snap.SnapName, vdev.VdevName, index)),
				Value: []byte(fmt.Sprintf("%d", chunk.Seq)),
			}
			commitChgs = append(commitChgs, chg)
		}
	}

	//Fill the response structure
	snapResponse := SnapResponseXML{
		SnapName: SnapName{
			Name:   Snap.SnapName,
			Sucess: true,
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

	replySize, err := PumiceDBServer.PmdbCopyBytesToBuffer(intrm.Response,  cbargs.ReplyBuf)
	if err != nil {
		log.Error("Failed to Copy result in the buffer: %s", err)
		return nil, fmt.Errorf("failed to copy result to buffer: %v", err)
	}

	//Empty return for the interface as we are filling the reply buf in the function
	return replySize, nil
}

func (handler *pmdbServerHandler) checkPMDBLiveness() {
	for {
		ok := handler.lookoutInstance.CheckLiveness(handler.peerUUID.String())
		if ok {
			fmt.Println("PMDB Server is live")
			return
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func (handler *pmdbServerHandler) exportTags() error {
	if handler.prometheus {
		handler.checkHTTPLiveness()
		handler.GossipData["Hport"] = strconv.Itoa(RecvdPort)
	}
	handler.GossipData["Type"] = "PMDB_SERVER"
	handler.GossipData["Rport"] = strconv.Itoa(int(handler.serfAgentHandler.RpcPort))
	handler.GossipData["RU"] = handler.raftUUID.String()
	handler.GossipData["CS"], _ = generateCheckSum(handler.GossipData)
	log.Info(handler.GossipData)
	for {
		handler.serfAgentHandler.SetNodeTags(handler.GossipData)
		time.Sleep(3 * time.Second)
	}

	return nil
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

func makeRange(min, max uint16) []uint16 {
	a := make([]uint16, max-min+1)
	for i := range a {
		a[i] = uint16(min + uint16(i))
	}
	return a
}

func decodeControlPlaneReq(input []byte, output *requestResponseLib.KVRequest) error {
	dec := gob.NewDecoder(bytes.NewBuffer(input))
	for {
		if err := dec.Decode(output); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}

	return nil
}

func (handler *pmdbServerHandler) checkHTTPLiveness() {
	var emptyByteArray []byte
	for {
		if RecvdPort == -1 {
			fmt.Println("HTTP Server failed to start")
			os.Exit(0)
		}
		_, err := httpClient.HTTP_Request(emptyByteArray, "127.0.0.1:"+strconv.Itoa(int(RecvdPort))+"/check", false)
		if err != nil {
			fmt.Println("HTTP Liveness - ", err)
		} else {
			fmt.Println("HTTP Liveness - HTTP Server is alive")
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func (handler *pmdbServerHandler) parseArgs() (*NiovaKVServer, error) {
	var tempRaftUUID, tempPeerUUID string
	var err error

	flag.StringVar(&tempRaftUUID, "r", "NULL", "raft uuid")
	flag.StringVar(&tempPeerUUID, "u", "NULL", "peer uuid")

	/* If log path is not provided, it will use Default log path.
	   default log path: /tmp/<peer-uuid>.log
	*/
	defaultLog := "/" + "tmp" + "/" + handler.peerUUID.String() + ".log"
	flag.StringVar(&handler.logDir, "l", defaultLog, "log dir")
	flag.StringVar(&handler.logLevel, "ll", "Info", "Log level")
	flag.StringVar(&handler.gossipClusterFile, "g", "NULL", "Serf agent port")
	flag.BoolVar(&handler.prometheus, "p", false, "Enable prometheus")
	flag.Parse()

	handler.raftUUID, _ = uuid.FromString(tempRaftUUID)
	handler.peerUUID, _ = uuid.FromString(tempPeerUUID)
	nso := &NiovaKVServer{}
	nso.raftUuid = handler.raftUUID
	nso.peerUuid = handler.peerUUID

	if nso == nil {
		err = errors.New("Not able to parse the arguments")
	} else {
		err = nil
	}

	return nso, err
}

func (handler *pmdbServerHandler) SerfMembership() map[string]bool {
	membership := handler.serfAgentHandler.GetMembersState()
	return membership
}

func extractPMDBServerConfigfromFile(path string) (*PumiceDBCommon.PeerConfigData, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(f)
	peerData := PumiceDBCommon.PeerConfigData{}

	for scanner.Scan() {
		text := scanner.Text()
		lastIndex := len(strings.Split(text, " ")) - 1
		key := strings.Split(text, " ")[0]
		value := strings.Split(text, " ")[lastIndex]
		switch key {
		case "CLIENT_PORT":
			buffer, err := strconv.ParseUint(value, 10, 16)
			peerData.ClientPort = uint16(buffer)
			if err != nil {
				return nil, errors.New("Client Port is out of range")
			}

		case "IPADDR":
			peerData.IPAddr = net.ParseIP(value)

		case "PORT":
			buffer, err := strconv.ParseUint(value, 10, 16)
			peerData.Port = uint16(buffer)
			if err != nil {
				return nil, errors.New("Port is out of range")
			}
		}
	}
	f.Close()

	return &peerData, err
}

func (handler *pmdbServerHandler) readPMDBServerConfig() error {
	folder := os.Getenv("NIOVA_LOCAL_CTL_SVC_DIR")
	files, err := ioutil.ReadDir(folder + "/")
	if err != nil {
		return err
	}
	handler.GossipData = make(map[string]string)

	for _, file := range files {
		if strings.Contains(file.Name(), ".peer") {
			//Extract required config from the file
			path := folder + "/" + file.Name()
			peerData, err := extractPMDBServerConfigfromFile(path)
			if err != nil {
				return err
			}

			uuid := file.Name()[:len(file.Name())-5]
			cuuid, err := compressionLib.CompressUUID(uuid)
			if err != nil {
				return err
			}

			tempGossipData, _ := compressionLib.CompressStructure(*peerData)
			handler.GossipData[cuuid] = tempGossipData[16:]
			//Since the uuid filled after compression, the cuuid wont be included in compressed string
			copy(peerData.UUID[:], []byte(cuuid))
			handler.ConfigData = append(handler.ConfigData, *peerData)
		}
	}

	return nil
}

func removeDuplicateStr(strSlice []string) []string {
	allKeys := make(map[string]bool)
	list := []string{}
	for _, item := range strSlice {
		if _, value := allKeys[item]; !value {
			allKeys[item] = true
			list = append(list, item)
		}
	}
	return list
}

func (handler *pmdbServerHandler) readGossipClusterFile() error {
	f, err := os.Open(handler.gossipClusterFile)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(f)
	/*
		Following is the format of gossipNodes file
		PMDB server addrs with space separated
		Start_port End_port
	*/
	scanner.Scan()
	IPAddrsTxt := strings.Split(scanner.Text(), " ")
	IPAddrs := removeDuplicateStr(IPAddrsTxt)
	for i := range IPAddrs {
		ipAddr := net.ParseIP(IPAddrs[i])
		handler.addrList = append(handler.addrList, ipAddr)
	}
	handler.nodeAddr = net.ParseIP(IPAddrs[0])
	//Read Ports
	scanner.Scan()
	Ports := strings.Split(scanner.Text(), " ")
	temp, _ := strconv.Atoi(Ports[0])
	handler.servicePortRangeS = uint16(temp)
	temp, _ = strconv.Atoi(Ports[1])
	handler.servicePortRangeE = uint16(temp)

	//Set port range array
	handler.portRange = makeRange(handler.servicePortRangeS, handler.servicePortRangeE)
	return nil
}

func generateCheckSum(data map[string]string) (string, error) {
	keys := make([]string, 0, len(data))
	var allDataArray []string
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		allDataArray = append(allDataArray, k+data[k])
	}

	byteArray, err := json.Marshal(allDataArray)
	checksum := crc32.ChecksumIEEE(byteArray)
	checkSumByteArray := make([]byte, 4)
	binary.LittleEndian.PutUint32(checkSumByteArray, uint32(checksum))
	return string(checkSumByteArray), err
}

func (handler *pmdbServerHandler) startSerfAgent() error {
	err := handler.readGossipClusterFile()
	if err != nil {
		return err
	}
	serfLog := "00"
	switch serfLog {
	case "ignore":
		defaultLogger.SetOutput(ioutil.Discard)
	default:
		f, err := os.OpenFile("serfLog.log", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			defaultLogger.SetOutput(os.Stderr)
		} else {
			defaultLogger.SetOutput(f)
		}
	}

	//defaultLogger.SetOutput(ioutil.Discard)
	serfAgentHandler := serfAgent.SerfAgentHandler{
		Name:              handler.peerUUID.String(),
		AddrList:          handler.addrList,
		Addr:              net.ParseIP("127.0.0.1"),
		AgentLogger:       defaultLogger.Default(),
		RaftUUID:          handler.raftUUID,
		ServicePortRangeS: handler.servicePortRangeS,
		ServicePortRangeE: handler.servicePortRangeE,
		AppType:           "PMDB",
	}

	//Start serf agent
	_, err = serfAgentHandler.SerfAgentStartup(true)
	if err != nil {
		log.Error("Error while starting serf agent ", err)
	}
	handler.readPMDBServerConfig()
	handler.serfAgentHandler = serfAgentHandler
	return err
}

type NiovaKVServer struct {
	raftUuid       uuid.UUID
	peerUuid       uuid.UUID
	columnFamilies string
	leaseObj       leaseServerLib.LeaseServerObject
	pso            *PumiceDBServer.PmdbServerObject
}

func (nso *NiovaKVServer) Init(cleanupPeerArgs *PumiceDBServer.PmdbCbArgs) {
	return
}

func (nso *NiovaKVServer) WritePrep(wrPrepArgs *PumiceDBServer.PmdbCbArgs) int64 {
	log.Trace("NiovaCtlPlane server: Write prep received")
	var copyErr error
	_, copyErr = nso.pso.CopyDataToBuffer(byte(1), wrPrepArgs.ContinueWr)
	if copyErr != nil {
		log.Error("Failed to Copy result in the buffer: %s", copyErr)
		return -1
	}
	return 0
}

func (nso *NiovaKVServer) Apply(applyArgs *PumiceDBServer.PmdbCbArgs) int64 {

	log.Trace("NiovaCtlPlane server: Apply request received")

	// Decode the input buffer into structure format
	applyNiovaKV := &requestResponseLib.KVRequest{}
	// register datatypes for decoding interface
	decodeErr := nso.pso.DecodeApplicationReq(applyArgs.Payload, applyNiovaKV)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}

	//For application request
	log.Trace("Key passed by client: ", applyNiovaKV.Key)

	// length of key.
	keyLength := len(applyNiovaKV.Key)

	byteToStr := string(applyNiovaKV.Value)

	// Length of value.
	valLen := len(byteToStr)

	log.Trace("Write the KeyValue by calling PmdbWriteKV")
	rc := nso.pso.WriteKV(applyArgs.UserID, applyArgs.PmdbHandler,
		applyNiovaKV.Key,
		int64(keyLength), byteToStr,
		int64(valLen), colmfamily)

	return int64(rc)
}

func (nso *NiovaKVServer) RetryWrite(RetryWriteArgs *PumiceDBServer.PmdbCbArgs) int64 {
	return 0
}

func (nso *NiovaKVServer) Read(readArgs *PumiceDBServer.PmdbCbArgs) int64 {

	log.Trace("NiovaCtlPlane server: Read request received")
	var copyErr error
	var replySize int64

	//Decode the request structure sent by client.
	reqStruct := &requestResponseLib.KVRequest{}
	decodeErr := nso.pso.DecodeApplicationReq(readArgs.Payload, reqStruct)

	if decodeErr != nil {
		log.Error("Failed to decode the read request")
		return -1
	}

	//Lease request
	log.Trace("Key passed by client: ", reqStruct.Key)
	keyLen := len(reqStruct.Key)
	log.Trace("Key length: ", keyLen)

	var readErr error
	var resultResponse requestResponseLib.KVResponse
	//var resultReq requestResponseLib.KVResponse
	//Pass the work as key to PmdbReadKV and get the value from pumicedb
	if reqStruct.Operation == requestResponseLib.KV_READ {

		log.Trace("read - ", reqStruct.SeqNum)
		readResult, err := nso.pso.ReadKV(readArgs.UserID, reqStruct.Key,
			int64(keyLen), colmfamily)
		singleReadMap := make(map[string][]byte)
		singleReadMap[reqStruct.Key] = readResult
		resultResponse = requestResponseLib.KVResponse{
			Key:       reqStruct.Key,
			ResultMap: singleReadMap,
		}
		readErr = err

	} else if reqStruct.Operation == requestResponseLib.KV_RANGE_READ {
		reqStruct.Prefix = reqStruct.Prefix
		log.Trace("sequence number - ", reqStruct.SeqNum)
		readResult, lastKey, seqNum, snapMiss, err := nso.pso.RangeReadKV(readArgs.UserID,
			reqStruct.Key,
			int64(keyLen), reqStruct.Prefix,
			(readArgs.ReplySize - int64(encodingOverhead)),
			reqStruct.Consistent, reqStruct.SeqNum, colmfamily)
		var cRead bool
		if lastKey != "" {
			cRead = true
		} else {
			cRead = false
		}
		resultResponse = requestResponseLib.KVResponse{
			Prefix:       reqStruct.Key,
			ResultMap:    readResult,
			ContinueRead: cRead,
			Key:          lastKey,
			SeqNum:       seqNum,
			SnapMiss:     snapMiss,
		}
		readErr = err
	} else {
		log.Error("Invalid operation: ", reqStruct.Operation)
		return -1
	}

	log.Trace("Response trace : ", resultResponse)
	if readErr == nil {
		//Copy the encoded result in replyBuffer
		replySize, copyErr = nso.pso.CopyDataToBuffer(resultResponse,
			readArgs.ReplyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
	} else {
		log.Error(readErr)
	}

	log.Trace("Reply size: ", replySize)

	return replySize
}
