package srvctlplanefuncs

import (
 "fmt"
 "encoding/gob"
 "encoding/xml"
 "encoding/binary"
 log "github.com/sirupsen/logrus"
 PumiceDBServer "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
 funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
 ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
 "C"
 "bytes"
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
	log.Info("Key to be read : ",key)
	readResult, err := PumiceDBServer.PmdbReadKV(cbArgs.UserID, key, int64(len(key)), colmfamily)
	if err != nil {
		log.Error("Range read failure ", err)
		return nil, err
	}

	replySize, err := PumiceDBServer.PmdbCopyBytesToBuffer(readResult,  cbArgs.ReplyBuf)
	if err != nil {
		log.Error("Failed to Copy result in the buffer: %s", err)
		return nil, fmt.Errorf("failed to copy result to buffer: %v", err)
	}

	return replySize, nil
}

func WritePrepCreateSnap(args ...interface{}) (interface{}, error) {
	
	var Snap ctlplfl.SnapXML

	// Decode the input buffer into structure format
	err := xml.Unmarshal(args[0].([]byte), &Snap)
	if err != nil {
		return nil, err
	}

	commitChgs := make([]funclib.CommitChg, 0)
	for index, chunk := range Snap.Chunks {
		//Convert sequence number to little endian byte array format
		chSeq := make([]byte, 8)
		binary.LittleEndian.PutUint64(chSeq, chunk.Seq)

		// Schema: {vdev}/snap/{chunk}/{Seq} : {Ref count}
		// TODO: Change the dummy ref count
		chg := funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/snap/%d/%ld", Snap.Vdev, index,chSeq)),
			Value: []byte{uint8(1)},
		}
		commitChgs = append(commitChgs, chg)
	}

	// Schema: snap/{name}:{blob}
	chg := funclib.CommitChg{
		Key: []byte(fmt.Sprintf("snap/%s",Snap.SnapName)),
		Value: args[0].([]byte),
	}

	commitChgs = append(commitChgs, chg)

	//Fill the response structure
	snapResponse := ctlplfl.SnapResponseXML{
		SnapName: ctlplfl.SnapName{
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
