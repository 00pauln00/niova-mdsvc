package libctlplanefuncs

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"reflect"

	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	PUT_DEVICE     = "PutDeviceInfo"
	GET_DEVICE     = "GetDeviceInfo"
	PUT_NISD       = "PutNisdCfg"
	GET_NISD       = "GetNisdCfg"
	GET_NISD_LIST  = "GetAllNisd"
	CREATE_VDEV    = "CreateVdev"
	CREATE_SNAP    = "CreateSnap"
	READ_SNAP_NAME = "ReadSnapByName"
	READ_SNAP_VDEV = "ReadSnapForVdev"
	PUT_PDU        = "PutPDU"
	GET_PDU        = "GetPDU"
	GET_RACK       = "GetRack"
	PUT_RACK       = "PutRack"
	GET_HYPERVISOR = "GetHypervisor"
	PUT_HYPERVISOR = "PutHypervisor"
	CHUNK_SIZE     = 8 * 1024 * 1024 * 1024
	NAME           = "name"
)

// Define Snapshot XML structure
type SnapName struct {
	Name    string `xml:"Name,attr"`
	Success bool   `xml:"Success"`
}

type SnapResponseXML struct {
	SnapName SnapName `xml:"Snap"`
}

type ChunkXML struct {
	Idx uint32 `xml:Idx,attr`
	Seq uint64 `xml:"Seq"`
}

type SnapXML struct {
	SnapName string     `xml:"SName,attr"`
	Vdev     string     `xml:"Vdev,attr"`
	Chunks   []ChunkXML `xml:"Chunk"`
}

type ResponseXML struct {
	Name    string `xml:"name"`
	Success bool
}

type DeviceInfo struct {
	DevID         string `xml:"DevID" json:"DevID"`
	SerialNumber  string `xml:"SerialNumber" json:"SerialNumber"`
	Status        uint16 `xml:"Status" json:"Status"`
	//Parent info
	HypervisorID  string `xml:"HyperVisorID" json:"HyperVisorID"`
	FailureDomain string `xml:"FailureDomain" json:"FailureDomain"`
	//Child info
	NisdID        string `xml:"NisdID" json:"NisdID"`
}

type Nisd struct {
	ClientPort    uint16 `xml:"ClientPort" json:"ClientPort" yaml:"client_port"`
	PeerPort      uint16 `xml:"PeerPort" json:"PeerPort" yaml:"peer_port"`
	NisdID        string `xml:"NisdID" json:"NisdID" yaml:"uuid"`
	DevID         string `xml:"DevID" json:"DevID" yaml:"name"`
	HyperVisorID  string `xml:"HyperVisorID" json:"HyperVisorID" yaml:"-"`
	FailureDomain string `xml:"FailureDomain" json:"FailureDomain" yaml:"-"`
	IPAddr        string `xml:"IPAddr" json:"IPAddr" yaml:"-"`
	InitDev       bool   `yaml:"init"`
	TotalSize     int64  `xml:"TotalSize"`
	AvailableSize int64  `xml:"AvailableSize"`
}

type PDU struct {
	ID string // Unique identifier for the PDU
}

type Rack struct {
	ID    string // Unique rack identifier
	PDUId string // Foreign key to PDU
}

type Hypervisor struct {
	ID        string // Unique hypervisor identifier
	RackID    string
	IPAddress string
}

type NisdChunk struct {
	Nisd  *Nisd
	Chunk []int
}

type Vdev struct {
	VdevID       string
	NisdToChkMap []NisdChunk
	Size         int64
	NumChunks    uint32
	NumReplica   uint8
	NumDataBlk   uint8
	NumParityBlk uint8
}

type GetReq struct {
	ID     string
	GetAll bool
}

func (vdev *Vdev) Init() error {

	id, err := uuid.NewV7()
	if err != nil {
		log.Error("failed to generate uuid:", err)
		return err
	}
	vdev.VdevID = id.String()
	vdev.NumChunks = uint32(Count8GBChunks(vdev.Size))
	vdev.NumReplica = 1
	vdev.NumDataBlk = 0
	vdev.NumParityBlk = 0
	return nil
}

type s3Config struct {
	URL  string `yaml:"url"`
	Opts string `yaml:"opts"`
	Auth string `yaml:"auth"`
}

type NisdCntrConfig struct {
	S3Config   s3Config         `yaml:"s3_config"`
	Gossip     pmCmn.GossipInfo `yaml:"gossip"`
	NisdConfig []*Nisd          `yaml:"nisd_config"`
}

func XMLEncode(data interface{}) ([]byte, error) {
	return xml.MarshalIndent(data, "", " ")
}

func XMLDecode(bin []byte, st interface{}) error {
	return xml.Unmarshal(bin, &st)
}

func XMLDecodeAll(bin []byte, slice interface{}) error {
	dec := xml.NewDecoder(bytes.NewReader(bin))
	sliceVal := reflect.ValueOf(slice)
	if sliceVal.Kind() != reflect.Ptr || sliceVal.Elem().Kind() != reflect.Slice {
		return errors.New("slice must be a pointer to a slice")
	}
	elemType := sliceVal.Elem().Type().Elem()
	sliceVal = sliceVal.Elem()

	for {
		elemPtr := reflect.New(elemType)
		err := dec.Decode(elemPtr.Interface())
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		sliceVal.Set(reflect.Append(sliceVal, elemPtr.Elem()))
	}
	return nil
}

func Count8GBChunks(size int64) int64 {
	rem := size % CHUNK_SIZE
	count := size / CHUNK_SIZE
	if rem == 0 {
		return count
	}
	return count + 1
}
