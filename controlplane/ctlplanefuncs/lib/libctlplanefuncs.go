package libctlplanefuncs

import (
	"encoding/xml"
	"fmt"

	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
)

const (
	PUT_DEVICE  = "PutDeviceCfg"
	GET_DEVICE  = "GetDeviceCfg"
	PUT_NISD    = "PutNisdCfg"
	GET_NISD    = "GetNisdCfg"
	CREATE_VDEV = "CreateVdev"
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
	NisdID        string `xml:"NisdID" json:"NisdID"`
	SerialNumber  string `xml:"SerialNumber" json:"SerialNumber"`
	Status        uint16 `xml:"Status" json:"Status"`
	HyperVisorID  string `xml:"HyperVisorID" json:"HyperVisorID"`
	FailureDomain string `xml:"FailureDomain" json:"FailureDomain"`
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

type NisdChunk struct {
	Nisd  *Nisd
	Chunk []int
}

type Vdev struct {
	VdevID       string
	NisdToChkMap map[string]NisdChunk
	Size         uint64
	NumChunks    uint32
	NumReplica   uint8
	NumDataBlk   uint8
	NumParityBlk uint8
}

// we need validation methods to check the nisdID
func (nisd *Nisd) GetConfKey() string {
	return fmt.Sprintf("/n/cfg/%s", nisd.NisdID)
}

// we need validation methods to check the deviceID
func (dev *DeviceInfo) GetConfKey() string {
	return fmt.Sprintf("/d/%s/cfg", dev.DevID)
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
