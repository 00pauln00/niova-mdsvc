package libctlplanefuncs

import (
	"bytes"
	"encoding/gob"
	"encoding/xml"
	"fmt"
)

const (
	PUT_DEVICE = "PutDeviceCfg"
	GET_DEVICE = "GetDeviceCfg"
	PUT_NISD   = "PutNisdCfg"
	GET_NISD   = "GetNisdCfg"
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
	ClientPort    uint16 `xml:"ClientPort" json:"ClientPort"`
	PeerPort      uint16 `xml:"PeerPort" json:"PeerPort"`
	NisdID        string `xml:"NisdID" json:"NisdID"`
	DevID         string `xml:"DevID" json:"DevID"`
	HyperVisorID  string `xml:"HyperVisorID" json:"HyperVisorID"`
	FailureDomain string `xml:"FailureDomain" json:"FailureDomain"`
	IPAddr        string `xml:"IPAddr" json:"IPAddr"`
}

// we need validation methods to check the nisdID
func (nisd *Nisd) GetKey() string {
	return fmt.Sprintf("/n/%s/cfg", nisd.NisdID)
}

// we need validation methods to check the deviceID
func (dev *DeviceInfo) GetKey() string {
	return fmt.Sprintf("/d/%s/cfg", dev.DevID)
}

func GobDecode(payload []byte, s interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(payload))
	return dec.Decode(s)
}

func GobEncode(s interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(s)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func XMLEncode(data interface{}) ([]byte, error) {
	return xml.MarshalIndent(data, "", " ")
}

func XMLDecode(bin []byte, st interface{}) error {
	return xml.Unmarshal(bin, &st)
}
