package libctlplanefuncs

import (
	"bytes"
	"encoding/gob"
	"encoding/xml"
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

type Device struct {
	DiskID string `xml:"DiskID" json:"DiskID"`
	NisdID string `xml:"NisdID" json:"NisdID"`
}

type DeviceInfo struct {
	Dev           Device `xml:"Device" json:"Device"`
	SerialNumber  string `xml:"SerialNumber" json:"SerialNumber"`
	Status        uint16 `xml:"Status" json:"Status"`
	HyperVisorID  string `xml:"HyperVisorID" json:"HyperVisorID"`
	FailureDomain string `xml:"FailureDomain" json:"FailureDomain"`
}

type Nisd struct {
	Dev           Device `xml:"Device" json:"Device"`
	ClientPort    uint16 `xml:"ClientPort" json:"ClientPort"`
	PeerPort      uint16 `xml:"PeerPort" json:"PeerPort"`
	HyperVisorID  string `xml:"HyperVisorID" json:"HyperVisorID"`
	FailureDomain string `xml:"FailureDomain" json:"FailureDomain"`
	IPAddr        string `xml:"IPAddr" json:"IPAddr"`
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
