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

type DeviceInfo struct {
	DevID        uuid.UUID
	NisdID      uuid.UUID
	SerialNumber string
	Status       uint16
	HyperVisorID string
	FailureDomain string
}

type Nisd struct {
	NisdID 		  uuid.UUID
	DevID         uuid.UUID
	ClientPort    uint16 `yaml:"client_port" xml:"ClientPort"`
	PeerPort      uint16 `yaml:"peer_port" xml:"PeerPort"`
	HyperVisorID  string
	FailureDomain string
	IPAddr        net.IP
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
