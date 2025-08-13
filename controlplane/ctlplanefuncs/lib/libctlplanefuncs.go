package libctlplanefuncs

import (
	"bytes"
	"encoding/gob"
	"encoding/xml"

	"github.com/google/uuid"
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

type Nisd struct {
	DeviceID   string    `yaml:"name" xml:"DeviceID"`
	DeviceUUID uuid.UUID `yaml:"uuid" xml:"DeviceUUID"`
	ClientPort uint16    `yaml:"client_port" xml:"ClientPort"`
	PeerPort   uint16    `yaml:"peer_port" xml:"PeerPort"`
}

type NisdResponseXML struct {
	DeviceID string `xml:"DeviceID"`
	Success  bool
}

type DeviceInfo struct {
	ID           string
	UniqID       uuid.UUID
	SerialNumber string
	Status       uint16
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
	return xml.Marshal(data)
}

func XMLDecode(bin []byte, st interface{}) error {
	return xml.Unmarshal(bin, &st)
}
