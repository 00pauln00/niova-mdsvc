package libctlplanefuncs

import "github.com/google/uuid"

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
	DeviceID   string    `xml:"DeviceID"`
	DeviceUUID uuid.UUID `xml:"DeviceUUID"`
	ClientPort uint16    `xml:"ClientPort"`
	PeerPort   uint16    `xml:"PeerPort"`
}

type NisdResponseXML struct {
	DeviceID string `xml:"DeviceID"`
	Success  bool
}
