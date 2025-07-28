package libctlplanefuncs

//Define Snapshot XML structure
type SnapName struct {
	Name string `xml:"Name,attr"`
	Success bool `xml:"Success"`
}

type SnapResponseXML struct {
	SnapName  SnapName `xml:"Snap"`
}

type ChunkXML struct {
	Seq  uint64  `xml:"Seq,attr"`
}

type SnapXML struct {
	SnapName string `xml:"SName,attr"`
	Vdev    string `xml:"Vdev,attr"`
	Chunks	[]ChunkXML `xml:"Chunk"`
}
