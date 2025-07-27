package libctlplanefuncs

//Define Snapshot XML structure
type SnapName struct {
	Name string `xml:"Name,attr"`
	Sucess bool `xml:"Success"`
}

type SnapResponseXML struct {
	SnapName  SnapName `xml:"Snap"`
}

type ChunkXML struct {
	Seq  uint64  `xml:"Seq,attr"`
}

type VdevXML struct {
	VdevName string `xml:"Name,attr"`
	Chunks   []ChunkXML `xml:"Chunk"`
}

type SnapXML struct {
	SnapName string `xml:"Name,attr"`
	Vdevs    []VdevXML `xml:"Vdev"`
}
