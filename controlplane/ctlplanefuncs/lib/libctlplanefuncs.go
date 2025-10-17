package libctlplanefuncs

import (
	"encoding/gob"
	"fmt"

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
	GET_VDEV       = "GetVdev"
	CREATE_SNAP    = "CreateSnap"
	READ_SNAP_NAME = "ReadSnapByName"
	READ_SNAP_VDEV = "ReadSnapForVdev"
	PUT_PDU        = "PutPDU"
	GET_PDU        = "GetPDU"
	GET_RACK       = "GetRack"
	PUT_RACK       = "PutRack"
	GET_HYPERVISOR = "GetHypervisor"
	PUT_HYPERVISOR = "PutHypervisor"
	PUT_PARTITION  = "PutPartition"
	GET_PARTITION  = "GetPartition"
	CHUNK_SIZE     = 8 * 1024 * 1024 * 1024
	NAME           = "name"

	UNINITIALIZED = 1
	INITIALIZED   = 2
	RUNNING       = 3
	FAILED        = 4
	STOPPED       = 5
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
	ID           string `xml:"ID" json:"ID"`
	Name         string `xml:"Name" json:"Name"` // For display purposes
	DevicePath   string `xml:"device_path,omitempty" json:"DevicePath"`
	SerialNumber string `xml:"SerialNumber" json:"SerialNumber"`
	State        uint16 `xml:"State" json:"State"`
	Size         int64  `xml:"Size" json:"Size"`
	//Parent info
	HypervisorID  string `xml:"HyperVisorID" json:"HyperVisorID"`
	FailureDomain string `xml:"FailureDomain" json:"FailureDomain"`
	//Child info
	Partitions []DevicePartition `json:"partitions,omitempty"`
}

type DevicePartition struct {
	PartitionID   string `json:"partition_id"`
	PartitionPath string `json:"partition_path"`
	NISDUUID      string `json:"nisd_uuid"`
	DevID         string `json:"Dev_Id"`
	Size          int64  `json:"size,omitempty"`
}

type Nisd struct {
	ClientPort    uint16 `xml:"ClientPort" json:"ClientPort" yaml:"client_port"`
	PeerPort      uint16 `xml:"PeerPort" json:"PeerPort" yaml:"peer_port"`
	ID            string `xml:"ID" json:"ID" yaml:"uuid"`
	DevID         string `xml:"DevID" json:"DevID" yaml:"name"`
	HyperVisorID  string `xml:"HyperVisorID" json:"HyperVisorID" yaml:"-"`
	FailureDomain string `xml:"FailureDomain" json:"FailureDomain" yaml:"-"`
	IPAddr        string `xml:"IPAddr" json:"IPAddr" yaml:"-"`
	InitDev       bool   `yaml:"init"`
	TotalSize     int64  `xml:"TotalSize" yaml:"-"`
	AvailableSize int64  `xml:"AvailableSize" yaml:"-"`
}

type PDU struct {
	ID            string `xml:"ID" json:"ID" yaml:"uuid"`
	Name          string `xml:"Name" json:"Name" yaml:"name"`
	Location      string `xml:"Location" json:"Location" yaml:"location"`
	PowerCapacity string `xml:"PowerCap" json:"PowerCapacity" yaml:"powercap"`
	Specification string `xml:"Spec" json:"Spec" yaml:"spec"`
	Racks         []Rack `xml:"Racks>rack" json: "Racks" yaml:"racks"`
}

type Rack struct {
	ID            string // Unique rack identifier
	Name          string
	PDUID         string // Foreign key to PDU
	Location      string
	Specification string
	Hypervisors   []Hypervisor
}

type Hypervisor struct {
	ID        string // Unique hypervisor identifier
	RackID    string
	Name      string
	IPAddress string
	PortRange string
	SSHPort   string // SSH port for connection
	Dev       []Device
}

type NisdChunk struct {
	Nisd  Nisd
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
	NisdConfig []Nisd           `yaml:"nisd_config"`
}

// String returns a string representation of the Device
func (d Device) String() string {
	state := ""
	if d.State == INITIALIZED {
		state = " [INIT]"
		if len(d.Partitions) > 0 {
			state += fmt.Sprintf(" [%d partitions]", len(d.Partitions))
		}
	}
	if d.Size > 0 {
		return fmt.Sprintf("%s (%s)%s", d.ID, formatBytes(d.Size), state)
	}
	return fmt.Sprintf("%s%s", d.ID, state)
}

// formatBytes formats a byte count in human readable form
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func Count8GBChunks(size int64) int64 {
	rem := size % CHUNK_SIZE
	count := size / CHUNK_SIZE
	if rem == 0 {
		return count
	}
	return count + 1
}

func RegisterGOBStructs() {
	gob.Register(Rack{})
	gob.Register(GetReq{})
	gob.Register(Hypervisor{})
	gob.Register(PDU{})
	gob.Register(Nisd{})
	gob.Register(Device{})
	gob.Register(DevicePartition{})
	gob.Register(ResponseXML{})
	gob.Register(Vdev{})
	gob.Register(NisdChunk{})
	gob.Register(SnapResponseXML{})
	gob.Register(SnapXML{})
}
