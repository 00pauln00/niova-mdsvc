package libctlplanefuncs

import (
	"encoding/gob"
	"encoding/xml"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"hash/fnv"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"
	"github.com/google/uuid"
)

const (
	PUT_DEVICE          = "PutDevice"
	GET_DEVICE          = "GetDevice"
	PUT_NISD            = "PutNisd"
	GET_NISD            = "GetNisd"
	GET_NISD_LIST       = "GetAllNisd"
	CREATE_VDEV         = "CreateVdev"
	GET_VDEV_CHUNK_INFO = "GetVdevsWithChunkInfo"
	GET_VDEV            = "GetVdevs"
	CREATE_SNAP         = "CreateSnap"
	READ_SNAP_NAME      = "ReadSnapByName"
	READ_SNAP_VDEV      = "ReadSnapForVdev"
	PUT_PDU             = "PutPDU"
	GET_PDU             = "GetPDU"
	GET_RACK            = "GetRack"
	PUT_RACK            = "PutRack"
	GET_HYPERVISOR      = "GetHypervisor"
	PUT_HYPERVISOR      = "PutHypervisor"
	PUT_PARTITION       = "PutPartition"
	GET_PARTITION       = "GetPartition"
	GET_VDEV_INFO       = "get_vdev_info" // new
	GET_ALL_VDEV        = "get_all_vdev"
	GET_CHUNK_NISD      = "get_chunk_nisd"
	GET_NISD_INFO       = "get_nisd_info"

	PUT_NISD_ARGS  = "PutNisdArgs"
	GET_NISD_ARGS  = "GetNisdArgs"
	CHUNK_SIZE     = 8 * 1024 * 1024 * 1024
	MAX_REPLY_SIZE = 4 * 1024 * 1024
	NAME           = "name"

	UNINITIALIZED = 1
	INITIALIZED   = 2
	RUNNING       = 3
	FAILED        = 4
	STOPPED       = 5

	FD_ANY    = -1
	FD_PDU    = 0
	FD_RACK   = 1
	FD_HV     = 2
	FD_DEVICE = 3
	FD_MAX    = 4
	HASH_SIZE = 8
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
	ID      string `xml:"ID"`
	Error   string
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

type NisdArgs struct {
	Defrag               bool   // -g Defrag
	MBCCnt               int    // -m
	MergeHCnt            int    // -M
	MCIBReadCache        int    // -r
	S3                   string // -s
	DSync                string // -D
	AllowDefragMCIBCache bool   // -x
}

type NetworkInfo struct {
	IPAddr string
	Port   uint16
}

type Nisd struct {
	XMLName       xml.Name    `xml:"NisdInfo"`
	PeerPort      uint16      `xml:"PeerPort" json:"PeerPort"`
	ID            string      `xml:"ID" json:"ID"`
	FailureDomain []string    `xml:"FailureDomain"`
	TotalSize     int64       `xml:"TotalSize"`
	AvailableSize int64       `xml:"AvailableSize"`
	SocketPath    string      `xml:"SocketPath"`
	NetInfo       NetInfoList `xml:"NetInfo"`
	NetInfoCnt    int         `xml:"NetInfoCnt"`
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
	ID          string // Unique hypervisor identifier
	RackID      string
	Name        string
	IPAddrs     []string
	PortRange   string
	SSHPort     string // SSH port for connection
	Dev         []Device
	RDMAEnabled bool
}

type NisdChunk struct {
	Nisd  Nisd
	Chunk []int
}

type NisdVdevAlloc struct {
	AvailableSize int64
	Ptr           *Nisd
}

type VdevCfg struct {
	XMLName      xml.Name `xml:"Vdev"`
	ID           string
	Size         int64
	NumChunks    uint32
	NumReplica   uint8
	NumDataBlk   uint8
	NumParityBlk uint8
}

type Vdev struct {
	Cfg          VdevCfg
	NisdToChkMap []NisdChunk
}

type Filter struct {
	ID   string
	Type int
}

type VdevReq struct {
	Vdev   *VdevCfg
	Filter Filter
}

type GetReq struct {
	ID     string
	GetAll bool
}

func (vdev *VdevCfg) Init() error {

	id, err := uuid.NewV7()
	if err != nil {
		log.Error("failed to generate uuid:", err)
		return err
	}
	vdev.ID = id.String()
	vdev.NumChunks = uint32(Count8GBChunks(vdev.Size))
	vdev.NumDataBlk = 0
	vdev.NumParityBlk = 0
	return nil
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

func MatchIPs(a, b []string) bool {
	set := make(map[string]struct{}, len(a))
	for _, v := range a {
		set[v] = struct{}{}
	}
	for _, v := range b {
		if _, ok := set[v]; !ok {
			return false
		}
	}
	return true
}

type ChunkNisd struct {
	XMLName     xml.Name `xml:"ChunkNisd"`
	NumReplicas uint8    `xml:"NREPLICAS"`
	NisdUUIDs   string   `xml:"NISDs"`
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
	gob.Register(VdevCfg{})
	gob.Register(ChunkNisd{})
	gob.Register(NisdArgs{})
	gob.Register(NetworkInfo{})
	gob.Register(Filter{})
	gob.Register(VdevReq{})
}

func (req *GetReq) ValidateRequest() error {
	if req.ID == "" {
		return fmt.Errorf("Invalid Request: Recieved empty ID")
	}
	return nil

}

func (a *NisdArgs) BuildCmdArgs() string {
	var parts []string

	if a.Defrag {
		parts = append(parts, "-g")
	}
	if a.MBCCnt != 0 {
		parts = append(parts, "-m", strconv.Itoa(a.MBCCnt))
	}
	if a.MergeHCnt != 0 {
		parts = append(parts, "-M", strconv.Itoa(a.MergeHCnt))
	}
	if a.MCIBReadCache != 0 {
		parts = append(parts, "-r", strconv.Itoa(a.MCIBReadCache))
	}
	if a.S3 != "" {
		parts = append(parts, "-s", a.S3)
	}
	if a.DSync != "" {
		parts = append(parts, "-D", a.DSync)
	}
	if a.AllowDefragMCIBCache {
		parts = append(parts, "-x")
	}

	return strings.Join(parts, " ")
}

func NisdAllocHash(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}

func (n *Nisd) Validate() error {
	if _, err := uuid.Parse(n.ID); err != nil {
		return errors.New("invalid ID uuid")
	}

	if len(n.FailureDomain) != FD_MAX {
		return errors.New("invalid NISD failure domain info")
	}
	for i := 0; i < FD_DEVICE; i++ {
		if _, err := uuid.Parse(n.FailureDomain[i]); err != nil {
			return fmt.Errorf("invalid FailureDomain[%d] uuid", i)
		}
	}

	if n.AvailableSize > n.TotalSize {
		return errors.New("available Size exceeds total size")
	}

	if len(n.NetInfo) != n.NetInfoCnt {
		return fmt.Errorf("network interface cnt %d doesn't match with the total interface details provided %d", n.NetInfoCnt, len(n.NetInfo))
	}

	return nil
}

func NextFailureDomain(fd int) (int, error) {
	if fd < FD_DEVICE {
		fd++
		return fd, nil
	}
	return fd, fmt.Errorf("max failure domain reached: %d", fd)
}

type NetInfoList []NetworkInfo

func (n NetInfoList) MarshalText() ([]byte, error) {
	parts := make([]string, 0, len(n))
	for _, ni := range n {
		parts = append(parts, ni.IPAddr+":"+strconv.FormatUint(uint64(ni.Port), 10))
	}
	return []byte(strings.Join(parts, ", ")), nil
}

func (n *NetInfoList) UnmarshalText(text []byte) error {
	raw := strings.TrimSpace(string(text))
	if raw == "" {
		return nil
	}

	entries := strings.Split(raw, ",")
	for _, e := range entries {
		host, portStr, err := net.SplitHostPort(strings.TrimSpace(e))
		if err != nil {
			return err
		}

		p, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil {
			return err
		}

		*n = append(*n, NetworkInfo{
			IPAddr: host,
			Port:   uint16(p),
		})
	}
	return nil
}

func (hv *Hypervisor) GetPrimaryIP() (string, error) {
	if len(hv.IPAddrs) == 0 {
		return "", fmt.Errorf("invalid ip address")
	}
	return hv.IPAddrs[0], nil
}

func (hv *Hypervisor) ValidateIPs() error {
	if len(hv.IPAddrs) == 0 {
		return fmt.Errorf("no network info available")
	}

	for _, ip := range hv.IPAddrs {
		parsed := net.ParseIP(strings.TrimSpace(ip))
		if parsed != nil {
			return fmt.Errorf("invalid ip %s ", ip)
		}
	}
	return nil
}
