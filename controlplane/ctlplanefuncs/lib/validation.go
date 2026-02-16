package libctlplanefuncs

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net"

	"github.com/go-playground/validator/v10"
)

var validate = validator.New()

type PDU struct {
	ID            string `validate:"required,uuid"`
	Name          string `validate:"required"`
	Location      string `validate:"required"`
	PowerCapacity string `validate:"required"`
	Specification string `validate:"required"`
	Racks         []Rack `validate:"dive"`
}

type Rack struct {
	ID            string       `validate:"required,uuid"`
	Name          string       `validate:"required"`
	PDUID         string       `validate:"required,uuid"`
	Location      string       `validate:"required"`
	Specification string       `validate:"required"`
	Hypervisors   []Hypervisor `validate:"dive"`
}

type Hypervisor struct {
	ID          string   `validate:"required,uuid"`
	RackID      string   `validate:"required"`
	Name        string   `validate:"required,uuid"`
	IPAddrs     []string `validate:"required"`
	PortRange   string   `validate:"required"`
	SSHPort     uint16   `validate:"required"`
	Dev         []Device `validate:"dive"`
	RDMAEnabled bool
}

type Device struct {
	ID            string            `validate:"required,uuid`
	Name          string            `validate:"required,min=1,max=128"`
	DevicePath    string            `validate:"omitempty,startswith=/"`
	SerialNumber  string            `validate:"required"`
	State         uint16            `validate:"required"`
	Size          int64             `validate:"required,gt=0"`
	HypervisorID  string            `validate:"required,uuid"`
	FailureDomain string            `validate:"required"`
	Partitions    []DevicePartition `validate:"dive"`
}

type DevicePartition struct {
	PartitionID   string `validate:"required"`
	PartitionPath string `validate:"required,startswith=/"`
	NISDUUID      string `validate:"required,uuid"`
	DevID         string `validate:"required,uuid"`
	Size          int64  `validate:"required,gt=0"`
}

type Nisd struct {
	XMLName       xml.Name    `xml:"NisdInfo" json:"-"`
	PeerPort      uint16      `xml:"PeerPort" json:"PeerPort" validate:"required,gt=0,lt=65536"`
	ID            string      `xml:"ID" json:"ID" validate:"required,uuid"`
	FailureDomain []string    `xml:"FailureDomain" json:"FailureDomain" validate:"required,len=4,dive,required"`
	TotalSize     int64       `xml:"TotalSize" json:"TotalSize" validate:"required,gt=0"`
	AvailableSize int64       `xml:"AvailableSize" json:"AvailableSize" validate:"required,gte=0,ltefield=TotalSize"`
	SocketPath    string      `xml:"SocketPath" json:"SocketPath" validate:"required,startswith=/"`
	NetInfo       NetInfoList `xml:"NetInfo" json:"NetInfo" validate:"required"`
	NetInfoCnt    int         `xml:"NetInfoCnt" json:"NetInfoCnt" validate:"required,eqfield=NetInfo.Len"`
}

type NetworkInfo struct {
	IPAddr string `validate:"required"`
	Port   uint16 `validate:"required,gt=0"`
}

func (p *PDU) Validate() error {
	if err := validate.Struct(p); err != nil {
		return err
	}

	for i := range p.Racks {
		if err := p.Racks[i].Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (r *Rack) Validate() error {
	if err := validate.Struct(r); err != nil {
		return err
	}

	for i := range r.Hypervisors {
		if err := r.Hypervisors[i].Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (h *Hypervisor) Validate() error {
	if err := validate.Struct(h); err != nil {
		return err
	}

	for i := range h.Dev {
		if err := h.Dev[i].Validate(); err != nil {
			return err
		}
	}

	for _, ip := range h.IPAddrs {
		if net.ParseIP(ip) == nil {
			return errors.New("invalid IP address in Hypervisor.IPAddrs")
		}
	}
	return nil
}

func (d *Device) Validate() error {
	if err := validate.Struct(d); err != nil {
		return err
	}

	for i := range d.Partitions {
		if err := d.Partitions[i].Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (p *DevicePartition) Validate() error {
	return validate.Struct(p)
}

func (n *Nisd) Validate() error {
	if err := validate.Struct(n); err != nil {
		return err
	}

	// NetInfo count must match actual entries
	if n.NetInfoCnt != len(n.NetInfo) {
		return fmt.Errorf(
			"netinfo count mismatch: NetInfoCnt=%d, actual=%d",
			n.NetInfoCnt,
			len(n.NetInfo),
		)
	}

	// validate each NetInfo entry (if it supports Validate)
	for i := range n.NetInfo {
		if v, ok := any(&n.NetInfo[i]).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return fmt.Errorf("NetInfo[%d]: %w", i, err)
			}
		}
	}

	return nil
}

func (req *GetReq) ValidateRequest() error {
	if req.ID == "" {
		return fmt.Errorf("Invalid Request: Recieved empty ID")
	}
	return nil

}
