package libctlplanefuncs

import (
	"errors"
	"net"

	"github.com/google/uuid"
)

func (p *PDU) Validate() error {

	if _, err := uuid.Parse(p.ID); err != nil {
		return errors.New("invalid UUID in PDU.ID")
	}

	return nil
}

func (r *Rack) Validate() error {
	if _, err := uuid.Parse(r.ID); err != nil {
		return errors.New("invalid UUID in Rack.ID")
	}

	if _, err := uuid.Parse(r.PDUID); err != nil {
		return errors.New("invalid UUID in Rack.PDUID")
	}

	return nil
}

func (d *Device) Validate() error {

	if _, err := uuid.Parse(d.HypervisorID); err != nil {
		return errors.New("invalid UUID in Device.HypervisorID")
	}

	for i := range d.Partitions {
		if err := d.Partitions[i].Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (p *DevicePartition) Validate() error {

	if _, err := uuid.Parse(p.NISDUUID); err != nil {
		return errors.New("invalid UUID in DevicePartition.NISDUUID")
	}

	return nil
}

func (h *Hypervisor) Validate() error {

	if _, err := uuid.Parse(h.ID); err != nil {
		return errors.New("invalid UUID in Hypervisor.ID")
	}

	if _, err := uuid.Parse(h.RackID); err != nil {
		return errors.New("invalid UUID in Hypervisor.RackID")
	}

	for _, ip := range h.IPAddrs {
		if net.ParseIP(ip) == nil {
			return errors.New("invalid IP address in Hypervisor.IPAddrs")
		}
	}

	return nil
}
