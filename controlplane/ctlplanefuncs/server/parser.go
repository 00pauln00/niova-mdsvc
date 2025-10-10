package srvctlplanefuncs

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
)

const ( // Key Prefixes
	BASE_KEY         = 0
	BASE_UUID_PREFIX = 1
	ELEMENT_KEY      = 2
	KEY_LEN          = 3
)

func decode(payload []byte, s interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(payload))
	return dec.Decode(s)
}

func encode(s interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(s)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type Entity interface{}

type ParseEntity interface {
	GetRootKey() string
	NewEntity(id string) Entity
	ParseField(entity Entity, parts []string, value []byte)
	GetEntity(entity Entity) Entity
}

func ParseEntities[T Entity](readResult map[string][]byte, pe ParseEntity) []T {
	entityMap := make(map[string]Entity)

	for k, v := range readResult {
		parts := strings.Split(strings.Trim(k, "/"), "/")
		if len(parts) < ELEMENT_KEY || parts[BASE_KEY] != pe.GetRootKey() {
			continue
		}

		id := parts[BASE_UUID_PREFIX]
		entity, exists := entityMap[id]
		if !exists {
			entity = pe.NewEntity(id)
			entityMap[id] = entity
		}
		pe.ParseField(entity, parts, v)
	}

	result := make([]T, 0, len(entityMap))
	for _, e := range entityMap {
		final := pe.GetEntity(e)
		result = append(result, final.(T))
	}
	return result
}

// Rack parser
type rackParser struct{}

func (rackParser) GetRootKey() string { return rackKey }
func (rackParser) NewEntity(id string) Entity {
	return &ctlplfl.Rack{ID: id}
}
func (rackParser) ParseField(entity Entity, parts []string, value []byte) {
	r := entity.(*ctlplfl.Rack)
	if len(parts) == KEY_LEN && parts[ELEMENT_KEY] == pduKey {
		r.PDUID = string(value)
	}
}

func (rackParser) GetEntity(entity Entity) Entity { return *entity.(*ctlplfl.Rack) }

// Hypervisor parser
type hvParser struct{}

func (hvParser) GetRootKey() string { return hvKey }
func (hvParser) NewEntity(id string) Entity {
	return &ctlplfl.Hypervisor{ID: id}
}
func (hvParser) ParseField(entity Entity, parts []string, value []byte) {
	hv := entity.(*ctlplfl.Hypervisor)
	if len(parts) == KEY_LEN {
		switch parts[ELEMENT_KEY] {
		case rackKey:
			hv.RackID = string(value)
		case IP_ADDR:
			hv.IPAddress = string(value)
		}
	}
}

func (hvParser) GetEntity(entity Entity) Entity { return *entity.(*ctlplfl.Hypervisor) }

// Device parser
type deviceParser struct{}

func (deviceParser) GetRootKey() string { return deviceCfgKey }
func (deviceParser) NewEntity(id string) Entity {
	return &ctlplfl.Device{ID: id}
}
func (deviceParser) ParseField(entity Entity, parts []string, value []byte) {
	dev := entity.(*ctlplfl.Device)
	if len(parts) == KEY_LEN {
		switch parts[ELEMENT_KEY] {
		case hvKey:
			dev.HypervisorID = string(value)
		case NISD_ID:
			dev.NisdID = string(value)
		case SERIAL_NUM:
			dev.SerialNumber = string(value)
		case STATUS:
			status, _ := strconv.Atoi(string(value))
			dev.Status = uint16(status)
		case FAILURE_DOMAIN:
			dev.FailureDomain = string(value)
		}
	}
}
func (deviceParser) GetEntity(entity Entity) Entity { return *entity.(*ctlplfl.Device) }

// nisd parser
type nisdParser struct{}

func (nisdParser) GetRootKey() string { return nisdCfgKey }

func (nisdParser) NewEntity(id string) Entity {
	return &ctlplfl.Nisd{ID: id}
}

func (nisdParser) ParseField(entity Entity, parts []string, value []byte) {
	nisd := entity.(*ctlplfl.Nisd)
	if len(parts) == KEY_LEN {
		switch parts[ELEMENT_KEY] {
		case DEVICE_NAME:
			nisd.DevID = string(value)
		case CLIENT_PORT:
			p, _ := strconv.Atoi(string(value))
			nisd.ClientPort = uint16(p)
		case PEER_PORT:
			p, _ := strconv.Atoi(string(value))
			nisd.PeerPort = uint16(p)
		case hvKey:
			nisd.HyperVisorID = string(value)
		case FAILURE_DOMAIN:
			nisd.FailureDomain = string(value)
		case IP_ADDR:
			nisd.IPAddr = string(value)
		case TOTAL_SPACE:
			ts, _ := strconv.Atoi(string(value))
			nisd.TotalSize = int64(ts)
		case AVAIL_SPACE:
			as, _ := strconv.Atoi(string(value))
			nisd.AvailableSize = int64(as)
		}
	}
}
func (nisdParser) GetEntity(entity Entity) Entity { return *entity.(*ctlplfl.Nisd) }

type pduParser struct{}

func (pduParser) GetRootKey() string { return pduKey }
func (pduParser) NewEntity(id string) Entity {
	return &ctlplfl.PDU{ID: id}
}
func (pduParser) ParseField(entity Entity, parts []string, value []byte) {
	pdu := entity.(*ctlplfl.PDU)
	if len(parts) < KEY_LEN {
		return
	}
	pdu.ID = parts[BASE_UUID_PREFIX]
}
func (pduParser) GetEntity(entity Entity) Entity { return *entity.(*ctlplfl.PDU) }
