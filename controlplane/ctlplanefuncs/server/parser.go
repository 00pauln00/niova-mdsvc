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

type ParseStrategy interface {
	RootKey() string
	NewEntity(id string) Entity
	ApplyField(entity Entity, parts []string, value []byte)
	Finalize(entity Entity) Entity
}

func ParseEntities[T any](readResult map[string][]byte, strategy ParseStrategy) []T {
	entityMap := make(map[string]Entity)

	for k, v := range readResult {
		parts := strings.Split(strings.Trim(k, "/"), "/")
		if len(parts) < 2 || parts[BASE_KEY] != strategy.RootKey() {
			continue
		}

		id := parts[BASE_UUID_PREFIX]
		entity, exists := entityMap[id]
		if !exists {
			entity = strategy.NewEntity(id)
			entityMap[id] = entity
		}

		strategy.ApplyField(entity, parts, v)
	}

	result := make([]T, 0, len(entityMap))
	for _, e := range entityMap {
		final := strategy.Finalize(e)
		result = append(result, final.(T))
	}
	return result
}

// Rack Strategy
type rackParser struct{}

func (rackParser) RootKey() string { return rackKey }
func (rackParser) NewEntity(id string) Entity {
	return &ctlplfl.Rack{ID: id}
}
func (rackParser) ApplyField(entity Entity, parts []string, value []byte) {
	r := entity.(*ctlplfl.Rack)
	if len(parts) == 3 && parts[2] == pduKey {
		r.PDUID = string(value)
	}
}
func (rackParser) Finalize(entity Entity) Entity { return *entity.(*ctlplfl.Rack) }

// Hypervisor Strategy
type hvParser struct{}

func (hvParser) RootKey() string { return hvKey }
func (hvParser) NewEntity(id string) Entity {
	return &ctlplfl.Hypervisor{ID: id}
}
func (hvParser) ApplyField(entity Entity, parts []string, value []byte) {
	hv := entity.(*ctlplfl.Hypervisor)
	if len(parts) == 3 {
		switch parts[2] {
		case rackKey:
			hv.RackID = string(value)
		case IP_ADDR:
			hv.IPAddress = string(value)
		}
	}
}
func (hvParser) Finalize(entity Entity) Entity { return *entity.(*ctlplfl.Hypervisor) }

// Device Strategy
type deviceParser struct{}

func (deviceParser) RootKey() string { return deviceCfgKey }
func (deviceParser) NewEntity(id string) Entity {
	return &ctlplfl.Device{DevID: id}
}
func (deviceParser) ApplyField(entity Entity, parts []string, value []byte) {
	dev := entity.(*ctlplfl.Device)
	if len(parts) == 3 {
		switch parts[2] {
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
func (deviceParser) Finalize(entity Entity) Entity { return *entity.(*ctlplfl.Device) }

// Device Strategy
type nisdParser struct{}

func (nisdParser) RootKey() string { return nisdCfgKey }

func (nisdParser) NewEntity(id string) Entity {
	return &ctlplfl.Nisd{NisdID: id}
}

func (nisdParser) ApplyField(entity Entity, parts []string, value []byte) {
	nisd := entity.(*ctlplfl.Nisd)
	if len(parts) == 3 {
		switch parts[2] {
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

func (nisdParser) Finalize(entity Entity) Entity { return *entity.(*ctlplfl.Nisd) }
