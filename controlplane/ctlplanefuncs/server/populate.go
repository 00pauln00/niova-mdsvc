package srvctlplanefuncs

import (
	"fmt"
	"strconv"

	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
)

// Population strategy
type PopulateInfra interface {
	Populate(entity Entity, commitChgs *[]funclib.CommitChg)
}

func PopulateEntities[T Entity](entity T, infra PopulateInfra) []funclib.CommitChg {
	commitChgs := make([]funclib.CommitChg, 0)
	infra.Populate(entity, &commitChgs)
	return commitChgs
}

type rackPopulator struct{}

func (rackPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg) {
	rack := entity.(*cpLib.Rack)
	*commitChgs = append(*commitChgs,
		funclib.CommitChg{
			Key: []byte(getConfKey(rackKey, rack.ID)),
		},
		funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", getConfKey(rackKey, rack.ID), pduKey)),
			Value: []byte(rack.PDUID),
		},
	)
}

type nisdPopulator struct{}

func (nisdPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg) {
	nisd := entity.(*cpLib.Nisd)
	key := getConfKey(nisdCfgKey, nisd.NisdID)

	// Schema: n_cfg/{nisdID}/{field} : {value}
	for _, field := range []string{DEVICE_NAME, CLIENT_PORT, PEER_PORT, hvKey, FAILURE_DOMAIN, IP_ADDR, TOTAL_SPACE, AVAIL_SPACE} {
		var value string
		switch field {
		case CLIENT_PORT:
			value = strconv.Itoa(int(nisd.ClientPort))
		case PEER_PORT:
			value = strconv.Itoa(int(nisd.PeerPort))
		case hvKey:
			value = nisd.HyperVisorID
		case FAILURE_DOMAIN:
			value = nisd.FailureDomain
		case IP_ADDR:
			value = nisd.IPAddr
		case TOTAL_SPACE:
			value = strconv.Itoa(int(nisd.TotalSize))
		case AVAIL_SPACE:
			value = strconv.Itoa(int(nisd.AvailableSize))
		case DEVICE_NAME:
			value = nisd.DevID
		default:
			continue
		}
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", key, field)),
			Value: []byte(value),
		})
	}
}

type devicePopulator struct{}

func (devicePopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg) {
	dev := entity.(*cpLib.Device)

	k := getConfKey(deviceCfgKey, dev.DevID)
	*commitChgs = append(*commitChgs, funclib.CommitChg{
		Key: []byte(fmt.Sprintf("%s", k)),
	})
	//Schema : /d/{devID}/{field} : {value}
	for _, field := range []string{NISD_ID, SERIAL_NUM, STATUS, hvKey, FAILURE_DOMAIN} {
		var value string
		switch field {
		case SERIAL_NUM:
			value = dev.SerialNumber
		case STATUS:
			value = strconv.Itoa(int(dev.Status))
		case hvKey:
			value = dev.HypervisorID
		case FAILURE_DOMAIN:
			value = dev.FailureDomain
		default:
			continue
		}

		if value == "" {
			continue
		}

		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", k, field)),
			Value: []byte(value),
		})
	}

	// Add parent to child relationship
	if dev.HypervisorID != "" {
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s/%s", hvKey, dev.HypervisorID, deviceCfgKey)),
			Value: []byte(dev.DevID),
		})
	}

	if dev.FailureDomain != "" {
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s/%s", FAILURE_DOMAIN, dev.FailureDomain, deviceCfgKey)),
			Value: []byte(dev.DevID),
		})
	}
}

type hvPopulator struct{}

func (hvPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg) {
	hv := entity.(*cpLib.Hypervisor)
	*commitChgs = append(*commitChgs, funclib.CommitChg{
		Key: []byte(getConfKey(hvKey, hv.ID)),
	},
		funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s/", getConfKey(hvKey, hv.ID), rackKey)),
			Value: []byte(hv.RackID),
		}, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", getConfKey(hvKey, hv.ID), IP_ADDR)),
			Value: []byte(hv.IPAddress),
		},
	)
}

type pduPopulator struct{}

func (pduPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg) {
	pdu := entity.(*cpLib.PDU)
	*commitChgs = append(*commitChgs, funclib.CommitChg{
		Key: []byte(getConfKey(pduKey, pdu.ID)),
	})
}
