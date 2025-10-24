package srvctlplanefuncs

import (
	"fmt"
	"strconv"

	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
)

// Population strategy
type PopulateInfra interface {
	Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string)
}

func PopulateEntities[T Entity](entity T, infra PopulateInfra, entityKey string) []funclib.CommitChg {
	commitChgs := make([]funclib.CommitChg, 0)
	infra.Populate(entity, &commitChgs, entityKey)
	return commitChgs
}

type rackPopulator struct{}

func (rackPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
	rack := entity.(*cpLib.Rack)
	key := getConfKey(entityKey, rack.ID)
	for _, field := range []string{NAME, LOCATION, SPEC, pduKey} {
		var value string
		switch field {
		case NAME:
			value = rack.Name
		case LOCATION:
			value = rack.Location
		case SPEC:
			value = rack.Specification
		case pduKey:
			value = rack.PDUID

		default:
			continue
		}
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", key, field)),
			Value: []byte(value),
		})
	}
}

type partitionPopulator struct{}

func (partitionPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
	pt := entity.(*cpLib.DevicePartition)
	key := getConfKey(entityKey, pt.PartitionID)
	for _, field := range []string{PARTITION_PATH, nisdKey, DEVICE_ID, SIZE} {
		var value string
		switch field {
		case PARTITION_PATH:
			value = pt.PartitionPath
		case nisdKey:
			value = pt.NISDUUID
		case DEVICE_ID:
			value = pt.DevID
		case SIZE:
			value = strconv.Itoa(int(pt.Size))
		default:
			continue
		}
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", key, field)),
			Value: []byte(value),
		})
	}
}

type nisdPopulator struct{}

func (nisdPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
	nisd := entity.(*cpLib.Nisd)
	key := getConfKey(entityKey, nisd.ID)

	// Schema: n_cfg/{nisdID}/{field} : {value}
	for _, field := range []string{DEVICE_ID, CLIENT_PORT, PEER_PORT, hvKey, FAILURE_DOMAIN, IP_ADDR, TOTAL_SPACE, AVAIL_SPACE} {
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
		case DEVICE_ID:
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

func (devicePopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
	dev := entity.(*cpLib.Device)

	k := getConfKey(entityKey, dev.ID)
	*commitChgs = append(*commitChgs, funclib.CommitChg{
		Key: []byte(fmt.Sprintf("%s", k)),
	})
	//Schema : /d/{devID}/{field} : {value}
	for _, field := range []string{NAME, DEVICE_PATH, STATE, SIZE, SERIAL_NUM, hvKey, FAILURE_DOMAIN} {
		var value string
		switch field {
		case SERIAL_NUM:
			value = dev.SerialNumber
		case STATE:
			value = strconv.Itoa(int(dev.State))
		case hvKey:
			value = dev.HypervisorID
		case FAILURE_DOMAIN:
			value = dev.FailureDomain
		case SIZE:
			value = strconv.Itoa(int(dev.Size))
		case DEVICE_PATH:
			value = dev.DevicePath
		case NAME:
			value = dev.Name

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
			Value: []byte(dev.ID),
		})
	}

	if dev.FailureDomain != "" {
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s/%s", FAILURE_DOMAIN, dev.FailureDomain, deviceCfgKey)),
			Value: []byte(dev.ID),
		})
	}
}

type hvPopulator struct{}

func (hvPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
	hv := entity.(*cpLib.Hypervisor)
	key := getConfKey(entityKey, hv.ID)
	for _, field := range []string{rackKey, NAME, IP_ADDR, PORT_RANGE, SSH_PORT} {
		var value string
		switch field {
		case NAME:
			value = hv.Name
		case IP_ADDR:
			value = hv.IPAddress
		case PORT_RANGE:
			value = hv.PortRange
		case SSH_PORT:
			value = hv.SSHPort
		case rackKey:
			value = hv.RackID

		default:
			continue
		}
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", key, field)),
			Value: []byte(value),
		})
	}
}

type pduPopulator struct{}

func (pduPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
	pdu := entity.(*cpLib.PDU)
	key := getConfKey(entityKey, pdu.ID)
	*commitChgs = append(*commitChgs,
		funclib.CommitChg{
			Key: []byte(getConfKey(pduKey, pdu.ID)),
		})
	for _, field := range []string{NAME, LOCATION, SPEC, POWER_CAP} {
		var value string
		switch field {
		case NAME:
			value = pdu.Name
		case LOCATION:
			value = pdu.Location
		case SPEC:
			value = pdu.Specification
		case POWER_CAP:
			value = pdu.PowerCapacity

		default:
			continue
		}
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", key, field)),
			Value: []byte(value),
		})
	}
}

type nisdArgsPopulator struct{}

func (nisdArgsPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
	args := entity.(*cpLib.NisdArgs)
	for _, field := range []string{DEFRAG, MBCCnt, MergeHCnt, MCIReadCache, S3, DSYNC, ALLOW_DEFRAG_MCIB_CACHE} {
		var value string
		switch field {
		case DEFRAG:
			value = strconv.FormatBool(args.Defrag)
		case MBCCnt:
			value = strconv.Itoa(args.MBCCnt)
		case MergeHCnt:
			value = strconv.Itoa(args.MergeHCnt)
		case MCIReadCache:
			value = strconv.Itoa(args.MCIBReadCache)
		case DSYNC:
			value = args.DSync
		case S3:
			value = args.S3
		case ALLOW_DEFRAG_MCIB_CACHE:
			value = strconv.FormatBool(args.AllowDefragMCIBCache)

		default:
			continue
		}
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", argsKey, field)),
			Value: []byte(value),
		})
	}
}
