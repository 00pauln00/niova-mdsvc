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

type RackPopulator struct{}

func (RackPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
	rack := entity.(*cpLib.Rack)
	key := GetConfKey(entityKey, rack.ID)
	for _, field := range []string{NAME, LOCATION, SPEC, PduKey} {
		var value string
		switch field {
		case NAME:
			value = rack.Name
		case LOCATION:
			value = rack.Location
		case SPEC:
			value = rack.Specification
		case PduKey:
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

type PartitionPopulator struct{}

func (PartitionPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
	pt := entity.(*cpLib.DevicePartition)
	key := GetConfKey(entityKey, pt.PartitionID)
	for _, field := range []string{PARTITION_PATH, NisdKey, DEVICE_ID, SIZE} {
		var value string
		switch field {
		case PARTITION_PATH:
			value = pt.PartitionPath
		case NisdKey:
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

type NisdPopulator struct{}

func (NisdPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
	nisd := entity.(*cpLib.Nisd)
	key := GetConfKey(entityKey, nisd.ID)

	// Schema: n_cfg/{nisdID}/{field} : {value}
	for _, field := range []string{DEVICE_ID, PEER_PORT, HvKey, FAILURE_DOMAIN, TOTAL_SPACE, AVAIL_SPACE, SOCKET_PATH, PduKey, RackKey, PtKey, NETWORK_INFO_CNT} {
		var value string
		switch field {
		case PEER_PORT:
			value = strconv.Itoa(int(nisd.PeerPort))
		case HvKey:
			value = nisd.FailureDomain[cpLib.HV_IDX]
		case PduKey:
			value = nisd.FailureDomain[cpLib.PDU_IDX]
		case RackKey:
			value = nisd.FailureDomain[cpLib.RACK_IDX]
		case TOTAL_SPACE:
			value = strconv.Itoa(int(nisd.TotalSize))
		case AVAIL_SPACE:
			value = strconv.Itoa(int(nisd.AvailableSize))
		case DEVICE_ID:
			value = nisd.FailureDomain[cpLib.DEVICE_IDX]
		case SOCKET_PATH:
			value = nisd.SocketPath
		case PtKey:
			value = nisd.FailureDomain[cpLib.PARTITION_IDX]
		case NETWORK_INFO_CNT:
			value = strconv.Itoa(nisd.NetInfoCnt)
		default:
			continue
		}
		if value == "" {
			continue
		}
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", key, field)),
			Value: []byte(value),
		})
	}

	for _, ni := range nisd.NetInfo {
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s/%s", key, NETWORK_INFO, ni.IPAddr)),
			Value: []byte(strconv.Itoa(int(ni.Port))),
		})
	}
}

type DevicePopulator struct{}

func (DevicePopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
	dev := entity.(*cpLib.Device)

	k := GetConfKey(entityKey, dev.ID)
	*commitChgs = append(*commitChgs, funclib.CommitChg{
		Key: []byte(fmt.Sprintf("%s", k)),
	})
	//Schema : /d/{devID}/{field} : {value}
	for _, field := range []string{NAME, DEVICE_PATH, STATE, SIZE, SERIAL_NUM, HvKey, FAILURE_DOMAIN} {
		var value string
		switch field {
		case SERIAL_NUM:
			value = dev.SerialNumber
		case STATE:
			value = strconv.Itoa(int(dev.State))
		case HvKey:
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
			Key:   []byte(fmt.Sprintf("%s/%s/%s", HvKey, dev.HypervisorID, DeviceCfgKey)),
			Value: []byte(dev.ID),
		})
	}

	if dev.FailureDomain != "" {
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s/%s", FAILURE_DOMAIN, dev.FailureDomain, DeviceCfgKey)),
			Value: []byte(dev.ID),
		})
	}
}

type HvPopulator struct{}

func (HvPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
	hv := entity.(*cpLib.Hypervisor)
	key := GetConfKey(entityKey, hv.ID)
	for _, field := range []string{RackKey, NAME, IP_ADDR, PORT_RANGE, SSH_PORT, ENABLE_RDMA} {
		var value string
		switch field {
		case NAME:
			value = hv.Name
		case PORT_RANGE:
			value = hv.PortRange
		case SSH_PORT:
			value = hv.SSHPort
		case RackKey:
			value = hv.RackID
		case ENABLE_RDMA:
			value = strconv.FormatBool(hv.RDMAEnabled)

		default:
			continue
		}
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", key, field)),
			Value: []byte(value),
		})
	}
	for i, ip := range hv.IPAddrs {
		*commitChgs = append(*commitChgs, funclib.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s/%d", key, IP_ADDR, i)),
			Value: []byte(ip),
		})
	}
}

type PduPopulator struct{}

func (PduPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
	pdu := entity.(*cpLib.PDU)
	key := GetConfKey(entityKey, pdu.ID)
	*commitChgs = append(*commitChgs,
		funclib.CommitChg{
			Key: []byte(GetConfKey(PduKey, pdu.ID)),
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

type NisdArgsPopulator struct{}

func (NisdArgsPopulator) Populate(entity Entity, commitChgs *[]funclib.CommitChg, entityKey string) {
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
			Key:   []byte(fmt.Sprintf("%s/%s", ArgsKey, field)),
			Value: []byte(value),
		})
	}
}
