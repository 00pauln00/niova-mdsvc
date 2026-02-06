package srvctlplanefuncs

import (
	"C"
	"fmt"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	PumiceDBServer "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

func ApplyPDU(args ...interface{}) (interface{}, error) {
	pdu := args[0].(ctlplfl.PDU)
	cbargs := args[1].(*PumiceDBServer.PmdbCbArgs)

	resp := &ctlplfl.ResponseXML{
		ID:   pdu.ID,
		Name: pdu.Name,
	}

	if err := pdu.Validate(); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	if err := validateKey(cbargs, colmfamily, getConfKey(pduKey, pdu.ID), resp); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	commitChgs := PopulateEntities[*ctlplfl.PDU](&pdu, pduPopulator{}, pduKey)

	if err := applyKV(commitChgs, cbargs); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	resp.Success = true
	return pmCmn.Encoder(pmCmn.GOB, resp)
}

func ApplyRack(args ...interface{}) (interface{}, error) {
	rack := args[0].(ctlplfl.Rack)
	cbargs := args[1].(*PumiceDBServer.PmdbCbArgs)

	resp := &ctlplfl.ResponseXML{
		ID:   rack.ID,
		Name: rack.Name,
	}

	if err := rack.Validate(); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	if err := validateKey(cbargs, colmfamily, getConfKey(rackKey, rack.ID), resp); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	commitChgs := PopulateEntities[*ctlplfl.Rack](&rack, rackPopulator{}, rackKey)

	if err := applyKV(commitChgs, cbargs); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	resp.Success = true
	return pmCmn.Encoder(pmCmn.GOB, resp)
}

func ApplyHypervisor(args ...interface{}) (interface{}, error) {
	hv := args[0].(ctlplfl.Hypervisor)
	cbargs := args[1].(*PumiceDBServer.PmdbCbArgs)

	resp := &ctlplfl.ResponseXML{
		ID:   hv.ID,
		Name: hv.Name,
	}

	if err := hv.Validate(); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	if err := validateKey(cbargs, colmfamily, getConfKey(hvKey, hv.ID), resp); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	commitChgs := PopulateEntities[*ctlplfl.Hypervisor](&hv, hvPopulator{}, hvKey)

	if err := applyKV(commitChgs, cbargs); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	resp.Success = true
	return pmCmn.Encoder(pmCmn.GOB, resp)
}

func ApplyNisd(args ...interface{}) (interface{}, error) {
	nisd := args[0].(ctlplfl.Nisd)
	cbargs := args[1].(*PumiceDBServer.PmdbCbArgs)

	resp := &ctlplfl.ResponseXML{
		ID:   nisd.ID,
		Name: "NISD",
	}

	if err := nisd.Validate(); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	if err := validateKey(cbargs, colmfamily, getConfKey(NisdCfgKey, nisd.ID), resp); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	commitChgs := PopulateEntities[*ctlplfl.Nisd](&nisd, nisdPopulator{}, NisdCfgKey)

	if err := applyKV(commitChgs, cbargs); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	if err := HR.AddNisd(&nisd); err != nil {
		log.Error("AddNisd()", err)
	}

	resp.Success = true
	return pmCmn.Encoder(pmCmn.GOB, resp)
}

func ApplyDevice(args ...interface{}) (interface{}, error) {
	dev := args[0].(ctlplfl.Device)
	cbargs := args[1].(*PumiceDBServer.PmdbCbArgs)

	resp := &ctlplfl.ResponseXML{
		ID:   dev.ID,
		Name: dev.Name,
	}

	if err := dev.Validate(); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	if err := validateKey(cbargs, colmfamily, getConfKey(deviceCfgKey, dev.ID), resp); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	commitChgs := PopulateEntities[*ctlplfl.Device](&dev, devicePopulator{}, deviceCfgKey)

	for _, pt := range dev.Partitions {
		commitChgs = append(
			commitChgs,
			PopulateEntities[*ctlplfl.DevicePartition](
				&pt,
				partitionPopulator{},
				fmt.Sprintf("%s/%s/%s", deviceCfgKey, dev.ID, ptKey),
			)...,
		)
	}

	if err := applyKV(commitChgs, cbargs); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	resp.Success = true
	return pmCmn.Encoder(pmCmn.GOB, resp)
}

func ApplyPartition(args ...interface{}) (interface{}, error) {
	pt := args[0].(ctlplfl.DevicePartition)
	cbargs := args[1].(*PumiceDBServer.PmdbCbArgs)

	resp := &ctlplfl.ResponseXML{
		ID:   pt.PartitionID,
		Name: pt.DevID,
	}

	if err := pt.Validate(); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	if err := validateKey(cbargs, colmfamily, getConfKey(deviceCfgKey, pt.PartitionID), resp); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	commitChgs := PopulateEntities[*ctlplfl.DevicePartition](&pt, partitionPopulator{}, ptKey)

	commitChgs = append(
		commitChgs,
		PopulateEntities[*ctlplfl.DevicePartition](
			&pt,
			partitionPopulator{},
			fmt.Sprintf("%s/%s/%s", deviceCfgKey, pt.DevID, ptKey),
		)...,
	)

	if err := applyKV(commitChgs, cbargs); err != nil {
		resp.Error = err.Error()
		resp.Success = false
		return pmCmn.Encoder(pmCmn.GOB, resp)
	}

	resp.Success = true
	return pmCmn.Encoder(pmCmn.GOB, resp)
}
