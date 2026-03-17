package handler

import (
	"fmt"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	srvctlplanefuncs "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server"

	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

func WPCreateVdev(args ...interface{}) (interface{}, error) {
	cpReq, ok := args[0].(ctlplfl.CPReq)
	if !ok {
		return ctlplfl.WPFuncError(fmt.Errorf("invalid argument: expecting type CPReq"))
	}
	req, ok := cpReq.Payload.(ctlplfl.VdevReq)
	if !ok {
		return ctlplfl.WPFuncError(fmt.Errorf("invalid payload: expecting type VdevReq"))
	}
	funcIntrm, err := srvctlplanefuncs.Server.VdevService.WritePrep(cpReq.Token, req)
	if err != nil {
		if srvctlplanefuncs.IsAuthError(err) {
			return ctlplfl.WPAuthError(err)
		}
		return ctlplfl.WPFuncError(err)
	}
	return pmCmn.Encoder(pmCmn.GOB, *funcIntrm)
}

func APCreateVdev(args ...interface{}) (interface{}, error) {
	cpReq, ok := args[0].(ctlplfl.CPReq)
	if !ok {
		return ctlplfl.InternalError(fmt.Errorf("invalid argument: expecting type CPReq"))
	}
	req, ok := cpReq.Payload.(ctlplfl.VdevReq)
	if !ok {
		return ctlplfl.InternalError(fmt.Errorf("invalid payload: expecting type VdevReq"))
	}
	cbArgs, ok := args[1].(*pumiceserver.PmdbCbArgs)
	if !ok {
		return ctlplfl.InternalError(fmt.Errorf("invalid cbargs argument"))
	}
	intrm, err := srvctlplanefuncs.DecodeFuncIntrm(cbArgs.AppData, uint32(cbArgs.AppDataSize))
	if err != nil {
		return ctlplfl.FuncError(fmt.Errorf("failed to decode apply changes: %v", err))
	}
	return srvctlplanefuncs.Server.VdevService.Apply(cbArgs, req, intrm)
}

func APDeleteVdev(args ...interface{}) (interface{}, error) {
	cpReq, ok := args[0].(ctlplfl.CPReq)
	if !ok {
		return nil, fmt.Errorf("invalid argument: expecting type CPReq")
	}
	req, ok := cpReq.Payload.(ctlplfl.DeleteVdevReq)
	if !ok {
		return nil, fmt.Errorf("invalid payload: expecting type DeleteVdevReq")
	}
	cbArgs, ok := args[1].(*pumiceserver.PmdbCbArgs)
	if !ok {
		return nil, fmt.Errorf("invalid cbargs argument")
	}
	return srvctlplanefuncs.Server.VdevService.Delete(cbArgs, cpReq.Token, req)
}

func ReadVdevInfo(args ...interface{}) (interface{}, error) {
	cbArgs, ok := args[0].(*pumiceserver.PmdbCbArgs)
	if !ok {
		return nil, fmt.Errorf("invalid cbargs argument")
	}
	cpReq, ok := args[1].(ctlplfl.CPReq)
	if !ok {
		return nil, fmt.Errorf("invalid argument: expecting type CPReq")
	}
	req, ok := cpReq.Payload.(ctlplfl.GetReq)
	if !ok {
		return nil, fmt.Errorf("invalid payload: expecting type GetReq")
	}
	return srvctlplanefuncs.Server.VdevService.ReadInfo(cbArgs, cpReq.Token, req)
}

func ReadAllVdevInfo(args ...interface{}) (interface{}, error) {
	cbArgs, ok := args[0].(*pumiceserver.PmdbCbArgs)
	if !ok {
		return nil, fmt.Errorf("invalid cbargs argument")
	}
	return srvctlplanefuncs.Server.VdevService.ReadAllInfo(cbArgs)
}

func ReadVdevsInfoWithChunkMapping(args ...interface{}) (interface{}, error) {
	cbArgs, ok := args[0].(*pumiceserver.PmdbCbArgs)
	if !ok {
		return nil, fmt.Errorf("invalid cbargs argument")
	}
	cpReq, ok := args[1].(ctlplfl.CPReq)
	if !ok {
		return nil, fmt.Errorf("invalid argument: expecting type CPReq")
	}
	req, ok := cpReq.Payload.(ctlplfl.GetReq)
	if !ok {
		return nil, fmt.Errorf("invalid payload: expecting type GetReq")
	}
	return srvctlplanefuncs.Server.VdevService.ReadInfoWithChunkMapping(cbArgs, cpReq.Token, req)
}
