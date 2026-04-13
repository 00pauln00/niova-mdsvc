package handler

import (
	"fmt"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	srvctlplanefuncs "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server"

	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

func WPNisdCfg(args ...interface{}) (interface{}, error) {
	cpReq, ok := args[0].(ctlplfl.CPReq)
	if !ok {
		return nil, fmt.Errorf("invalid argument: expecting type CPReq")
	}
	nisd, ok := cpReq.Payload.(ctlplfl.Nisd)
	if !ok {
		return nil, fmt.Errorf("invalid payload: expecting type Nisd")
	}
	log.Debug("WPNisdCfg request received for NISD:", nisd.ID)

	funcIntrm, err := srvctlplanefuncs.Server.NisdService.WritePrep(cpReq.Token, nisd)
	if err != nil {
		log.Error("WPNisdCfg failed for NISD:", nisd.ID, " error:", err)
		if srvctlplanefuncs.IsAuthError(err) {
			return ctlplfl.WPAuthError(err)
		}
		return ctlplfl.WPFuncError(err)
	}

	log.Debug("WPNisdCfg successfully prepared response for NISD:", nisd.ID)
	return pmCmn.Encoder(pmCmn.GOB, *funcIntrm)
}

func ApplyNisd(args ...interface{}) (interface{}, error) {
	cbArgs, ok := args[1].(*pumiceserver.PmdbCbArgs)
	if !ok {
		return nil, fmt.Errorf("invalid cbargs argument")
	}

	cpReq, ok := args[0].(ctlplfl.CPReq)
	if !ok {
		return nil, fmt.Errorf("invalid argument: expecting type CPReq")
	}
	nisd, ok := cpReq.Payload.(ctlplfl.Nisd)
	if !ok {
		return nil, fmt.Errorf("invalid payload: expecting type Nisd")
	}

	intrm, err := srvctlplanefuncs.DecodeFuncIntrm(cbArgs.AppData, uint32(cbArgs.AppDataSize))
	if err != nil {
		return ctlplfl.FuncError(err)
	}

	resp, err := srvctlplanefuncs.Server.NisdService.Apply(cbArgs, nisd, intrm)
	if err != nil {
		return ctlplfl.FuncError(err)
	}
	return resp, nil
}

func ReadNisdConfig(args ...interface{}) (interface{}, error) {
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

	return srvctlplanefuncs.Server.NisdService.ReadConfig(cbArgs, cpReq.Token, req)
}

func ReadAllNisdConfigs(args ...interface{}) (interface{}, error) {
	cbArgs, ok := args[0].(*pumiceserver.PmdbCbArgs)
	if !ok {
		return nil, fmt.Errorf("invalid cbargs argument")
	}
	cpReq, ok := args[1].(ctlplfl.CPReq)
	if !ok {
		return nil, fmt.Errorf("invalid argument: expecting type CPReq")
	}
	return srvctlplanefuncs.Server.NisdService.ReadAllNisdConfigs(cbArgs, cpReq.Token)
}

func RdNisdArgs(args ...interface{}) (interface{}, error) {
	cbArgs, ok := args[0].(*pumiceserver.PmdbCbArgs)
	if !ok {
		return nil, fmt.Errorf("invalid cbargs argument")
	}
	cpReq, ok := args[1].(ctlplfl.CPReq)
	if !ok {
		return nil, fmt.Errorf("invalid argument: expecting type CPReq")
	}
	return srvctlplanefuncs.Server.NisdService.RdNisdArgs(cbArgs, cpReq.Token)
}

func WPNisdArgs(args ...interface{}) (interface{}, error) {
	cpReq, ok := args[0].(ctlplfl.CPReq)
	if !ok {
		return nil, fmt.Errorf("invalid argument: expecting type CPReq")
	}
	nisdArgs, ok := cpReq.Payload.(ctlplfl.NisdArgs)
	if !ok {
		return nil, fmt.Errorf("invalid payload: expecting type NisdArgs")
	}
	funcIntrm, err := srvctlplanefuncs.Server.NisdService.WPNisdArgs(cpReq.Token, nisdArgs)
	if err != nil {
		if srvctlplanefuncs.IsAuthError(err) {
			return ctlplfl.WPAuthError(err)
		}
		return ctlplfl.WPFuncError(err)
	}
	return pmCmn.Encoder(pmCmn.GOB, *funcIntrm)
}

func ReadChunkNisd(args ...interface{}) (interface{}, error) {
	cbargs, ok := args[0].(*pumiceserver.PmdbCbArgs)
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
	if err := req.ValidateRequest(); err != nil {
		log.Error("failed to validate request:", err)
		return ctlplfl.FuncError(err)
	}

	return srvctlplanefuncs.Server.NisdService.ReadChunkNisd(cbargs, cpReq.Token, req)
}
