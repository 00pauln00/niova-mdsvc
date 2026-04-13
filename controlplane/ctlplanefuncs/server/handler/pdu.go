package handler

import (
	"fmt"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	srvctlplanefuncs "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server"

	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

func WPPDUCfg(args ...interface{}) (interface{}, error) {
	cpReq, ok := args[0].(ctlplfl.CPReq)
	if !ok {
		return ctlplfl.WPFuncError(fmt.Errorf("invalid argument: expecting type CPReq"))
	}
	pdu, ok := cpReq.Payload.(ctlplfl.PDU)
	if !ok {
		return ctlplfl.WPFuncError(fmt.Errorf("invalid payload: expecting type PDU"))
	}
	funcIntrm, err := srvctlplanefuncs.Server.PDUService.WPPDUCfg(cpReq.Token, pdu)
	if err != nil {
		if srvctlplanefuncs.IsAuthError(err) {
			return ctlplfl.WPAuthError(err)
		}
		return ctlplfl.WPFuncError(err)
	}
	return pmCmn.Encoder(pmCmn.GOB, *funcIntrm)
}

func ReadPDUCfg(args ...interface{}) (interface{}, error) {
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
	return srvctlplanefuncs.Server.PDUService.ReadPDUCfg(cbArgs, cpReq.Token, req)
}
