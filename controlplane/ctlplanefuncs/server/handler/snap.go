package handler

import (
	"fmt"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	srvctlplanefuncs "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server"

	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"

	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

func WPCreateSnap(args ...interface{}) (interface{}, error) {
	cpReq, ok := args[0].(ctlplfl.CPReq)
	if !ok {
		return ctlplfl.WPFuncError(fmt.Errorf("invalid argument: expecting type CPReq"))
	}
	snap, ok := cpReq.Payload.(ctlplfl.SnapXML)
	if !ok {
		return ctlplfl.WPFuncError(fmt.Errorf("invalid payload: expecting type SnapXML"))
	}
	funcIntrm, err := srvctlplanefuncs.Server.SnapService.WritePrepCreateSnap(snap)
	if err != nil {
		return ctlplfl.WPFuncError(err)
	}
	return pmCmn.Encoder(pmCmn.GOB, *funcIntrm)
}

func ReadSnapByName(args ...interface{}) (interface{}, error) {
	cbArgs, ok := args[0].(*pumiceserver.PmdbCbArgs)
	if !ok {
		return nil, fmt.Errorf("invalid cbargs argument")
	}
	cpReq, ok := args[1].(ctlplfl.CPReq)
	if !ok {
		return nil, fmt.Errorf("invalid argument: expecting type CPReq")
	}
	snap, ok := cpReq.Payload.(ctlplfl.SnapXML)
	if !ok {
		return nil, fmt.Errorf("invalid payload: expecting type SnapXML")
	}
	return srvctlplanefuncs.Server.SnapService.ReadSnapByName(cbArgs, snap)
}

func ReadSnapForVdev(args ...interface{}) (interface{}, error) {
	cbArgs, ok := args[0].(*pumiceserver.PmdbCbArgs)
	if !ok {
		return nil, fmt.Errorf("invalid cbargs argument")
	}
	cpReq, ok := args[1].(ctlplfl.CPReq)
	if !ok {
		return nil, fmt.Errorf("invalid argument: expecting type CPReq")
	}
	snap, ok := cpReq.Payload.(ctlplfl.SnapXML)
	if !ok {
		return nil, fmt.Errorf("invalid payload: expecting type SnapXML")
	}
	return srvctlplanefuncs.Server.SnapService.ReadSnapForVdev(cbArgs, snap)
}
