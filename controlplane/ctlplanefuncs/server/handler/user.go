package handler

import (
	"fmt"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	srvctlplanefuncs "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server"
	userlib "github.com/00pauln00/niova-mdsvc/controlplane/user/lib"

	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

func WPPutUser(args ...interface{}) (interface{}, error) {
	cpReq, ok := args[0].(ctlplfl.CPReq)
	if !ok {
		return nil, fmt.Errorf("invalid argument: expecting type CPReq")
	}
	req, ok := cpReq.Payload.(userlib.UserReq)
	if !ok {
		return nil, fmt.Errorf("invalid payload: expecting type UserReq")
	}
	cbArgs, ok := args[1].(*pumiceserver.PmdbCbArgs)
	if !ok {
		return nil, fmt.Errorf("invalid cbargs argument")
	}
	return srvctlplanefuncs.Server.UserService.PutUser(cpReq.Token, req, cbArgs)
}

func RdGetUser(args ...interface{}) (interface{}, error) {
	cbArgs, ok := args[0].(*pumiceserver.PmdbCbArgs)
	if !ok {
		return nil, fmt.Errorf("invalid cbargs argument")
	}
	cpReq, ok := args[1].(ctlplfl.CPReq)
	if !ok {
		return nil, fmt.Errorf("invalid argument: expecting type CPReq")
	}
	req, ok := cpReq.Payload.(userlib.GetReq)
	if !ok {
		return nil, fmt.Errorf("invalid payload: expecting type GetReq")
	}
	return srvctlplanefuncs.Server.UserService.GetUser(cbArgs, cpReq.Token, req)
}

func APCreateAdminUser(args ...interface{}) (interface{}, error) {
	cpReq, ok := args[0].(ctlplfl.CPReq)
	if !ok {
		return nil, fmt.Errorf("invalid argument: expecting type CPReq")
	}
	req, ok := cpReq.Payload.(userlib.UserReq)
	if !ok {
		return nil, fmt.Errorf("invalid payload: expecting type UserReq")
	}
	cbArgs, ok := args[1].(*pumiceserver.PmdbCbArgs)
	if !ok {
		return nil, fmt.Errorf("invalid cbargs argument")
	}
	return srvctlplanefuncs.Server.UserService.CreateAdminUser(req, cbArgs)
}

func RdLogin(args ...interface{}) (interface{}, error) {
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
	return srvctlplanefuncs.Server.UserService.Login(cbArgs, req)
}
