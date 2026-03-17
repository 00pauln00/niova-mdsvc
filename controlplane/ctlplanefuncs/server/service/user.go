package service

import (
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	userlib "github.com/00pauln00/niova-mdsvc/controlplane/user/lib"
	userserver "github.com/00pauln00/niova-mdsvc/controlplane/user/server"

	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

type UserService struct{}

func NewUserService() *UserService {
	return &UserService{}
}

func (s *UserService) PutUser(token string, req userlib.UserReq, cbArgs *pumiceserver.PmdbCbArgs) (interface{}, error) {
	cpReq := ctlplfl.CPReq{Token: token, Payload: req}
	return userserver.PutUser(cpReq, cbArgs)
}

func (s *UserService) GetUser(cbArgs *pumiceserver.PmdbCbArgs, token string, req userlib.GetReq) (interface{}, error) {
	cpReq := ctlplfl.CPReq{Token: token, Payload: req}
	return userserver.GetUser(cbArgs, cpReq)
}

func (s *UserService) CreateAdminUser(req userlib.UserReq, cbArgs *pumiceserver.PmdbCbArgs) (interface{}, error) {
	cpReq := ctlplfl.CPReq{Payload: req}
	return userserver.CreateAdminUser(cpReq, cbArgs)
}

func (s *UserService) Login(cbArgs *pumiceserver.PmdbCbArgs, req ctlplfl.GetReq) (interface{}, error) {
	cpReq := ctlplfl.CPReq{Payload: req}
	return userserver.Login(cbArgs, cpReq)
}
