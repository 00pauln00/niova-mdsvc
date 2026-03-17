package srvctlplanefuncs

import (
	"errors"

	authz "github.com/00pauln00/niova-mdsvc/controlplane/authorizer"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	userlib "github.com/00pauln00/niova-mdsvc/controlplane/user/lib"

	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	pumiceserver "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
	storageiface "github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/interface"
)

// ErrAuth represents an authentication or authorization failure returned by
// ValidateAndAuthorizeRBAC. Handlers use IsAuthError to distinguish auth
// errors from other types of errors and return appropriate responses to the client.
type ErrAuth struct{ Err error }

func (e *ErrAuth) Error() string { return e.Err.Error() }

// IsAuthError reports whether err is an ErrAuth.
func IsAuthError(err error) bool {
	var e *ErrAuth
	return errors.As(err, &e)
}

type IPDUService interface {
	WPPDUCfg(token string, pdu ctlplfl.PDU) (*funclib.FuncIntrm, error)
	ReadPDUCfg(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error)
}

type IRackService interface {
	WPRackCfg(token string, rack ctlplfl.Rack) (*funclib.FuncIntrm, error)
	ReadRackCfg(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error)
}

type IHypervisorService interface {
	WPHyperVisorCfg(token string, hypv ctlplfl.Hypervisor) (*funclib.FuncIntrm, error)
	ReadHyperVisorCfg(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error)
}

type IDeviceService interface {
	WritePrepDeviceInfo(token string, dev ctlplfl.Device) (*funclib.FuncIntrm, error)
	ReadDeviceInfo(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error)
	WritePrepPartition(token string, pt ctlplfl.DevicePartition) (*funclib.FuncIntrm, error)
	ReadPartition(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error)
}

type INisdService interface {
	WritePrep(token string, nisd ctlplfl.Nisd) (*funclib.FuncIntrm, error)
	WPNisdArgs(token string, nisdArgs ctlplfl.NisdArgs) (*funclib.FuncIntrm, error)
	Apply(cbArgs *pumiceserver.PmdbCbArgs, nisd ctlplfl.Nisd, intrm funclib.FuncIntrm) (interface{}, error)
	ReadConfig(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error)
	ReadAllNisdConfigs(cbArgs *pumiceserver.PmdbCbArgs, token string) (interface{}, error)
	RdNisdArgs(cbArgs *pumiceserver.PmdbCbArgs, token string) (interface{}, error)
	ReadChunkNisd(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error)
}

type IVdevService interface {
	WritePrep(token string, req ctlplfl.VdevReq) (*funclib.FuncIntrm, error)
	Apply(cbArgs *pumiceserver.PmdbCbArgs, req ctlplfl.VdevReq, intrm funclib.FuncIntrm) (interface{}, error)
	Delete(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.DeleteVdevReq) (interface{}, error)
	ReadInfo(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error)
	ReadAllInfo(cbArgs *pumiceserver.PmdbCbArgs) (interface{}, error)
	ReadInfoWithChunkMapping(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error)
}

type ISnapService interface {
	WritePrepCreateSnap(snap ctlplfl.SnapXML) (*funclib.FuncIntrm, error)
	ReadSnapByName(cbArgs *pumiceserver.PmdbCbArgs, snap ctlplfl.SnapXML) (interface{}, error)
	ReadSnapForVdev(cbArgs *pumiceserver.PmdbCbArgs, snap ctlplfl.SnapXML) (interface{}, error)
}

type IUserService interface {
	PutUser(token string, req userlib.UserReq, cbArgs *pumiceserver.PmdbCbArgs) (interface{}, error)
	GetUser(cbArgs *pumiceserver.PmdbCbArgs, token string, req userlib.GetReq) (interface{}, error)
	CreateAdminUser(req userlib.UserReq, cbArgs *pumiceserver.PmdbCbArgs) (interface{}, error)
	Login(cbArgs *pumiceserver.PmdbCbArgs, req ctlplfl.GetReq) (interface{}, error)
}

type ControlPlaneServer struct {
	Store             storageiface.DataStore
	Authorizer        *authz.Authorizer
	Hierarchy         *Hierarchy
	ColumnFamily      string
	PDUService        IPDUService
	RackService       IRackService
	HypervisorService IHypervisorService
	DeviceService     IDeviceService
	NisdService       INisdService
	VdevService       IVdevService
	SnapService       ISnapService
	UserService       IUserService
}

func NewControlPlaneServer(store storageiface.DataStore, columnFamily string) (*ControlPlaneServer, error) {
	authorizer := authz.NewAuthorizer()

	s := &ControlPlaneServer{
		Store:        store,
		Authorizer:   authorizer,
		Hierarchy:    &HR,
		ColumnFamily: columnFamily,
	}

	return s, nil
}

var Server *ControlPlaneServer
