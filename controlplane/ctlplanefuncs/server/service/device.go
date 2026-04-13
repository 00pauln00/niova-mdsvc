package service

import (
	"fmt"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"

	authz "github.com/00pauln00/niova-mdsvc/controlplane/authorizer"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	srvctlplanefuncs "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server"
	"github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server/repository"

	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

type DeviceService struct {
	*BaseService
	repo *repository.StorageRepository
}

func NewDeviceService(repo *repository.StorageRepository, base *BaseService) *DeviceService {
	return &DeviceService{BaseService: base, repo: repo}
}

func (s *DeviceService) WritePrepDeviceInfo(token string, dev ctlplfl.Device) (*funclib.FuncIntrm, error) {
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.WPDeviceInfo); err != nil {
		return nil, err
	}
	resp := ctlplfl.ResponseXML{Name: dev.ID, Success: true}
	commitChgs := srvctlplanefuncs.PopulateEntities(&dev, srvctlplanefuncs.DevicePopulator{}, srvctlplanefuncs.DeviceCfgKey)
	for _, pt := range dev.Partitions {
		ptCommits := srvctlplanefuncs.PopulateEntities(&pt, srvctlplanefuncs.PartitionPopulator{}, fmt.Sprintf("%s/%s/%s", srvctlplanefuncs.DeviceCfgKey, dev.ID, srvctlplanefuncs.PtKey))
		commitChgs = append(commitChgs, ptCommits...)
	}
	return &funclib.FuncIntrm{Changes: commitChgs, Response: resp}, nil
}

func (s *DeviceService) ReadDeviceInfo(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error) {
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.RdDeviceInfo); err != nil {
		return ctlplfl.AuthError(err)
	}
	key := srvctlplanefuncs.GetConfKey(srvctlplanefuncs.DeviceCfgKey, req.ID)
	readResult, err := s.repo.RangeReadPrefix(cbArgs.Store, key, cbArgs.ReplySize)
	if err != nil {
		log.Error("Range read failure ", err)
		return ctlplfl.FuncError(err)
	}
	deviceList := srvctlplanefuncs.ParseEntities[ctlplfl.Device](readResult.ResultMap, srvctlplanefuncs.DeviceWithPartitionParser{})
	log.Debugf("ReadDeviceInfo: returning device info for key %s", key)
	return ctlplfl.EncodeResponse(deviceList)
}

func (s *DeviceService) WritePrepPartition(token string, pt ctlplfl.DevicePartition) (*funclib.FuncIntrm, error) {
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.WPCreatePartition); err != nil {
		return nil, err
	}
	resp := ctlplfl.ResponseXML{Name: pt.PartitionID, Success: true}
	commitChgs := srvctlplanefuncs.PopulateEntities(&pt, srvctlplanefuncs.PartitionPopulator{}, srvctlplanefuncs.PtKey)
	devPTCommits := srvctlplanefuncs.PopulateEntities(&pt, srvctlplanefuncs.PartitionPopulator{}, fmt.Sprintf("%s/%s/%s", srvctlplanefuncs.DeviceCfgKey, pt.DevID, srvctlplanefuncs.PtKey))
	commitChgs = append(commitChgs, devPTCommits...)
	return &funclib.FuncIntrm{Changes: commitChgs, Response: resp}, nil
}

func (s *DeviceService) ReadPartition(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error) {
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.ReadPartition); err != nil {
		return ctlplfl.AuthError(err)
	}
	key := srvctlplanefuncs.PtKey
	if !req.GetAll {
		key = srvctlplanefuncs.GetConfKey(srvctlplanefuncs.PtKey, req.ID)
	}
	readResult, err := s.repo.RangeReadPrefix(cbArgs.Store, key, cbArgs.ReplySize)
	if err != nil {
		log.Error("Range read failure ", err)
		return ctlplfl.FuncError(err)
	}
	pt := srvctlplanefuncs.ParseEntities[ctlplfl.DevicePartition](readResult.ResultMap, srvctlplanefuncs.PtParser{})
	log.Debugf("ReadPartition: returning %d partition(s) for key %s", len(pt), key)
	return ctlplfl.EncodeResponse(pt)
}
