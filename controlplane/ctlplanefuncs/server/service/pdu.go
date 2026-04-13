package service

import (
	log "github.com/00pauln00/niova-lookout/pkg/xlog"

	authz "github.com/00pauln00/niova-mdsvc/controlplane/authorizer"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	srvctlplanefuncs "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server"
	"github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server/repository"

	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

type PDUService struct {
	*BaseService
	repo *repository.StorageRepository
}

func NewPDUService(repo *repository.StorageRepository, base *BaseService) *PDUService {
	return &PDUService{
		BaseService: base,
		repo:        repo,
	}
}

func (s *PDUService) WPPDUCfg(token string, pdu ctlplfl.PDU) (*funclib.FuncIntrm, error) {
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.WPPDUCfg); err != nil {
		log.Errorf("WPPDUCfg: auth failure: %v", err)
		return nil, err
	}
	resp := &ctlplfl.ResponseXML{
		Name:    pdu.ID,
		Success: true,
	}
	commitChgs := srvctlplanefuncs.PopulateEntities(&pdu, srvctlplanefuncs.PduPopulator{}, srvctlplanefuncs.PduKey)
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: resp,
	}

	return &funcIntrm, nil
}

func (s *PDUService) ReadPDUCfg(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error) {
	key := srvctlplanefuncs.PduKey
	if !req.GetAll {
		key = srvctlplanefuncs.GetConfKey(srvctlplanefuncs.PduKey, req.ID)
	}
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.ReadPDUCfg); err != nil {
		log.Errorf("ReadPDUCfg: auth failure: %v", err)
		return ctlplfl.AuthError(err)
	}
	readResult, err := s.repo.RangeReadPrefix(cbArgs.Store, key, cbArgs.ReplySize)
	if err != nil {
		log.Error("Range read failure: ", err)
		return ctlplfl.FuncError(err)
	}
	pduList := srvctlplanefuncs.ParseEntities[ctlplfl.PDU](readResult.ResultMap, srvctlplanefuncs.PduParser{})
	log.Debugf("ReadPDUCfg: returning %d PDU config(s) for key %s", len(pduList), key)
	return ctlplfl.EncodeResponse(pduList)
}
