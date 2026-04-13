package service

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"

	authz "github.com/00pauln00/niova-mdsvc/controlplane/authorizer"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	srvctlplanefuncs "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server"
	"github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server/repository"

	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

type NisdService struct {
	*BaseService
	repo *repository.StorageRepository
}

func NewNisdService(repo *repository.StorageRepository, base *BaseService) *NisdService {
	return &NisdService{
		BaseService: base,
		repo:        repo,
	}
}

func (s *NisdService) WritePrep(token string, nisd ctlplfl.Nisd) (*funclib.FuncIntrm, error) {
	if err := nisd.Validate(); err != nil {
		return nil, err
	}
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.WPNisdCfg); err != nil {
		return nil, err
	}

	commitChgs := srvctlplanefuncs.PopulateEntities(&nisd, srvctlplanefuncs.NisdPopulator{}, srvctlplanefuncs.NisdCfgKey)
	resp := ctlplfl.ResponseXML{
		Name:    nisd.ID,
		Success: true,
	}

	return &funclib.FuncIntrm{Changes: commitChgs, Response: resp}, nil
}

func (s *NisdService) Apply(cbArgs *pumiceserver.PmdbCbArgs, nisd ctlplfl.Nisd, intrm funclib.FuncIntrm) (interface{}, error) {
	if cpResp, ok := intrm.Response.(ctlplfl.CPResp); ok && cpResp.Error != nil {
		log.Debugf("Apply: forwarding write-prep error: %s", cpResp.Error.Message)
		return pmCmn.Encoder(pmCmn.GOB, cpResp)
	}

	if err := s.repo.ApplyChanges(cbArgs.Store, intrm.Changes); err != nil {
		return nil, err
	}
	if err := s.hierarchy.AddNisd(&nisd); err != nil {
		return nil, err
	}
	resp, err := ctlplfl.EncodeResponse(intrm.Response)
	if err != nil {
		return ctlplfl.FuncError(fmt.Errorf("failed to encode response: %v", err))
	}
	return resp, nil
}

func (s *NisdService) ReadConfig(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error) {
	if err := req.ValidateRequest(); err != nil {
		return ctlplfl.FuncError(err)
	}
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.ReadNisdConfig); err != nil {
		return ctlplfl.AuthError(err)
	}
	key := srvctlplanefuncs.GetConfKey(srvctlplanefuncs.NisdCfgKey, req.ID)
	log.Trace("fetching nisd details for key : ", key)
	readResult, err := s.repo.RangeReadPrefix(cbArgs.Store, key, cbArgs.ReplySize)
	if err != nil {
		log.Error("Range read failure ", err)
		return ctlplfl.FuncError(err)
	}
	nisdList := srvctlplanefuncs.ParseEntities[ctlplfl.Nisd](readResult.ResultMap, srvctlplanefuncs.NisdParser{})
	if len(nisdList) == 0 {
		return ctlplfl.FuncError(fmt.Errorf("nisd not found for id: %s", req.ID))
	}
	return ctlplfl.EncodeResponse(nisdList[0])
}

func (s *NisdService) ReadAllNisdConfigs(cbArgs *pumiceserver.PmdbCbArgs, token string) (interface{}, error) {
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.ReadAllNisdConfigs); err != nil {
		return ctlplfl.AuthError(err)
	}
	readResult, err := s.repo.RangeReadPrefix(cbArgs.Store, srvctlplanefuncs.NisdCfgKey, cbArgs.ReplySize)
	if err != nil {
		log.Error("Range read failure ", err)
		return ctlplfl.FuncError(err)
	}
	nisdList := srvctlplanefuncs.ParseEntities[ctlplfl.Nisd](readResult.ResultMap, srvctlplanefuncs.NisdParser{})
	log.Debugf("ReadAllNisdConfigs: returning %d nisd configs", len(nisdList))
	return ctlplfl.EncodeResponse(nisdList)
}

func (s *NisdService) RdNisdArgs(cbArgs *pumiceserver.PmdbCbArgs, token string) (interface{}, error) {
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.RdNisdArgs); err != nil {
		log.Errorf("RdNisdArgs: auth failure: %v", err)
		return ctlplfl.AuthError(err)
	}
	readResult, err := s.repo.RangeReadPrefix(cbArgs.Store, srvctlplanefuncs.ArgsKey, cbArgs.ReplySize)
	if err != nil {
		log.Error("Range read failure: ", err)
		return ctlplfl.FuncError(err)
	}
	var nisdArgs ctlplfl.NisdArgs
	for k, v := range readResult.ResultMap {
		parts := strings.Split(strings.Trim(k, "/"), "/")
		if len(parts) < 2 {
			continue
		}
		switch parts[srvctlplanefuncs.BASE_UUID_PREFIX] {
		case srvctlplanefuncs.DEFRAG:
			nisdArgs.Defrag, _ = strconv.ParseBool(string(v))
		case srvctlplanefuncs.MBCCnt:
			nisdArgs.MBCCnt, _ = strconv.Atoi(string(v))
		case srvctlplanefuncs.MergeHCnt:
			nisdArgs.MergeHCnt, _ = strconv.Atoi(string(v))
		case srvctlplanefuncs.MCIReadCache:
			nisdArgs.MCIBReadCache, _ = strconv.Atoi(string(v))
		case srvctlplanefuncs.DSYNC:
			nisdArgs.DSync = string(v)
		case srvctlplanefuncs.S3:
			nisdArgs.S3 = string(v)
		case srvctlplanefuncs.ALLOW_DEFRAG_MCIB_CACHE:
			nisdArgs.AllowDefragMCIBCache, _ = strconv.ParseBool(string(v))
		}
	}
	return ctlplfl.EncodeResponse(nisdArgs)
}

func (s *NisdService) WPNisdArgs(token string, nisdArgs ctlplfl.NisdArgs) (*funclib.FuncIntrm, error) {
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.WPNisdArgs); err != nil {
		log.Errorf("WPNisdArgs: auth failure: %v", err)
		return nil, err
	}
	resp := &ctlplfl.ResponseXML{
		Name:    "nisd-args",
		Success: true,
	}
	commitChgs := srvctlplanefuncs.PopulateEntities(&nisdArgs, srvctlplanefuncs.NisdArgsPopulator{}, srvctlplanefuncs.ArgsKey)
	return &funclib.FuncIntrm{Changes: commitChgs, Response: resp}, nil
}

func (s *NisdService) ReadChunkNisd(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error) {
	tc, err := srvctlplanefuncs.ValidateToken(token)
	if err != nil {
		log.Errorf("ReadChunkNisd: token validation failed: %v", err)
		return ctlplfl.AuthError(err)
	}
	keys := strings.Split(strings.Trim(req.ID, "/"), "/")
	if len(keys) < 2 {
		log.Errorf("invalid request ID format %q: expected vdevID/chunkIndex", req.ID)
		return ctlplfl.FuncError(fmt.Errorf("invalid request ID format: expected vdevID/chunkIndex"))
	}

	vdevID, chunk := keys[0], keys[1]
	// Check authorization: ownership of the vdev implies access to its chunks
	if s.authorizer != nil {
		attributes := map[string]string{"vdev": vdevID}
		if !s.authorizer.Authorize(authz.ReadChunkNisd, tc.UserID, []string{tc.Role}, attributes, cbArgs.Store, srvctlplanefuncs.Colmfamily) {
			log.Errorf("user %s with role %s not authorized to read chunk nisd for vdev %s", tc.UserID, tc.Role, vdevID)
			return ctlplfl.AuthError(fmt.Errorf("authorization failed"))
		}
	}
	vcKey := path.Clean(srvctlplanefuncs.GetConfKey(srvctlplanefuncs.VdevKey, path.Join(vdevID, srvctlplanefuncs.ChunkKey, chunk))) + "/"
	log.Info("searching for key:", vcKey)
	rqResult, err := s.repo.RangeReadPrefix(cbArgs.Store, vcKey, cbArgs.ReplySize)
	if err != nil {
		log.Error("RangeReadKV failure: ", err)
		return ctlplfl.FuncError(err)
	}

	var ids []string
	for _, v := range rqResult.ResultMap {
		ids = append(ids, string(v))
	}
	chunkInfo := ctlplfl.ChunkNisd{
		NisdUUIDs:   strings.Join(ids, ","),
		NumReplicas: uint8(len(rqResult.ResultMap)),
	}

	log.Debugf("ReadChunkNisd: returning chunk-nisd info for vdev %s chunk %s", vdevID, chunk)
	return ctlplfl.EncodeResponse(chunkInfo)
}
