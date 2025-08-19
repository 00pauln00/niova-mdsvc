package clictlplanefuncs

import (
	"encoding/xml"
	"errors"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	sd "github.com/00pauln00/niova-pumicedb/go/pkg/utils/servicediscovery"
)

// Client side interferace for control plane functions
type CliCFuncs struct {
	appUUID       string
	writeSeq      uint64
	sdObj         *sd.ServiceDiscoveryHandler
	writePathLock sync.Mutex
}

func InitCliCFuncs(appUUID string, key string, gossipConfigPath string) *CliCFuncs {
	ccf := CliCFuncs{
		appUUID:  appUUID,
		writeSeq: uint64(0),
	}

	ccf.sdObj = &sd.ServiceDiscoveryHandler{
		HTTPRetry: 10,
		SerfRetry: 5,
		RaftUUID:  key,
	}
	stop := make(chan int)
	log.Info("Staring Client API using gossip path: ", gossipConfigPath)
	go ccf.sdObj.StartClientAPI(stop, gossipConfigPath)

	log.Info("Init CP functions successfull: ", appUUID)
	return &ccf
}

func (ccf *CliCFuncs) request(rqb []byte, urla string, isWrite bool) ([]byte, error) {
	ccf.sdObj.TillReady("PROXY", 5)
	log.Info("sending request to url: ", urla)
	rsp, err := ccf.sdObj.Request(rqb, "/func?"+urla, isWrite)
	if err != nil {
		log.Error("request failure: ", err)
		return nil, err
	}
	return rsp, nil
}

func (ccf *CliCFuncs) doWrite(urla string, rqb []byte) ([]byte, error) {
	ccf.writePathLock.Lock()
	defer ccf.writePathLock.Unlock()

	rncui := fmt.Sprintf("%s:0:0:0:%d", ccf.appUUID, ccf.writeSeq)
	ccf.writeSeq += 1
	urla += "&rncui=" + rncui
	rsb, err := ccf.request(rqb, urla, true)

	return rsb, err
}

func encode(data interface{}) ([]byte, error) {
	return xml.Marshal(data)
}

func decode(bin []byte, st interface{}) error {
	return xml.Unmarshal(bin, &st)
}

func (ccf *CliCFuncs) CreateSnap(vdev string, chunkSeq []uint64, snapName string) error {
	urla := "name=CreateSnap"

	chks := make([]ctlplfl.ChunkXML, 0)
	for idx, seq := range chunkSeq {
		chks = append(chks,
			ctlplfl.ChunkXML{
				Idx: uint32(idx),
				Seq: seq,
			})
	}
	var Snap ctlplfl.SnapXML
	Snap.SnapName = snapName
	Snap.Vdev = vdev
	Snap.Chunks = chks

	rqb, err := encode(Snap)
	if err != nil {
		return err
	}

	rsb, err := ccf.doWrite(urla, rqb)
	if err != nil {
		return err
	}

	var snapRes ctlplfl.SnapResponseXML
	err = decode(rsb, snapRes)
	if err != nil {
		return err
	}

	if !snapRes.SnapName.Success {
		return errors.New("Snap not created")
	}

	return nil
}

func (ccf *CliCFuncs) ReadSnapByName(name string) ([]byte, error) {
	urla := "name=ReadSnapByName"

	var snap ctlplfl.SnapXML
	snap.SnapName = name
	rqb, err := encode(snap)
	if err != nil {
		return nil, err
	}

	return ccf.request(rqb, urla, false)
}

func (ccf *CliCFuncs) ReadSnapForVdev(vdev string) ([]byte, error) {
	urla := "name=ReadSnapForVdev"

	var snap ctlplfl.SnapXML
	snap.Vdev = vdev
	rqb, err := encode(snap)
	if err != nil {
		return nil, err
	}

	return ccf.request(rqb, urla, false)
}

func (ccf *CliCFuncs) WriteNisd(nisd ctlplfl.Nisd) error {
	urla := "name=WriteNisd"

	rqb, err := ctlplfl.XMLEncode(nisd)
	if err != nil {
		return err
	}

	_, err = ccf.doWrite(urla, rqb)
	if err != nil {
		return err
	}

	return nil
}

func (ccf *CliCFuncs) WriteDevice(dev ctlplfl.DeviceInfo) error {
	urla := "name=WriteDevice"

	rqb, err := ctlplfl.XMLEncode(dev)
	if err != nil {
		log.Error("failed to encode device info: ", err)
		return err
	}

	_, err = ccf.doWrite(urla, rqb)
	if err != nil {
		log.Error("failed to write dev info: ", err)
		return err
	}

	return nil
}

func (ccf *CliCFuncs) GetDeviceUUID(device string) ([]byte, error) {
	key := "/d/" + device + "/n_"
	log.Info("get device uuid key:", key)
	return ccf.QueryPMDB(key, "ReadDeviceUUID")
}

func (ccf *CliCFuncs) PutDeviceCfg(device ctlplfl.DeviceInfo) error {
	var resp ctlplfl.ResponseXML

	res, err := ccf.QueryPMDB(device, "PutDeviceCfg")
	if err != nil {
		return err
	}

	err = ctlplfl.XMLDecode(res, &resp)
	if err != nil {
		log.Error("failed to decode device cfg: ", err)
		return err
	}

	if !resp.Success {
		log.Error("failed to read device cfg: ", resp.Name)
		return fmt.Errorf("failed to read device cfg: %s", resp.Name)
	}

	return nil
}

func (ccf *CliCFuncs) GetDeviceCfg(device string) (ctlplfl.DeviceInfo, error) {
	var dev ctlplfl.DeviceInfo
	var err error

	dev.DevID = device

	res, err := ccf.QueryPMDB(dev, "GetDeviceCfg")
	if err != nil {
		return dev, err
	}

	err = ctlplfl.XMLDecode(res, &dev)
	if err != nil {
		log.Error("failed to decode device cfg: ", err)
		return dev, err
	}
	log.Info("response from CP: ", dev)
	return dev, nil
}

func (ccf *CliCFuncs) PutNisdCfg(Nisd ctlplfl.Nisd) error {

	res, err := ccf.QueryPMDB(Nisd, "PutNisdCfg")
	if err != nil {
		return err
	}

	var resp ctlplfl.ResponseXML
	err = ctlplfl.XMLDecode(res, &resp)
	if err != nil {
		log.Error("failed to decode nisd cfg: ", err)
		return err
	}

	if !resp.Success {
		log.Error("failed to read nisd cfg: ", resp.Name)
		return fmt.Errorf("failed to read nisd cfg: %s", resp.Name)
	}

	return nil
}

func (ccf *CliCFuncs) GetNisdCfgs(NisdID string) (ctlplfl.Nisd, error) {

	var err error
	ni := ctlplfl.Nisd{}
	ni.NisdID = NisdID
	res, err := ccf.QueryPMDB(ni, "GetNisdCfg")
	if err != nil {
		return ni, err
	}

	err = ctlplfl.XMLDecode(res, &ni)
	if err != nil {
		log.Error("failed to decode nisd details: ", err)
		return ni, err
	}

	return ni, nil
}

func (ccf *CliCFuncs) QueryPMDB(key interface{}, method string) ([]byte, error) {

	urla := "name=" + method
	rqb, err := ctlplfl.XMLEncode(key)
	if err != nil {
		return nil, err
	}

	log.Debug("Sending request to CP on endpoint: ", urla)
	res, err := ccf.request(rqb, urla, false)
	if err != nil {
		return nil, err
	}

	log.Trace("query pmdb: ", res)
	return res, nil
}
