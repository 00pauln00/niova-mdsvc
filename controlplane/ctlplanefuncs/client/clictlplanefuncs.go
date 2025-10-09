package clictlplanefuncs

import (
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	sd "github.com/00pauln00/niova-pumicedb/go/pkg/utils/servicediscovery"
)

type EncodeType int

const (
	XML EncodeType = iota
	GoBinary
)

// Client side interferace for control plane functions
type CliCFuncs struct {
	appUUID  string
	writeSeq uint64
	sdObj    *sd.ServiceDiscoveryHandler
	encType  EncodeType
}

func InitCliCFuncs(appUUID string, key string, gossipConfigPath string) *CliCFuncs {
	ccf := CliCFuncs{
		appUUID:  appUUID,
		writeSeq: uint64(0),
		encType:  XML, // Default encoding type
	}

	ccf.sdObj = &sd.ServiceDiscoveryHandler{
		HTTPRetry: 10,
		SerfRetry: 5,
		RaftUUID:  key,
	}
	stop := make(chan int)
	log.Info("Staring Client API using gossip path: ", gossipConfigPath)
	go func() {
		err := ccf.sdObj.StartClientAPI(stop, gossipConfigPath)
		if err != nil {
			log.Fatal("Error while starting client API : ", err)
		}
	}()
	log.Info("Successfully initialized controlplane client: ", appUUID)

	return &ccf
}

func (ccf *CliCFuncs) encode(data interface{}) ([]byte, error) {
	if ccf.encType == GoBinary {
		return pmCmn.GobEncode(data)
	} else if ccf.encType == XML {
		return ctlplfl.XMLEncode(data)
	}
	return nil, errors.New("unsupported encoding type")
}

func (ccf *CliCFuncs) decode(bin []byte, st interface{}) error {
	if ccf.encType == GoBinary {
		return pmCmn.GobDecode(bin, st)
	} else if ccf.encType == XML {
		return ctlplfl.XMLDecode(bin, st)
	}
	return errors.New("unsupported decoding type")
}

func (ccf *CliCFuncs) request(rqb []byte, urla string, isWrite bool) ([]byte, error) {
	ccf.sdObj.TillReady("PROXY", 5)
	rsp, err := ccf.sdObj.Request(rqb, "/func?"+urla, isWrite)
	if err != nil {
		log.Error("failed to send request to server: ", err)
		return nil, err
	}
	return rsp, nil
}

func (ccf *CliCFuncs) _put(urla string, rqb []byte) ([]byte, error) {
	rncui := fmt.Sprintf("%s:0:0:0:%d", ccf.appUUID, ccf.writeSeq)
	ccf.writeSeq += 1
	urla += "&rncui=" + rncui
	rsb, err := ccf.request(rqb, urla, true)

	return rsb, err
}

func (ccf *CliCFuncs) put(data, resp interface{}, urla string) error {
	url := "name=" + urla
	rqb, err := ccf.encode(data)
	if err != nil {
		log.Error("failed to encode data: ", err)
		return err
	}

	rsb, err := ccf._put(url, rqb)
	if err != nil {
		log.Error("failed to send request(_put): ", err)
		return err
	}

	err = ccf.decode(rsb, resp)
	if err != nil {
		log.Error("failed to decode response: ", err)
		return err
	}

	return nil
}

func (ccf *CliCFuncs) get(data, resp interface{}, urla string) error {
	url := "name=" + urla
	rqb, err := ccf.encode(data)
	if err != nil {
		log.Error("failed to encode data: ", err)
		return err
	}

	rsb, err := ccf.request(rqb, url, false)
	if err != nil {
		log.Error("request failed: ", err)
		return err
	}
	err = ccf.decode(rsb, resp)
	if err != nil {
		log.Error("failed to decode response: ", err)
		return err
	}
	return nil
}

func (ccf *CliCFuncs) CreateSnap(vdev string, chunkSeq []uint64, snapName string) error {
	urla := fmt.Sprintf("%s=%s", ctlplfl.NAME, ctlplfl.CREATE_SNAP)

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

	rqb, err := ccf.encode(Snap)
	if err != nil {
		return err
	}

	rsb, err := ccf._put(urla, rqb)
	if err != nil {
		return err
	}

	var snapRes ctlplfl.SnapResponseXML
	err = ccf.decode(rsb, snapRes)
	if err != nil {
		return err
	}

	if !snapRes.SnapName.Success {
		return errors.New("Snap not created")
	}

	return nil
}

func (ccf *CliCFuncs) ReadSnapByName(name string) ([]byte, error) {
	urla := fmt.Sprintf("%s=%s", ctlplfl.NAME, ctlplfl.READ_SNAP_NAME)

	var snap ctlplfl.SnapXML
	snap.SnapName = name
	rqb, err := ccf.encode(snap)
	if err != nil {
		return nil, err
	}

	return ccf.request(rqb, urla, false)
}

func (ccf *CliCFuncs) ReadSnapForVdev(vdev string) ([]byte, error) {
	urla := fmt.Sprintf("%s=%s", ctlplfl.NAME, ctlplfl.READ_SNAP_VDEV)

	var snap ctlplfl.SnapXML
	snap.Vdev = vdev
	rqb, err := ccf.encode(snap)
	if err != nil {
		return nil, err
	}

	return ccf.request(rqb, urla, false)
}

func (ccf *CliCFuncs) PutDeviceInfo(device *ctlplfl.Device) (*ctlplfl.ResponseXML, error) {
	resp := &ctlplfl.ResponseXML{}
	err := ccf.put(device, resp, ctlplfl.PUT_DEVICE)
	if err != nil {
		log.Error("PutDeviceInfo failed: ", err)
		return nil, err
	}
	return resp, nil
}

// TODO make changes to use new GetRequest struct
func (ccf *CliCFuncs) GetDeviceInfo(req ctlplfl.GetReq) (*ctlplfl.Device, error) {
	dev := &ctlplfl.Device{}
	err := ccf.get(req, dev, ctlplfl.GET_DEVICE)
	if err != nil {
		log.Error("failed to fet device info: ", err)
		return nil, err
	}
	return dev, nil
}

func (ccf *CliCFuncs) PutNisdCfg(ncfg *ctlplfl.Nisd) (*ctlplfl.ResponseXML, error) {
	resp := &ctlplfl.ResponseXML{}
	err := ccf.put(ncfg, resp, ctlplfl.PUT_NISD)
	if err != nil {
		log.Error("PutNisdCfg failed: ", err)
		return nil, err
	}
	return resp, nil
}

func (ccf *CliCFuncs) GetNisdCfg(req ctlplfl.GetReq) (*ctlplfl.Nisd, error) {
	ncfg := &ctlplfl.Nisd{}
	err := ccf.get(req, ncfg, ctlplfl.GET_NISD)
	if err != nil {
		log.Error("failed to fet nisd info: ", err)
		return nil, err
	}
	return ncfg, nil
}

func (ccf *CliCFuncs) CreateVdev(vdev *ctlplfl.Vdev) error {
	return ccf.put(vdev.Size, vdev, ctlplfl.CREATE_VDEV)
}

func (ccf *CliCFuncs) PutPDU(req *ctlplfl.PDU) (*ctlplfl.ResponseXML, error) {
	resp := &ctlplfl.ResponseXML{}
	err := ccf.put(req, resp, ctlplfl.PUT_PDU)
	if err != nil {
		log.Error("PutPDUs failed: ", err)
		return nil, err
	}
	return resp, nil
}

func (ccf *CliCFuncs) GetPDUs(req *ctlplfl.GetReq) ([]ctlplfl.PDU, error) {
	pdus := make([]ctlplfl.PDU, 0)
	err := ccf.get(req, &pdus, ctlplfl.GET_PDU)
	if err != nil {
		log.Error("GetPDUs failed: ", err)
		return nil, err
	}
	return pdus, nil
}

func (ccf *CliCFuncs) PutRack(req *ctlplfl.Rack) (*ctlplfl.ResponseXML, error) {
	resp := &ctlplfl.ResponseXML{}
	err := ccf.put(req, resp, ctlplfl.PUT_RACK)
	if err != nil {
		log.Error("PutRack failed: ", err)
		return nil, err
	}
	return resp, nil
}

func (ccf *CliCFuncs) GetRacks(req *ctlplfl.GetReq) ([]ctlplfl.Rack, error) {
	racks := make([]ctlplfl.Rack, 0)
	err := ccf.get(req, &racks, ctlplfl.GET_RACK)
	if err != nil {
		log.Error("GetRacks failed: ", err)
		return nil, err
	}
	return racks, nil
}

func (ccf *CliCFuncs) PutHypervisor(req *ctlplfl.Hypervisor) (*ctlplfl.ResponseXML, error) {
	resp := &ctlplfl.ResponseXML{}
	err := ccf.put(req, resp, ctlplfl.PUT_HYPERVISOR)
	if err != nil {
		log.Error("PutHypervisor failed: ", err)
		return nil, err
	}

	return resp, nil
}

func (ccf *CliCFuncs) GetHypervisor(req *ctlplfl.GetReq) ([]ctlplfl.Hypervisor, error) {
	hypervisors := make([]ctlplfl.Hypervisor, 0)
	err := ccf.get(req, &hypervisors, ctlplfl.GET_HYPERVISOR)
	if err != nil {
		log.Error("GetHypervisor failed: ", err)
		return nil, err
	}

	return hypervisors, nil
}
