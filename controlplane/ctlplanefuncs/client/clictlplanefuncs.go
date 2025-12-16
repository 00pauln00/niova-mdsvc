package clictlplanefuncs

import (
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	sd "github.com/00pauln00/niova-pumicedb/go/pkg/utils/servicediscovery"
)

// Client side interferace for control plane functions
type CliCFuncs struct {
	appUUID  string
	writeSeq uint64
	sdObj    *sd.ServiceDiscoveryHandler
	encType  pmCmn.Format
}

func InitCliCFuncs(appUUID string, key string, gossipConfigPath string) *CliCFuncs {
	ccf := CliCFuncs{
		appUUID:  appUUID,
		writeSeq: uint64(0),
		encType:  pmCmn.JSON, // Default encoding type
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
	rqb, err := pmCmn.Encoder(ccf.encType, data)
	if err != nil {
		log.Error("failed to encode data: ", err)
		return err
	}

	rsb, err := ccf._put(url, rqb)
	if err != nil {
		log.Error("failed to send request(_put): ", err)
		return err
	}
	if rsb == nil {
		return fmt.Errorf("failed to fetch response from control plane: %v", err)
	}

	err = pmCmn.Decoder(ccf.encType, rsb, resp)
	if err != nil {
		log.Error("failed to decode response in put: ", err)
		return err
	}

	return nil
}

func (ccf *CliCFuncs) get(data, resp interface{}, urla string) error {
	url := "name=" + urla
	rqb, err := pmCmn.Encoder(ccf.encType, data)
	if err != nil {
		log.Error("failed to encode data: ", err)
		return err
	}

	rsb, err := ccf.request(rqb, url, false)
	if err != nil {
		log.Error("request failed: ", err)
		return err
	}
	if rsb == nil {
		return fmt.Errorf("failed to fetch response from control plane: %v", err)
	}
	err = pmCmn.Decoder(ccf.encType, rsb, resp)
	if err != nil {
		log.Error("failed to decode response in get: ", err)
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

	rqb, err := pmCmn.Encoder(ccf.encType, Snap)
	if err != nil {
		return err
	}

	rsb, err := ccf._put(urla, rqb)
	if err != nil {
		return err
	}

	var snapRes ctlplfl.SnapResponseXML
	err = pmCmn.Decoder(ccf.encType, rsb, snapRes)
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
	rqb, err := pmCmn.Encoder(ccf.encType, snap)
	if err != nil {
		return nil, err
	}

	return ccf.request(rqb, urla, false)
}

func (ccf *CliCFuncs) ReadSnapForVdev(vdev string) ([]byte, error) {
	urla := fmt.Sprintf("%s=%s", ctlplfl.NAME, ctlplfl.READ_SNAP_VDEV)

	var snap ctlplfl.SnapXML
	snap.Vdev = vdev
	rqb, err := pmCmn.Encoder(ccf.encType, snap)
	if err != nil {
		return nil, err
	}

	return ccf.request(rqb, urla, false)
}

func (ccf *CliCFuncs) PutDevice(device *ctlplfl.Device) (*ctlplfl.ResponseXML, error) {
	resp := &ctlplfl.ResponseXML{}
	err := ccf.put(device, resp, ctlplfl.PUT_DEVICE)
	if err != nil {
		log.Error("PutDevice failed: ", err)
		return nil, err
	}
	return resp, nil
}

// TODO make changes to use new GetRequest struct
func (ccf *CliCFuncs) GetDevices(req ctlplfl.GetReq) ([]ctlplfl.Device, error) {
	dev := make([]ctlplfl.Device, 0)
	err := ccf.get(req, &dev, ctlplfl.GET_DEVICE)
	if err != nil {
		log.Error("failed to get device info: ", err)
		return nil, err
	}
	return dev, nil
}

func (ccf *CliCFuncs) PutNisd(ncfg *ctlplfl.Nisd) (*ctlplfl.ResponseXML, error) {
	resp := &ctlplfl.ResponseXML{}
	err := ccf.put(ncfg, resp, ctlplfl.PUT_NISD)
	if err != nil {
		log.Error("PutNisd failed: ", err)
		return nil, err
	}
	return resp, nil
}

func (ccf *CliCFuncs) GetNisds() ([]ctlplfl.Nisd, error) {
	ncfg := make([]ctlplfl.Nisd, 0)
	err := ccf.get(nil, &ncfg, ctlplfl.GET_NISD_LIST)
	if err != nil {
		log.Error("failed to fet nisd info: ", err)
		return nil, err
	}
	return ncfg, nil
}

func (ccf *CliCFuncs) GetNisd(req ctlplfl.GetReq) (*ctlplfl.Nisd, error) {
	ncfg := &ctlplfl.Nisd{}
	err := ccf.get(req, ncfg, ctlplfl.GET_NISD)
	if err != nil {
		log.Error("failed to fet nisd info: ", err)
		return nil, err
	}
	return ncfg, nil
}

func (ccf *CliCFuncs) CreateVdev(vdev *ctlplfl.Vdev) (*ctlplfl.ResponseXML, error) {
	resp := &ctlplfl.ResponseXML{}
	err := ccf.put(vdev, resp, ctlplfl.CREATE_VDEV)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (ccf *CliCFuncs) GetVdevsWithChunkInfo(req *ctlplfl.GetReq) ([]ctlplfl.Vdev, error) {
	vdevs := make([]ctlplfl.Vdev, 0)
	err := ccf.get(req, &vdevs, ctlplfl.GET_VDEV_CHUNK_INFO)
	if err != nil {
		log.Error("GetVdevsWithChunkInfo failed: ", err)
		return nil, err
	}

	return vdevs, nil
}

func (ccf *CliCFuncs) PutPartition(devp *ctlplfl.DevicePartition) (*ctlplfl.ResponseXML, error) {
	resp := &ctlplfl.ResponseXML{}
	err := ccf.put(devp, resp, ctlplfl.PUT_PARTITION)
	if err != nil {
		log.Error("Put Partition failed: ", err)
		return nil, err
	}
	return resp, nil
}

func (ccf *CliCFuncs) GetPartition(req ctlplfl.GetReq) ([]ctlplfl.DevicePartition, error) {
	pts := make([]ctlplfl.DevicePartition, 0)
	err := ccf.get(req, &pts, ctlplfl.GET_PARTITION)
	if err != nil {
		log.Error("Get Partition failed: ", err)
		return nil, err
	}
	return pts, nil
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

func (ccf *CliCFuncs) PutNisdArgs(req *ctlplfl.NisdArgs) (*ctlplfl.ResponseXML, error) {
	resp := &ctlplfl.ResponseXML{}
	err := ccf.put(req, resp, ctlplfl.PUT_NISD_ARGS)
	if err != nil {
		log.Error("PutNisdArgs failed: ", err)
		return nil, err
	}

	return resp, nil
}

func (ccf *CliCFuncs) GetNisdArgs() (ctlplfl.NisdArgs, error) {
	var args ctlplfl.NisdArgs
	url := "name=" + ctlplfl.GET_NISD_ARGS
	rsb, err := ccf.request(nil, url, false)
	if err != nil {
		log.Error("request failed: ", err)
		return args, err
	}
	if rsb == nil {
		return args, fmt.Errorf("failed to fetch response from control plane: %v", err)
	}
	err = pmCmn.Decoder(ccf.encType, rsb, &args)
	if err != nil {
		log.Error("failed to decode response: ", err)
		return args, err
	}

	return args, nil
}

func (ccf *CliCFuncs) GetVdevCfg(req *ctlplfl.GetReq) (ctlplfl.VdevCfg, error) {
	vdev := ctlplfl.VdevCfg{}
	err := ccf.get(req, &vdev, ctlplfl.GET_VDEV_INFO)
	if err != nil {
		log.Error("Read Vdev Cfg failed: ", err)
		return vdev, err
	}

	return vdev, nil
}

func (ccf *CliCFuncs) GetChunkNisd(req *ctlplfl.GetReq) (ctlplfl.ChunkNisd, error) {
	cn := ctlplfl.ChunkNisd{}
	log.Info("fetching chunk Info for:", req.ID)
	err := ccf.get(req, &cn, ctlplfl.GET_CHUNK_NISD)
	if err != nil {
		log.Error("GetHypervisor failed: ", err)
		return cn, err
	}

	return cn, nil
}
