package clictlplanefuncs

import (
	"fmt"
	"strconv"

	log "github.com/sirupsen/logrus"

	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
)

func mergeInto(dst map[string]map[string]interface{}, src map[string]interface{}) {
	for k, inner := range src {
		srcInner, ok := inner.(map[string]interface{})
		if !ok {
			continue
		}

		dstInner, ok := dst[k]
		if !ok {
			dstInner = make(map[string]interface{}, len(srcInner))
			dst[k] = dstInner
		}

		for ik, iv := range srcInner {
			existing, ok := dstInner[ik]
			if !ok {
				dstInner[ik] = iv
				continue
			}

			switch ex := existing.(type) {

			case []interface{}:
				// existing is already a slice
				switch v := iv.(type) {
				case []interface{}:
					// flatten
					dstInner[ik] = append(ex, v...)
				default:
					dstInner[ik] = append(ex, v)
				}

			default:
				// existing is scalar
				switch v := iv.(type) {
				case []interface{}:
					// existing + flattened v
					s := make([]interface{}, 0, 1+len(v))
					s = append(s, ex)
					s = append(s, v...)
					dstInner[ik] = s
				default:
					// existing + iv, both scalars
					dstInner[ik] = []interface{}{ex, v}
				}
			}
		}
	}
}

func (ccf *CliCFuncs) getAll(req *cpLib.GetReq, urla string) (*map[string]map[string]interface{}, error) {
	resp := &cpLib.Response{}
	res := make(map[string]map[string]interface{}, 0)
	for {
		url := "name=" + urla
		rqb, err := pmCmn.Encoder(ccf.encType, req)
		if err != nil {
			log.Error("failed to encode data: ", err)
			return &res, err
		}

		rsb, err := ccf.request(rqb, url, false)
		if err != nil {
			log.Error("request failed: ", err)
			return &res, err
		}
		if rsb == nil {
			return &res, fmt.Errorf("failed to fetch response from control plane: %v", err)
		}
		err = pmCmn.Decoder(ccf.encType, rsb, resp)
		if err != nil {
			log.Error("failed to decode response in get: ", err)
			return &res, err
		}
		mergeInto(res, resp.Result.(map[string]interface{}))
		if resp.LastKey == "" {
			break
		}
		req.LastKey = resp.LastKey
		req.SeqNum = resp.SeqNum

	}

	return &res, nil
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

func (ccf *CliCFuncs) GetPDUs(req *cpLib.GetReq) ([]cpLib.PDU, error) {
	pdus := make([]cpLib.PDU, 0)
	err := ccf.get(req, &pdus, cpLib.GET_PDU)
	if err != nil {
		log.Error("GetPDUs failed: ", err)
		return nil, err
	}
	return pdus, nil
}

// TODO make changes to use new GetRequest struct
func (ccf *CliCFuncs) GetDeviceInfo(req cpLib.GetReq) ([]cpLib.Device, error) {
	dev := make([]cpLib.Device, 0)
	err := ccf.get(req, &dev, cpLib.GET_DEVICE)
	if err != nil {
		log.Error("failed to get device info: ", err)
		return nil, err
	}
	return dev, nil
}

func (ccf *CliCFuncs) GetNisdCfg(req cpLib.GetReq) ([]cpLib.Nisd, error) {
	ncfg := make([]cpLib.Nisd, 0)
	err := ccf.get(req, &ncfg, cpLib.GET_NISD)
	if err != nil {
		log.Error("failed to fet nisd info: ", err)
		return nil, err
	}
	return ncfg, nil
}

func (ccf *CliCFuncs) GetVdevs(req *cpLib.GetReq) ([]cpLib.Vdev, error) {
	vdevs := make([]cpLib.Vdev, 0)
	err := ccf.get(req, &vdevs, cpLib.GET_VDEV)
	if err != nil {
		log.Error("GetHypervisor failed: ", err)
		return nil, err
	}

	return vdevs, nil
}

func (ccf *CliCFuncs) GetRacks(req *cpLib.GetReq) ([]cpLib.Rack, error) {
	racks := make([]cpLib.Rack, 0)
	err := ccf.get(req, &racks, cpLib.GET_RACK)
	if err != nil {
		log.Error("GetRacks failed: ", err)
		return nil, err
	}
	return racks, nil
}

func (ccf *CliCFuncs) GetHypervisor(req *cpLib.GetReq) (map[string]Entity, error) {
	acc, err := ccf.getAll(req, cpLib.GET_HYPERVISOR)
	if err != nil {
		log.Error("GetHypervisor failed: ", err)
		return nil, err
	}
	res := ParseEntities[cpLib.Hypervisor](acc, hv{})
	log.Info("Hyper Visor:", res)
	return res, nil
}

func (ccf *CliCFuncs) GetVdevCont() (map[string]*cpLib.Vdev, error) {
	req := &cpLib.GetReq{
		GetAll:       true,
		IsConsistent: true,
	}

	// accumulate all pages into this

	acc, err := ccf.getAll(req, cpLib.GET_VDEV_CONT)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*cpLib.Vdev, len(*acc))

	for vdevID, flat := range *acc {
		vd := &cpLib.Vdev{
			VdevID:       vdevID,
			NisdToChkMap: make([]cpLib.NisdChunk, 0),
		}

		for k, raw := range flat {
			switch k {
			case cpLib.SIZE:
				if s, ok := raw.(string); ok {
					if n, err := strconv.ParseInt(s, 10, 64); err == nil {
						vd.Size = n
					}
				}

			case cpLib.NUM_CHUNKS:
				if s, ok := raw.(string); ok {
					if n, err := strconv.ParseInt(s, 10, 64); err == nil {
						vd.NumChunks = uint32(n)
					}
				}

			case cpLib.NUM_REPLICAS:
				if s, ok := raw.(string); ok {
					if n, err := strconv.ParseInt(s, 10, 64); err == nil {
						vd.NumReplica = uint8(n)
					}
				}

			default:
				var ints []int

				switch t := raw.(type) {
				case []interface{}:
					ints = make([]int, 0, len(t))
					for _, x := range t {
						f, ok := x.(float64)
						if ok {
							ints = append(ints, int(f))
						}
					}
				case interface{}:
					f, ok := t.(float64)
					if ok {
						ints = []int{int(f)}
					}
				}

				if len(ints) > 0 {
					vd.NisdToChkMap = append(vd.NisdToChkMap, cpLib.NisdChunk{
						Nisd:  cpLib.Nisd{ID: k},
						Chunk: ints,
					})
				}
			}
		}

		out[vdevID] = vd
	}
	return out, nil
}

func (ccf *CliCFuncs) GetPartition(req cpLib.GetReq) ([]cpLib.DevicePartition, error) {
	pts := make([]cpLib.DevicePartition, 0)
	err := ccf.get(req, &pts, cpLib.GET_PARTITION)
	if err != nil {
		log.Error("Get Partition failed: ", err)
		return nil, err
	}
	return pts, nil
}
