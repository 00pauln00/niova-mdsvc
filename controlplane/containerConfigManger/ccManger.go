package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	log "github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v3"

	cpClient "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/client"
	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"

	"github.com/google/uuid"
)

type ContainerConfig struct {
	Nisd_Config []cpLib.Nisd `yaml: "nisd_config"`
}

func populateNisd(nisd map[string][]byte, UniqID string) cpLib.Nisd {
	cp, _ := strconv.ParseUint(string(nisd[fmt.Sprintf("/n/%s/conf_cp", UniqID)]), 10, 16)
	pp, _ := strconv.ParseUint(string(nisd[fmt.Sprintf("/n/%s/conf_pp", UniqID)]), 10, 16)
	return cpLib.Nisd{
		DeviceID:   string(nisd[fmt.Sprintf("/n/%s/conf_d", UniqID)]),
		DeviceUUID: uuid.MustParse(UniqID),
		ClientPort: uint16(cp),
		PeerPort:   uint16(pp),
	}
}

func generateConfig(nisd map[string][]byte, UniqID, config string) error {
	err := os.MkdirAll(filepath.Dir(config), os.ModePerm)
	if err != nil {
		log.Error("Error creating directories:", err)
		return err
	}
	cc := ContainerConfig{
		Nisd_Config: []cpLib.Nisd{
			populateNisd(nisd, UniqID),
		},
	}

	configY, err := yaml.Marshal(cc)
	if err != nil {
		log.Error("Error marshaling YAML:", err)
		return err
	}
	err = os.WriteFile(config, configY, 0644)
	if err != nil {
		log.Error("Error writing YAML file:", err)
		return err
	}

	return nil
}

func main() {

	raftID := flag.String("r", "", "pass the raft uuid")
	configPath := flag.String("c", "", "pass the gossip config path")
	setupConfig := flag.String("sc", "./config.yaml", "pass the gossip config path")
	deviceID := flag.String("d", "", "pass the nisd device id")
	flag.Parse()
	log.Infof("starting config app - raft: %s, config: %s, device id: %s", *raftID, *configPath, *deviceID)
	c := cpClient.InitCliCFuncs(uuid.New().String(), *raftID, *configPath)
	uid, err := c.GetDeviceUUID(*deviceID)
	if err != nil {
		log.Error("failed to get device uuid", err)
	}
	log.Info("device uuid: ", string(uid))
	nisd, err := c.GetNisdDetails(string(uid))
	if err != nil {
		log.Error("failed to get nisd details:", err)
	}
	log.Info("nisd details: ", nisd)
	generateConfig(nisd, string(uid), *setupConfig)
}
