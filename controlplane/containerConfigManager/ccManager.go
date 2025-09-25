package main

import (
	"flag"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v3"

	cpClient "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/client"
	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"

	"github.com/google/uuid"
)

const GEN_CONF_FILE = "config-gen.yaml"

// loadConfig loads config from file if present, otherwise creates a new one.
func loadConfig(setupConfig string, cc *cpLib.NisdCntrConfig) error {

	if err := os.MkdirAll(filepath.Dir(setupConfig), os.ModePerm); err != nil {
		return err
	}
	_, err := os.Stat(setupConfig)
	if err != nil {
		log.Error("failed to stat config file: ", err)
		return err
	}
	data, err := os.ReadFile(setupConfig)
	if err != nil {
		log.Error("failed to read config file: ", err)
		return err
	}
	if err := yaml.Unmarshal(data, cc); err != nil {
		log.Error("failed to unmarshal config file: ", err)
		return err
	}

	return nil
}

func main() {

	raftID := flag.String("r", "", "pass the raft uuid")
	configPath := flag.String("c", "./gossipNodes", "pass the gossip config path")
	setupConfig := flag.String("sc", "./config.yaml", "pass the gossip config path")
	flag.Parse()
	log.Infof("starting config app - raft: %s, config: %s", *raftID, *configPath)

	c := cpClient.InitCliCFuncs(uuid.New().String(), *raftID, *configPath)
	var conf cpLib.NisdCntrConfig
	conf.NisdConfig = make([]*cpLib.Nisd, 0)
	err := loadConfig(*setupConfig, &conf)
	if err != nil {
		log.Error("failed to load config file: ", err)
		os.Exit(-1)
	}

	for _, nisd := range conf.NisdConfig {
		devInfo := cpLib.DeviceInfo{
			DevID: nisd.DevID,
		}
		err := c.GetDeviceInfo(&devInfo)
		if err != nil {
			log.Error("failed to get device uuid", err)
			os.Exit(-1)
		}
		log.Debug("fetched device info from control plane: ", devInfo)
		nisd.NisdID = devInfo.NisdID
		err = c.GetNisdCfg(nisd)
		if err != nil {
			log.Error("failed to get nisd details:", err)
			os.Exit(-1)
		}
		log.Debug("fetched nisd info from control plane: ", nisd)
	}

	configY, err := yaml.Marshal(conf)
	if err != nil {
		log.Error("Error marshaling YAML:", err)
		os.Exit(-1)
	}
	err = os.WriteFile(filepath.Join(filepath.Dir(*setupConfig), GEN_CONF_FILE), configY, 0644)
	if err != nil {
		log.Error("Error writing YAML file:", err)
		os.Exit(-1)
	}
	log.Info("generated nisd configuration yaml file for container automation: ", conf)
}
