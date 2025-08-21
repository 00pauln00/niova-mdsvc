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

type s3Config struct {
	URL  string `yaml:"url"`
	Opts string `yaml:"opts"`
	Auth string `yaml:"auth"`
}

type NisdConfig struct {
	DevID   string `yaml:"name"`
	NisdID  string `yaml:"uuid"`
	CPort   uint16 `yaml:"client_port"`
	PPort   uint16 `yaml:"peer_port"`
	InitDev bool   `yaml:"init"`
}

type Gossip struct {
	IPAddr []string `yaml:"ipaddr"`
	Ports  []uint16 `yaml:"ports"`
}

type NisdCntrConfig struct {
	S3Config   s3Config      `yaml:"s3_config"`
	Gossip     Gossip        `yaml:"gossip"`
	NisdConfig []*NisdConfig `yaml:"nisd_config"`
}

func updateNisdConfig(nisdConf *NisdConfig, nisdCP cpLib.Nisd) {
	nisdConf.DevID = nisdCP.DevID
	nisdConf.NisdID = nisdCP.NisdID
	nisdConf.CPort = nisdCP.ClientPort
	nisdConf.PPort = nisdCP.PeerPort
}

// loadConfig loads config from file if present, otherwise creates a new one.
func loadConfig(setupConfig string, cc *NisdCntrConfig) error {
	if err := os.MkdirAll(filepath.Dir(setupConfig), os.ModePerm); err != nil {
		return err
	}

	if _, err := os.Stat(setupConfig); err == nil {
		data, err := os.ReadFile(setupConfig)
		if err != nil {
			return err
		}
		if err := yaml.Unmarshal(data, cc); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
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
	var conf NisdCntrConfig
	conf.NisdConfig = make([]*NisdConfig, 0)
	err := loadConfig(*setupConfig, &conf)
	if err != nil {
		log.Error("failed to load or create config file: ", err)
		os.Exit(-1)
	}

	for _, nisd := range conf.NisdConfig {
		devInfo := cpLib.DeviceInfo{
			DevID: nisd.DevID,
		}
		err := c.GetDeviceCfg(&devInfo)
		if err != nil {
			log.Error("failed to get device uuid", err)
			os.Exit(-1)
		}
		log.Info("fetched device info from control plane: ", devInfo)
		nisdCP := cpLib.Nisd{
			DevID:  devInfo.DevID,
			NisdID: devInfo.NisdID,
		}
		err = c.GetNisdCfg(&nisdCP)
		if err != nil {
			log.Error("failed to get nisd details:", err)
			os.Exit(-1)
		}
		updateNisdConfig(nisd, nisdCP)
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
