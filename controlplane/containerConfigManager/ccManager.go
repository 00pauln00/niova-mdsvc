package main

import (
	"flag"
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v3"

	cpClient "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/client"
	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"

	"github.com/google/uuid"
)

type s3Config struct {
	URL  string `yaml:"url"`
	Opts string `yaml:"opts"`
	Auth string `yaml:"auth"`
}

type NisdConfig struct {
	Name   string `yaml:"name"`
	Nisd   string `yaml:"uuid"`
	Client uint16 `yaml:"client_port"`
	Peer   uint16 `yaml:"peer_port"`
}

type Gossip struct {
	IPAddr []string `yaml:"ipaddr"`
	Ports  []uint16 `yaml:"ports"`
}

type ContainerConfig struct {
	S3Config   s3Config     `yaml:"s3_config"`
	Gossip     Gossip       `yaml:"gossip"`
	NisdConfig []NisdConfig `yaml:"nisd_config"`
}

func populateNisd(nisd cpLib.Nisd) NisdConfig {
	return NisdConfig{
		Name:   nisd.DevID,
		Nisd:   nisd.NisdID,
		Client: nisd.ClientPort,
		Peer:   nisd.PeerPort,
	}
}

// LoadOrCreateConfig loads config from file if present, otherwise creates a new one.
func LoadOrCreateConfig(setupConfig string, cc *ContainerConfig) error {
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
	deviceList := flag.String("d", "", "pass the nisd device id")
	flag.Parse()
	deviceIDs := strings.Split(*deviceList, ",")
	log.Infof("starting config app - raft: %s, config: %s, device id: %s", *raftID, *configPath, *deviceList)

	c := cpClient.InitCliCFuncs(uuid.New().String(), *raftID, *configPath)
	var cc ContainerConfig
	cc.NisdConfig = make([]NisdConfig, 0)
	err := LoadOrCreateConfig(*setupConfig, &cc)
	if err != nil {
		log.Error("failed to load or create config file: ", err)
		os.Exit(-1)
	}
	for _, deviceID := range deviceIDs {
		devInfo := cpLib.DeviceInfo{
			DevID: deviceID,
		}
		err := c.GetDeviceCfg(&devInfo)
		if err != nil {
			log.Error("failed to get device uuid", err)
			os.Exit(-1)
		}
		log.Info("device info: ", devInfo)
		nisdInfo := cpLib.Nisd{
			DevID:  devInfo.DevID,
			NisdID: devInfo.NisdID,
		}
		err = c.GetNisdCfg(&nisdInfo)
		if err != nil {
			log.Error("failed to get nisd details:", err)
			os.Exit(-1)
		}
		cc.NisdConfig = append(cc.NisdConfig, populateNisd(nisdInfo))
	}

	log.Info("config struct: ", cc)
	configY, err := yaml.Marshal(cc)
	if err != nil {
		log.Error("Error marshaling YAML:", err)
		os.Exit(-1)
	}
	err = os.WriteFile(filepath.Dir(*setupConfig)+"/config-gen.yaml", configY, 0644)
	if err != nil {
		log.Error("Error writing YAML file:", err)
		os.Exit(-1)
	}
}
