package main

import (
	"bufio"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
)

type SSHClient struct {
	client *ssh.Client
}

func NewSSHClient(host string) (*SSHClient, error) {
	usr, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to get current user: %v", err)
	}

	keyPath := filepath.Join(usr.HomeDir, ".ssh", "id_rsa")
	key, err := os.ReadFile(keyPath)
	if err != nil {
		keyPath = filepath.Join(usr.HomeDir, ".ssh", "id_ed25519")
		key, err = os.ReadFile(keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read SSH key from ~/.ssh/id_rsa or ~/.ssh/id_ed25519: %v", err)
		}
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %v", err)
	}

	config := &ssh.ClientConfig{
		User: usr.Username,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	if !strings.Contains(host, ":") {
		host = host + ":22"
	}

	client, err := ssh.Dial("tcp", host, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", host, err)
	}

	return &SSHClient{client: client}, nil
}

func (s *SSHClient) Close() error {
	return s.client.Close()
}

func (s *SSHClient) RunCommand(cmd string) (string, error) {
	session, err := s.client.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	output, err := session.CombinedOutput(cmd)
	return string(output), err
}

func (s *SSHClient) GetDevices() ([]ctlplfl.Device, error) {
	devices := make([]ctlplfl.Device, 0)

	byIdOutput, err := s.RunCommand("ls -la /dev/disk/by-id/ 2>/dev/null | grep -v '^total' | grep -v '^d' | awk '{print $9, $11}'")
	if err == nil && byIdOutput != "" {
		devices = append(devices, parseByIdDevices(byIdOutput)...)
	}

	lsblkOutput, err := s.RunCommand("lsblk -d -n -o NAME,SIZE,SERIAL,TYPE | grep 'disk'")
	if err != nil {
		return devices, fmt.Errorf("failed to get device list: %v", err)
	}

	lsblkDevices := parseLsblkDevices(lsblkOutput)

	deviceMap := make(map[string]*Device)
	for i := range devices {
		deviceMap[devices[i].Name] = &devices[i]
	}

	for _, lsblkDev := range lsblkDevices {
		if existing, exists := deviceMap[lsblkDev.Name]; exists {
			existing.Size = lsblkDev.Size
		} else {
			devices = append(devices, lsblkDev)
		}
	}

	return devices, nil
}

func parseByIdDevices(output string) []ctlplfl.Device {
	devices := make([]Device, 0)
	lines := strings.Split(strings.TrimSpace(output), "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) >= 2 {
			id := parts[0]
			target := parts[1]

			// Skip partition devices (contain "-part" in the ID)
			if strings.Contains(id, "-part") {
				continue
			}

			if strings.Contains(target, "../") {
				deviceName := strings.TrimPrefix(filepath.Base(target), "../")

				// Additional check: skip if device name contains 'p' followed by digits (nvme partitions)
				// or ends with digits (sda1, sdb2, etc.)
				if regexp.MustCompile(`p\d+$|[a-z]\d+$`).MatchString(deviceName) {
					continue
				}

				devices = append(devices, Device{
					ID:   id,
					Name: deviceName,
				})
			}
		}
	}

	return devices
}

// parseSizeToBytes converts human-readable size strings (like "1T", "500G", "1.2G") to bytes
// Examples:
//
//	"1T" -> 1099511627776 (1 * 1024^4)
//	"500G" -> 536870912000 (500 * 1024^3)
//	"1.5G" -> 1610612736 (1.5 * 1024^3)
//	"2048M" -> 2147483648 (2048 * 1024^2)
//	"1024K" -> 1048576 (1024 * 1024)
//	"12345" -> 12345 (raw bytes)
func parseSizeToBytes(sizeStr string) int64 {
	if sizeStr == "" {
		return 0
	}

	// Remove any whitespace
	sizeStr = strings.TrimSpace(sizeStr)
	if sizeStr == "" {
		return 0
	}

	// Extract numeric part and unit
	re := regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGTPE]?)$`)
	matches := re.FindStringSubmatch(strings.ToUpper(sizeStr))
	if len(matches) < 3 {
		// Try to parse as raw number (bytes)
		if bytes, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
			return bytes
		}
		return 0
	}

	// Parse the numeric part
	value, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0
	}

	// Apply multiplier based on unit
	unit := matches[2]
	var multiplier int64 = 1

	switch unit {
	case "K":
		multiplier = 1024
	case "M":
		multiplier = 1024 * 1024
	case "G":
		multiplier = 1024 * 1024 * 1024
	case "T":
		multiplier = 1024 * 1024 * 1024 * 1024
	case "P":
		multiplier = 1024 * 1024 * 1024 * 1024 * 1024
	case "E":
		multiplier = 1024 * 1024 * 1024 * 1024 * 1024 * 1024
	case "":
		multiplier = 1 // No unit means bytes
	}

	return int64(value * float64(multiplier))
}

func parseLsblkDevices(output string) []ctlplfl.Device {
	devices := make([]Device, 0)
	scanner := bufio.NewScanner(strings.NewReader(output))

	// Updated regex to handle NAME SIZE SERIAL TYPE format, making SERIAL optional
	re := regexp.MustCompile(`^(\S+)\s+(\S+)\s+(\S*)\s+disk`)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		matches := re.FindStringSubmatch(line)
		if len(matches) >= 4 {
			name := matches[1]
			sizeStr := matches[2] // Size string like "1T", "500G", "1.2G", etc.
			serialNum := matches[3]

			log.Info("Device name in parseLsblkDevices: ", name)
			// Additional check: ensure this is really a physical device, not a partition
			//if regexp.MustCompile(`p\d+$|[a-z]\d+$`).MatchString(name) {
			//	log.Info("Skipping devices: ", name)
			//	continue
			//}

			// Convert size string to bytes
			sizeBytes := parseSizeToBytes(sizeStr)
			log.Info("Device %s: size string '%s' parsed to %d bytes", name, sizeStr, sizeBytes)

			devices = append(devices, Device{
				ID:           name,
				Name:         name,
				Size:         sizeBytes,
				SerialNumber: serialNum,
			})
		}
	}

	return devices
}
