package main

import (
	"bufio"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/ssh"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
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

			if strings.Contains(target, "../") {
				deviceName := strings.TrimPrefix(filepath.Base(target), "../")
				devices = append(devices, Device{
					ID:   id,
					Name: deviceName,
				})
			}
		}
	}

	return devices
}

func parseLsblkDevices(output string) []ctlplfl.Device {
	devices := make([]Device, 0)
	scanner := bufio.NewScanner(strings.NewReader(output))

	re := regexp.MustCompile(`^(\S+)\s+(\S+)\s+disk`)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		matches := re.FindStringSubmatch(line)
		if len(matches) >= 3 {
			name := matches[1]
			_ = matches[2] // sizeStr - TODO: Parse size strings like "1T", "500G" etc. properly

			// Convert size string to bytes (simplified - just use 0 for now)
			serialNum := matches[3]
			var sizeBytes int64 = 0

			devices = append(devices, Device{
				ID: uuid.New().String(),
				Name:  name,
				Size:  sizeBytes,
				SerialNumber: serialNum,
			})
		}
	}

	return devices
}
