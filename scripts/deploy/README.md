Here is the updated guide aligned with the **YAML-based config** and the new scripts.

---

# ⚠️ Hokkaido Test Cluster Deployment Guide

> **WARNING**
> These scripts are **specific to the Hokkaido Test Cluster**.
> Do **not** reuse them in other environments without validation.

---

## 🧩 Prerequisites

1. The **same NFS volume** must be mounted on **I/O nodes 4–8**.
2. The **local filesystem `/var`** must be available on **I/O nodes 4–8**.
3. Passwordless SSH must be configured between nodes (required for `pdsh`).

---

## 🗂️ Configuration

All deployment parameters are defined in a single YAML file.

### `config.yaml`

```yaml
nodes:
  - <io-04.ip>
  - <io-05.ip>
  - <io-06.ip>
  - <io-07.ip>
  - <io-08.ip>

gossip:
  port_range:
    start: 10010
    end: 10020

ports:
  peer: 10000
  client: 10005

base_dir: /work/ctlplane
```

This file controls:

* Cluster membership
* Gossip port range
* Peer and client ports
* Base directory for configs, logs, and databases

No hardcoded IPs or paths remain in the scripts.

---

## 🚀 Deployment Steps

1. Copy all required files to the Niova installation directory:

   ```bash
   cp ./* <NIOVA_DIR_PATH>
   ```

2. Run deployment using the YAML config:

   ```bash
   sudo ./deploy.sh config.yaml
   ```

This will:

* Generate RAFT, peer, and gossip configs
* Populate `${base_dir}/configs`
* Start CTLPlane processes on all nodes listed in `nodes`

---

## 💡 Notes

* The **CTL interface is node-local**.
* **inotify does not work over NFS**, so CTL queries must be run on the same node where the process executes.

---

## 🔍 Verification

Process verification using node list from `config.yaml`:

```bash
pdsh -w 192.168.96.8[4-8] 'ps -ef | grep CTLPlane'
```

---

## 🛑 Stopping CTLPlane

```bash
pdsh -w 192.168.96.8[4-8] 'sudo killall CTLPlane_pmdbServer'
pdsh -w 192.168.96.8[4-8] 'sudo killall CTLPlane_proxy'
```

---

## 🗄️ Logs and Configuration Location

All generated artifacts are stored under:

```
<base_dir>   # defined in config.yaml
```

For Hokkaido:

```
/work/ctlplane
```

Includes:

* `configs/` — RAFT, peer, gossip configs
* `logs/` — server and proxy logs
* `db/` — RAFT databases

---
