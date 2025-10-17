# RocksDB KV Tree Viewer

This project provides a web interface to **scan RocksDB databases** (including remote `raftdb/db` directories) and visualize them as a **collapsible key-value tree**. The backend is built with **Flask**, and the frontend uses **React + Vite + TailwindCSS**.

# ⚠️ Warning: Restricted Usage

**Important:**  

This application **must only be used within the Hokkaido cluster**.  

## Table of Contents
- [Requirements](#requirements)
- [Setup Backend (Flask)](#setup-backend-flask)
- [Setup Frontend (React)](#setup-frontend-react)
- [Run the App](#run-the-app)
- [Using the App](#using-the-app)

## Requirements
- Python 3.10+
- Node.js 18+ and npm
- RocksDB CLI tool (`ldb`) installed and accessible
- Ensure `pdsh` is installed
- Ensure a controlplane node is running in `192.168.96.84`
- Ensure `.raftdb` is in `/var/ctlplane`


## Setup Backend (Flask)
1. Create a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/macOS
# Windows PowerShell: .\venv\Scripts\Activate.ps1
```
2. Install dependencies:
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## Setup Frontend (React)
1. Navigate to frontend folder:
```bash
cd frontend
```
2. Install npm dependencies:
```bash
npm install
```

## Run the App
### Flask backend:
```bash
cd rocksdb-ldb-kv-tree
source venv/bin/activate
python app.py
```
### React frontend:
```bash
cd frontend
npm run dev
```

## Using the App
1. Open browser: `http://127.0.0.1:5173`
2. Click **Load** to see KV tree

