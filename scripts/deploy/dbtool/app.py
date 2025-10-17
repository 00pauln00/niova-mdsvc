from flask import Flask, jsonify, request
import subprocess
import os
from utils import parse_ldb_output, build_tree

app = Flask(__name__)

REMOTE_HOST = "192.168.96.84"
CTL_DIR = "/var/ctlplane"
DELIMITER_DEFAULT = "/"

@app.route("/api/kv-tree", methods=["POST"])
def kv_trees():
    print("Here")
    try:
        ls_cmd = f"ls {CTL_DIR}"
        result = subprocess.run(
            ["pdsh", "-w", REMOTE_HOST, ls_cmd],
            capture_output=True,
            text=True,
            timeout=15
        )
        if result.returncode != 0:
            return jsonify({"error": f"Failed to list {CTL_DIR}: {result.stderr.strip()}"}), 500
        
        uuid = result.stdout.split()[1]

        raftdb_path = os.path.join(CTL_DIR, uuid, "db")
        scan_cmd = f"sudo ldb scan --db={raftdb_path} --column_family=PMDBTS_CF"
    
        scan_result = subprocess.run(
            ["pdsh", "-w", REMOTE_HOST, scan_cmd],
            capture_output=True,
            text=True,
            timeout=30
        ) 
        kv_pairs = parse_ldb_output(scan_result.stdout)
        tree = build_tree(kv_pairs)
        return jsonify({"tree": tree, "count": len(kv_pairs)})

    except subprocess.TimeoutExpired:
        return jsonify({"error": "Command timed out"}), 500
    except Exception as e:
        print(e)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=5000)
