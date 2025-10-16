from flask import Flask, jsonify, request
import subprocess
import os
from utils import parse_ldb_output, build_tree

app = Flask(__name__)

REMOTE_HOST = "192.168.96.84"
CTL_DIR = "/var/ctl"
DELIMITER_DEFAULT = "/"

@app.route("/api/kv-trees", methods=["GET"])
def kv_trees():
    delimiter = request.args.get("delimiter", DELIMITER_DEFAULT)
    try:
        # 1️⃣ List UUID directories in /var/ctl on remote host
        ls_cmd = f"ls {CTL_DIR}"
        result = subprocess.run(
            ["pdsh", "-w", REMOTE_HOST, ls_cmd],
            capture_output=True,
            text=True,
            timeout=15
        )
        if result.returncode != 0:
            return jsonify({"error": f"Failed to list {CTL_DIR}: {result.stderr.strip()}"}), 500

        uuids = result.stdout.split()
        all_trees = {}

        # 2️⃣ Iterate each UUID and locate raftdb/db
        for uuid in uuids:
            raftdb_path = os.path.join(CTL_DIR, uuid, "raftdb", "db")
            scan_cmd = f"sudo ldb scan --db={raftdb_path} --column_family=PMDBTS_CF"
            
            scan_result = subprocess.run(
                ["pdsh", "-w", REMOTE_HOST, scan_cmd],
                capture_output=True,
                text=True,
                timeout=30
            )
            if scan_result.returncode != 0:
                all_trees[uuid] = {"error": scan_result.stderr.strip()}
                continue

            kv_pairs = parse_ldb_output(scan_result.stdout)
            tree = build_tree(kv_pairs, delimiter)
            all_trees[uuid] = tree

        return jsonify({"trees": all_trees})

    except subprocess.TimeoutExpired:
        return jsonify({"error": "Command timed out"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=5000)
