import React, { useState } from "react";
import TreeNode from "./TreeNode.jsx";

export default function App() {
  const [dbPath, setDbPath] = useState("");
  const [delimiter, setDelimiter] = useState("/");
  const [tree, setTree] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function loadTree() {
    setLoading(true);
    setError("");
    try {
      const res = await fetch("/api/kv-tree", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ db_path: dbPath, delimiter }),
      });
      const data = await res.json();
      if (data.error) setError(data.error);
      else setTree(data.tree);
    } catch (err) {
      setError(String(err));
    }
    setLoading(false);
  }

  return (
    <div className="p-6 max-w-3xl mx-auto">
      <h1 className="text-2xl font-bold mb-4 text-blue-700">RocksDB KV Tree Viewer</h1>

      <div className="flex gap-2 mb-4">
        <input
          type="text"
          placeholder="/path/to/rocksdb"
          value={dbPath}
          onChange={(e) => setDbPath(e.target.value)}
          className="flex-1 border rounded p-2"
        />
        <input
          type="text"
          value={delimiter}
          onChange={(e) => setDelimiter(e.target.value)}
          className="w-16 border rounded p-2 text-center"
        />
        <button
          onClick={loadTree}
          disabled={loading}
          className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
        >
          {loading ? "Loading..." : "Load"}
        </button>
      </div>

      {error && <div className="text-red-600 mb-2">{error}</div>}

      <div className="bg-white rounded shadow p-4 overflow-auto">
        {tree ? (
          Object.entries(tree).map(([k, v]) => <TreeNode key={k} nodeKey={k} data={v} />)
        ) : (
          <div className="text-gray-400">Enter a RocksDB path and click Load.</div>
        )}
      </div>
    </div>
  );
}
