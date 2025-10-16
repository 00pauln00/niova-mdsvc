import React, { useState } from "react";

export default function TreeNode({ nodeKey, data }) {
  const [open, setOpen] = useState(false);
  const isLeaf = typeof data !== "object" || data === null;

  return (
    <div className="ml-4 my-1">
      <div
        onClick={() => !isLeaf && setOpen(!open)}
        className={`cursor-pointer flex items-center ${isLeaf ? "" : "font-semibold text-blue-700"}`}
      >
        {!isLeaf && <span className="mr-1">{open ? "📂" : "📁"}</span>}
        <span>{nodeKey}</span>
      </div>
      {isLeaf && (
        <div className="ml-6 text-gray-600 text-sm whitespace-pre-wrap bg-gray-100 rounded p-1 mt-1">
          {data}
        </div>
      )}
      {!isLeaf && open && (
        <div className="ml-4 border-l border-gray-200 pl-2">
          {Object.entries(data).map(([k, v]) => (
            <TreeNode key={k} nodeKey={k} data={v} />
          ))}
        </div>
      )}
    </div>
  );
}

