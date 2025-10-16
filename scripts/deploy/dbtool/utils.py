def parse_ldb_output(output: str):
    lines = output.splitlines()
    kv_pairs = []
    key = None
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if "==>" in line:
            k, v = line.split("==>", 1)
            k = k[15:]
            kv_pairs.append({"key": k.strip(), "value": v.strip()})
    return kv_pairs

def build_tree(kv_pairs, delimiter="/"):
    tree = {}
    for pair in kv_pairs:
        parts = pair["key"].split(delimiter)
        current = tree
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        current[parts[-1]] = pair["value"]
    return tree
