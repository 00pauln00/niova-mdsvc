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
        key = pair.get("key")
        value = pair.get("value")
        parts = key.split(delimiter)
        current = tree

        for part in parts[:-1]:
            # If part exists but is a string, convert it into a dict to hold subkeys
            if part in current and not isinstance(current[part], dict):
                current[part] = {"__value__": current[part]}
            current = current.setdefault(part, {})

        # If the last key already exists as a dict (previous subkeys),
        # store the value separately to preserve structure
        if parts[-1] in current and isinstance(current[parts[-1]], dict):
            current[parts[-1]]["__value__"] = value
        else:
            current[parts[-1]] = value

    return tree

