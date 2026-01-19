import tempfile
import os
import json


def save_json_atomic(path: str, obj):
    """
    Safely write JSON `obj` to `path` atomically and fsync both file and directory (POSIX best-effort).
    """
    d = os.path.dirname(path) or "."
    # mkstemp ensures the temp file is on the same filesystem for atomic os.replace
    fd, tmp = tempfile.mkstemp(dir=d)
    try:
        # write text then fsync file
        with os.fdopen(fd, "w") as f:
            json.dump(obj, f, default=str)
            f.flush()
            os.fsync(f.fileno())
        # atomic rename
        os.replace(tmp, path)
        # try to fsync directory to make rename durable
        try:
            dirfd = os.open(d, os.O_DIRECTORY)
            try:
                os.fsync(dirfd)
            finally:
                os.close(dirfd)
        except Exception:
            # best-effort; ignore on platforms where not supported
            pass
    finally:
        # cleanup leftover temp file if something failed before rename
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass


def flatten_instrument_list(nested_data):
    """
    Flattens a hierarchical instrument list (Asset -> {Metadata, Instruments...})
    into a flat dictionary (InstrumentID -> InstrumentData) with inherited metadata.
    """
    flat = {}
    for asset, data in nested_data.items():
        # Common inheritance fields
        common = {
            k: v
            for k, v in data.items()
            if k in ["timezone", "market_open", "market_close"]
        }
        for k, v in data.items():
            if isinstance(v, dict) and "orderbookId" in v:
                # It's an instrument
                merged = v.copy()
                # Merge inheritables if not present/override (usually parent is default)
                for ck, cv in common.items():
                    if ck not in merged:
                        merged[ck] = cv
                flat[k] = merged
    return flat


def find_key_by_orderbook_id(data, target_id):
    # Try flat lookup first (if data is already flattened or old format)
    for key, item in data.items():
        if isinstance(item, dict):
            if item.get("orderbookId") == target_id:
                return key
            # Check nested children
            for child_key, child_item in item.items():
                if (
                    isinstance(child_item, dict)
                    and child_item.get("orderbookId") == target_id
                ):
                    return child_key
    return None
