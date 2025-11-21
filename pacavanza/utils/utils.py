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


def find_key_by_orderbook_id(data, target_id):
    for key, item in data.items():
        if item["orderbookId"] == target_id:
            return key
    return None
