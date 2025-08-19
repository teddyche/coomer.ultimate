import os, hashlib
from log import log_info, log_error

def sha256_file(filepath):
    hash_sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

def rename_if_tmp_match(tmp_path, final_path, cdn_url):
    try:
        if not os.path.exists(tmp_path):
            return False
        local_sha = sha256_file(tmp_path)
        if local_sha and local_sha in cdn_url:
            os.rename(tmp_path, final_path)
            log_info(f"[Fix] Renommé {os.path.basename(tmp_path)} → {os.path.basename(final_path)} (SHA OK)")
            return True
    except Exception as e:
        log_error(f"[Fix] Erreur rename {tmp_path} → {final_path} : {e}")
    return False
