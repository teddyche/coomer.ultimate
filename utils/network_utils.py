# utils.py
import requests
import os
from urllib.parse import urlparse
from utils.file_utils import sha256_file
from core.log import log_error

CDN_NODES = ["n1", "n2", "n3", "n4"]

def generate_alternative_urls(url):
    """Génère des URLs alternatives à partir d'une URL CDN.
    Utile si un node est down ou bloque la requête.
    """
    from urllib.parse import urlparse

    try:
        parsed = urlparse(url)
        hostname_parts = parsed.hostname.split(".")
        if hostname_parts[0].startswith("n") and hostname_parts[0][1:].isdigit():
            node_num = int(hostname_parts[0][1:])
            alternatives = []
            for n in range(1, 5):  # 4 nodes en exemple
                if n != node_num:
                    new_host = hostname_parts.copy()
                    new_host[0] = f"n{n}"
                    alt_url = parsed._replace(netloc=".".join(new_host)).geturl()
                    alternatives.append(alt_url)
            return alternatives
    except Exception:
        pass
    return []


def build_media_url(path):
    if path.startswith("/data"):
        return f"https://coomer.st{path}"
    return f"https://coomer.st/data{path}"

def get_remote_file_size(url):
    try:
        parsed = urlparse(url)
        cdn_path = parsed.path
        for node in CDN_NODES:
            test_url = f"https://{node}.coomer.st{cdn_path}"
            response = requests.head(test_url, timeout=5)
            if response.status_code == 200 and "Content-Length" in response.headers:
                return int(response.headers["Content-Length"])
        return None
    except Exception:
        return None

def verify_hash_from_cdn_path(local_path, cdn_path):
    """
    Vérifie si le SHA256 du fichier correspond au nom de fichier CDN
    """
    try:
        expected_hash = os.path.splitext(os.path.basename(cdn_path))[0]
        actual_hash = sha256_file(local_path)
        return actual_hash == expected_hash
    except Exception as e:
        log_error(f"[verify_hash_from_cdn_path] Erreur : {e}")
        return False
