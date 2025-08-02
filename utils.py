from log import log_info, log_error
import os
import json
from urllib.parse import urlparse
import subprocess
import hashlib
import requests
import re
import time

CDN_NODES = ["n1", "n2", "n3", "n4"]
API_BASE_URL = "https://coomer.st/api/v1/"

def fetch_medias_paginated(service, username, on_media_callback):
    log_info(f"[API] Streaming medias for {service}/{username}")
    seen_ids = set()
    offset = 0
    limit = 50
    while True:
        url = f"https://coomer.st/api/v1/{service}/user/{username}?o={offset}"
        log_info(f"[API] Requête : {url}")
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                log_error(f"[API] Erreur HTTP {resp.status_code} pour {url}")
                break
            data = resp.json()
            if not isinstance(data, list) or not data:
                break

            log_info(f"[API] Page : {len(data)} posts")
            for item in data:
                media_id = item.get("id")
                if media_id in seen_ids:
                    continue
                seen_ids.add(media_id)

                file = item.get("file", {})
                path = file.get("path")
                name = file.get("name")
                if not path or not name:
                    continue

                url_cdn = build_media_url(path)
                size_http = get_remote_file_size(url_cdn)
                log_info(f"[MEDIA] URL CDN : {url_cdn}")
                log_info(f"[MEDIA] Taille HTTP : {size_http} octets")

                media = {
                    "id": media_id,
                    "name": name,
                    "path": path,
                    "title": item.get("title", ""),
                    "added": item.get("added"),
                    "url": url_cdn,
                    "size_http": size_http,
                    "percent": "0",
                    "downloaded": False,
                    "retry_count": 0,
                    "error": "",
                }
                on_media_callback(media)
            offset += limit
            time.sleep(0.3)
        except Exception as e:
            log_error(f"[API] fetch_medias_paginated error: {e}")
            break

def fetch_medias_from_api(service, username, check_cdn=False):
    log_info(f"[API] Fetching medias for {service}/{username}")
    seen_ids = set()
    seen_names = set()
    offset = 0
    limit = 50

    while True:
        url = f"https://coomer.st/api/v1/{service}/user/{username}?o={offset}"
        log_info(f"[API] Requête : {url}")
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                log_error(f"[API] Erreur HTTP {resp.status_code} pour {url}")
                break

            try:
                data = resp.json()
            except Exception:
                log_error("[API] Réponse invalide (pas un JSON)")
                break

            if not isinstance(data, list):
                log_error("[API] Erreur : réponse inattendue (liste attendue)")
                break

            if not data:
                log_info("[API] Plus de pages, arrêt pagination.")
                break

            log_info(f"[API] Page {offset // limit + 1} : {len(data)} posts")
            page_medias = []
            added_names = 0
            skipped_names = 0

            for item in data:
                media_id = item.get("id")
                if media_id in seen_ids:
                    continue
                seen_ids.add(media_id)

                title = item.get("title", "")
                added = item.get("added")

                # 1. Fichier principal
                file = item.get("file")
                if file and file.get("path") and file.get("name"):
                    name = file["name"]
                    if name in seen_names:
                        skipped_names += 1
                    else:
                        url_cdn = build_media_url(file["path"])
                        ext = os.path.splitext(name)[1].lower()
                        type_detected = (
                            "video" if ext in [".mp4", ".m4v", ".mov", ".webm", ".avi", ".mkv", ".flv"]
                            else "image" if ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]
                            else "autre"
                        )
                        media = {
                            "id": str(media_id),
                            "name": name,
                            "path": file["path"],
                            "title": title,
                            "added": added,
                            "url": url_cdn,
                            "size_http": None,
                            "cdn_checked": False,
                            "percent": "0",
                            "downloaded": False,
                            "retry_count": 0,
                            "error": "",
                            "status": "",
                            "type": type_detected
                        }
                        page_medias.append(media)
                        seen_names.add(name)
                        added_names += 1

                # 2. Attachments
                attachments = item.get("attachments", [])
                for i, att in enumerate(attachments):
                    name = att.get("name")
                    if not att.get("path") or not name:
                        continue
                    if name in seen_names:
                        skipped_names += 1
                        continue

                    url_cdn = build_media_url(att["path"])
                    ext = os.path.splitext(name)[1].lower()
                    type_detected = (
                        "video" if ext in [".mp4", ".m4v", ".mov", ".webm", ".avi", ".mkv", ".flv"]
                        else "image" if ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]
                        else "autre"
                    )

                    media = {
                        "id": f"{media_id}_att{i}",
                        "name": name,
                        "path": att["path"],
                        "title": title,
                        "added": added,
                        "url": url_cdn,
                        "size_http": None,
                        "cdn_checked": False,
                        "percent": "0",
                        "downloaded": False,
                        "retry_count": 0,
                        "error": "",
                        "status": "",
                        "type": type_detected
                    }
                    page_medias.append(media)
                    seen_names.add(name)
                    added_names += 1

            log_info(f"[PAGE] {added_names} ajoutés, {skipped_names} doublons ignorés depuis offset {offset}")
            yield page_medias
            offset += limit
            time.sleep(0.2)

        except Exception as e:
            log_error(f"[API] Erreur fetch_medias_from_api : {e}")
            break

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

def get_fansly_username_from_id(user_id: str) -> str | None:
    url = f"https://apiv3.fansly.com/api/v1/account?ids={user_id}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://fansly.com",
        "Referer": "https://fansly.com/"
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            json_data = r.json()
            users = json_data.get("response", [])
            if users and "username" in users[0]:
                return users[0]["username"]
        else:
            print(f"[get_fansly_username_from_id] HTTP {r.status_code} - fallback to user_id")
    except Exception as e:
        print(f"[get_fansly_username_from_id] Error: {e}")
    
    return user_id  # 🔁 failover propre
    
def enrich_media_status(medias, download_path):
    from utils import sha256_file, rename_if_tmp_match
    subdir_v = os.path.join(download_path, "v")
    subdir_p = os.path.join(download_path, "p")
    for media in medias:
        media_name = media.get("name")
        url = media.get("url", "")
        if not media_name or not url:
            continue

        ext = os.path.splitext(media_name)[1].lower()
        subdir = subdir_v if ext in [".mp4", ".m4v", ".mov", ".webm", ".avi", ".mkv", ".flv"] else subdir_p
        final_path = os.path.join(subdir, media_name)

        tmp_path = final_path + ".tmp"
        if os.path.exists(tmp_path) and not os.path.exists(final_path):
            rename_if_tmp_match(tmp_path, final_path, url)

        if os.path.exists(final_path):
            local_sha = sha256_file(final_path)
            if local_sha and local_sha in url:
                media["percent"] = "100"
                media["status"] = "Completed"
                media["hash_check"] = ""
    return medias

def extract_profile_info(url: str):
    """
    Extrait le service (onlyfans, fansly, etc.) et le nom d'utilisateur depuis une URL Coomer.
    Exemple : https://coomer.st/onlyfans/user/xlunamaex -> ("onlyfans", "xlunamaex")
    """
    pattern = r"https?://(?:www\.)?coomer\.st/([^/]+)/user/([^/?#]+)"
    match = re.match(pattern, url.strip())
    if match:
        service, username = match.groups()
        return service.lower(), username
    return None, None

def get_settings(filepath="settings.json"):
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return json.load(f)
    return {}

def is_video(media):
    return media.get("type") == "video"

def detect_type_from_name(name):
    ext = os.path.splitext(name.lower())[1]
    if ext in [".mp4", ".m4v", ".mov", ".webm", ".avi", ".flv", ".mkv"]:
        return "video"
    elif ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]:
        return "image"
    else:
        return "autre"

def save_settings(settings, filepath="settings.json"):
    with open(filepath, "w") as f:
        json.dump(settings, f, indent=2)

def get_remote_file_size(url):
    try:
        parsed = urlparse(url)
        cdn_path = parsed.path  # ex: /data/xx/xx/xxxxxxx.mp4
        for node in CDN_NODES:
            test_url = f"https://{node}.coomer.st{cdn_path}"
            response = requests.head(test_url, timeout=5)
            if response.status_code == 200 and "Content-Length" in response.headers:
                return int(response.headers["Content-Length"])
        return None
    except Exception as e:
        return None

def format_file_size(size_bytes):
    if size_bytes is None:
        return ""
    try:
        size_bytes = int(size_bytes)
        if size_bytes >= 1_000_000_000:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} Go"
        elif size_bytes >= 1_000_000:
            return f"{size_bytes / (1024 * 1024):.2f} Mo"
        elif size_bytes >= 1_000:
            return f"{size_bytes / 1024:.2f} Ko"
        else:
            return f"{size_bytes} o"
    except Exception:
        return "?"

def format_bytes(size):
    if size is None:
        return "?"
    for unit in ['o', 'Ko', 'Mo', 'Go']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} To"

def sha256_file(filepath):
    """Calcule le hash SHA256 d'un fichier local"""
    hash_sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

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

def build_media_url(path):
    if path.startswith("/data"):
        return f"https://coomer.st{path}"
    return f"https://coomer.st/data{path}"

def render_progress_bar(percent: int) -> str:
    filled = int(percent / 10)
    empty = 10 - filled
    return f"{'█' * filled}{'░' * empty} {percent:>3}%"
