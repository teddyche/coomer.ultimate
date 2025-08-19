# core/profile_manager.py
from __future__ import annotations

import os
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from core.log import log_info, log_error, log_warning, log_debug
from utils.api_utils import fetch_medias_from_api
from utils.file_utils import sha256_file
from utils.media_utils import enrich_media_status
from utils.profile_utils import extract_profile_info
from media_utils import clean_profile_folder


@dataclass(frozen=True)
class ProfileKey:
    service: str
    username: str

    def as_str(self) -> str:
        return f"{self.service}:{self.username}"


@dataclass
class ProfileRow:
    key: ProfileKey
    medias: List[dict]
    last_update: str
    custom_base_dir: str  # base dir choisi pour ce profil (sans /service/username)
    download_path: str    # chemin complet .../<base>/<service>/<username>


class ProfileManager:
    """
    Gère TOUT ce qui touche aux profils (fichiers JSON, chemins, API, import).
    Aucune dépendance Tkinter ici.
    """

    def __init__(
        self,
        data_dir: str,
        default_download_dir: str,
        profile_dirs: Dict[str, str] | None = None,
    ):
        self.data_dir = data_dir
        self.default_download_dir = default_download_dir
        self.profile_dirs = dict(profile_dirs or {})

        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.default_download_dir, exist_ok=True)

    # ---------- Helpers chemins ----------
    def _profile_json_path(self, key: ProfileKey) -> str:
        return os.path.join(self.data_dir, key.service, f"{key.username}.json")

    def _profile_base_dir(self, key: ProfileKey) -> str:
        # base dir éventuellement custom (sans /service/username)
        custom = self.profile_dirs.get(key.as_str(), self.default_download_dir)
        return os.path.abspath(custom)

    def profile_download_path(self, key: ProfileKey) -> str:
        # chemin complet final pour les fichiers (avec /service/username)
        return os.path.join(self._profile_base_dir(key), key.service, key.username)

    # ---------- IO JSON ----------
    def load_profile(self, key: ProfileKey) -> ProfileRow | None:
        json_path = self._profile_json_path(key)
        if not os.path.exists(json_path):
            return None
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            log_warning(f"[PM] JSON corrompu: {json_path} ({e})")
            return None

        medias = data.get("medias", [])
        last_update = data.get("last_update", "1970-01-01T00:00:00+00:00")
        row = ProfileRow(
            key=key,
            medias=medias,
            last_update=last_update,
            custom_base_dir=self._profile_base_dir(key),
            download_path=self.profile_download_path(key),
        )
        return row

    def save_profile(self, row: ProfileRow) -> None:
        json_path = self._profile_json_path(row.key)
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        payload = {
            "medias": row.medias,
            "last_update": row.last_update,
            "profile_name": row.key.username,
            "custom_dir": os.path.join(self._profile_base_dir(row.key), row.key.service, row.key.username),
        }
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    # ---------- Découverte ----------
    def list_profiles(self) -> Iterable[ProfileRow]:
        # parcours data_dir/<service>/*.json
        for service in os.listdir(self.data_dir):
            sdir = os.path.join(self.data_dir, service)
            if not os.path.isdir(sdir):
                continue
            for filename in os.listdir(sdir):
                if not filename.endswith(".json"):
                    continue
                username = filename[:-5]
                row = self.load_profile(ProfileKey(service, username))
                if row:
                    yield row

    # ---------- Tailles ----------
    @staticmethod
    def compute_folder_sizes(download_path: str) -> Tuple[int, int]:
        """Retourne (videos_bytes, photos_bytes) en scannant v/ et p/"""
        def dir_size(path: str) -> int:
            total = 0
            if not os.path.isdir(path):
                return 0
            for dp, _, files in os.walk(path):
                for fn in files:
                    try:
                        total += os.path.getsize(os.path.join(dp, fn))
                    except Exception:
                        pass
            return total

        v = dir_size(os.path.join(download_path, "v"))
        p = dir_size(os.path.join(download_path, "p"))
        return v, p

    # ---------- Move / Chemin custom ----------
    def move_profile_dir(self, key: ProfileKey, new_base_dir: str) -> None:
        src = self.profile_download_path(key)
        dst = os.path.join(os.path.abspath(new_base_dir), key.service, key.username)
        os.makedirs(os.path.dirname(dst), exist_ok=True)

        if os.path.exists(src):
            log_info(f"[PM] Move {src} → {dst}")
            # déplace tous les fichiers en préservant l’arbo
            files: List[Tuple[str, str]] = []
            for root, _, filenames in os.walk(src):
                for fname in filenames:
                    full = os.path.join(root, fname)
                    rel = os.path.relpath(full, src)
                    files.append((full, rel))
            for full, rel in files:
                final = os.path.join(dst, rel)
                os.makedirs(os.path.dirname(final), exist_ok=True)
                shutil.move(full, final)

        # enregistre le custom dir (base)
        self.profile_dirs[key.as_str()] = os.path.abspath(new_base_dir)

    # ---------- Refresh via API ----------
    def refresh_profile(self, key: ProfileKey) -> Tuple[int, int, int]:
        """
        Récupère les nouveaux médias depuis l'API et met à jour le JSON.
        Retourne (nb_new, nb_total, nb_total_after_enrich)
        """
        row = self.load_profile(key) or ProfileRow(
            key=key, medias=[], last_update="1970-01-01T00:00:00+00:00",
            custom_base_dir=self._profile_base_dir(key),
            download_path=self.profile_download_path(key),
        )

        try:
            last_dt = datetime.fromisoformat(row.last_update)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
        except Exception:
            last_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)

        seen = {m.get("name") for m in row.medias}
        new_medias: List[dict] = []
        offset = 0

        while True:
            try:
                for post in self._fetch_posts_paginated(key.service, key.username, offset):
                    pub = post.get("published")
                    if not pub:
                        continue
                    try:
                        pdt = datetime.fromisoformat(pub)
                        if pdt.tzinfo is None:
                            pdt = pdt.replace(tzinfo=timezone.utc)
                    except Exception:
                        continue

                    if pdt < last_dt:
                        # on s'arrête quand on a dépassé la last_update
                        raise StopIteration

                    files = [post.get("file")] if post.get("file") else []
                    files += post.get("attachments") or []
                    for f in files:
                        if not f:
                            continue
                        name = f.get("name")
                        path = f.get("path")
                        if not name or not path or name in seen:
                            continue
                        seen.add(name)
                        media = {
                            "name": name,
                            "cdn_path": path,
                            "status": "Missing",
                            "type": "video" if name.lower().endswith(
                                (".mp4", ".webm", ".mkv", ".mov", ".m4v", ".avi", ".flv")
                            ) else "image",
                            "local_size": 0,
                            "size_http": 0,
                            "percent": 0,
                            "hash_check": "",
                            "error": "",
                        }
                        new_medias.append(media)
                offset += 50
            except StopIteration:
                break
            except Exception as e:
                log_error(f"[PM] API error on refresh: {e}")
                break

        if new_medias:
            log_info(f"[PM] +{len(new_medias)} nouveaux médias pour {key.as_str()}")
        else:
            log_info(f"[PM] Aucun nouveau média pour {key.as_str()}")

        # enrichit avec le status local
        all_medias = row.medias + new_medias
        download_path = self.profile_download_path(key)
        all_medias = enrich_media_status(all_medias, download_path)

        row.medias = all_medias
        row.last_update = datetime.now(timezone.utc).isoformat()
        self.save_profile(row)
        return len(new_medias), len(row.medias), len(all_medias)

    def delete_profile(self, key: ProfileKey):
        json_path = os.path.join(self.data_dir, key.service, f"{key.username}.json")
        dl_path = self.profile_download_path(key)  # prend en compte custom/base dir

        try:
            if os.path.exists(json_path):
                os.remove(json_path)
            if os.path.exists(dl_path):
                shutil.rmtree(dl_path)
            # oublie le custom dir enregistré
            if key.as_str() in self.profile_dirs:
                del self.profile_dirs[key.as_str()]
            log_info(f"[PM] Deleted profile {key.as_str()}")
        except Exception as e:
            raise RuntimeError(f"Suppression échouée: {e}") from e

    @staticmethod
    def _fetch_posts_paginated(service: str, username: str, offset: int) -> List[dict]:
        """
        Utilise utils.api_utils.fetch_medias_from_api si tu veux,
        mais ici on renvoie des 'posts' uniformes pour la boucle ci‑dessus.
        """
        # On s’aligne sur ton util courant: fetch_medias_from_api renvoie des pages de médias,
        # mais pour le refresh on a besoin des posts. Si ton util expose aussi un fetch_posts,
        # remplace cette fonction par l’appel direct.
        # Par défaut, on fallback en utilisant l’API simple (déplacée ici si besoin).
        import requests
        url = f"https://coomer.st/api/v1/{service}/user/{username}?o={offset}"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("posts") if isinstance(data, dict) else data

    # ---------- Import d’un dossier déjà téléchargé ----------
    def import_existing(self, selected_dir: str, url: str) -> ProfileKey:
        if not selected_dir:
            raise ValueError("selected_dir is empty")
        service, username = extract_profile_info(url)
        key = ProfileKey(service, str(username))
        log_info(f"[PM] Import existing: dir={selected_dir} url={url} → {key.as_str()}")

        # 1) Fetch API AVANT clean (tous médias)
        all_medias: List[dict] = []
        for page in fetch_medias_from_api(key.service, key.username):
            all_medias.extend(page)

        # 2) Clean du dossier (service/username + v|p|o)
        clean_profile_folder(selected_dir, key.service, key.username)
        cleaned_path = os.path.join(selected_dir, key.service, key.username)

        # 3) SHA256 matching
        local_files: List[str] = []
        for sub in ("v", "p", "o"):
            d = os.path.join(cleaned_path, sub)
            if not os.path.isdir(d):
                continue
            for fn in os.listdir(d):
                p = os.path.join(d, fn)
                if os.path.isfile(p):
                    local_files.append(p)

        for fpath in local_files:
            sha = sha256_file(fpath)
            matched = False
            for media in all_medias:
                if sha and sha in media.get("url", ""):
                    media["downloaded"] = True
                    media["status"] = "Completed"
                    media["percent"] = "100"
                    media["error"] = ""
                    matched = True

                    expected = media.get("name")
                    actual = os.path.basename(fpath)
                    if expected and actual != expected:
                        try:
                            os.rename(fpath, os.path.join(os.path.dirname(fpath), expected))
                            log_info(f"[PM] Rename: {actual} → {expected}")
                        except Exception as e:
                            log_warning(f"[PM] Rename failed: {actual} → {expected} ({e})")

                    tmp = fpath + ".tmp"
                    if os.path.exists(tmp):
                        try:
                            os.remove(tmp)
                        except Exception as e:
                            log_warning(f"[PM] Remove tmp failed: {tmp} ({e})")
                    break
            if not matched:
                log_warning(f"[PM] No SHA match for {os.path.basename(fpath)}")

        # 4) Sauvegarde JSON
        row = ProfileRow(
            key=key,
            medias=all_medias,
            last_update=datetime.now(timezone.utc).isoformat(),
            custom_base_dir=self._profile_base_dir(key),
            download_path=self.profile_download_path(key),
        )
        self.save_profile(row)

        # 5) Enregistre le base_dir choisi pour ce profil
        self.profile_dirs[key.as_str()] = os.path.abspath(selected_dir)
        return key
