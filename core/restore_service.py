# core/restore_service.py

from __future__ import annotations

import os
from typing import Dict, List, Tuple, Any

from log import log_info, log_warning, log_error
from utils.file_utils import sha256_file
from utils.media_utils import detect_type_from_name


class RestoreService:
    """
    Service pur (sans UI) qui:
      - relit l’état des fichiers (final + .tmp) sur disque,
      - remet en cohérence les champs des médias,
      - normalise les statuts actifs (Downloading/Retrying/Waiting -> Paused).
    """

    def restore_from_disk(
        self,
        medias: List[dict],
        dirs: Dict[str, str],
        *,
        skip_sha: bool = True,
    ) -> None:
        """
        Met à jour chaque media en fonction des fichiers présents.
        Args:
            medias: liste de médias (mutée en place)
            dirs: {
                "local": <profil_dir>,
                "video": <profil_dir>/v,
                "image": <profil_dir>/p,
            }
            skip_sha: si True, ne calcule pas de SHA (plus rapide).
        """
        local_dir = dirs.get("local")
        video_dir = dirs.get("video") or os.path.join(local_dir, "v")
        image_dir = dirs.get("image") or os.path.join(local_dir, "p")

        log_info(f"[RESTORE] Scan dirs — local={local_dir}  v={video_dir}  p={image_dir}")

        for media in medias:
            name = media.get("name", "")
            if not name:
                log_warning("[RESTORE] Média sans nom — ignoré")
                continue

            # type (image / video / autre)
            mtype = media.get("type") or detect_type_from_name(name)
            media["type"] = mtype

            subdir = video_dir if mtype == "video" else image_dir if mtype == "image" else local_dir
            final_path = os.path.join(subdir, name)
            tmp_path = final_path + ".tmp"

            # Valeurs par défaut cohérentes
            media.setdefault("local_size", 0)
            media.setdefault("size_http", 0)
            media.setdefault("hash_check", "")
            media.setdefault("error", "")
            media.setdefault("speed", "")
            media["percent"] = int((media.get("local_size", 0) / (media.get("size_http") or 1)) * 100) if media.get("size_http") else 0

            # .tmp présent => Paused (reprise possible)
            if os.path.exists(tmp_path) and not os.path.exists(final_path):
                try:
                    sz = os.path.getsize(tmp_path)
                except Exception as e:
                    log_warning(f"[RESTORE] size(tmp) erreur pour {tmp_path}: {e}")
                    sz = 0

                media["local_size"] = sz
                media["status"] = "Paused"
                media["percent"] = int((sz / (media.get("size_http") or 1)) * 100) if media.get("size_http") else 0
                media["hash_check"] = ""
                log_info(f"[RESTORE] {name} -> Paused (.tmp {sz} bytes)")
                continue

            # Fichier final présent
            if os.path.exists(final_path):
                try:
                    sz = os.path.getsize(final_path)
                except Exception as e:
                    log_warning(f"[RESTORE] size(final) erreur pour {final_path}: {e}")
                    sz = 0

                media["local_size"] = sz
                if not media.get("size_http"):
                    media["size_http"] = sz  # best effort

                if skip_sha:
                    # On considère "Completed" si taille > 0 (rapide)
                    if sz > 0:
                        media["status"] = "Completed"
                        media["percent"] = 100
                        media["hash_check"] = ""
                        log_info(f"[RESTORE] {name} -> Completed (SHA ignoré, {sz} bytes)")
                    else:
                        media.update({"status": "Missing", "percent": 0, "hash_check": ""})
                        log_info(f"[RESTORE] {name} -> Missing (fichier vide)")
                else:
                    # Vérif SHA “light” : on compare le hash au tag du nom (si présent)
                    try:
                        actual = sha256_file(final_path)
                        # attendu: dernier segment avant l’extension si c’est un hash tronqué
                        # ex: ..._abcdef123456.mp4 -> "abcdef123456"
                        expected = _extract_expected_hash_from_name(name) or _extract_expected_hash_from_url(media.get("url", ""))
                        if expected and actual.startswith(expected):
                            media["status"] = "Completed"
                            media["percent"] = 100
                            media["hash_check"] = ""
                            log_info(f"[RESTORE] {name} -> Completed (SHA ok)")
                        elif sz > 0:
                            media.update({"status": "Incomplete", "percent": 0, "hash_check": actual})
                            log_warning(f"[RESTORE] {name} -> Incomplete (SHA mismatch)")
                        else:
                            media.update({"status": "Missing", "percent": 0, "hash_check": ""})
                            log_info(f"[RESTORE] {name} -> Missing (fichier vide)")
                    except Exception as e:
                        log_warning(f"[RESTORE] {name} -> Incomplete (erreur SHA: {e})")
                        media.update({"status": "Incomplete", "percent": 0, "hash_check": ""})

                continue

            # Aucun fichier local
            media.update({
                "status": "Missing",
                "local_size": 0,
                "percent": 0,
                "hash_check": "",
            })
            log_info(f"[RESTORE] {name} -> Missing (aucun fichier trouvé)")

    def normalize_active_statuses(self, medias: List[dict]) -> int:
        """
        Convertit Downloading/Retrying/Waiting -> Paused, nettoie speed/error.
        Retourne le nombre d’éléments modifiés.
        """
        changed = 0
        for m in medias:
            if m.get("status") in ("Downloading", "Retrying", "Waiting"):
                m["status"] = "Paused"
                m["speed"] = ""
                m["error"] = ""
                changed += 1
        if changed:
            log_info(f"[RESTORE] {changed} média(s) normalisé(s) en Paused")
        return changed

    def compute_summary(self, medias: List[dict]) -> Dict[str, int]:
        """
        Calcule un petit résumé pour l’UI (libre à toi de l’afficher ailleurs).
        """
        total = len(medias)
        counts = {
            "total": total,
            "completed": sum(1 for m in medias if m.get("status") == "Completed"),
            "downloading": sum(1 for m in medias if m.get("status") == "Downloading"),
            "waiting": sum(1 for m in medias if m.get("status") == "Waiting"),
            "retrying": sum(1 for m in medias if m.get("status") == "Retrying"),
            "failed": sum(1 for m in medias if m.get("status") == "Failed"),
            "incomplete": sum(1 for m in medias if m.get("status") == "Incomplete"),
            "paused": sum(1 for m in medias if m.get("status") == "Paused"),
            "videos": sum(1 for m in medias if m.get("type") == "video"),
            "images": sum(1 for m in medias if m.get("type") == "image"),
        }
        counts["others"] = total - counts["videos"] - counts["images"]
        return counts


# --- Helpers internes ---------------------------------------------------------

def _extract_expected_hash_from_name(filename: str) -> str | None:
    """
    Essaie de récupérer un tag hash-like du nom (ex: *_abcdef1234.mp4 -> 'abcdef1234').
    Si absent, retourne None.
    """
    try:
        stem = os.path.splitext(filename)[0]
        # on prend le dernier segment après un underscore
        last = stem.split("_")[-1]
        # heuristique simple: hash hexa 8+ chars
        return last if len(last) >= 8 and all(c in "0123456789abcdef" for c in last.lower()) else None
    except Exception:
        return None


def _extract_expected_hash_from_url(url: str) -> str | None:
    """
    Si l’URL contient un nom de fichier hashé, récupère la partie avant l’extension.
    """
    try:
        base = os.path.basename(url.split("?")[0])
        stem = os.path.splitext(base)[0]
        return stem if stem else None
    except Exception:
        return None
