# core/download_manager.py
import os
import time
import math
import random
import requests
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from log import log_info, log_error, log_warning
from utils.network_utils import verify_hash_from_cdn_path
from media_utils import is_valid_video, is_valid_image


class DownloadManager:
    CDN_NODES = ["n1", "n2", "n3", "n4"]
    PER_CHUNK_TIMEOUT = 30  # sec sans aucun chunk -> retry
    CONNECT_TIMEOUT = 10
    READ_TIMEOUT = 30
    MAX_RETRIES_PER_NODE = 3
    MAX_TOTAL_RETRIES = 8   # garde-fou global (tous nœuds confondus)

    @staticmethod
    def generate_alternative_urls(original_url: str):
        parsed = urlparse(original_url)
        path = parsed.path
        base = "coomer.st"
        urls = [f"https://{base}{path}"]
        for node in DownloadManager.CDN_NODES:
            urls.append(f"https://{node}.{base}{path}")
        return urls

    @staticmethod
    def download_file(
        url,
        final_path,
        on_progress=None,
        resume=True,
        retry_delay=10,
        should_stop=None,
        window_id=None
    ):
        # chemins
        tmp_path = final_path if final_path.endswith(".tmp") else final_path + ".tmp"
        os.makedirs(os.path.dirname(final_path) or ".", exist_ok=True)

        all_urls = DownloadManager.generate_alternative_urls(url)
        log_prefix = f"[DL] [Window {window_id}]" if window_id else "[DL]"
        log_info(f"{log_prefix} ▶️ Début téléchargement pour {final_path} depuis {url}")

        # session HTTP réutilisable + pool raccord
        session = requests.Session()
        session.mount("https://", HTTPAdapter(pool_connections=8, pool_maxsize=32, max_retries=0))
        session.mount("http://", HTTPAdapter(pool_connections=4, pool_maxsize=16, max_retries=0))

        total_retries = 0

        try:
            for candidate_url in all_urls:
                log_info(f"{log_prefix} 🌐 Test CDN : {candidate_url}")
                per_node_retries = 0

                while per_node_retries < DownloadManager.MAX_RETRIES_PER_NODE:
                    if should_stop and should_stop():
                        log_info(f"{log_prefix} ⛔ Téléchargement interrompu")
                        return False, "Stopped"

                    # Reprise éventuelle
                    headers, mode = {}, "wb"
                    downloaded = 0
                    if resume and os.path.exists(tmp_path):
                        downloaded = os.path.getsize(tmp_path)
                        if downloaded > 0:
                            headers["Range"] = f"bytes={downloaded}-"
                            mode = "ab"
                            log_info(f"{log_prefix} 🔄 Reprise à {downloaded} bytes")

                    try:
                        r = session.get(
                            candidate_url,
                            headers=headers,
                            stream=True,
                            timeout=(DownloadManager.CONNECT_TIMEOUT, DownloadManager.READ_TIMEOUT),
                        )
                        status = r.status_code

                        # Gestion spécifique des codes
                        if status in (403, 404):
                            r.close()
                            log_warning(f"{log_prefix} ⚠️ {status} sur {candidate_url}, bascule CDN")
                            break  # sort de la boucle per-node -> passe au CDN suivant

                        # 416 Range Not Satisfiable -> probablement déjà complet
                        if status == 416:
                            r.close()
                            # Vérifie/renomme si possible
                            if os.path.exists(tmp_path):
                                if DownloadManager._verify_file(tmp_path, final_path, url, total=0):
                                    try:
                                        os.replace(tmp_path, final_path)
                                        if on_progress:
                                            on_progress(os.path.getsize(final_path), "0 B/s", os.path.getsize(final_path))
                                        return True, None
                                    except Exception as e:
                                        log_error(f"{log_prefix} Échec renommage 416: {e}")
                            # sinon, on repart de zéro
                            if os.path.exists(tmp_path):
                                try:
                                    os.remove(tmp_path)
                                except Exception:
                                    pass
                            per_node_retries += 1
                            total_retries += 1
                            DownloadManager._sleep_with_jitter(retry_delay, per_node_retries)
                            continue

                        r.raise_for_status()

                        # Si le serveur ignore la Range (status 200) alors qu'on voulait reprendre
                        if status == 200 and downloaded > 0:
                            # On repart proprement de zéro
                            try:
                                os.remove(tmp_path)
                            except FileNotFoundError:
                                pass
                            downloaded = 0
                            mode = "wb"

                        # Total attendu
                        content_len = r.headers.get("Content-Length")
                        total = int(content_len) + downloaded if content_len else 0

                        last_report_t = 0.0
                        min_emit_interval = 0.1
                        last_chunk_time = time.time()
                        last_time = time.time()
                        last_bytes = downloaded

                        with open(tmp_path, mode) as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                if should_stop and should_stop():
                                    r.close()
                                    return False, "Stopped"

                                now = time.time()
                                # watchdog per-chunk
                                if now - last_chunk_time > DownloadManager.PER_CHUNK_TIMEOUT:
                                    r.close()
                                    raise TimeoutError("Aucun chunk reçu pendant 30 secondes")

                                if not chunk:
                                    continue

                                f.write(chunk)
                                downloaded += len(chunk)
                                last_chunk_time = now

                                # vitesse + progress throttlé
                                if (now - last_report_t) >= min_emit_interval and on_progress:
                                    speed_str = DownloadManager._calc_speed(downloaded, last_bytes, now, last_time)
                                    last_bytes, last_time = downloaded, now
                                    try:
                                        on_progress(downloaded, speed_str, total)
                                    except Exception:
                                        # ne jamais faire planter le thread à cause du callback UI
                                        pass
                                    last_report_t = now

                        r.close()

                        # Vérification fichier téléchargé
                        if not DownloadManager._verify_file(tmp_path, final_path, url, total):
                            per_node_retries += 1
                            total_retries += 1
                            if total_retries >= DownloadManager.MAX_TOTAL_RETRIES:
                                log_error(f"{log_prefix} 💀 Échec (max retries global atteint)")
                                return False, "Échec complet"
                            DownloadManager._sleep_with_jitter(retry_delay, per_node_retries)
                            continue

                        # Renommage atomique
                        try:
                            os.replace(tmp_path, final_path)
                        except Exception as e:
                            log_error(f"{log_prefix} Échec renommage : {e}")
                            per_node_retries += 1
                            total_retries += 1
                            DownloadManager._sleep_with_jitter(retry_delay, per_node_retries)
                            continue

                        # Progress final
                        try:
                            if on_progress:
                                size_final = os.path.getsize(final_path)
                                on_progress(size_final, "0 B/s", total or size_final)
                        except Exception:
                            pass

                        return True, None

                    except requests.HTTPError as e:
                        per_node_retries += 1
                        total_retries += 1
                        # si code déjà géré plus haut, on n'arrive pas ici
                        log_warning(f"{log_prefix} ⚠️ HTTPError: {e} → retry {per_node_retries}/{DownloadManager.MAX_RETRIES_PER_NODE}")
                        if total_retries >= DownloadManager.MAX_TOTAL_RETRIES:
                            log_error(f"{log_prefix} 💀 Échec (max retries global atteint)")
                            return False, "Échec complet"
                        DownloadManager._sleep_with_jitter(retry_delay, per_node_retries)

                    except (requests.ReadTimeout, requests.ConnectTimeout, requests.ConnectionError, TimeoutError) as e:
                        per_node_retries += 1
                        total_retries += 1
                        log_warning(f"{log_prefix} ⚠️ Erreur réseau : {e} → retry {per_node_retries}/{DownloadManager.MAX_RETRIES_PER_NODE}")
                        if total_retries >= DownloadManager.MAX_TOTAL_RETRIES:
                            log_error(f"{log_prefix} 💀 Échec (max retries global atteint)")
                            return False, "Échec complet"
                        DownloadManager._sleep_with_jitter(retry_delay, per_node_retries)

                    except Exception as e:
                        per_node_retries += 1
                        total_retries += 1
                        log_warning(f"{log_prefix} ⚠️ Erreur : {e} → retry {per_node_retries}/{DownloadManager.MAX_RETRIES_PER_NODE}")
                        if total_retries >= DownloadManager.MAX_TOTAL_RETRIES:
                            log_error(f"{log_prefix} 💀 Échec (max retries global atteint)")
                            return False, "Échec complet"
                        DownloadManager._sleep_with_jitter(retry_delay, per_node_retries)

            log_error(f"{log_prefix} 💀 Échec complet")
            return False, "Échec complet"

        finally:
            try:
                session.close()
            except Exception:
                pass

    @staticmethod
    def _sleep_with_jitter(base_delay, attempt):
        # backoff exponentiel + jitter (évite les rafales synchrones)
        delay = base_delay * (2 ** max(0, attempt - 1))
        delay = min(delay, 30)  # cap raisonnable
        time.sleep(delay * (0.8 + 0.4 * random.random()))

    @staticmethod
    def _calc_speed(downloaded, last_downloaded, current_time, last_time):
        time_diff = current_time - last_time
        if time_diff <= 0.01:
            return "0 B/s"
        delta = downloaded - last_downloaded
        speed = max(0, delta) / time_diff
        if speed < 1024:
            return f"{speed:.1f} B/s"
        if speed < 1024 * 1024:
            return f"{speed/1024:.1f} KB/s"
        return f"{speed/(1024*1024):.1f} MB/s"

    @staticmethod
    def _verify_file(tmp_path, final_path, url, total):
        if not os.path.exists(tmp_path):
            raise Exception("Fichier .tmp manquant après téléchargement")

        real_size = os.path.getsize(tmp_path)

        # Si on a un total connu, exige au moins 95%
        if total and real_size < total * 0.95:
            log_warning(f"Incomplet {real_size}/{total}")
            return False

        # Vérif checksum (si applicable à ce CDN)
        if not verify_hash_from_cdn_path(tmp_path, url):
            log_warning("Checksum invalide")
            return False

        # Validation basique selon extension
        lower = final_path.lower()
        if lower.endswith((".mp4", ".webm", ".mkv", ".m4v")) and not is_valid_video(tmp_path):
            log_warning("Vidéo invalide")
            return False
        if lower.endswith((".jpg", ".jpeg", ".png", ".webp")) and not is_valid_image(tmp_path):
            log_warning("Image invalide")
            return False

        return True
