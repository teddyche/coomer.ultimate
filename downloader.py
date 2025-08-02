import os
import time
import requests
from urllib.parse import urlparse
from log import log_info, log_error, log_warning
from utils import verify_hash_from_cdn_path, build_media_url
from media_utils import is_valid_video, is_valid_image

CDN_NODES = ["n1", "n2", "n3", "n4"]

def generate_alternative_urls(original_url):
    parsed = urlparse(original_url)
    path = parsed.path
    base = "coomer.st"
    urls = [f"https://{base}{path}"]
    for node in CDN_NODES:
        urls.append(f"https://{node}.{base}{path}")
    return urls

def download_file(url, final_path, on_progress=None, resume=True, retry_delay=10, should_stop=None, window_id=None):
    tmp_path = final_path if final_path.endswith(".tmp") else final_path + ".tmp"
    all_urls = generate_alternative_urls(url)
    log_prefix = f"[DL] [Window {window_id}]" if window_id else "[DL]"
    log_info(f"{log_prefix} ▶️ Début téléchargement pour {final_path} depuis {url}")
    last_progress_update = 0
    min_progress_interval = 0.1
    session = requests.Session()

    for candidate_url in all_urls:
        log_info(f"{log_prefix} 🌐 Test CDN : {candidate_url}")

        retries = 0
        max_retries = 3
        while retries < max_retries:
            if should_stop and should_stop():
                log_info(f"{log_prefix} ⛔ Téléchargement interrompu avant démarrage de {candidate_url}")
                session.close()
                return False, "Stopped"

            headers = {}
            mode = "wb"
            downloaded = 0

            if resume and os.path.exists(tmp_path):
                downloaded = os.path.getsize(tmp_path)
                headers["Range"] = f"bytes={downloaded}-"
                mode = "ab"
                log_info(f"{log_prefix} 🔄 Reprise à {downloaded} bytes")

            try:
                log_info(f"{log_prefix} 📡 Connexion au serveur pour {candidate_url}")
                with session.get(candidate_url, headers=headers, stream=True, timeout=(10, 30)) as r:
                    r.raise_for_status()
                    total = int(r.headers.get("Content-Length", 0)) + downloaded if r.headers.get("Content-Length") else 0
                    log_info(f"{log_prefix} 📦 Taille attendue : {total} bytes")

                    with open(tmp_path, mode) as f:
                        chunk_count = 0
                        last_chunk_time = time.time()
                        last_downloaded = downloaded
                        last_time = time.time()

                        for chunk in r.iter_content(chunk_size=8192):
                            chunk_count += 1
                            if should_stop and should_stop():
                                log_info(f"{log_prefix} ⛔ Interruption pendant le chunk {chunk_count} pour {tmp_path}")
                                session.close()
                                return False, "Stopped"
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                current_time = time.time()

                                time_diff = current_time - last_time
                                if time_diff > 0.01:
                                    speed = (downloaded - last_downloaded) / time_diff
                                    speed_str = (
                                        f"{speed:.1f} B/s" if speed < 1024 else
                                        f"{speed/1024:.1f} KB/s" if speed < 1024*1024 else
                                        f"{speed/(1024*1024):.1f} MB/s"
                                    )
                                    log_info(f"{log_prefix} 📥 Reçu chunk {chunk_count} ({len(chunk)} bytes), total téléchargé : {downloaded}/{total}, vitesse : {speed_str}")
                                else:
                                    speed_str = "0 B/s"

                                last_downloaded = downloaded
                                last_time = current_time
                                last_chunk_time = current_time

                                if on_progress and (current_time - last_progress_update >= min_progress_interval):
                                    try:
                                        last_progress_update = current_time
                                        on_progress(downloaded, speed_str, total)
                                    except Exception as e:
                                        log_info(f"{log_prefix} 🚫 Erreur dans on_progress pour {tmp_path} : {e}")

                            if time.time() - last_chunk_time > 30:
                                raise Exception("Aucun chunk reçu pendant 30 secondes")

                    if not os.path.exists(tmp_path):
                        raise Exception("Fichier .tmp manquant après téléchargement")

                    real_size = os.path.getsize(tmp_path)
                    log_info(f"{log_prefix} 📏 Taille réelle : {real_size} bytes (attendu : {total} bytes)")

                    if total > 0 and real_size < total * 0.95:
                        log_warning(f"{log_prefix} ⏳ Incomplet ({real_size}/{total}, {(real_size/total)*100:.1f}%) → retry")
                        retries += 1
                        time.sleep(retry_delay)
                        continue

                    if not verify_hash_from_cdn_path(tmp_path, url):
                        log_warning(f"{log_prefix} ❌ Checksum invalide → retry")
                        retries += 1
                        time.sleep(retry_delay)
                        continue

                    if final_path.endswith((".mp4", ".webm", ".mkv")) and not is_valid_video(tmp_path):
                        log_warning(f"{log_prefix} ❌ Vidéo invalide → retry")
                        retries += 1
                        time.sleep(retry_delay)
                        continue
                    if final_path.endswith((".jpg", ".png", ".webp", ".jpeg")) and not is_valid_image(tmp_path):
                        log_warning(f"{log_prefix} ❌ Image invalide → retry")
                        retries += 1
                        time.sleep(retry_delay)
                        continue

                    # ✅ Renommage sécurisé
                    if not os.path.exists(tmp_path):
                        raise Exception("Fichier .tmp manquant juste avant renommage")

                    try:
                        os.rename(tmp_path, final_path)
                    except FileNotFoundError as e:
                        raise Exception(f"Échec renommage : {e}")

                    if not os.path.exists(final_path):
                        raise Exception("Fichier final non trouvé après renommage")

                    log_info(f"{log_prefix} ✅ Téléchargement terminé pour {final_path}")

                    if on_progress:
                        try:
                            on_progress(real_size, "0 B/s", real_size)
                            log_info(f"{log_prefix} 📢 Mise à jour finale à 100% pour {final_path}")
                        except Exception as e:
                            log_info(f"{log_prefix} 🚫 Erreur dans on_progress final pour {final_path} : {e}")

                    session.close()
                    return True, None

            except requests.exceptions.RequestException as e:
                retries += 1
                log_warning(f"{log_prefix} ⚠️ Erreur réseau (tentative {retries}/{max_retries}) : {e} → retry dans {retry_delay}s")
                time.sleep(retry_delay)
            except Exception as e:
                retries += 1
                log_warning(f"{log_prefix} ⚠️ Exception (tentative {retries}/{max_retries}) : {e} → retry dans {retry_delay}s")
                time.sleep(retry_delay)

        log_error(f"{log_prefix} 💀 Échec complet pour : {url}")
        session.close()
        return False, "Échec complet sur tous les CDN"
