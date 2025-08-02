from PIL import Image
import subprocess
import os
from log import log_info, log_error, log_debug, log_warning
import shutil

def is_valid_image(path):
    try:
        with Image.open(path) as img:
            img.verify()
        return True
    except Exception:
        return False

def is_valid_video(path):
    try:
        # Vérifie que ffprobe retourne bien des infos
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_format", "-show_streams", path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return result.returncode == 0
    except Exception:
        return False

def clean_profile_folder(profile_dir, service, username):
    log_info(f"[Clean] 📁 Nettoyage du dossier : {profile_dir}")

    # Nouveau dossier racine : .../onlyfans/valentina/v etc.
    service_dir = os.path.join(profile_dir, service, username)
    v_dir = os.path.join(service_dir, "v")
    p_dir = os.path.join(service_dir, "p")
    o_dir = os.path.join(service_dir, "o")

    for d in [v_dir, p_dir, o_dir]:
        os.makedirs(d, exist_ok=True)

    img_ext = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
    vid_ext = {".mp4", ".m4v", ".mov", ".webm", ".avi", ".mkv", ".flv"}

    for root, _, files in os.walk(profile_dir):
        for fname in files:
            if fname.startswith("."):
                # Supprime le fichier caché si possible
                try:
                    os.remove(os.path.join(root, fname))
                    log_info(f"[Clean] 🗑️ Fichier caché supprimé : {fname}")
                except Exception as e:
                    log_warning(f"[Clean] ⚠️ Erreur suppression fichier caché : {fname} ({e})")
                continue

            fpath = os.path.join(root, fname)

            # Ignore les fichiers déjà bien placés
            if any(fpath.startswith(d + os.sep) for d in [v_dir, p_dir, o_dir]):
                continue

            ext = os.path.splitext(fname)[1].lower()
            dest_dir = (
                p_dir if ext in img_ext else
                v_dir if ext in vid_ext else
                o_dir
            )

            try:
                new_path = os.path.join(dest_dir, fname)
                shutil.move(fpath, new_path)
                log_info(f"[Clean] 🔁 {fname} → {os.path.basename(dest_dir)}")
            except Exception as e:
                log_warning(f"[Clean] ⚠️ Erreur move {fname} : {e}")

    # Supprimer anciens dossiers s'ils sont vides
    for root, dirs, _ in os.walk(profile_dir, topdown=False):
        for d in dirs:
            if d.startswith("."):
                continue  # ignore dossier caché
            dir_path = os.path.join(root, d)
            if not os.listdir(dir_path):
                try:
                    os.rmdir(dir_path)
                    log_info(f"[Clean] 🧹 Dossier vide supprimé : {dir_path}")
                except Exception as e:
                    log_warning(f"[Clean] ⚠️ Suppression échouée : {dir_path} ({e})")
