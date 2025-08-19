import os
import shutil

from log import log_warning, log_info


def move_into_standard_dir(selected_dir, service, username):
    base_dir = os.path.join("downloads", service, username)
    os.makedirs(base_dir, exist_ok=True)

    log_info(f"[Move] 📦 Déplacement dans : {base_dir}")

    for fname in os.listdir(selected_dir):
        src = os.path.join(selected_dir, fname)
        dst = os.path.join(base_dir, fname)
        try:
            shutil.move(src, dst)
            log_info(f"[Move] 🔁 {fname} → {base_dir}")
        except Exception as e:
            log_warning(f"[Move] ⚠️ Erreur move {fname} : {e}")

    # Supprimer l’ancien dossier si vide
    try:
        if not os.listdir(selected_dir):
            os.rmdir(selected_dir)
            log_info(f"[Move] 🧹 Ancien dossier supprimé : {selected_dir}")
    except Exception as e:
        log_warning(f"[Move] ⚠️ Suppression échouée : {e}")

    return base_dir