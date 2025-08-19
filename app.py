# --- Standard library ---
import os
import re
import json
import shutil
import platform
import threading
import subprocess
from datetime import datetime, timezone
from urllib.parse import urlparse
import tkinter as tk
from tkinter import messagebox
from tkinter.simpledialog import askstring
import time

# --- Third-party libraries ---
import requests

from event_bus import event_bus
# --- Local imports ---
from ui.app_ui import AppUI
from media_window import MediaWindow
from core.profile_manager import ProfileManager, ProfileKey
from media_utils import clean_profile_folder
from core.log import log_info, log_error, log_debug, log_warning
from settings import load_settings, save_settings
from utils.format_utils import format_bytes
from utils.file_utils import sha256_file
from utils.media_utils import enrich_media_status
from utils.profile_utils import extract_profile_info, get_fansly_username_from_id
from utils.api_utils import fetch_medias_from_api

SETTINGS_PATH = "settings.json"

# Load settings
if os.path.exists(SETTINGS_PATH):
    try:
        with open(SETTINGS_PATH, "r") as f:
            SETTINGS = json.load(f)
    except Exception:
        SETTINGS = {}
else:
    SETTINGS = {}

DEFAULT_DOWNLOAD_DIR = SETTINGS.get("download_dir", "downloads")


class App:
    def __init__(self, root):
        self.root = root

        # charge en premier
        self.settings = load_settings()
        self.download_dir = self.settings.get("download_dir", "downloads")
        self.data_dir = "data"
        self.profile_ids = {}
        self.profile_names = {}
        self.profile_download_dirs = dict(self.settings.get("profile_dirs", {}))

        self._reload_after_id = None
        self._allowed_reasons = {
            "manual_refresh",  # bouton Update
            "profile_added",  # ajout d’un profil
            "import_done",  # import d’un dossier existant
            "dir_changed",  # changement de dossier
        }

        def _schedule_load_profiles(sort=True, delay_ms=200):
            if self._reload_after_id:
                try:
                    self.root.after_cancel(self._reload_after_id)
                except Exception:
                    pass

            def _run():
                self._reload_after_id = None
                self.load_profiles(sort=sort)

            self._reload_after_id = self.root.after(delay_ms, _run)

        def _on_profile_update(data=None):
            reason = None;
            sort = True
            if isinstance(data, dict):
                reason = data.get("reason")
                if data.get("no_sort") is True:
                    sort = False

            if reason not in self._allowed_reasons:
                log_debug(f"[App] profile:update ignoré (reason={reason})")
                return

            log_info(f"[App] profile:update accepté (reason={reason}, sort={sort})")
            _schedule_load_profiles(sort=sort, delay_ms=200)

        event_bus.subscribe("profile:update", _on_profile_update)


        def _spy_update(data=None):
            try:
                import traceback
                stack = "".join(traceback.format_stack(limit=8))
                log_debug(f"[SPY] profile:update payload={data}\nFrom:\n{stack}")
            except Exception:
                pass

        event_bus.subscribe("profile:update", _spy_update)

        # UI puis manager
        self.ui = AppUI(root, controller=self)
        self.pm = ProfileManager(
            data_dir=self.data_dir,
            default_download_dir=self.download_dir,
            profile_dirs=self.profile_download_dirs,
        )

        self.load_profiles()

    # --------- Actions globales (appelées par l’UI) ---------
    def change_download_dir(self):
        from tkinter import filedialog
        selected_dir = filedialog.askdirectory(title="Choisir le dossier global de téléchargement")
        if not selected_dir:
            return
        self.download_dir = selected_dir
        self.settings["download_dir"] = selected_dir
        save_settings(self.settings)

        # maj du manager pour les prochains calculs/insertions
        self.pm.default_download_dir = selected_dir

        messagebox.showinfo("Succès", f"Dossier de téléchargement mis à jour:\n{selected_dir}")

        # ⬇️ au lieu de self.load_profiles()
        self.root.after(0, lambda: event_bus.publish("profile:update", {
            "reason": "dir_changed",
            "no_sort": True
        }))

    def on_right_click(self, event):
        item_id = self.ui.tree.identify_row(event.y)
        if not item_id:
            return

        def open_dir():
            values = self.ui.tree.item(item_id)["values"]
            path = values[9] if len(values) >= 10 else None
            if path and os.path.exists(path):
                if platform.system() == "Darwin":
                    subprocess.Popen(["open", path])
                elif platform.system() == "Windows":
                    os.startfile(path)
                else:
                    subprocess.Popen(["xdg-open", path])

        def update_profile():
            self.refresh_profile(item_id)

        # ⬅️ IMPORTANT: call the class method via the threaded wrapper WITH item_id
        self.ui.popup_menu(
            event.x_root, event.y_root,
            [
                ("Update", update_profile),
                ("Open Folder", open_dir),
                ("Copier URL profil", lambda: self.copy_profile_url(item_id)),
                ("Changer dossier", lambda: self.change_profile_dir_threaded(item_id)),
                ("Supprimer", lambda item_id=item_id: self._delete_profile_cmd(item_id)),
            ],
        )

    def change_profile_dir_threaded(self, item_id):
        threading.Thread(target=self.change_profile_dir, args=(item_id,), daemon=True).start()

    def _delete_profile_cmd(self, item_id):
        vals = self.ui.tree.item(item_id)["values"]
        key = ProfileKey(vals[0], str(vals[1]).replace("📁 ", ""))

        if not messagebox.askyesno("Confirmation", f"Supprimer le profil {key.username} et ses données ?"):
            return
        try:
            self.pm.delete_profile(key)
            # mets à jour les settings locaux si tu stockes les custom dirs
            if key.as_str() in self.profile_download_dirs:
                del self.profile_download_dirs[key.as_str()]
                self.settings["profile_dirs"] = self.profile_download_dirs
                save_settings(self.settings)

            # enlève la ligne dans l’UI
            self.ui.tree.delete(item_id)
            log_info(f"[Delete] Profil supprimé : {key.username}")
        except Exception as e:
            log_error(f"[Delete] Échec suppression : {e}")
            messagebox.showerror("Erreur", str(e))

    def compute_profile_progress(medias):
        """
        Retourne (percent, status_text, status_tag, counts)
        percent = % complété en EXCLUANT les médias "Ignored"
        status_text = texte/emoji à afficher
        status_tag = tag pour colorer la ligne
        counts = dict utile si tu veux (completed, total, ignored)
        """
        # on ne compte que les médias pertinents
        relevant = [m for m in medias if m.get("type") in ("video", "image")]

        total = len(relevant)
        ignored = sum(1 for m in relevant if (m.get("status") == "Ignored"))
        completed = sum(1 for m in relevant if (m.get("status") == "Completed"))
        # tout le reste (Missing, Paused, Failed, etc.) = non complété

        effective_total = total - ignored
        if effective_total <= 0:
            percent = 100.0  # tout ignoré => considéré "à jour"
        else:
            percent = round((completed / effective_total) * 100.0, 1)

        # statut visuel
        if percent >= 100.0:
            status_text = "✓ 100%"
            status_tag = "status.done"
        elif percent <= 0.0:
            status_text = "✗ 0%"
            status_tag = "status.none"
        else:
            status_text = f"⏳ {percent}%"
            status_tag = "status.progress"

        return percent, status_text, status_tag, {
            "total": total,
            "ignored": ignored,
            "completed": completed,
            "effective_total": effective_total
        }

    def _set_row_moving_state(self, item_id, is_moving: bool):
        tree = self.ui.tree
        if not tree.exists(item_id):
            return
        vals = list(tree.item(item_id, "values"))
        # Nom d’affichage (col 1 = username avec éventuel 📁)
        display_name = str(vals[1])
        if is_moving:
            if not display_name.startswith("📁 "):
                display_name = "📁 " + display_name
            vals[1] = display_name
            vals[2] = "Moving…"
            tree.item(item_id, values=tuple(vals), tags=("status.moving",))
        else:
            # Recalcule le tag via le % actuel (col 8)
            try:
                percent = float(str(vals[7]).replace("%", "").strip())
                if percent >= 100.0:
                    tag = "status.done"
                elif percent <= 0.0:
                    tag = "status.none"
                else:
                    tag = "status.progress"
            except Exception:
                tag = "status.progress"
            tree.item(item_id, values=tuple(vals), tags=(tag,))

    def _set_row_moving_progress(self, item_id, percent: float):
        tree = self.ui.tree
        if not tree.exists(item_id):
            return
        vals = list(tree.item(item_id, "values"))
        vals[7] = f"{percent:.1f}%"
        # On garde la ligne grisée pendant le move
        tree.item(item_id, values=tuple(vals), tags=("status.moving",))

    def change_profile_dir(self, item_id):
        from tkinter import filedialog

        vals = self.ui.tree.item(item_id)["values"]
        key = ProfileKey(vals[0], str(vals[1]).replace("📁 ", ""))

        new_dir = filedialog.askdirectory(title="Choisir un nouveau dossier pour ce profil")
        if not new_dir:
            return

        # Paths source/destination (pour le calcul du % temps réel)
        try:
            # chemin actuel affiché en col 10
            current_path = str(vals[9])
            if not current_path or not os.path.isdir(current_path):
                # fallback via ProfileManager
                current_path = self.pm.get_profile_dir(key)

            # chemin final attendu
            abs_base = os.path.abspath(new_dir)
            final_path = os.path.join(abs_base, key.service, key.username)

            # taille totale à “copier”
            total_src_bytes = self.calculate_folder_size(current_path)
        except Exception:
            total_src_bytes = 0

        # 1) État visuel "moving"
        self._set_row_moving_state(item_id, True)

        # 2) Thread de move + poll de progression
        moving_done = threading.Event()
        move_error = {"err": None}

        def _do_move():
            try:
                # déplace (et met à jour mapping interne du manager)
                self.pm.move_profile_dir(key, new_dir)

                # persist settings côté App
                abs_dir = os.path.abspath(new_dir)
                self.profile_download_dirs[key.as_str()] = abs_dir
                self.settings.setdefault("profile_dirs", {})
                self.settings["profile_dirs"][key.as_str()] = abs_dir
                save_settings(self.settings)

                # sync éventuel du cache interne du manager
                try:
                    self.pm.profile_dirs[key.as_str()] = abs_dir
                except Exception:
                    pass
            except Exception as e:
                move_error["err"] = e
            finally:
                moving_done.set()

        def _poll_progress():
            # Si total inconnu/0, on fait juste clignoter de 0→100 à la fin
            if total_src_bytes <= 0:
                while not moving_done.is_set():
                    time.sleep(0.25)
                self.root.after(0, lambda: self._set_row_moving_progress(item_id, 100.0))
                return

            while not moving_done.is_set():
                try:
                    # calcule la taille déjà présente à destination
                    moved_bytes = self.calculate_folder_size(final_path)
                    percent = min(100.0, (moved_bytes / total_src_bytes) * 100.0)
                    self.root.after(0, lambda p=percent: self._set_row_moving_progress(item_id, p))
                except Exception:
                    pass
                time.sleep(0.25)

            # un dernier update à 100% à la fin (au cas où)
            self.root.after(0, lambda: self._set_row_moving_progress(item_id, 100.0))

        t_move = threading.Thread(target=_do_move, daemon=True)
        t_poll = threading.Thread(target=_poll_progress, daemon=True)
        t_move.start()
        t_poll.start()

        def _finish():
            if move_error["err"]:
                log_error(f"[Move] {move_error['err']}")
                messagebox.showerror("Erreur", str(move_error["err"]))
                # on enlève l’état “moving” même en cas d’erreur
                self._set_row_moving_state(item_id, False)
                return

            # broadcast un refresh (garde le tri) → va aussi remettre le 📁 proprement
            self.root.after(0, lambda: event_bus.publish("profile:update", {
                "reason": "dir_changed",
                "no_sort": True
            }))

            # Replace l’état visuel si la ligne existe encore
            self._set_row_moving_state(item_id, False)
            messagebox.showinfo("Succès", f"Nouveau dossier défini pour {key.username}")

        # watcher pour appeler _finish quand c’est fini
        def _watch_done():
            if moving_done.is_set():
                _finish()
            else:
                self.root.after(150, _watch_done)

        self.root.after(150, _watch_done)

    # ---------- utils ----------
    def calculate_folder_size(self, folder_path):
        if not os.path.exists(folder_path):
            return 0
        total_size = 0
        for dirpath, _, filenames in os.walk(folder_path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                total_size += os.path.getsize(file_path)
        return total_size

    def get_size_thread(self, folder_path, callback):
        def worker():
            try:
                size = self.calculate_folder_size(folder_path)
                callback(size)
            except Exception as e:
                print(f"Error calculating size for {folder_path}: {e}")
        threading.Thread(target=worker, daemon=True).start()

    # ---------- refresh/update ----------
    def refresh_profile(self, item_id):
        threading.Thread(target=self._refresh_profile_worker, args=(item_id,), daemon=True).start()

    def _refresh_profile_worker(self, item_id):
        vals = self.ui.tree.item(item_id)["values"]
        key = ProfileKey(service=vals[0], username=str(vals[1]).replace("📁 ", ""))
        try:
            new_cnt, total_before, total_after = self.pm.refresh_profile(key)

            # ⬇️ au lieu de self.root.after(0, self.load_profiles)
            self.root.after(0, lambda: event_bus.publish("profile:update", {
                "reason": "manual_refresh",
                "no_sort": True
            }))

            msg = f"📥 Update terminé\n{new_cnt} nouveaux médias" if new_cnt else "📥 Update terminé\nAucune nouvelle publication"
            self.root.after(100, lambda: messagebox.showinfo("Mise à jour du profil", msg))
        except Exception as e:
            log_error(f"[Refresh] {key.as_str()} → {e}")
            self.root.after(0, lambda: messagebox.showerror("Erreur", str(e)))

    # ---------- Handlers déclenchés par l’UI ----------
    def handle_update_selected(self):
        selected = self.ui.tree.selection()
        if selected:
            item_id = selected[0]
            self.refresh_profile(item_id)

    def handle_open_dir_selected(self):
        selected = self.ui.tree.selection()
        if selected:
            item_id = selected[0]
            self.open_profile_dir(item_id)

    def handle_download_selected(self):
        selected = self.ui.tree.selection()
        if selected:
            item_id = selected[0]
            self.download_all_selected(item_id)  # garde ta logique existante

    def handle_change_dir_selected(self):
        selected = self.ui.tree.selection()
        if selected:
            item_id = selected[0]
            self.change_profile_dir_threaded(item_id)

    def handle_add_already_downloaded(self):
        self.root.after(0, self.prompt_profile_import)

    # ---------- Reste inchangé / adapté à l’UI ----------
    def open_profile_dir(self, item_id):
        try:
            values = self.ui.tree.item(item_id)["values"]
            if len(values) < 10:
                log_warning(f"[Open Dir] ⚠️ Valeurs incomplètes pour item {item_id}")
                return
            path = values[9]
            if not os.path.isdir(path):
                log_warning(f"[Open Dir] ⚠️ Dossier introuvable : {path}")
                return

            if platform.system() == "Darwin":
                subprocess.run(["open", path])
            elif platform.system() == "Windows":
                os.startfile(path)
            elif platform.system() == "Linux":
                subprocess.run(["xdg-open", path])
            else:
                log_warning(f"[Open Dir] ❌ OS non supporté : {platform.system()}")

        except Exception as e:
            log_error(f"[Open Dir] ❌ Erreur ouverture : {e}")

    def copy_profile_url(self, item_id):
        try:
            values = self.ui.tree.item(item_id, "values")
            if not values or len(values) < 2:
                log_warning("[URL] Données de profil incomplètes")
                return
            service = values[0]
            username = str(values[1]).replace("📁 ", "")
            url = f"https://coomer.st/{service}/user/{username}"
            self.root.clipboard_clear()
            self.root.clipboard_append(url)
            self.root.update()
            log_info(f"[URL] Copié dans presse-papiers : {url}")
        except Exception as e:
            log_error(f"[URL] Erreur copie URL : {e}")

    def load_profiles(self, *, sort: bool = True):
        log_info(f"[App] load_profiles(sort={sort})")
        tree = self.ui.tree
        tree.delete(*tree.get_children())
        self.profile_ids.clear()

        # Tags (idempotent)
        try:
            tree.tag_configure("status.done", foreground="#00c853")
            tree.tag_configure("status.progress", foreground="#ffd600")
            tree.tag_configure("status.none", foreground="#ff5252")
            tree.tag_configure("status.moving", foreground="#9e9e9e")
        except Exception:
            pass

        total_profiles = 0
        total_medias = 0

        for row in self.pm.list_profiles():
            medias = row.medias or []
            videos = [m for m in medias if m.get("type") == "video"]
            photos = [m for m in medias if m.get("type") == "image"]

            videos_rel = [m for m in videos if m.get("status") != "Ignored"]
            photos_rel = [m for m in photos if m.get("status") != "Ignored"]

            def is_completed(m):
                st = (m.get("status") or "").strip()
                if st == "Completed":
                    return True
                try:
                    p = float(str(m.get("percent", 0)).replace("%", ""))
                    return p >= 100
                except Exception:
                    return False

            videos_completed = sum(1 for m in videos_rel if is_completed(m))
            photos_completed = sum(1 for m in photos_rel if is_completed(m))
            videos_total = len(videos_rel)
            photos_total = len(photos_rel)

            effective_total = videos_total + photos_total
            completed_all = videos_completed + photos_completed
            percent = 100.0 if effective_total == 0 else round((completed_all / effective_total) * 100.0, 1)

            if percent >= 100.0:
                status_text = "✓ 100%";
                status_tag = "status.done"
            elif percent <= 0.0:
                status_text = "✗ 0%";
                status_tag = "status.none"
            else:
                status_text = f"⏳ {percent}%";
                status_tag = "status.progress"

            default_path = os.path.abspath(os.path.join(self.download_dir, row.key.service, row.key.username))
            display_name = f"📁 {row.key.username}" if os.path.abspath(
                row.download_path) != default_path else row.key.username

            item_id = tree.insert(
                "", "end",
                values=(
                    row.key.service,
                    display_name,
                    status_text,
                    f"{videos_completed}/{videos_total}",
                    f"{photos_completed}/{photos_total}",
                    "0 MB",
                    "0 MB",
                    f"{percent:.1f}%",
                    row.last_update.split(".")[0].replace("T", " "),
                    row.download_path,
                ),
                tags=(status_tag,),
            )
            self.profile_ids[row.key.as_str()] = item_id

            v_dir = os.path.join(row.download_path, "v")
            p_dir = os.path.join(row.download_path, "p")
            self.get_size_thread(v_dir, lambda v, item_id=item_id: self._update_sizes(item_id, v, 0))
            self.get_size_thread(p_dir, lambda p, item_id=item_id: self._update_sizes(item_id, 0, p))

            total_profiles += 1
            total_medias += len(medias)

        # ---- Tri optionnel ----
        if sort:
            items = list(tree.get_children(""))

            def _norm(v: str) -> str:
                return (v or "").replace("📁", "").strip().lower()

            items.sort(key=lambda iid: _norm(tree.item(iid, "values")[1]))
            for idx, iid in enumerate(items):
                tree.move(iid, "", idx)

        self.ui.set_stats(f"Stats globales: {total_profiles} profils, {total_medias} médias")

    def _update_sizes(self, item_id, v_bytes, p_bytes):
        tree = self.ui.tree
        if not tree.exists(item_id): return
        vals = list(tree.item(item_id)["values"])
        if v_bytes:
            vals[5] = format_bytes(v_bytes)
        if p_bytes:
            vals[6] = format_bytes(p_bytes)
        tree.item(item_id, values=tuple(vals))

    def treeview_sort_column(self, col, reverse):
        tree = self.ui.tree
        try:
            items = [(tree.set(k, col), k) for k in tree.get_children('')]

            def remove_visual_prefix(val):
                if not isinstance(val, str):
                    return val
                return re.sub(r"^[^\w\d]+", "", val).strip()

            def convert(val):
                try:
                    if val is None:
                        return (1, 0.0)
                    val = str(val).strip()
                    val = remove_visual_prefix(val)

                    m = re.match(r"^([\d.,]+)\s?(Go|Mo|GB|MB)$", val, re.IGNORECASE)
                    if m:
                        num, unit = m.groups()
                        num = float(num.replace(",", "."))
                        unit = unit.lower()
                        if unit in ("go", "gb"):
                            return (1, num * 1024)
                        elif unit in ("mo", "mb"):
                            return (1, num)
                        return (1, num)

                    if "/" in val:
                        return (1, float(val.split("/")[0]))
                    elif "%" in val:
                        return (1, float(val.replace("%", "")))
                    elif val.replace(".", "", 1).isdigit():
                        return (1, float(val))
                    else:
                        return (0, val.lower())
                except Exception as e:
                    log_warning(f"[Sort] ⚠️ conversion échouée : {val} → {e}")
                    return (0, str(val))

            items.sort(key=lambda t: convert(t[0]), reverse=reverse)
            for index, (_, k) in enumerate(items):
                tree.move(k, '', index)
            tree.heading(col, command=lambda: self.treeview_sort_column(col, not reverse))
        except Exception as e:
            log_error(f"[Sort] Erreur tri colonne '{col}': {e}")

    # ---------- Ajout / import ----------
    def add_profile_threaded(self):
        threading.Thread(target=self._add_profile_worker, daemon=True).start()

    def on_profile_double_click(self, _event):
        selection = self.ui.tree.selection()
        if not selection:
            return
        profile_values = self.ui.tree.item(selection[0])["values"]
        service = profile_values[0]
        username = str(profile_values[1]).replace("📁 ", "")
        json_path = f"data/{service}/{username}.json"
        if not os.path.exists(json_path):
            messagebox.showerror("Erreur", f"Fichier JSON introuvable pour {username}")
            log_error(f"[DoubleClick] Aucun JSON trouvé pour {username} ({json_path})")
            return

        profile_key = f"{service}:{username}"
        base_dir = self.profile_download_dirs.get(profile_key, self.download_dir)
        local_dir = os.path.join(base_dir, service, username)

        log_info(f"[DoubleClick] Ouverture de {username} (fichier: {json_path})")
        try:
            with open(json_path, 'r') as f:
                medias_data = json.load(f)
        except json.JSONDecodeError as e:
            log_error(f"[JSON] JSON corrompu : {json_path} ({e})")
            messagebox.showerror("Erreur JSON", f"Le fichier {json_path} est corrompu ou incomplet.\n\nDétail :\n{e}")
            return

        MediaWindow(tk.Toplevel(self.root), service, username, local_dir, json_path, medias_data)

    def handle_add_already_downloaded(self):
        self.root.after(0, self.prompt_profile_import)

    def prompt_profile_import(self):
        from tkinter import filedialog
        selected_dir = filedialog.askdirectory(title="Choisir le dossier du profil déjà téléchargé")
        if not selected_dir:
            log_warning("[Import] Dossier non sélectionné, opération annulée.")
            return

        url = askstring("Entrer l'URL Coomer", "Entre l'URL Coomer/Fansly du profil (ex: https://coomer.su/onlyfans/martine)")
        if not url or not url.startswith("http"):
            log_warning("[Import] URL invalide ou manquante, opération annulée.")
            return

        threading.Thread(target=self.add_already_downloaded, args=(selected_dir, url), daemon=True).start()

    def add_already_downloaded(self, selected_dir, url):
        try:
            key = self.pm.import_existing(selected_dir, url)
            self.settings["profile_dirs"][key.as_str()] = selected_dir
            save_settings(self.settings)

            # ⬇️ au lieu de self.root.after(0, self.load_profiles)
            self.root.after(0, lambda: event_bus.publish("profile:update", {
                "reason": "import_done",
                "no_sort": True
            }))

            log_info(f"[Import] Done for {key.as_str()}")
        except Exception as e:
            log_error(f"[Import] {e}")
            messagebox.showerror("Import", str(e))

    def _add_profile_worker(self):
        if not self.data_dir:
            self.data_dir = "data"

        url = self.ui.read_add_url()
        if not url:
            return

        service, raw_username = extract_profile_info(url)
        raw_username = str(raw_username)

        username = raw_username
        profile_name = username

        if service == "fansly" and raw_username.isdigit():
            resolved = get_fansly_username_from_id(raw_username)
            if resolved:
                username = resolved
                profile_name = resolved
                self.profile_names[f"{service}:resolved"] = resolved
                log_info(f"[RESOLVED] Fansly ID {raw_username} → {username}")
            else:
                log_warning(f"[FANSLY RESOLVE] Échec résolution pour {raw_username}")
                messagebox.showerror("Erreur", f"Impossible de résoudre l'identifiant Fansly : {raw_username}")
                return

        username = str(username)
        profile_key = f"{service}:{username}"

        save_dir = os.path.join(self.data_dir, service)
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, f"{username}.json")

        # insertion d'une ligne "chargement"
        def insert_loading_row():
            tree = self.ui.tree
            profile_id = tree.insert(
                "", tk.END,
                values=(service, username, "", "0/0", "0/0", "0 MB", "0 MB", "0%", "chargement...", ""),
                tags=("loading",)
            )
            self.profile_ids[profile_key] = profile_id
            log_debug(f"[INSERT] {username} (chargement) inséré avec ID {profile_id}")

        self.root.after(0, insert_loading_row)

        medias = []

        def is_completed(m):
            try:
                return float(str(m.get("percent", 0)).replace("%", "")) >= 100
            except:
                return False

        def update_row():
            tree = self.ui.tree
            real_id = self.profile_ids.get(profile_key)
            if not real_id or not tree.exists(real_id):
                return
            videos = [m for m in medias if m.get("type") == "video"]
            photos = [m for m in medias if m.get("type") == "image"]
            video_completed = sum(1 for m in videos if m.get("percent") == "100")
            photo_completed = sum(1 for m in photos if m.get("percent") == "100")
            total_videos = len(videos)
            total_photos = len(photos)
            percent = round((video_completed + photo_completed) / (total_videos + total_photos) * 100) if (total_videos + total_photos) else 0
            last_update_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            tree.item(real_id, values=(
                service, username, "",
                f"{video_completed}/{total_videos}",
                f"{photo_completed}/{total_photos}",
                "0 MB", "0 MB",
                f"{percent}%", last_update_str,
                os.path.join(self.download_dir, service, username)
            ))

        def finalize():
            tree = self.ui.tree
            real_id = self.profile_ids.get(profile_key)
            if not real_id or not tree.exists(real_id):
                return
            update_row()
            videos = [m for m in medias if m.get("type") == "video"]
            photos = [m for m in medias if m.get("type") == "image"]
            total = len(medias)
            completed = sum(1 for m in medias if m.get("status") == "Completed" or is_completed(m))
            percent = round((completed / total) * 100) if total else 0
            last_update_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            tree.item(real_id, values=(
                service, username, "",
                f"{sum(1 for m in videos if is_completed(m))}/{len(videos)}",
                f"{sum(1 for m in photos if is_completed(m))}/{len(photos)}",
                "0 MB", "0 MB",
                f"{percent}%", last_update_str,
                os.path.join(self.download_dir, service, username)
            ))
            tree.item(real_id, tags=("green" if percent == 100 else "yellow" if percent > 0 else "gray",))
            log_info(f"[FINALIZE] Profil {username} terminé : {len(medias)} médias")

        try:
            for page in fetch_medias_from_api(service, raw_username):
                for media in page:
                    ext = os.path.splitext(media["name"])[1].lower()
                    if ext in [".mp4", ".m4v", ".mov", ".webm", ".avi", ".mkv", ".flv"]:
                        media["type"] = "video"
                    elif ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]:
                        media["type"] = "image"
                    else:
                        media["type"] = "autre"
                    medias.append(media)

                    try:
                        base_dir = self.profile_download_dirs.get(profile_key, self.download_dir)
                        download_path = os.path.join(base_dir, service, username)
                        enrich_media_status(medias, download_path)
                        with open(save_path, "w") as f:
                            json.dump({
                                "medias": medias,
                                "last_update": datetime.now(timezone.utc).isoformat(),
                                "profile_name": username,
                                "custom_dir": os.path.abspath(os.path.join(self.download_dir, service, username))
                            }, f, indent=2)
                        self.root.after(0, update_row)
                    except Exception as e:
                        log_error(f"[JSON Write Error] {e}")
        except Exception as e:
            log_error(f"[API Fetch Error] {e}")
            return

        self.root.after(0, finalize)


if __name__ == "__main__":
    root = tk.Tk()
    root.title("Coomer Ultimate v1.0")
    root.geometry("1800x900")
    app = App(root)
    root.mainloop()
