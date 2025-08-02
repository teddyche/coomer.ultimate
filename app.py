import os
import json
import threading
import re
import tkinter as tk
from tkinter import ttk, messagebox
from media_window import MediaWindow
from utils import format_bytes, extract_profile_info, fetch_medias_from_api, rename_if_tmp_match
from datetime import datetime, timezone
import requests
import hashlib
from bs4 import BeautifulSoup
from log import log_info, log_error, log_debug, log_warning
from tkinter import filedialog
from utils import sha256_file
from urllib.parse import urlparse
import subprocess
import shutil
from utils import enrich_media_status, get_fansly_username_from_id
from tkinter.simpledialog import askstring
from media_utils import clean_profile_folder
from settings import load_settings, save_settings
import platform

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

class App:

    def setup_theme(self):
        style = ttk.Style(self.root)
        self.root.configure(bg="#2e2e2e")  # Fond sombre général

        style.theme_use("clam")

        # Champs de saisie
        style.configure("TEntry",
            fieldbackground="#3a3a3a",
            foreground="#ffffff",
            insertcolor="#ffffff",
            padding=5
        )

        # Boutons
        style.configure("TButton",
            padding=6,
            relief="flat",
            background="#444444",
            foreground="#ffffff",
            font=("Segoe UI", 10)
        )
        style.map("TButton",
            background=[("active", "#5a5a5a")],
            foreground=[("active", "#ffffff")]
        )

        # Labels
        style.configure("TLabel",
            background="#2e2e2e",
            foreground="#dddddd",
            font=("Segoe UI", 10)
        )

        # Cases à cocher
        style.configure("TCheckbutton",
            background="#2e2e2e",
            foreground="#dddddd"
        )

        # Boutons radio
        style.configure("TRadiobutton",
            background="#2e2e2e",
            foreground="#dddddd"
        )

        # Combobox
        style.configure("TCombobox",
            fieldbackground="#3a3a3a",
            background="#3a3a3a",
            foreground="#ffffff",
            padding=5
        )
        style.map("TCombobox",
            background=[("active", "#5a5a5a")]
        )

        # Barre de progression (horizontal)
        style.configure("Horizontal.TProgressbar",
            troughcolor="#444444",
            background="#888888",
            thickness=20
        )

        # Treeview sombre et épuré
        style.configure("Treeview",
            background="#2e2e2e",
            foreground="#ffffff",
            fieldbackground="#2e2e2e",
            rowheight=25,
            font=("Segoe UI", 10)
        )
        style.map("Treeview",
            background=[("selected", "#444444")],
            foreground=[("selected", "#ffffff")]
        )

        style.configure("Treeview.Heading",
            background="#3a3a3a",
            foreground="#ffffff",
            font=("Segoe UI", 10, "bold")
        )

    def __init__(self, root):
        self.root = root
        self.download_dir = DEFAULT_DOWNLOAD_DIR
        self.data_dir = "data"
        self.profile_ids = {}
        self.profile_names = {}
        self.profile_download_dirs = {}
        self.profile_download_dirs.update(SETTINGS.get("profile_dirs", {}))
        self.setup_theme()
        self.setup_ui()
        self.load_profiles()
        self.settings = load_settings()

    def setup_ui(self):
        self.root.configure(bg="#2e2e2e")
        style = ttk.Style()
        style.theme_use("clam")

        # Toolbar
        toolbar = tk.Frame(self.root, bg="#2e2e2e")
        toolbar.pack(fill=tk.X, padx=10, pady=5)

        refresh_btn = ttk.Button(toolbar, text="🔄 Rafraîchir", command=self.load_profiles)
        refresh_btn.pack(side=tk.LEFT, padx=5)

        settings_btn = ttk.Button(toolbar, text="⚙️ Settings", command=self.change_download_dir)
        settings_btn.pack(side=tk.LEFT, padx=5)

        add_label = ttk.Label(toolbar, text="➕ Ajouter profil (URL)")
        add_label.pack(side=tk.LEFT, padx=5)

        self.add_entry = ttk.Entry(toolbar, width=40)
        self.add_entry.pack(side=tk.LEFT, padx=5)

        add_btn = ttk.Button(toolbar, text="Ajouter", command=self.add_profile_threaded)
        add_btn.pack(side=tk.LEFT, padx=5)

        # Stats label
        self.stats_label = ttk.Label(self.root, text="Stats globales: 0 profils, 0 médias", anchor="w")
        self.stats_label.pack(fill=tk.X, padx=10, pady=5)

        # Action Frame
        self.profile_action_frame = ttk.Frame(self.root)
        self.profile_action_frame.pack(fill=tk.X, padx=10, pady=(0, 5))

        btn_update = ttk.Button(self.profile_action_frame, text="🔁 UPDATE", command=self.handle_update_selected)
        btn_open = ttk.Button(self.profile_action_frame, text="📂 OPEN DIR", command=self.handle_open_dir_selected)
        btn_download = ttk.Button(self.profile_action_frame, text="📥 DOWNLOAD", command=self.handle_download_selected)
        btn_change_dir = ttk.Button(self.profile_action_frame, text="✂️ CHANGE DIR", command=self.handle_change_dir_selected)
        self.btn_add_already_downloaded = ttk.Button(self.profile_action_frame, text="ADD EXISTINGS", command=self.handle_add_already_downloaded)

        for btn in [btn_update, btn_open, btn_download, btn_change_dir]:
            btn.pack(side=tk.LEFT, padx=5, pady=3)

        self.btn_add_already_downloaded.pack(side=tk.LEFT, padx=5, pady=3)
        self.btn_add_already_downloaded.config(state="normal")

        def on_tree_select(event):
            selected = self.tree.selection()
            state = "normal" if selected else "disabled"
            for btn in [btn_update, btn_open, btn_download, btn_change_dir]:
                btn.config(state=state)

        # Treeview
        self.tree = ttk.Treeview(
            self.root,
            columns=("service", "profile", "status", "videos_dl_total", "photos_dl_total", "video_size", "photo_size", "completed", "last_update", "download_path"),
            show="headings",
            selectmode="browse"
        )

        # Headings
        self.tree.heading("service", text="Service", command=lambda: self.treeview_sort_column("service", False))
        self.tree.heading("profile", text="Profil", command=lambda: self.treeview_sort_column("profile", False))
        self.tree.heading("status", text="Statut", command=lambda: self.treeview_sort_column("status", False))
        self.tree.heading("videos_dl_total", text="Vidéos (dl/total)", command=lambda: self.treeview_sort_column("videos_dl_total", False))
        self.tree.heading("photos_dl_total", text="Photos (dl/total)", command=lambda: self.treeview_sort_column("photos_dl_total", False))
        self.tree.heading("video_size", text="Taille Vidéo (Mo)", command=lambda: self.treeview_sort_column("video_size", False))
        self.tree.heading("photo_size", text="Taille Photo (Mo)", command=lambda: self.treeview_sort_column("photo_size", False))
        self.tree.heading("completed", text="% Complété", command=lambda: self.treeview_sort_column("completed", False))
        self.tree.heading("last_update", text="Dernière maj", command=lambda: self.treeview_sort_column("last_update", False))
        self.tree.heading("download_path", text="Chemin", command=lambda: self.treeview_sort_column("download_path", False))

        # Colonnes
        self.tree.column("service", width=80)
        self.tree.column("profile", width=150)
        self.tree.column("status", width=80)
        self.tree.column("videos_dl_total", width=100)
        self.tree.column("photos_dl_total", width=100)
        self.tree.column("video_size", width=100)
        self.tree.column("photo_size", width=100)
        self.tree.column("completed", width=80)
        self.tree.column("last_update", width=120)
        self.tree.column("download_path", width=250, anchor="w")

        self.tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        self.tree.bind("<Double-Button-1>", self.on_profile_double_click)
        self.tree.bind("<Button-3>", self.on_right_click)
        self.tree.bind("<<TreeviewSelect>>", on_tree_select)
        on_tree_select(None)

        # Désactive tous les anciens tags flashy
        self.tree.tag_configure("clean", background="#2e2e2e", foreground="#ffffff")

    def change_download_dir(self):
        selected_dir = filedialog.askdirectory(title="Choisir le dossier global de téléchargement")
        if selected_dir:
            self.download_dir = selected_dir
            SETTINGS["download_dir"] = selected_dir
            with open(SETTINGS_PATH, "w") as f:
                json.dump(SETTINGS, f, indent=2)
            tk.messagebox.showinfo("Succès", f"Dossier de téléchargement mis à jour:\n{selected_dir}")

    def on_right_click(self, event):
        item_id = self.tree.identify_row(event.y)
        if not item_id:
            return

        def change_profile_dir():
            self.change_profile_dir_threaded(item_id)

        def update_profile():
            self.refresh_profile(item_id)

        def open_dir():
            profile_values = self.tree.item(item_id)["values"]
            service, username = profile_values[0], profile_values[1]
            path = os.path.join(self.download_dir, service, username)
            if os.path.exists(path):
                subprocess.Popen(["open", path])

        def delete_profile():
            profile_values = self.tree.item(item_id)["values"]
            service, username = profile_values[0], profile_values[1]
            json_path = os.path.join("data", service, f"{username}.json")
            dir_path = os.path.join(self.download_dir, service, str(username))

            if messagebox.askyesno("Confirmation", f"Supprimer le profil {username} et ses données ?"):
                try:
                    if os.path.exists(json_path):
                        os.remove(json_path)
                    if os.path.exists(dir_path):
                        shutil.rmtree(dir_path)
                    self.tree.delete(item_id)
                    log_info(f"[Delete] Profil supprimé : {username}")
                except Exception as e:
                    log_error(f"[Delete] Échec suppression : {e}")

        menu = tk.Menu(self.root, tearoff=0, bg="#2f2f2f", fg="#ffffff")
        menu.add_command(label=" Update", command=update_profile)
        menu.add_command(label=" Open Folder", command=open_dir)
        menu.add_command(label=" Copier URL profil", command=lambda: self.copy_profile_url(item_id))
        menu.add_command(label=" Changer dossier", command=change_profile_dir)
        menu.add_command(label=" Supprimer", command=delete_profile)
        menu.post(event.x_root, event.y_root)

    def change_profile_dir_threaded(self, item_id):
        threading.Thread(target=self.change_profile_dir, args=(item_id,), daemon=True).start()

    def change_profile_dir(self, item_id):
        profile_values = self.tree.item(item_id)["values"]
        service, username = profile_values[0], str(profile_values[1]).replace("📁 ", "")
        profile_key = f"{service}:{username}"

        current_dir = os.path.join(self.download_dir, service, username)
        new_dir = filedialog.askdirectory(title="Choisir un nouveau dossier pour ce profil")
        if new_dir:
            new_profile_dir = os.path.join(new_dir, service, username)
            try:
                if os.path.exists(current_dir):
                    os.makedirs(os.path.dirname(new_profile_dir), exist_ok=True)
                    shutil.move(current_dir, new_profile_dir)
                    log_info(f"[Move] Contenu déplacé vers : {new_profile_dir}")

                self.profile_download_dirs[profile_key] = os.path.abspath(new_dir)
                SETTINGS["profile_dirs"] = self.profile_download_dirs
                with open(SETTINGS_PATH, "w") as f:
                    json.dump(SETTINGS, f, indent=2)
                self.root.after(0, lambda: messagebox.showinfo("Succès", f"Nouveau dossier défini pour {username}"))

                # Reload profil à chaud
                self.root.after(0, self.load_profiles)

            except Exception as e:
                log_error(f"[Move] Erreur de déplacement : {e}")
                self.root.after(0, lambda: messagebox.showerror("Erreur", f"Échec déplacement : {e}"))

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

    def refresh_profile(self, item_id):
        threading.Thread(target=self._refresh_profile_worker, args=(item_id,), daemon=True).start()

    def _refresh_profile_worker(self, item_id):
        profile_values = self.tree.item(item_id)["values"]
        service, display_name = profile_values[0], profile_values[1]
        username = str(display_name).replace("📁 ", "")
        json_path = f"data/{service}/{username}.json"
        download_path = os.path.join(self.download_dir, service, str(username))

        # 1. Charger JSON existant
        existing_data = {"medias": [], "last_update": "1970-01-01T00:00:00+00:00"}
        if os.path.exists(json_path):
            try:
                with open(json_path, "r") as f:
                    existing_data = json.load(f)
            except Exception as e:
                log_warning(f"[Update] ⚠️ JSON corrompu, fallback clean : {e}")

        medias = existing_data.get("medias", [])
        last_update = existing_data.get("last_update", "1970-01-01T00:00:00+00:00")
        last_update_dt = datetime.fromisoformat(last_update)
        if last_update_dt.tzinfo is None:
            last_update_dt = last_update_dt.replace(tzinfo=timezone.utc)

        seen_names = {m.get("name") for m in medias}

        # 2. Fetch uniquement les nouveaux posts
        new_medias = []
        offset = 0
        while True:
            try:
                url = f"https://coomer.su/api/v1/{service}/user/{username}?o={offset}"
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()
                posts = data.get("posts") if isinstance(data, dict) else data
                if not isinstance(posts, list) or not posts:
                    break
            except Exception as e:
                log_error(f"[Update] ❌ Erreur API : {e}")
                break

            stop_fetch = False
            for post in posts:
                try:
                    pub = post.get("published")
                    if not pub:
                        continue
                    published_dt = datetime.fromisoformat(pub)
                    if published_dt.tzinfo is None:
                        published_dt = published_dt.replace(tzinfo=timezone.utc)

                    if published_dt < last_update_dt:
                        stop_fetch = True
                        break

                    files = [post.get("file")] if post.get("file") else []
                    files += post.get("attachments") or []

                    for f in files:
                        if not f:
                            continue
                        name = f.get("name")
                        path = f.get("path")
                        if not name or not path or name in seen_names:
                            continue
                        seen_names.add(name)

                        media = {
                            "name": name,
                            "cdn_path": path,
                            "status": "Missing",
                            "type": "video" if name.endswith((".mp4", ".webm", ".mkv")) else "image",
                            "local_size": 0,
                            "size_http": 0,
                            "percent": 0,
                            "hash_check": "",
                            "error": "",
                        }
                        new_medias.append(media)
                except Exception as e:
                    log_warning(f"[Update] ⚠️ Skip post: {e}")

            if stop_fetch:
                break
            offset += 50

        if new_medias:
            log_info(f"[Update] 🆕 {len(new_medias)} nouveaux médias pour {username}")
        else:
            log_info(f"[Update] ✅ Aucun nouveau média pour {username}")

        # 3. Enrichir tout
        final_medias = medias + new_medias
        final_medias = enrich_media_status(final_medias, download_path)

        # 4. Sauver le JSON
        try:
            os.makedirs(os.path.dirname(json_path), exist_ok=True)
            with open(json_path, "w") as f:
                json.dump({
                    "medias": final_medias,
                    "last_update": datetime.now(timezone.utc).isoformat(),
                    "profile_name": username
                }, f, indent=2)
            log_info(f"[Update] ✅ Profil {username} mis à jour ({len(final_medias)} médias)")
        except Exception as e:
            log_error(f"[Update] 💾 Erreur écriture JSON : {e}")

        self.root.after(0, self.load_profiles)

        # 5. Messagebox résultat
        num_videos = sum(1 for m in new_medias if m["type"] == "video")
        num_images = sum(1 for m in new_medias if m["type"] == "image")
        if new_medias:
            message = f"📥 Update terminé\n{num_videos} vidéo(s), {num_images} image(s) ajoutées"
        else:
            message = "📥 Update terminé\nAucune nouvelle publication"
        self.root.after(100, lambda: messagebox.showinfo("Mise à jour du profil", message))

    def handle_update_selected(self):
        selected = self.tree.selection()
        if selected:
            item_id = selected[0]
            threading.Thread(target=self._refresh_profile_worker, args=(item_id,), daemon=True).start()

    def handle_open_dir_selected(self):
        selected = self.tree.selection()
        if selected:
            item_id = selected[0]
            self.open_profile_dir(item_id)

    def handle_download_selected(self):
        selected = self.tree.selection()
        if selected:
            item_id = selected[0]
            self.download_all_selected(item_id)  # ou adapte à ta fonction

    def handle_change_dir_selected(self):
        selected = self.tree.selection()
        if selected:
            item_id = selected[0]
            self.change_profile_dir_threaded(item_id)

    def handle_add_already_downloaded(self):
        print("🟨 [ADD] Add an already existing profile")
        self.root.after(0, self.prompt_profile_import)

    def prompt_profile_import(self):
        selected_dir = filedialog.askdirectory(title="Choisir le dossier du profil déjà téléchargé")
        if not selected_dir:
            log_warning("[Import] Dossier non sélectionné, opération annulée.")
            return

        url = askstring("Entrer l'URL Coomer", "Entre l'URL Coomer/Fansly du profil (ex: https://coomer.su/onlyfans/martine)")
        if not url or not url.startswith("http"):
            log_warning("[Import] URL invalide ou manquante, opération annulée.")
            return

        # Lance ensuite le vrai traitement dans un thread
        threading.Thread(target=self.add_already_downloaded, args=(selected_dir, url), daemon=True).start()

    def open_profile_dir(self, item_id):
        try:
            # Récupère le chemin depuis la colonne "download_path"
            values = self.tree.item(item_id)["values"]
            if len(values) < 10:
                log_warning(f"[Open Dir] ⚠️ Valeurs incomplètes pour item {item_id}")
                return

            path = values[9]  # colonne 'download_path'

            if not os.path.isdir(path):
                log_warning(f"[Open Dir] ⚠️ Dossier introuvable : {path}")
                return

            # Ouvre selon l’OS
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
            values = self.tree.item(item_id, "values")
            if not values or len(values) < 2:
                log_warning("[URL] Données de profil incomplètes")
                return

            service = values[0]
            username = values[1]

            if not service or not username:
                log_warning("[URL] Service ou identifiant manquant")
                return

            url = f"https://coomer.st/{service}/user/{username}"
            self.root.clipboard_clear()
            self.root.clipboard_append(url)
            self.root.update()
            log_info(f"[URL] Copié dans presse-papiers : {url}")

        except Exception as e:
            log_error(f"[URL] Erreur copie URL : {e}")

    def load_profiles(self):
        if not os.path.exists(self.data_dir):
            return

        self.tree.delete(*self.tree.get_children())
        self.profile_ids.clear()
        total_profiles = 0
        total_medias = 0

        def update_row_with_sizes(item_id, video_size, photo_size):
            if self.tree.exists(item_id):
                values = self.tree.item(item_id)["values"]
                new_values = list(values)
                # Appel de format_bytes avec un seul argument (la taille)
                new_values[5] = format_bytes(video_size) if video_size else "0 MB"
                new_values[6] = format_bytes(photo_size) if photo_size else "0 MB"
                self.tree.item(item_id, values=tuple(new_values))

        for service in os.listdir(self.data_dir):
            service_path = os.path.join(self.data_dir, service)
            if not os.path.isdir(service_path):
                continue
            for username_file in os.listdir(service_path):
                if username_file.endswith(".json"):
                    username = username_file.replace(".json", "")
                    json_path = os.path.join(service_path, username_file)
                    try:
                        with open(json_path, "r") as f:
                            data = json.load(f)
                            profile_key = f"{service}:{username}"

                            # Détecter chemin custom
                            custom_dir = self.profile_download_dirs.get(profile_key, self.download_dir)
                            default_path = os.path.abspath(os.path.join(self.download_dir, service, username))
                            custom_path = os.path.abspath(os.path.join(custom_dir, service, username))

                            # Nom affiché (📁 prefix si custom)
                            display_name = username
                            if custom_path != default_path:
                                display_name = f"📁 {username}"

                            medias = data.get("medias", [])

                            for m in medias:
                                if "type" not in m:
                                    ext = os.path.splitext(m.get("name", ""))[1].lower()
                                    if ext in [".mp4", ".m4v", ".mov", ".webm", ".avi", ".mkv", ".flv"]:
                                        m["type"] = "video"
                                    elif ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]:
                                        m["type"] = "image"
                                    else:
                                        m["type"] = "autre"

                            videos = [m for m in medias if m.get("type") == "video"]
                            photos = [m for m in medias if m.get("type") == "image"]

                            def is_completed(m):
                                val = str(m.get("percent", "0")).replace("%", "")
                                try:
                                    return float(val) >= 100
                                except:
                                    return False

                            completed = sum(1 for m in medias if is_completed(m))
                            total = len(medias)
                            percent = round((completed / total) * 100, 1) if total else 0

                            video_completed = sum(1 for m in videos if is_completed(m))
                            photo_completed = sum(1 for m in photos if is_completed(m))

                            last_update = data.get("last_update", "-")
                            try:
                                last_update_display = datetime.fromisoformat(last_update).strftime("%Y-%m-%d %H:%M:%S")
                            except Exception:
                                last_update_display = last_update

                            download_path_display = custom_path
                            item_id = self.tree.insert(
                                "", tk.END,
                                values=(
                                    service, display_name, "",
                                    f"{video_completed}/{len(videos)}",
                                    f"{photo_completed}/{len(photos)}",
                                    "0 MB", "0 MB",  # Placeholder pour tailles
                                    f"{percent:.1f}%", last_update_display,
                                    download_path_display
                                )
                            )
                            self.profile_ids[profile_key] = item_id

                            # Calcul des tailles en thread avec tous les arguments nécessaires
                            video_dir = os.path.join(custom_path, "v")
                            photo_dir = os.path.join(custom_path, "p")
                            self.get_size_thread(video_dir, lambda v_size, item_id=item_id: update_row_with_sizes(item_id, v_size, 0))
                            self.get_size_thread(photo_dir, lambda p_size, item_id=item_id: update_row_with_sizes(item_id, 0, p_size))

                            if percent == 100:
                                self.tree.item(item_id, tags=("green",))
                            elif percent > 0:
                                self.tree.item(item_id, tags=("yellow",))
                            else:
                                self.tree.item(item_id, tags=("gray",))

                            total_profiles += 1
                            total_medias += total

                            with open(json_path, "w") as f:
                                json.dump(data, f, indent=2)

                    except Exception as e:
                        log_error(f"[Erreur JSON] {json_path}: {e}")

        self.stats_label.config(text=f"Stats globales: {total_profiles} profils, {total_medias} médias")

    def treeview_sort_column(self, col, reverse):
        try:
            items = [(self.tree.set(k, col), k) for k in self.tree.get_children('')]

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

                    # Ex: "1.07 Go", "546.11 Mo", "3.49 GB", "0 MB"
                    match = re.match(r"^([\d.,]+)\s?(Go|Mo|GB|MB)$", val, re.IGNORECASE)
                    if match:
                        num, unit = match.groups()
                        num = float(num.replace(",", "."))
                        unit = unit.lower()

                        if unit in ("go", "gb"):
                            return (1, num * 1024)
                        elif unit in ("mo", "mb"):
                            return (1, num)
                        else:
                            return (1, num)

                    # Autres cas numériques
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
                self.tree.move(k, '', index)

            self.tree.heading(col, command=lambda: self.treeview_sort_column(col, not reverse))

        except Exception as e:
            log_error(f"[Sort] Erreur tri colonne '{col}': {e}")
        
    def add_profile_threaded(self):
        threading.Thread(target=self._add_profile_worker, daemon=True).start()

    def on_profile_double_click(self, event):
        selection = self.tree.selection()
        if not selection:
            return

        profile_values = self.tree.item(selection[0])["values"]
        service = profile_values[0]
        username = str(profile_values[1]).replace("📁 ", "")  # Patch safe

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


    def add_already_downloaded(self, selected_dir, url):
        if not selected_dir:
            log_warning("[Import] Dossier non sélectionné, opération annulée.")
            return

        if not url or not url.startswith("http"):
            log_warning("[Import] URL invalide ou manquante, opération annulée.")
            return

        parsed = urlparse(url)
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 3:
            log_error(f"[Import] ❌ URL invalide : {url}")
            return

        service = parts[0]
        username = parts[2] if parts[1] == "user" else parts[1]
        log_info(f"[Import] ✅ Dossier sélectionné : {selected_dir}")
        log_info(f"[Import] ✅ URL saisie : {url}")
        log_info(f"[Import] 🧬 Profil détecté : {service} / {username}")

        # 🔁 Fetch API AVANT clean
        all_medias = []
        try:
            for page in fetch_medias_from_api(service, username):
                all_medias.extend(page)
            log_info(f"[Import] ✅ {len(all_medias)} médias récupérés depuis API")
        except Exception as e:
            log_error(f"[Import] ❌ Échec API : {e}")
            return

        # Nettoyage avec sous-dossier service/username
        clean_profile_folder(selected_dir, service, username)

        # 🔎 SHA256 sur tous les fichiers locaux dans ./service/username/v|p|o
        cleaned_path = os.path.join(selected_dir, service, username)
        local_paths = []
        for subfolder in ["v", "p", "o"]:
            full_dir = os.path.join(cleaned_path, subfolder)
            if not os.path.isdir(full_dir):
                continue
            for fname in os.listdir(full_dir):
                fpath = os.path.join(full_dir, fname)
                if os.path.isfile(fpath):
                    local_paths.append(fpath)

        log_info(f"[Import] 🔎 Analyse de {len(local_paths)} fichiers locaux pour SHA256")

        for fpath in local_paths:
            sha = sha256_file(fpath)
            matched = False

            for media in all_medias:
                if sha and sha in media.get("url", ""):
                    media["downloaded"] = True
                    media["status"] = "Completed"
                    media["percent"] = "100"
                    media["error"] = ""
                    matched = True

                    expected_name = media["name"]
                    actual_name = os.path.basename(fpath)
                    if actual_name != expected_name:
                        new_path = os.path.join(os.path.dirname(fpath), expected_name)
                        try:
                            os.rename(fpath, new_path)
                            log_info(f"[Import] 🔁 Renommé : {actual_name} → {expected_name}")
                        except Exception as e:
                            log_warning(f"[Import] ⚠️ Erreur rename {actual_name} : {e}")

                    tmp_path = fpath + ".tmp"
                    if os.path.exists(tmp_path):
                        try:
                            os.remove(tmp_path)
                        except Exception as e:
                            log_warning(f"[Import] ⚠️ Suppression .tmp échouée : {e}")

                    log_info(f"[Import] ✅ SHA match : {media.get('name', '?')} → OK")
                    break

            if not matched:
                log_warning(f"[Import] ⚠️ Aucun match SHA pour : {os.path.basename(fpath)}")

        # Écriture JSON
        json_path = os.path.join("data", service, f"{username}.json")
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        profile_data = {
            "service": service,
            "username": username,
            "url": url,
            "last_update": datetime.now().isoformat(),
            "medias": all_medias
        }
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(profile_data, f, indent=2)
        log_info(f"[Import] 💾 JSON mis à jour : {json_path}")

        # Sauvegarde settings
        if hasattr(self, "settings"):
            self.settings["profile_dirs"][f"{service}:{username}"] = selected_dir
            save_settings(self.settings)
            
    def _add_profile_worker(self):
        if not self.data_dir:
            self.data_dir = "data"

        url = self.add_entry.get().strip()
        if not url:
            return

        service, raw_username = extract_profile_info(url)
        raw_username = str(raw_username)  # Force en string tout de suite

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

        username = str(username)  # Toujours en string pour Treeview etc.
        profile_key = f"{service}:{username}"

        save_dir = os.path.join(self.data_dir, service)
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, f"{username}.json")

        def insert_loading_row():
            profile_id = self.tree.insert(
                "", tk.END,
                values=(service, username, "", "0/0", "0/0", "0 MB", "0 MB", "0%", "chargement...", ""),
                tags=("loading",)
            )
            self.profile_ids[profile_key] = profile_id
            log_debug(f"[INSERT] {username} (chargement) inséré avec ID {profile_id}")

        self.root.after(0, insert_loading_row)

        medias = []

        def update_row():
            real_id = self.profile_ids.get(profile_key)
            if not real_id or not self.tree.exists(real_id):
                return
            videos = [m for m in medias if m.get("type") == "video"]
            photos = [m for m in medias if m.get("type") == "image"]
            video_completed = sum(1 for m in videos if m.get("percent") == "100")
            photo_completed = sum(1 for m in photos if m.get("percent") == "100")
            total_videos = len(videos)
            total_photos = len(photos)
            percent = round((video_completed + photo_completed) / (total_videos + total_photos) * 100) if (total_videos + total_photos) else 0
            last_update_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            self.tree.item(real_id, values=(
                service, username, "",
                f"{video_completed}/{total_videos}",
                f"{photo_completed}/{total_photos}",
                "0 MB", "0 MB",  # Placeholder pour tailles
                f"{percent}%", last_update_str,
                os.path.join(self.download_dir, service, username)
            ))

        def finalize():
            real_id = self.profile_ids.get(profile_key)
            if not real_id or not self.tree.exists(real_id):
                return
            update_row()
            videos = [m for m in medias if m.get("type") == "video"]
            photos = [m for m in medias if m.get("type") == "image"]
            total = len(medias)
            completed = sum(
                1 for m in medias
                if m.get("status") == "Completed"
                or float(str(m.get("percent", 0)).replace("%", "")) >= 100
            )
            percent = round((completed / total) * 100) if total else 0
            last_update_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            self.tree.item(real_id, values=(
                service, username, "",
                f"{sum(1 for m in videos if is_completed(m))}/{len(videos)}",
                f"{sum(1 for m in photos if is_completed(m))}/{len(photos)}",
                "0 MB", "0 MB",  # Placeholder pour tailles
                f"{percent}%", last_update_str,
                os.path.join(self.download_dir, service, username)
            ))
            self.tree.item(real_id, tags=("green" if percent == 100 else "yellow" if percent > 0 else "gray",))
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
    root.geometry("1200x700")  # Ajusté pour plus d'espace
    app = App(root)
    root.mainloop()