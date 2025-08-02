import os
import json
import threading
import sys
import tkinter as tk
from tkinter import ttk, messagebox
import webbrowser
import requests
from urllib.parse import urlparse
from downloader import download_file, build_media_url, generate_alternative_urls
from utils import fetch_medias_from_api, get_remote_file_size, verify_hash_from_cdn_path, format_bytes, sha256_file, extract_profile_info, is_video
from media_utils import is_valid_image, is_valid_video
from log import log_info, log_error, log_warning
import subprocess
import time
from datetime import datetime
import hashlib
from event_bus import event_bus
from tkinter import filedialog
from utils import detect_type_from_name, render_progress_bar
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
import uuid

GLOBAL_RUNNING_DOWNLOADS = 0
GLOBAL_DOWNLOAD_LOCK = threading.Lock()
GLOBAL_MAX_CONCURRENT_DOWNLOADS = 20  # Limite globale pour toutes les fenêtres

CDN_NODES = ["n1", "n2", "n3", "n4"]
MAX_CONCURRENT_DOWNLOADS = 20
download_queue = []
running_downloads = 0
is_downloading_all = False
queue_lock = threading.Lock()

class MediaWindow:
    def __init__(self, root, service, username, local_dir, json_path, medias_data):
        self.window_id = str(uuid.uuid4())  # Identifiant unique pour la fenêtre
        self.download_queue = []  # File d'attente spécifique à l'instance
        self.running_downloads = 0  # Compteur spécifique à l'instance
        self.queue_processor_running = True  # Contrôle du queue_processor
        self.root = root
        self.service = service
        self.username = str(username)
        self.json_path = json_path
        self.medias_data = medias_data
        self.medias = medias_data.get("medias", [])
        self.is_active = True
        self.is_closing = False
        self.restoring = True
        self.restore_progress_running = True
        self.profile_key = f"{service}:{self.username}"
        self.last_sorted_column = None
        self.sort_reverse = False
        self.item_id_cache = {}
        self.last_tagged = {}
        self.root.title(f"Coomer Ultimate v1.0 – {username} ({service})")
        self.monitor_queue()
        self.save_lock = Lock()  # Verrou pour le JSON
        self.load_global_settings()
        self.global_settings = getattr(self, "global_settings", {})
        self.download_dir = self.global_settings.get("download_dir", "downloads")
        self.profile_download_dirs = self.global_settings.get("profile_dirs", {})
        base_dir = self.profile_download_dirs.get(self.profile_key, self.download_dir)
        self.local_dir = os.path.abspath(os.path.join(base_dir, self.service, self.username))
        self.video_dir = os.path.join(self.local_dir, "v")
        self.image_dir = os.path.join(self.local_dir, "p")
        os.makedirs(self.video_dir, exist_ok=True)
        os.makedirs(self.image_dir, exist_ok=True)
        os.makedirs(self.local_dir, exist_ok=True)
        log_info(f"[INIT] Fenêtre {self.window_id} pour {self.profile_key} → {self.local_dir}")

        print(f"[DEBUG] Loaded {len(self.medias)} medias for {self.profile_key}")

        # Modern theme
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview", 
                        background="#2b2b2b", 
                        foreground="#ffffff", 
                        fieldbackground="#2b2b2b", 
                        rowheight=28,
                        font=("Segoe UI", 10))
        style.configure("Treeview.Heading", 
                        background="#3c3f41", 
                        foreground="#ffffff", 
                        font=("Segoe UI", 10, "bold"))
        style.map("Treeview.Heading",
                  background=[("active", "#4a4d4f")])
        style.configure("TLabel", background="#252526", foreground="#ffffff", font=("Segoe UI", 10))
        style.configure("TFrame", background="#252526")
        style.configure("TRadiobutton", background="#252526", foreground="#ffffff", font=("Segoe UI", 10))
        style.configure("TCheckbutton", background="#252526", foreground="#ffffff", font=("Segoe UI", 10))
        style.configure("TButton", background="#3c3f41", foreground="#ffffff", font=("Segoe UI", 10))

        # Main frame
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Notebook principal pour Vidéos/Photos
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # Onglet Vidéos
        self.video_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.video_frame, text="Vidéos")
        self.video_notebook = ttk.Notebook(self.video_frame)
        self.video_notebook.pack(fill=tk.BOTH, expand=True)

        # Vidéos: Not Downloaded
        self.video_not_downloaded_frame = ttk.Frame(self.video_notebook)
        self.video_notebook.add(self.video_not_downloaded_frame, text="Not Downloaded")
        video_nd_button_frame = ttk.Frame(self.video_not_downloaded_frame)
        video_nd_button_frame.pack(fill=tk.X, pady=5)
        ttk.Button(video_nd_button_frame, text="DOWNLOAD ALL", command=lambda: self.download_all_not_downloaded("video")).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="DOWNLOAD", command=self.download_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="PAUSE", command=lambda: self.pause_downloads("video")).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="IGNORE", command=self.ignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="OPEN FOLDER", command=self.open_video_folder).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="CHECKSUM", command=self.check_sha256_all_video_not_downloaded).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="GET SIZES", command=lambda: self.get_all_sizes_thread(media_type="video")).pack(side=tk.LEFT, padx=5)
        self.video_not_downloaded_tree = ttk.Treeview(
            self.video_not_downloaded_frame,
            columns=("name", "local_size", "http_size", "percent", "speed", "status", "checksum", "type", "url", "error", "retry_count", "hash_check"),
            show="headings",
            style="Treeview"
        )
        video_nd_vsb = ttk.Scrollbar(self.video_not_downloaded_frame, orient="vertical", command=self.video_not_downloaded_tree.yview)
        video_nd_hsb = ttk.Scrollbar(self.video_not_downloaded_frame, orient="horizontal", command=self.video_not_downloaded_tree.xview)
        self.video_not_downloaded_tree.configure(yscrollcommand=video_nd_vsb.set, xscrollcommand=video_nd_hsb.set)
        video_nd_vsb.pack(side=tk.RIGHT, fill=tk.Y)
        video_nd_hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.video_not_downloaded_tree.heading("name", text="Nom")
        self.video_not_downloaded_tree.heading("local_size", text="Taille locale")
        self.video_not_downloaded_tree.heading("http_size", text="Taille HTTP")
        self.video_not_downloaded_tree.heading("percent", text="Pourcentage")
        self.video_not_downloaded_tree.heading("speed", text="Vitesse")  # Nouvelle colonne
        self.video_not_downloaded_tree.heading("status", text="Statut")
        self.video_not_downloaded_tree.heading("checksum", text="Checksum")
        self.video_not_downloaded_tree.heading("type", text="Type")
        self.video_not_downloaded_tree.heading("url", text="URL")
        self.video_not_downloaded_tree.heading("error", text="Erreur")
        self.video_not_downloaded_tree.heading("retry_count", text="Tentatives")
        self.video_not_downloaded_tree.heading("hash_check", text="Vérif. Hash")
        self.video_not_downloaded_tree.column("name", width=250)
        self.video_not_downloaded_tree.column("local_size", width=150)
        self.video_not_downloaded_tree.column("http_size", width=150)
        self.video_not_downloaded_tree.column("percent", width=200)
        self.video_not_downloaded_tree.column("status", width=100)
        self.video_not_downloaded_tree.column("checksum", width=100)
        self.video_not_downloaded_tree.column("type", width=100)
        self.video_not_downloaded_tree.column("url", width=250)
        self.video_not_downloaded_tree.column("error", width=200)
        self.video_not_downloaded_tree.column("retry_count", width=80)
        self.video_not_downloaded_tree.column("hash_check", width=100)
        self.video_not_downloaded_tree.column("speed", width=100)  # Largeur pour la vitesse
        self.video_not_downloaded_tree.pack(fill=tk.BOTH, expand=True)

        # Vidéos: Completed
        self.video_completed_frame = ttk.Frame(self.video_notebook)
        self.video_notebook.add(self.video_completed_frame, text="Completed")
        video_c_button_frame = ttk.Frame(self.video_completed_frame)
        video_c_button_frame.pack(fill=tk.X, pady=5)
        ttk.Button(video_c_button_frame, text="CHECK FILES", command=lambda: self.check_all_completed_files("video")).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_c_button_frame, text="IGNORE", command=self.ignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_c_button_frame, text="OPEN FOLDER", command=self.open_video_folder).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_c_button_frame, text="CHECKSUM", command=self.check_sha256_all_video_not_downloaded).pack(side=tk.LEFT, padx=5)
        self.video_completed_tree = ttk.Treeview(
            self.video_completed_frame,
            columns=("name", "local_size", "http_size", "percent", "status", "checksum", "type", "url", "error", "retry_count", "hash_check"),
            show="headings",
            style="Treeview"
        )
        video_c_vsb = ttk.Scrollbar(self.video_completed_frame, orient="vertical", command=self.video_completed_tree.yview)
        video_c_hsb = ttk.Scrollbar(self.video_completed_frame, orient="horizontal", command=self.video_completed_tree.xview)
        self.video_completed_tree.configure(yscrollcommand=video_c_vsb.set, xscrollcommand=video_c_hsb.set)
        video_c_vsb.pack(side=tk.RIGHT, fill=tk.Y)
        video_c_hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.video_completed_tree.pack(fill=tk.BOTH, expand=True)

        # Onglet Photos
        self.image_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.image_frame, text="Photos")
        self.image_notebook = ttk.Notebook(self.image_frame)
        self.image_notebook.pack(fill=tk.BOTH, expand=True)

        # Suivre les Treeview chargés
        self.loaded_treeviews = {"video_not_downloaded": True}

        # Binding pour les changements d'onglet dans video_notebook
        self.video_notebook.bind("<<NotebookTabChanged>>", self.on_video_notebook_tab_changed)

        # Binding pour les changements d'onglet dans image_notebook
        self.image_notebook.bind("<<NotebookTabChanged>>", self.on_image_notebook_tab_changed)

        # Photos: Not Downloaded
        self.image_not_downloaded_frame = ttk.Frame(self.image_notebook)
        self.image_notebook.add(self.image_not_downloaded_frame, text="Not Downloaded")
        image_nd_button_frame = ttk.Frame(self.image_not_downloaded_frame)
        image_nd_button_frame.pack(fill=tk.X, pady=5)
        ttk.Button(image_nd_button_frame, text="DOWNLOAD ALL", command=lambda: self.download_all_not_downloaded("image")).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_nd_button_frame, text="PAUSE", command=lambda: self.pause_downloads("image")).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_nd_button_frame, text="IGNORE", command=self.ignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_nd_button_frame, text="OPEN FOLDER", command=self.open_image_folder).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_nd_button_frame, text="CHECKSUM", command=self.check_sha256_all_image_not_downloaded).pack(side=tk.LEFT, padx=5)
        self.image_not_downloaded_tree = ttk.Treeview(
            self.image_not_downloaded_frame,
            columns=("name", "local_size", "http_size", "percent", "speed", "status", "checksum", "type", "url", "error", "retry_count", "hash_check"),
            show="headings",
            style="Treeview"
        )
        image_nd_vsb = ttk.Scrollbar(self.image_not_downloaded_frame, orient="vertical", command=self.image_not_downloaded_tree.yview)
        image_nd_hsb = ttk.Scrollbar(self.image_not_downloaded_frame, orient="horizontal", command=self.image_not_downloaded_tree.xview)
        self.image_not_downloaded_tree.configure(yscrollcommand=image_nd_vsb.set, xscrollcommand=image_nd_hsb.set)
        image_nd_vsb.pack(side=tk.RIGHT, fill=tk.Y)
        image_nd_hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.image_not_downloaded_tree.heading("name", text="Nom")
        self.image_not_downloaded_tree.heading("local_size", text="Taille locale")
        self.image_not_downloaded_tree.heading("http_size", text="Taille HTTP")
        self.image_not_downloaded_tree.heading("percent", text="Pourcentage")
        self.image_not_downloaded_tree.heading("speed", text="Vitesse")  # Nouvelle colonne
        self.image_not_downloaded_tree.heading("status", text="Statut")
        self.image_not_downloaded_tree.heading("checksum", text="Checksum")
        self.image_not_downloaded_tree.heading("type", text="Type")
        self.image_not_downloaded_tree.heading("url", text="URL")
        self.image_not_downloaded_tree.heading("error", text="Erreur")
        self.image_not_downloaded_tree.heading("retry_count", text="Tentatives")
        self.image_not_downloaded_tree.heading("hash_check", text="Vérif. Hash")
        self.image_not_downloaded_tree.column("name", width=200)
        self.image_not_downloaded_tree.column("local_size", width=100)
        self.image_not_downloaded_tree.column("http_size", width=100)
        self.image_not_downloaded_tree.column("percent", width=100)
        self.image_not_downloaded_tree.column("status", width=100)
        self.image_not_downloaded_tree.column("checksum", width=100)
        self.image_not_downloaded_tree.column("type", width=100)
        self.image_not_downloaded_tree.column("url", width=250)
        self.image_not_downloaded_tree.column("error", width=200)
        self.image_not_downloaded_tree.column("retry_count", width=80)
        self.image_not_downloaded_tree.column("hash_check", width=100)
        self.image_not_downloaded_tree.column("speed", width=100)  # Largeur pour la vitesse
        self.image_not_downloaded_tree.pack(fill=tk.BOTH, expand=True)

        # Photos: Completed
        self.image_completed_frame = ttk.Frame(self.image_notebook)
        self.image_notebook.add(self.image_completed_frame, text="Completed")
        image_c_button_frame = ttk.Frame(self.image_completed_frame)
        image_c_button_frame.pack(fill=tk.X, pady=5)
        ttk.Button(image_c_button_frame, text="IGNORE", command=self.ignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_c_button_frame, text="OPEN FOLDER", command=self.open_image_folder).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_c_button_frame, text="CHECKSUM", command=lambda: self.check_all_completed_files("image")).pack(side=tk.LEFT, padx=5)
        self.image_completed_tree = ttk.Treeview(
            self.image_completed_frame,
            columns=("name", "local_size", "http_size", "percent", "status", "checksum", "type", "url", "error", "retry_count", "hash_check"),
            show="headings",
            style="Treeview"
        )
        image_c_vsb = ttk.Scrollbar(self.image_completed_frame, orient="vertical", command=self.image_completed_tree.yview)
        image_c_hsb = ttk.Scrollbar(self.image_completed_frame, orient="horizontal", command=self.image_completed_tree.xview)
        self.image_completed_tree.configure(yscrollcommand=image_c_vsb.set, xscrollcommand=image_c_hsb.set)
        image_c_vsb.pack(side=tk.RIGHT, fill=tk.Y)
        image_c_hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.image_completed_tree.pack(fill=tk.BOTH, expand=True)

        # Charger les libellés depuis JSON
        try:
            with open("lang/en.json", "r", encoding="utf-8") as f:
                self.labels = json.load(f)
        except Exception as e:
            log_error(f"[LANG] Erreur chargement libellés multilangue : {e}")
            self.labels = {"columns": {}}

        # Définir les colonnes modernes
        self.columns = {
            "not_downloaded": ["name", "local_size", "http_size", "speed", "percent", "status", "hash_check", "extension", "error", "url", "retry_count"],
            "completed": ["name", "local_size", "http_size", "percent", "status", "hash_check", "extension", "error", "url", "retry_count"]
        }
        col_widths = {
            "name": 250,
            "local_size": 150,
            "http_size": 150,
            "speed": 150,  # Colonne speed avant percent
            "percent": 250,
            "status": 100,
            "hash_check": 100,
            "extension": 100,
            "error": 200,
            "url": 250,
            "retry_count": 80
        }

        # Appliquer les colonnes en fonction du sous-onglet
        for tree in [self.video_not_downloaded_tree, self.video_completed_tree,
                     self.image_not_downloaded_tree, self.image_completed_tree]:
            subtab = "not_downloaded" if tree in [self.video_not_downloaded_tree, self.image_not_downloaded_tree] else "completed"
            tree["columns"] = self.columns[subtab]
            for col in self.columns[subtab]:
                label = self.labels["columns"].get(col, col.title())
                width = col_widths.get(col, 100)
                tree.heading(col, text=label, command=lambda c=col, t=tree: self.sort_column(c, t))
                tree.column(col, width=width, stretch=True)

        # Lier le clic droit aux quatre Treeviews
        for tree, tree_type, subtab in [
            (self.video_not_downloaded_tree, "video", "not_downloaded"),
            (self.video_completed_tree, "video", "completed"),
            (self.image_not_downloaded_tree, "image", "not_downloaded"),
            (self.image_completed_tree, "image", "completed")
        ]:
            tree.bind("<Button-3>", lambda event, tt=tree_type, st=subtab: self.on_right_click(event, tt, st))

        # Filter frame
        filter_frame = ttk.Frame(main_frame)
        filter_frame.pack(fill=tk.X, pady=5)

        self.media_stats_label = ttk.Label(main_frame, text="")
        self.media_stats_label.pack(fill=tk.X, pady=5)

        self.filter_vars = {
            "Missing": tk.BooleanVar(value=False),
            "Completed": tk.BooleanVar(value=False),
            "Error": tk.BooleanVar(value=False),
            "Waiting": tk.BooleanVar(value=False),
            "Downloading": tk.BooleanVar(value=False),
            "Retrying": tk.BooleanVar(value=False),
            "Failed": tk.BooleanVar(value=False),
            "Incomplete": tk.BooleanVar(value=False),
            "Paused": tk.BooleanVar(value=False)
        }

        event_bus.subscribe(f"update:{self.profile_key}", self.on_event_update)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self.start_queue_processor()
        threading.Thread(target=self.restore_progress_background, daemon=True).start()
        self.insert_media_in_treeview()
        self.retry_failed_downloads_loop(interval_minutes=5)
        self.update_media_stats()

    def on_video_notebook_tab_changed(self, event):
        selected_tab = self.video_notebook.select()
        tab_name = self.video_notebook.tab(selected_tab, "text").lower()
        tree_key = f"video_{'completed' if tab_name == 'completed' else 'not_downloaded'}"
        
        log_info(f"[NOTEBOOK] [Window {self.window_id}] Changement d'onglet : {tab_name} (tree_key={tree_key})")
        
        if tree_key not in self.loaded_treeviews:
            log_info(f"[NOTEBOOK] [Window {self.window_id}] Chargement de {tree_key}")
            self.root.config(cursor="wait")
            self.insert_media_in_treeview(tree_type="video", status="completed" if tab_name == "completed" else "not_downloaded")
            self.loaded_treeviews[tree_key] = True
            self.root.config(cursor="")
            log_info(f"[NOTEBOOK] [Window {self.window_id}] {tree_key} chargé")
        else:
            log_info(f"[NOTEBOOK] [Window {self.window_id}] {tree_key} déjà chargé, aucun rechargement")

    def on_image_notebook_tab_changed(self, event):
        selected_tab = self.image_notebook.select()
        tab_name = self.image_notebook.tab(selected_tab, "text")
        tree_key = f"image_{'completed' if tab_name == 'Completed' else 'not_downloaded'}"
        
        if tree_key not in self.loaded_treeviews:
            log_info(f"[NOTEBOOK] [Window {self.window_id}] Chargement de {tree_key} suite au changement d'onglet")
            self.root.config(cursor="wait")
            self.insert_media_in_treeview(tree_type="image", status="completed" if tab_name == "Completed" else "not_downloaded")
            self.loaded_treeviews[tree_key] = True
            self.root.config(cursor="")
            log_info(f"[NOTEBOOK] [Window {self.window_id}] {tree_key} chargé")

    def sort_column(self, col, tree):
        log_info(f"[SORT] Tri colonne demandée : {col}")

        self.current_sort_col = col

        if self.last_sorted_column == col:
            self.sort_reverse = not self.sort_reverse
        else:
            self.last_sorted_column = col
            self.sort_reverse = False

        def get_sort_key(item):
            try:
                value = tree.item(item, "values")[tree["columns"].index(col)]
                if value is None or value.strip() in ["–", ""]:
                    return (0, 0)

                if col in ["local_size", "http_size"]:
                    parsed = self._size_to_bytes(value)
                    log_info(f"[SORT] ✅ {col}='{value}' → {parsed} bytes")
                    return (1, parsed)

                if col in ["percent", "%", "% Complété"]:
                    try:
                        val_num = "".join(c for c in value if c.isdigit() or c in ",.").replace(",", ".")
                        parsed = float(val_num)
                        log_info(f"[SORT] ✅ {col}='{value}' → {parsed}%")
                        return (1, parsed)
                    except Exception as e:
                        log_warning(f"[SORT] Erreur parsing %: {e}")
                        return (0, 0)

                return (1, value.lower() if isinstance(value, str) else value)
            except Exception as e:
                log_warning(f"[SORT] Erreur tri colonne {col}: {e}")
                return (0, 0)

        try:
            items = [(get_sort_key(i), i) for i in tree.get_children()]
            items.sort(reverse=self.sort_reverse, key=lambda x: x[0])
            for index, (_, item) in enumerate(items):
                tree.move(item, "", index)

            for c in tree["columns"]:
                base = tree.heading(c)["text"].split(" ")[0]
                suffix = " ↓" if self.sort_reverse else " ↑" if c == col else ""
                tree.heading(c, text=base + suffix)
        except Exception as e:
            log_error(f"[SORT] Échec tri colonne {col}: {e}")

    def safe_update_tree(self, item_id, tree_type, subtab, **kwargs):
        if self.is_closing or not self.is_active:
            log_info(f"[Tree] 🚫 Update tree ignoré pour {item_id} (fenêtre fermée)")
            return

        try:
            tree = (
                self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree
            )
        except AttributeError:
            log_warning(f"[Tree] ⚠️ Tree non défini pour {item_id} (probablement fermé)")
            return

        if not tree.winfo_exists():
            log_info(f"[Tree] 🚫 Widget détruit, skip update pour {item_id}")
            return

        try:
            current_vals = tree.item(item_id, "values")
            new_vals = kwargs.get("values", current_vals)
            tree.item(item_id, values=new_vals)

            status_idx = tree["columns"].index("status")
            status_val = new_vals[status_idx].strip().lower()
            tag = f"{status_val}.{tree_type}"

            if self.last_tagged.get(item_id) != tag:
                tree.item(item_id, tags=(tag,))
                self.last_tagged[item_id] = tag

            self.resort_treeview_if_needed(tree)
        except tk.TclError as e:
            log_info(f"[Tree] 🚫 Erreur update item {item_id} (probablement fermé) : {e}")
        except Exception as e:
            log_warning(f"[Tree] Erreur inattendue update item {item_id} : {e}")

    def _size_to_bytes(self, size_str):
        try:
            if not isinstance(size_str, str):
                log_warning(f"[PARSE] 🟡 Pas une string : {size_str}")
                return 0

            original = size_str
            size_str = size_str.strip().replace(",", ".")
            parts = size_str.split()

            if len(parts) != 2:
                log_warning(f"[PARSE] 🔴 Mauvais format (≠ 2 parties) : '{original}' → {parts}")
                return 0

            number_str, unit = parts
            try:
                number = float(number_str)
            except ValueError:
                log_warning(f"[PARSE] 🔴 Impossible de parser le nombre : '{number_str}' dans '{original}'")
                return 0

            unit = unit.lower().replace("o", "")  # "Mo" → "m", "Go" → "g"
            factors = {
                "k": 1e3,
                "m": 1e6,
                "g": 1e9,
                "t": 1e12,
            }

            for prefix, factor in factors.items():
                if unit.startswith(prefix):
                    result = number * factor
                    log_info(f"[PARSE] ✅ '{original}' → {result} bytes")
                    return result

            log_warning(f"[PARSE] ⚠️ Unité inconnue : '{unit}' dans '{original}'")
            return number
        except Exception as e:
            log_error(f"[PARSE] ❌ Exception size_str='{size_str}' : {e}")
            return 0

    def restore_progress_background(self):
        log_info(f"[RESTORE] [Window {self.window_id}] Début de la restauration du progrès")
        try:
            if self.is_closing or not self.is_active or not self.check_ui_alive() or not self.restore_progress_running:
                log_info(f"[RESTORE] [Window {self.window_id}] Arrêt de la restauration : fenêtre fermée ou arrêt demandé")
                return

            self.root.config(cursor="wait")
            self.restore_progress_from_files()
            log_info(f"[RESTORE] [Window {self.window_id}] Progrès restauré depuis les fichiers")

            if self.is_closing or not self.is_active or not self.check_ui_alive() or not self.restore_progress_running:
                log_info(f"[RESTORE] [Window {self.window_id}] Arrêt avant update_status_summary")
                self.root.config(cursor="")
                return

            self.update_status_summary()
            log_info(f"[RESTORE] [Window {self.window_id}] Résumé des statuts mis à jour")

            if self.is_closing or not self.is_active or not self.check_ui_alive() or not self.restore_progress_running:
                log_info(f"[RESTORE] [Window {self.window_id}] Arrêt avant insert_media_in_treeview")
                self.root.config(cursor="")
                return

            self.insert_media_in_treeview(tree_type="video", status="not_downloaded")
            log_info(f"[RESTORE] [Window {self.window_id}] Vidéos non téléchargées insérées dans le treeview")

            if self.is_closing or not self.is_active or not self.check_ui_alive() or not self.restore_progress_running:
                log_info(f"[RESTORE] [Window {self.window_id}] Arrêt avant update_media_stats")
                self.root.config(cursor="")
                return

            self.update_media_stats()
            log_info(f"[RESTORE] [Window {self.window_id}] Statistiques mises à jour")

            self.root.config(cursor="")
            log_info(f"[RESTORE] [Window {self.window_id}] Restauration terminée")

            self.restoring = False
        except Exception as e:
            log_error(f"[RESTORE] [Window {self.window_id}] Erreur lors de la restauration : {e}")
            self.root.config(cursor="")

    def load_global_settings(self):
        settings_path = os.path.join(os.getcwd(), "settings.json")
        try:
            if os.path.exists(settings_path):
                with open(settings_path, "r", encoding="utf-8") as f:
                    self.global_settings = json.load(f)
                    log_info(f"[SETTINGS] settings.json chargé depuis {settings_path}")
            else:
                self.global_settings = {}
                log_warning(f"[SETTINGS] settings.json introuvable à {settings_path}")
        except Exception as e:
            self.global_settings = {}
            log_error(f"[SETTINGS] Erreur chargement settings.json : {e}")

    def on_close(self):
        log_info(f"[CLOSE] [Window {self.window_id}] Fermeture de la fenêtre pour {self.profile_key}")
        self.is_closing = True
        self.is_active = False
        self.queue_processor_running = False
        self.restore_progress_running = False  # Arrêter restore_progress_background

        # Mettre les médias en cours en "Paused"
        with self.save_lock:
            for media in self.medias:
                if media.get("status") in ["Downloading", "Retrying"]:
                    media["status"] = "Paused"
                    media["error"] = ""
                    self.refresh_media_row(media)
                    log_info(f"[CLOSE] [Window {self.window_id}] {media.get('name')} mis en Paused")

            self.download_queue.clear()
            self.running_downloads = 0
            log_info(f"[CLOSE] [Window {self.window_id}] File d'attente vidée, téléchargements réinitialisés")

        try:
            if self.root.winfo_exists():
                self.root.destroy()
                log_info(f"[CLOSE] [Window {self.window_id}] Fenêtre détruite")
            else:
                log_info(f"[CLOSE] [Window {self.window_id}] Fenêtre déjà détruite")
        except Exception as e:
            log_warning(f"[CLOSE] [Window {self.window_id}] Erreur lors de la destruction de la fenêtre : {e}")
        active_threads = threading.enumerate()
        log_info(f"[CLOSE] [Window {self.window_id}] Threads actifs : {len(active_threads)}")
        for thread in active_threads:
            log_info(f"[CLOSE] [Window {self.window_id}] Thread : {thread.name}")

    def apply_filter(self):
        if not all(hasattr(self, tree) and getattr(self, tree).winfo_exists() for tree in [
            "video_not_downloaded_tree", "video_completed_tree",
            "image_not_downloaded_tree", "image_completed_tree"
        ]):
            return

        active_status_filters = [key for key, var in self.filter_vars.items() if var.get()]
        try:
            for tree in [self.video_not_downloaded_tree, self.video_completed_tree,
                         self.image_not_downloaded_tree, self.image_completed_tree]:
                tree.delete(*tree.get_children())
            self.item_id_cache.clear()
        except Exception as e:
            log_error(f"[FILTER] Échec clear tree : {e}")
            return

        try:
            self.configure_tree_tags()
        except Exception as e:
            log_warning(f"[FILTER] Erreur configuration tags couleurs : {e}")

        for media in self.medias:
            if "type" not in media or not media["type"]:
                ext = os.path.splitext(media.get("name", "") or "")[1].lower()
                if ext in [".mp4", ".m4v", ".mov", ".webm", ".avi", ".flv", ".mkv"]:
                    media["type"] = "video"
                elif ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]:
                    media["type"] = "image"
                else:
                    media["type"] = "autre"

            status = media.get("status", "Missing")
            type_ = media.get("type", "autre")

            if active_status_filters and status in active_status_filters:
                continue

            if type_ == "video":
                self.insert_single_media(media, "video", "completed" if status == "Completed" else "not_downloaded")
            elif type_ == "image":
                self.insert_single_media(media, "image", "completed" if status == "Completed" else "not_downloaded")

    def configure_tree_tags(self):
        try:
            tags = {
                "waiting.video": dict(background="#4a4a4a"),
                "downloading.video": dict(background="#1e88e5", foreground="white"),
                "retrying.video": dict(background="#6a1b9a"),
                "failed.video": dict(background="#d32f2f"),
                "paused.video": dict(background="#7575a3"),
                "waiting.image": dict(background="#555555"),
                "downloading.image": dict(background="#0288d1", foreground="white"),
                "retrying.image": dict(background="#7b1fa2"),
                "failed.image": dict(background="#c62828"),
                "paused.image": dict(background="#7575a3"),
                "completed.video": dict(background="#2e7d32", foreground="white"),
                "completed.image": dict(background="#388e3c", foreground="white"),
                "ignored.video": dict(background="#444444", foreground="#aaaaaa", font=("Segoe UI", 10, "italic")),
                "ignored.image": dict(background="#444444", foreground="#aaaaaa", font=("Segoe UI", 10, "italic")),
                "video": dict(background="#3e2723"),
                "image": dict(background="#263238"),
                "corrupted": dict(background="#4e342e"),
                "downloading": dict(background="#0288d1", foreground="white", font=("Segoe UI", 10, "bold")),
                "incomplete": dict(background="#fbc02d"),
                "waiting": dict(background="#4a4a4a"),
                "retrying": dict(background="#6a1b9a"),
                "failed": dict(background="#d32f2f"),
                "paused": dict(background="#7575a3"),
                "missing.image": dict(background="#212121", foreground="white"),
                "missing.video": dict(background="#212121", foreground="white"),
            }
            for tree in [self.video_not_downloaded_tree, self.video_completed_tree,
                         self.image_not_downloaded_tree, self.image_completed_tree]:
                for tag, options in tags.items():
                    tree.tag_configure(tag, **options)
        except Exception as e:
            log_warning(f"[TREEVIEW] Erreur configuration tags couleurs : {e}")

    def start_download(self, media):
        threading.Thread(target=self.download_file_thread, args=(media,), daemon=True).start()

    def update_media_stats(self):
        if self.is_closing or not self.is_active or not self.check_ui_alive() or not self.restore_progress_running:
            log_info(f"[STATS] [Window {self.window_id}] Mise à jour des stats annulée : fenêtre fermée")
            return

        try:
            total = sum(1 for m in self.medias if m.get("status") not in ["Ignored", "Autre"])
            images = sum(1 for m in self.medias if m.get("type") == "image")
            videos = sum(1 for m in self.medias if m.get("type") == "video")
            autres = total - images - videos
            completed = sum(1 for m in self.medias if m.get("status") == "Completed")

            self.media_stats_label.config(
                text=f" {completed}/{total}   Images: {images}   Videos: {videos}   Autres: {autres} — {round((completed / total) * 100, 2) if total else 0}%"
            )
            log_info(f"[STATS] [Window {self.window_id}] Stats mises à jour : {completed}/{total}")
        except Exception as e:
            log_warning(f"[STATS] [Window {self.window_id}] Erreur lors de la mise à jour des stats : {e}")

    def get_remote_file_size(self, url):
        try:
            parsed = urlparse(url)
            cdn_path = parsed.path
            timeout = 10
            log_info(f"[HEAD] Tentative de récupération de la taille pour URL: {url} avec timeout={timeout}")

            base_url = f"https://coomer.st{cdn_path}"
            log_info(f"[HEAD] Essai sur base URL: {base_url}")
            response = requests.head(base_url, timeout=timeout)
            if response.status_code == 200 and "Content-Length" in response.headers:
                log_info(f"[HEAD] Succès sur base URL, taille: {response.headers['Content-Length']}")
                return int(response.headers["Content-Length"])

            for node in CDN_NODES:
                fallback_url = f"https://{node}.coomer.st{cdn_path}"
                log_info(f"[HEAD] Essai sur CDN node {node}: {fallback_url}")
                response = requests.head(fallback_url, timeout=timeout)
                if response.status_code == 200 and "Content-Length" in response.headers:
                    log_info(f"[HEAD] Succès sur {node}, taille: {response.headers['Content-Length']}")
                    return int(response.headers["Content-Length"])

            log_warning(f"[HEAD] Aucun nœud n'a répondu avec succès pour {url}")
            return None

        except Exception as e:
            log_warning(f"[HEAD] Erreur lors de la récupération de la taille HTTP pour {url}: {e}")
            return None

    def setup_dirs(self):
        self.default_dir = os.path.join(self.download_root, self.service, self.username)
        custom_dir = self.data.get("download_dir")
        if custom_dir:
            self.local_dir = custom_dir
        else:
            self.local_dir = self.default_dir

        self.video_dir = os.path.join(self.local_dir, "v")
        self.image_dir = os.path.join(self.local_dir, "p")
        os.makedirs(self.video_dir, exist_ok=True)
        os.makedirs(self.image_dir, exist_ok=True)

    def refresh_profile(self):
        self.restore_progress_from_files(skip_sha256_verify=True)
        self.update_status_summary()
        self.insert_media_in_treeview()
        self.update_media_stats()
        self.apply_filter()

    def restore_progress_from_files(self, skip_sha256_verify=True):
        from utils import sha256_file

        log_info(f"[RESTORE] [Window {self.window_id}] Using video_dir: {self.video_dir}")
        log_info(f"[RESTORE] [Window {self.window_id}] Using image_dir: {self.image_dir}")
        log_info(f"[RESTORE] [Window {self.window_id}] Using local_dir: {self.local_dir}")

        for media in self.medias:
            name = media.get("name", "")
            if not name:
                log_warning(f"[RESTORE] [Window {self.window_id}] Média sans nom, ignoré")
                continue

            media["type"] = detect_type_from_name(name)
            log_info(f"[RESTORE] [Window {self.window_id}] {name} → Type détecté : {media['type']}")

            if media["type"] == "video":
                subdir = os.path.join(self.local_dir, "v")
            elif media["type"] == "image":
                subdir = os.path.join(self.local_dir, "p")
            else:
                subdir = self.local_dir

            dest_path = os.path.join(subdir, name)
            tmp_path = dest_path + ".tmp"
            log_info(f"[RESTORE] [Window {self.window_id}] Vérification de {name} à {dest_path}")

            if os.path.exists(tmp_path):
                media["local_size"] = os.path.getsize(tmp_path)
                media["status"] = "Paused"
                media["percent"] = 0
                media.setdefault("size_http", 0)
                media["hash_check"] = ""
                log_info(f"[RESTORE] [Window {self.window_id}] {name} → Paused (.tmp trouvé, {media['local_size']} bytes, size_http={media['size_http']})")

            elif os.path.exists(dest_path):
                size = os.path.getsize(dest_path)
                media["local_size"] = size
                expected_size = media.get("size_http", 0)
                log_info(f"[RESTORE] [Window {self.window_id}] {name} → Taille locale : {size} bytes, taille attendue : {expected_size} bytes")

                if not expected_size:
                    media["size_http"] = size
                    expected_size = size
                    log_info(f"[RESTORE] [Window {self.window_id}] {name} → size_http défini à {size} bytes (aucune taille attendue initiale)")
                else:
                    media["size_http"] = max(size, expected_size)
                    log_info(f"[RESTORE] [Window {self.window_id}] {name} → size_http mis à jour à {media['size_http']} bytes")

                if not skip_sha256_verify:
                    try:
                        actual_hash = sha256_file(dest_path)
                        expected_hash = name.split("_")[-1].split(".")[0]
                        log_info(f"[RESTORE] [Window {self.window_id}] {name} → SHA256 calculé : {actual_hash}, attendu : {expected_hash}")
                        if expected_hash and actual_hash.startswith(expected_hash):
                            media["status"] = "Completed"
                            media["percent"] = 100
                            media["hash_check"] = ""
                            log_info(f"[RESTORE] [Window {self.window_id}] {name} → Completed (SHA256 ok, {size} bytes)")
                        elif size > 0:
                            media["status"] = "Incomplete"
                            media["percent"] = 0
                            media["hash_check"] = actual_hash
                            log_warning(f"[RESTORE] [Window {self.window_id}] {name} → Incomplete (SHA256 mismatch, {size} bytes)")
                        else:
                            media["status"] = "Missing"
                            media["local_size"] = 0
                            media["percent"] = 0
                            media["hash_check"] = ""
                            log_info(f"[RESTORE] [Window {self.window_id}] {name} → Fichier vide")
                    except Exception as e:
                        log_warning(f"[RESTORE] [Window {self.window_id}] {name} → Erreur SHA256 : {e}, marqué comme Incomplete")
                        media["status"] = "Incomplete"
                        media["percent"] = 0
                        media["hash_check"] = ""
                else:
                    if size > 0:
                        media["status"] = "Completed"
                        media["percent"] = 100
                        media["hash_check"] = ""
                        log_info(f"[RESTORE] [Window {self.window_id}] {name} → Completed (SHA256 sauté, taille {size} bytes, attendu {expected_size} bytes)")
                    else:
                        media["status"] = "Missing"
                        media["local_size"] = 0
                        media["percent"] = 0
                        media["hash_check"] = ""
                        log_info(f"[RESTORE] [Window {self.window_id}] {name} → Fichier vide")
            else:
                media["status"] = "Missing"
                media["local_size"] = 0
                media["percent"] = 0
                media["hash_check"] = ""
                log_info(f"[RESTORE] [Window {self.window_id}] {name} → Missing (fichier non trouvé à {dest_path})")

        for media in self.medias:
            name = media.get("name", "")
            if media.get("status") == "Paused":
                log_info(f"[RESTORE] [Window {self.window_id}] {name} → Laissé en Paused")
            elif media.get("status") == "Downloading":
                media["status"] = "Paused"
                log_info(f"[RESTORE] [Window {self.window_id}] {name} → Converti de Downloading à Paused")

    def update_status_summary(self):
        if self.is_closing or not self.is_active or not self.check_ui_alive() or not self.restore_progress_running:
            log_info(f"[SUMMARY] [Window {self.window_id}] Mise à jour du résumé annulée : fenêtre fermée")
            return

        try:
            total = len(self.medias)
            completed = sum(1 for m in self.medias if m.get("status") == "Completed")
            downloading = sum(1 for m in self.medias if m.get("status") == "Downloading")
            waiting = sum(1 for m in self.medias if m.get("status") == "Waiting")
            failed = sum(1 for m in self.medias if m.get("status") == "Failed")
            retrying = sum(1 for m in self.medias if m.get("status") == "Retrying")
            incomplete = sum(1 for m in self.medias if m.get("status") == "Incomplete")
            paused = sum(1 for m in self.medias if m.get("status") == "Paused")

            percent = round((completed / total) * 100, 1) if total > 0 else 0

            self.media_stats_label.config(
                text=f" {completed}/{total} —  {downloading} —  {waiting} —  {retrying} —  {failed} —  {incomplete} —  {paused} —  Vidéos: {self.stats_videos} | Images: {self.stats_images} | Autres: {self.stats_autres} — {percent}%"
            )
            log_info(f"[SUMMARY] [Window {self.window_id}] Résumé mis à jour : {completed}/{total}")
        except Exception as e:
            log_warning(f"[SUMMARY] [Window {self.window_id}] Erreur lors de la mise à jour du résumé : {e}")

    def resort_treeview_if_needed(self, tree):
        if not self.last_sorted_column:
            return

        try:
            col = self.last_sorted_column
            reverse = self.sort_reverse

            def sort_key(item):
                if not tree.exists(item):
                    return ""
                val = tree.set(item, col)
                try:
                    return int(val)
                except ValueError:
                    return val.lower()

            children = [item for item in tree.get_children('') if tree.exists(item)]
            children.sort(key=sort_key, reverse=reverse)

            for index, item in enumerate(children):
                if tree.exists(item):
                    tree.move(item, '', index)
        except Exception as e:
            log_warning(f"[UI] Erreur tri treeview: {e}")

    def insert_media_in_treeview(self, tree_type="video", status="not_downloaded"):
        start_time = time.time()
        if self.is_closing or not self.is_active or not self.check_ui_alive() or not self.restore_progress_running:
            log_warning(f"[TREEVIEW] [Window {self.window_id}] Arbre non disponible ou détruit, insertion annulée")
            return

        try:
            tree_attr = f"{tree_type}_{status}_tree"
            if not hasattr(self, tree_attr) or not getattr(self, tree_attr).winfo_exists():
                log_warning(f"[TREEVIEW] [Window {self.window_id}] {tree_attr} non disponible ou détruit, insertion annulée")
                return

            tree = getattr(self, tree_attr)
            tree.delete(*tree.get_children())
            log_info(f"[TREEVIEW] [Window {self.window_id}] {tree_attr} vidé")
        except Exception as e:
            log_warning(f"[TREEVIEW] [Window {self.window_id}] Erreur lors du vidage de {tree_attr} : {e}")
            return

        if tree_type == "video" and status == "not_downloaded":
            self.stats_videos = 0
            self.stats_images = 0
            self.stats_autres = 0
            for media in self.medias:
                type_ = media.get("type", "").strip().lower()
                if not type_:
                    ext = os.path.splitext(media.get("name", "").lower())[1]
                    if ext in [".mp4", ".m4v", ".mov", ".webm", ".avi", ".flv", ".mkv"]:
                        type_ = "video"
                    elif ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]:
                        type_ = "image"
                    else:
                        type_ = "autre"
                    media["type"] = type_
                if type_ == "video":
                    self.stats_videos += 1
                elif type_ == "image":
                    self.stats_images += 1
                else:
                    self.stats_autres += 1
            log_info(f"[TREEVIEW] [Window {self.window_id}] Stats calculées : vidéos={self.stats_videos}, images={self.stats_images}, autres={self.stats_autres}")

        inserted = 0
        for media in self.medias:
            if self.is_closing or not self.is_active or not self.check_ui_alive() or not self.restore_progress_running:
                log_warning(f"[TREEVIEW] [Window {self.window_id}] Arrêt de l'insertion des médias : fenêtre fermée")
                return

            if not isinstance(media, dict):
                log_warning(f"[TREEVIEW] [Window {self.window_id}] Entrée média non valide (non-dict), ignorée")
                continue

            name = media.get("name", "")
            media_status = media.get("status", "Missing").strip()
            type_ = media.get("type", "").strip().lower()

            if not name:
                log_warning(f"[TREEVIEW] [Window {self.window_id}] Média sans nom, ignoré")
                continue

            if type_ == tree_type and (
                (status == "completed" and media_status == "Completed") or
                (status == "not_downloaded" and media_status != "Completed")
            ):
                log_info(f"[TREEVIEW] [Window {self.window_id}] Insertion prévue pour {name} (status={media_status}, type={type_}) dans {tree_attr}")
                try:
                    self.insert_single_media(media, tree_type, status)
                    inserted += 1
                    log_info(f"[TREEVIEW] [Window {self.window_id}] Inséré {name} (status={media_status}, type={type_}) dans {tree_attr}")
                except Exception as e:
                    log_warning(f"[TREEVIEW] [Window {self.window_id}] Erreur lors de l'insertion de {name} : {e}")

        log_info(f"[TREEVIEW] [Window {self.window_id}] Insertion terminée pour {tree_attr} : {inserted} éléments en {time.time() - start_time:.2f}s")

        if self.is_closing or not self.is_active or not self.check_ui_alive() or not self.restore_progress_running:
            log_warning(f"[TREEVIEW] [Window {self.window_id}] Arrêt avant configuration des tags ou mise à jour du résumé")
            return

        try:
            self.configure_tree_tags()
            log_info(f"[TREEVIEW] [Window {self.window_id}] Tags de couleur configurés pour {tree_attr}")
        except Exception as e:
            log_warning(f"[TREEVIEW] [Window {self.window_id}] Erreur configuration tags couleurs : {e}")

        try:
            self.update_status_summary()
            log_info(f"[TREEVIEW] [Window {self.window_id}] Résumé des statuts mis à jour")
        except Exception as e:
            log_warning(f"[TREEVIEW] [Window {self.window_id}] Erreur update_status_summary : {e}")

    def insert_single_media(self, media, tree_type, subtab):
        if not all(hasattr(self, tree) and getattr(self, tree).winfo_exists() for tree in [
            "video_not_downloaded_tree", "video_completed_tree",
            "image_not_downloaded_tree", "image_completed_tree"
        ]):
            log_warning(f"[INSERT] [Window {self.window_id}] Treeview non disponible, insertion annulée")
            return

        tree = (
            self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
            self.video_completed_tree if tree_type == "video" and subtab == "completed" else
            self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
            self.image_completed_tree
        )

        try:
            media.setdefault("name", "???")
            media.setdefault("speed", "0 B/s")
            media.setdefault("status", "Missing")
            media.setdefault("error", "")
            media.setdefault("url", "")
            media.setdefault("hash_check", "")
            media.setdefault("local_size", 0)
            media.setdefault("size_http", 0)

            name = media["name"]
            ext = os.path.splitext(name)[1][1:].lower() or "unknown"
            downloaded = media["local_size"]
            total = media["size_http"]
            status = media["status"]
            error = media["error"]
            hash_check = media["hash_check"]
            url = media["url"]
            speed = media.get("speed", "0 B/s")

            percent = int((downloaded / total) * 100) if total else 0
            percent_str = render_progress_bar(percent)

            if tree in [self.video_not_downloaded_tree, self.image_not_downloaded_tree]:
                values = (
                    name,
                    format_bytes(downloaded),
                    format_bytes(total),
                    speed,
                    percent_str,
                    status,
                    hash_check,
                    ext,
                    error,
                    url,
                    str(media.get("retry_count", 0))
                )
            else:
                values = (
                    name,
                    format_bytes(downloaded),
                    format_bytes(total),
                    percent_str,
                    status,
                    hash_check,
                    ext,
                    error,
                    url,
                    str(media.get("retry_count", 0))
                )

            log_info(f"[INSERT] [Window {self.window_id}] {name} → status={status} type={tree_type} tree={tree_type}/{subtab} avec speed={speed}")

            combined_tag = f"{status.lower()}.{tree_type}"
            fallback_tags = [status.lower(), tree_type, "missing"]

            item_id = tree.insert("", tk.END, values=values, tags=(combined_tag, *fallback_tags))
            self.item_id_cache[(name, tree_type, subtab)] = item_id

        except Exception as e:
            log_error(f"[INSERT] [Window {self.window_id}] Erreur insertion média {tree_type}/{subtab}: {e}")

    def on_right_click(self, event, tree_type, subtab):
        tree = (self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree)
        iid = tree.identify_row(event.y)
        col = tree.identify_column(event.x)
        col_name = tree["columns"][int(col.replace("#", "")) - 1] if col else ""

        menu = tk.Menu(self.root, tearoff=0)
        if iid:
            tree.selection_set(iid)
            menu.add_command(label="Télécharger", command=lambda: self.enqueue_download(iid, tree_type, subtab))
            menu.add_command(label="Ouvrir", command=lambda: self.open_selected_media(iid, tree_type, subtab))
            menu.add_command(label="Ouvrir dossier destination", command=lambda: self.open_media_dir(iid, tree_type, subtab))
            menu.add_command(label="Re-vérifier SHA256", command=lambda: self.verify_sha256(iid, tree_type, subtab))
            menu.add_command(label="Preview", command=lambda: self.preview_media(iid, tree_type, subtab))
            menu.add_command(label="Force Complete", command=lambda: self.force_complete_media(iid, tree_type, subtab))
            menu.add_command(label="Forcer Retry", command=lambda: self.force_retry(iid, tree_type, subtab))
            menu.add_command(label="Get Size", command=lambda: self.update_file_size(iid, tree_type, subtab))
            menu.add_command(label="Repair", command=lambda: self.repair_file(iid, tree_type, subtab))

        if col_name == "status":
            status_menu = tk.Menu(menu, tearoff=0)
            for status, var in self.filter_vars.items():
                status_menu.add_checkbutton(
                    label=status,
                    variable=var,
                    command=self.apply_filter
                )
            menu.add_cascade(label="Filtrer Statut (exclure)", menu=status_menu)

        menu.add_separator()
        menu.add_command(label="Download All", command=self.download_all)
        menu.add_command(label="Download All Videos" if tree_type == "video" else "Download All Pictures",
                         command=self.download_all_videos if tree_type == "video" else self.download_all_pictures)
        menu.add_command(label="Changer dossier de téléchargement", command=self.change_download_directory)
        menu.post(event.x_root, event.y_root)

    def download_all_not_downloaded(self, tree_type):
        threading.Thread(target=self._download_all_not_downloaded_thread, args=(tree_type,), daemon=True).start()

    def _download_all_not_downloaded_thread(self, tree_type):
        log_info(f"[Download All {tree_type}] [Window {self.window_id}] Début lancement, running_downloads={self.running_downloads}, queue_size={len(self.download_queue)}")
        to_enqueue = [
            m for m in self.medias
            if m.get("type") == tree_type and m.get("status") in ("Missing", "Paused", "Failed", "Incomplete")
            and m not in self.download_queue
            and m.get("status") not in ["Downloading", "Retrying", "Ignored"]
        ]
        log_info(f"[Download All {tree_type}] [Window {self.window_id}] Lancement pour {len(to_enqueue)} médias éligibles")
        for media in to_enqueue:
            if self.is_closing or not self.is_active:
                log_info(f"[Download All {tree_type}] [Window {self.window_id}] Arrêt : fenêtre fermée")
                break
            
            # Ajouter à la file
            self.enqueue_media(media)
            
            # Essayer de lancer immédiatement si possible
            if self.running_downloads < MAX_CONCURRENT_DOWNLOADS:
                self.start_next_in_queue()
            else:
                log_info(f"[Download All {tree_type}] [Window {self.window_id}] Limite atteinte ({self.running_downloads}/{MAX_CONCURRENT_DOWNLOADS}), en attente")
                while self.running_downloads >= MAX_CONCURRENT_DOWNLOADS and self.is_active and not self.is_closing:
                    time.sleep(0.1)  # Attente plus courte pour réactivité

        log_info(f"[Download All {tree_type}] [Window {self.window_id}] Tous les médias éligibles en file (queue_size={len(self.download_queue)})")

    def pause_downloads(self, tree_type):
        with self.save_lock:
            self.download_queue[:] = [m for m in self.download_queue if m.get("type") != tree_type]
            for media in self.medias:
                if media.get("type") == tree_type and media.get("status") in ["Downloading", "Retrying"]:
                    media["status"] = "Paused"
                    media["error"] = ""
                    self.refresh_media_row(media)
            self.running_downloads = sum(1 for m in self.download_queue if m.get("type") == tree_type)
            log_info(f"[Pause {tree_type}] Téléchargements arrêtés, file vidée, {self.running_downloads} restants")

    def preview_media(self, item_id, tree_type, subtab):
        tree = (self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree)
        media_name = tree.item(item_id, "values")[0]
        media = next((m for m in self.medias if m.get("name") == media_name), None)
        if not media:
            log_warning("[Preview] Média non trouvé")
            return

        subdir = self.video_dir if is_video(media) else self.image_dir
        tmp_path = os.path.join(subdir, media_name + ".tmp")
        preview_path = os.path.join(subdir, f".preview_{media_name}")

        if not os.path.exists(tmp_path):
            log_warning(f"[Preview] Fichier tmp absent : {tmp_path}")
            return

        try:
            import shutil, platform
            shutil.copy(tmp_path, preview_path)
            if platform.system() == "Darwin":
                subprocess.run(["open", preview_path])
            elif platform.system() == "Windows":
                os.startfile(preview_path)
            else:
                subprocess.run(["xdg-open", preview_path])
        except Exception as e:
            log_error(f"[Preview] Erreur : {e}")

    def force_retry(self, item_id, tree_type, subtab):
        tree = (self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree)
        media_name = tree.item(item_id, "values")[0]
        media = next((m for m in self.medias if m.get("name") == media_name), None)
        if not media:
            log_error(f"[FORCE RETRY] Média non trouvé : {media_name}")
            return

        if media.get("status") == "Downloading":
            log_info(f"[FORCE RETRY] Ignoré (déjà en cours) : {media_name}")
            return

        media["status"] = "Waiting"
        media["error"] = ""
        media["retry_count"] = media.get("retry_count", 0) + 1
        self.enqueue_download(item_id, tree_type, subtab)
        log_info(f"[FORCE RETRY] Ajout en queue : {media_name}")

    def open_selected_media(self, item_id, tree_type, subtab):
        tree = (self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree)
        media_name = tree.item(item_id, "values")[0]
        media = next((m for m in self.medias if m.get("name") == media_name), None)
        if not media:
            log_warning("[Open] Média non trouvé")
            return

        subdir = self.video_dir if is_video(media) else self.image_dir
        final_path = os.path.join(subdir, media_name)

        if not os.path.exists(final_path):
            log_warning(f"[Open] Fichier non trouvé : {final_path}")
            return

        try:
            import platform, subprocess
            if platform.system() == "Darwin":
                subprocess.run(["open", final_path])
            elif platform.system() == "Windows":
                os.startfile(final_path)
            else:
                subprocess.run(["xdg-open", final_path])
        except Exception as e:
            log_error(f"[Open] Erreur à l'ouverture : {e}")

    def force_complete_media(self, item_id, tree_type, subtab):
        tree = (self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree)

        media_name = tree.item(item_id, "values")[0]
        media = next((m for m in self.medias if m.get("name") == media_name), None)
        if not media:
            log_error("[ForceComplete] Média non trouvé")
            return

        if media.get("status") == "Completed":
            log_info(f"[ForceComplete] Ignoré : {media_name} est déjà Completed")
            return

        subdir = self.video_dir if is_video(media) else self.image_dir
        tmp_path = os.path.join(subdir, media_name + ".tmp")
        final_path = os.path.join(subdir, media_name)

        if not os.path.exists(tmp_path):
            if os.path.exists(final_path):
                log_warning(f"[ForceComplete] Fichier tmp absent, mais {media_name} existe déjà → marquage forcé en Completed")

                media["status"] = "Completed"
                media["error"] = ""
                media["retry_count"] = 0
                media["hash_check"] = " (forced.no_tmp)"
                media["local_size"] = os.path.getsize(final_path)
                media["percent"] = 100
                media["speed"] = ""

                self.refresh_media_row(media, move_to_completed=True)
                self.save_json()
                return
            else:
                log_warning(f"[ForceComplete] ❌ Fichier tmp et fichier final absents : {media_name}")
                return

        try:
            os.rename(tmp_path, final_path)

            media["status"] = "Completed"
            media["error"] = ""
            media["retry_count"] = 0
            media["hash_check"] = " (forced)"
            media["local_size"] = os.path.getsize(final_path)
            media["percent"] = 100
            media["speed"] = ""

            log_info(f"[ForceComplete] Forcé : {media_name} → Completed")
            self.refresh_media_row(media, move_to_completed=True)
            self.save_json()

        except Exception as e:
            log_error(f"[ForceComplete] Rename échoué : {e}")

    def open_media_dir(self, item_id, tree_type, subtab):
        tree = (self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree)
        media_name = tree.item(item_id, "values")[0]
        file_path = os.path.join(self.local_dir, media_name)
        dir_path = os.path.dirname(file_path)
        if os.path.exists(dir_path):
            subprocess.Popen(["open" if sys.platform == "darwin" else "xdg-open", dir_path])

    def verify_sha256(self, item_id, tree_type, subtab):
        tree = (self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree)
        media = next((m for m in self.medias if m.get("name") == tree.item(item_id, "values")[0]), None)
        if not media:
            log_error(f"[SHA256] Média non trouvé pour {item_id}")
            return

        subdir = self.video_dir if is_video(media) else self.image_dir
        final_path = os.path.join(subdir, media["name"])
        tmp_path = final_path + ".tmp"

        path = tmp_path if os.path.exists(tmp_path) else final_path
        if not os.path.exists(path):
            log_warning(f"[SHA256] Fichier absent : {path}")
            media["status"] = "Missing"
            media["hash_check"] = ""
            self.refresh_media_row(media)
            return

        ok = verify_hash_from_cdn_path(path, media.get("url", ""))
        media["hash_check"] = "" if ok else ""
        media["status"] = "Completed" if ok else "Failed"
        media["local_size"] = os.path.getsize(path)

        if ok and path.endswith(".tmp"):
            try:
                os.rename(path, final_path)
                media["name"] = os.path.basename(final_path)
            except Exception as e:
                log_error(f"[SHA256] Rename .tmp échoué : {e}")

        log_info(f"[SHA256] {media['name']} → {media['hash_check']}")
        self.refresh_media_row(media, move_to_completed=ok)
        self.save_json()

    def download_all(self):
        self.download_all_generic()

    def _download_all_thread(self):
        to_enqueue = [m for m in self.medias if m.get("status") in ["Missing", "Failed", "Incomplete", "Paused"]]
        self.enqueue_media_batch(to_enqueue)

    def download_all_videos(self):
        self.download_all_generic(filter_func=is_video)

    def _download_all_videos_thread(self):
        to_enqueue = [m for m in self.medias if is_video(m) and m.get("status") in ["Missing", "Failed", "Incomplete", "Paused"]]
        self.enqueue_media_batch(to_enqueue)

    def download_all_pictures(self):
        self.download_all_generic(filter_func=lambda m: m.get("type") == "image")

    def _download_all_pictures_thread(self):
        to_enqueue = [m for m in self.medias if m.get("type") == "image" and m.get("status") in ["Missing", "Failed", "Incomplete", "Paused"]]
        self.enqueue_media_batch(to_enqueue)

    def download_all_generic(self, filter_func=None):
        global running_downloads

        def eligible(media):
            return media.get("status") in ["Missing", "Waiting"] and (filter_func(media) if filter_func else True)

        self.download_queue = [m for m in self.medias if eligible(m)]
        log_info(f"[POOL] 🎯 {len(self.download_queue)} fichiers éligibles")

        def start_next():
            global running_downloads
            if not self.download_queue:
                return

            with queue_lock:
                if running_downloads >= MAX_CONCURRENT_DOWNLOADS:
                    return
                media_item = self.download_queue.pop(0)
                running_downloads += 1
                log_info(f"[POOL] ▶️ Start {media_item.get('filename')} (en cours : {running_downloads})")

            def worker():
                global running_downloads
                try:
                    self.download_file_thread(media_item)
                finally:
                    with queue_lock:
                        running_downloads -= 1
                        log_info(f"[POOL] 🔁 Fin thread (restant : {running_downloads})")
                    self.root.after(50, start_next)  # relance le suivant

            threading.Thread(target=worker, daemon=True).start()

        # Démarre jusqu'à MAX
        for _ in range(MAX_CONCURRENT_DOWNLOADS):
            start_next()

    def enqueue_media(self, media):
        media_name = media.get("name")
        
        if getattr(self, "restoring", False):
            log_info(f"[Queue] Skip enqueue {media_name} (restoration phase)")
            return

        if not media_name:
            log_warning("[Queue] Média sans nom détecté, ignoré.")
            return

        if media.get("status") == "Completed":
            log_info(f"[Queue] Ignoré : {media_name} est déjà Completed")
            return

        # Vérifier si le média est déjà dans la file
        if media in self.download_queue:
            log_info(f"[Queue] [Window {self.window_id}] {media_name} déjà dans la file, ignoré")
            return

        # Vérifier si le média est en cours de téléchargement
        if media.get("status") in ["Downloading", "Retrying"]:
            log_info(f"[Queue] [Window {self.window_id}] {media_name} en cours ({media.get('status')}), ignoré")
            return

        tree_type = "video" if is_video(media) else "image"
        subtab = "not_downloaded"
        tree = self.video_not_downloaded_tree if tree_type == "video" else self.image_not_downloaded_tree

        item_id = self.item_id_cache.get((media_name, tree_type, subtab))
        if item_id and tree.exists(item_id):
            if tree.item(item_id, "values")[0] == media_name:
                self.enqueue_download(item_id, tree_type, subtab)
                return

        log_error(f"[Queue] Item non trouvé ou supprimé dans le TreeView pour : {media_name}")

    def enqueue_download(self, item_id, tree_type, subtab):
        tree = (self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree)
        media_name = tree.item(item_id, "values")[0]
        media = next((m for m in self.medias if m.get("name") == media_name), None)
        log_info(f"[Queue] [Window {self.window_id}] Ajout de {media_name} à la file (queue_size={len(self.download_queue)})")

        if not media:
            log_error(f"[Queue] [Window {self.window_id}] Média non trouvé pour : {media_name}")
            return

        if media.get("status") == "Completed":
            log_info(f"[Queue] [Window {self.window_id}] Ignoré (déjà complété) : {media_name}")
            return

        if media in self.download_queue:
            log_info(f"[Queue] [Window {self.window_id}] {media_name} déjà en file")
            return

        log_info(f"[Queue] [Window {self.window_id}] Préparation pour {media_name} (status={media.get('status')})")
        media["status"] = "Waiting"
        media["error"] = ""
        media["hash_check"] = ""

        subdir = self.video_dir if is_video(media) else self.image_dir
        tmp_path = os.path.join(subdir, media["name"] + ".tmp")
        if os.path.exists(tmp_path):
            size = os.path.getsize(tmp_path)
            media["local_size"] = size
            total = media.get("size_http", 0)
            media["percent"] = int((size / total) * 100) if total else 0
        else:
            media["local_size"] = 0
            media["percent"] = 0

        if "size_http" not in media or not media["size_http"]:
            media["size_http"] = 0

        self.refresh_media_row(media)
        self.download_queue.append(media)
        log_info(f"[Queue] [Window {self.window_id}] {media_name} → Ajouté à la file de téléchargement (queue_size={len(self.download_queue)})")

    def refresh_media_row(self, media, move_to_completed=False):
        log_info(f"[REFRESH] [Window {self.window_id}] Tentative de mise à jour pour {media.get('name')}, speed={media.get('speed')}")
        if self.is_closing or not self.is_active or not self.check_ui_alive():
            log_info(f"[REFRESH] [Window {self.window_id}] 🚫 Mise à jour ignorée pour {media.get('name')}")
            return
        if self.is_closing and media.get("status") == "Paused":
            log_info(f"[REFRESH] Ignoré pour {media.get('name')} : fenêtre fermée et statut Paused")
            return

        media_name = media.get("name")
        if not media_name:
            log_warning(f"[REFRESH] [Window {self.window_id}] Média sans nom, ignoré")
            return

        tree_type = "video" if is_video(media) else "image"
        status = media.get("status", "Missing")
        downloaded = media.get("local_size", 0)
        total = media.get("size_http", 0)
        percent_val = int((downloaded / total) * 100) if total else 0
        speed = media.get("speed", "0 B/s")

        if percent_val >= 100 and status == "Completed":
            move_to_completed = True

        subtab = "completed" if move_to_completed or (status == "Completed" and percent_val >= 100) else "not_downloaded"
        log_info(f"[REFRESH] [Window {self.window_id}] {media_name} → cible : {tree_type}/{subtab} (status={status}, percent={percent_val}%)")

        tree = (
            self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
            self.video_completed_tree if tree_type == "video" and subtab == "completed" else
            self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
            self.image_completed_tree
        )

        old_subtab = "not_downloaded" if subtab == "completed" else "completed"
        old_tree = (
            self.video_not_downloaded_tree if tree_type == "video" and old_subtab == "not_downloaded" else
            self.video_completed_tree if tree_type == "video" and old_subtab == "completed" else
            self.image_not_downloaded_tree if tree_type == "image" and old_subtab == "not_downloaded" else
            self.image_completed_tree
        )

        old_key = (media_name, tree_type, old_subtab)
        old_item_id = self.item_id_cache.get(old_key)
        if old_item_id and old_tree.winfo_exists():
            try:
                if old_tree.exists(old_item_id):
                    old_tree.delete(old_item_id)
                    log_info(f"[REFRESH] [Window {self.window_id}] Supprimé {media_name} de {tree_type}/{old_subtab}")
                del self.item_id_cache[old_key]
            except Exception as e:
                log_warning(f"[REFRESH] [Window {self.window_id}] Erreur suppression {media_name} de {tree_type}/{old_subtab}: {e}")

        tree_key = f"{tree_type}_{subtab}"
        if tree_key not in self.loaded_treeviews and subtab == "completed":
            log_info(f"[REFRESH] [Window {self.window_id}] {tree_key} non chargé, insertion unique pour {media_name}")
            self.insert_single_media(media, tree_type, subtab)
            return

        item_id = self.item_id_cache.get((media_name, tree_type, subtab))
        if not item_id or not tree.exists(item_id):
            log_info(f"[REFRESH] [Window {self.window_id}] {media_name} non trouvé dans {tree_type}/{subtab}, insertion")
            self.insert_single_media(media, tree_type, subtab)
            return

        # Ajuster les valeurs en fonction de la présence de la colonne speed
        if tree in [self.video_not_downloaded_tree, self.image_not_downloaded_tree]:
            values = (
                media_name,
                format_bytes(downloaded),
                format_bytes(total),
                speed,
                render_progress_bar(percent_val),
                status,
                media.get("hash_check", ""),
                os.path.splitext(media_name)[1][1:].lower(),
                media.get("error", ""),
                media.get("url", ""),
                str(media.get("retry_count", 0))
            )
        else:
            values = (
                media_name,
                format_bytes(downloaded),
                format_bytes(total),
                render_progress_bar(percent_val),
                status,
                media.get("hash_check", ""),
                os.path.splitext(media_name)[1][1:].lower(),
                media.get("error", ""),
                media.get("url", ""),
                str(media.get("retry_count", 0))
            )

        try:
            self.safe_update_tree(item_id, tree_type, subtab, values=values)
            combined_tag = f"{status.lower()}.{tree_type}"
            self.safe_update_tree(item_id, tree_type, subtab, tags=(combined_tag,))
            log_info(f"[REFRESH] [Window {self.window_id}] Mis à jour {media_name} dans {tree_type}/{subtab} avec speed={speed}")
        except Exception as e:
            log_warning(f"[REFRESH] [Window {self.window_id}] Erreur mise à jour {media_name} dans {tree_type}/{subtab}: {e}")

    def download_selected_file(self):
        tree = self.get_current_video_tree()
        item_id = self.get_selected_item_id(tree)
        if item_id:
            self.enqueue_download(item_id, "video", "not_downloaded")
        else:
            messagebox.showwarning("Aucun fichier", "Veuillez sélectionner un fichier dans la liste.")

    def download_media(self, item_id, tree_type, subtab):
        if self.is_closing:
            log_info(f"[DL] 🚫 Téléchargement annulé car {self.username} fermé")
            return
        if not all(hasattr(self, tree) and getattr(self, tree).winfo_exists() for tree in [
            "video_not_downloaded_tree", "video_completed_tree",
            "image_not_downloaded_tree", "image_completed_tree"
        ]):
            log_warning("[Download] Treeview indisponible")
            return

        tree = (self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree)
        
        if not tree.exists(item_id):
            log_warning(f"[DL] Item {item_id} non trouvé dans {tree_type}/{subtab}")
            return

        media_name = tree.item(item_id, "values")[0]
        media = next((m for m in self.medias if m.get("name") == media_name), None)
        if not media:
            log_error(f"[DL] Média non trouvé pour {media_name}")
            return

        if media.get("status") == "Completed":
            log_info(f"[DL] Ignoré : {media_name} déjà Completed")
            return

        subdir = self.video_dir if is_video(media) else self.image_dir
        tmp_path = os.path.join(subdir, media_name + ".tmp")
        final_path = os.path.join(subdir, media_name)

        if os.path.exists(tmp_path):
            current_size = os.path.getsize(tmp_path)
            media["local_size"] = current_size
            total_size = media.get("size_http", 0)
            media["percent"] = int((current_size / total_size) * 100) if total_size else 0
        else:
            media["local_size"] = 0
            media["percent"] = 0

        media["status"] = "Downloading"
        media["error"] = ""
        self.refresh_media_row(media)

        def download_callback(progress, speed, total_size):
            if self.is_closing:
                return False
            media["local_size"] = progress
            media["speed"] = speed if speed else "0 B/s"
            media["size_http"] = total_size
            percent = int((progress / total_size) * 100) if total_size else 0
            media["percent"] = percent
            media["status"] = "Downloading" if os.path.exists(tmp_path) else "Completed"
            if percent >= 100:
                try:
                    os.rename(tmp_path, final_path)
                    media["status"] = "Completed"
                    log_info(f"[DL] Renommé {tmp_path} → {final_path}")
                except Exception as e:
                    log_error(f"[DL] Échec renommage : {e}")
                    media["status"] = "Incomplete"  # temporairement, le renommage échoue
            self.refresh_media_row(media)  # Rafraîchissement à chaque mise à jour
            return True

        def error_callback(error_msg):
            if self.is_closing:
                return
            media["status"] = "Failed"
            media["error"] = error_msg
            self.refresh_media_row(media)
            log_error(f"[DL] Erreur pour {media_name}: {error_msg}")

        url = media.get("url", "")
        if not url:
            alternative_urls = generate_alternative_urls(media_name)
            for alt_url in alternative_urls:
                try:
                    response = requests.head(alt_url, timeout=10)
                    if response.status_code == 200:
                        url = alt_url
                        media["url"] = url
                        break
                except Exception:
                    continue
            if not url:
                error_callback("Aucune URL valide trouvée")
                return

        def should_stop():
            return self.is_closing

        threading.Thread(target=lambda: download_file(
            url, tmp_path, download_callback, resume=True, should_stop=should_stop, window_id=self.window_id
        ), daemon=True).start()
        log_info(f"[DL] Démarré téléchargement de {media_name} depuis {url}")

    def check_all_completed_files(self, tree_type):
        completed_media = [m for m in self.medias if m.get("type") == tree_type and m.get("status") == "Completed"]
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(self.verify_sha256_for_media, completed_media)

    def verify_sha256_for_media(self, media):
        subdir = self.video_dir if is_video(media) else self.image_dir
        final_path = os.path.join(subdir, media["name"])
        if not os.path.exists(final_path):
            media["status"] = "Missing"
            media["hash_check"] = ""
            self.refresh_media_row(media)
            return

        ok = verify_hash_from_cdn_path(final_path, media.get("url", ""))
        media["hash_check"] = "" if ok else "Mismatch"
        media["status"] = "Completed" if ok else "Failed"
        media["local_size"] = os.path.getsize(final_path)
        self.refresh_media_row(media)
        self.save_json()

    def check_sha256_all_video_not_downloaded(self):
        self.check_sha256_for_tree(self.video_not_downloaded_tree)

    def check_sha256_all_image_not_downloaded(self):
        self.check_sha256_for_tree(self.image_not_downloaded_tree)

    def check_sha256_for_tree(self, tree):
        selected_items = tree.selection()
        if not selected_items:
            messagebox.showwarning("Aucun fichier", "Veuillez sélectionner au moins un fichier.")
            return
        for item_id in selected_items:
            media_name = tree.item(item_id, "values")[0]
            media = next((m for m in self.medias if m.get("name") == media_name), None)
            if media:
                self.verify_sha256(item_id, "video" if tree in [self.video_not_downloaded_tree, self.video_completed_tree] else "image",
                                 "not_downloaded" if tree in [self.video_not_downloaded_tree, self.image_not_downloaded_tree] else "completed")

    def ignore_selected_file(self):
        tree = self.get_current_video_tree()
        item_id = self.get_selected_item_id(tree)
        if item_id:
            media_name = tree.item(item_id, "values")[0]
            media = next((m for m in self.medias if m.get("name") == media_name), None)
            if media:
                media["status"] = "Ignored"
                media["error"] = ""
                media["percent"] = 0
                media["local_size"] = 0
                media["speed"] = ""
                media["hash_check"] = ""

                # Supprimer le fichier local (final et .tmp)
                subdir = self.video_dir if is_video(media) else self.image_dir
                final_path = os.path.join(subdir, media_name)
                tmp_path = final_path + ".tmp"

                for path in [final_path, tmp_path]:
                    if os.path.exists(path):
                        try:
                            os.remove(path)
                            log_info(f"[Ignore] 🗑️ Fichier supprimé : {path}")
                        except Exception as e:
                            log_warning(f"[Ignore] ⚠️ Erreur suppression {path} : {e}")

                self.refresh_media_row(media, move_to_completed=False)
                self.save_json()
        else:
            messagebox.showwarning("Aucun fichier", "Veuillez sélectionner un fichier dans la liste.")

    def open_video_folder(self):
        if os.path.exists(self.video_dir):
            subprocess.Popen(["open" if sys.platform == "darwin" else "xdg-open", self.video_dir])

    def open_image_folder(self):
        if os.path.exists(self.image_dir):
            subprocess.Popen(["open" if sys.platform == "darwin" else "xdg-open", self.image_dir])

    def get_current_video_tree(self):
        current_tab = self.video_notebook.tab(self.video_notebook.select(), "text").lower()
        return self.video_not_downloaded_tree if current_tab == "not downloaded" else self.video_completed_tree

    def get_selected_item_id(self, tree):
        selected = tree.selection()
        return selected[0] if selected else None

    def get_all_sizes_thread(self, media_type):
        threading.Thread(target=self.get_all_sizes, args=(media_type,), daemon=True).start()

    def get_all_sizes(self, media_type):
        tree = self.video_not_downloaded_tree if media_type == "video" else self.image_not_downloaded_tree
        for item_id in tree.get_children():
            self.update_file_size(item_id, media_type, "not_downloaded")

    def download_file_thread(self, media):
        log_info(f"[DL] [Window {self.window_id}] 🧵 Thread DL lancé pour {media.get('name')}")
        if self.is_closing or not self.is_active:
            log_info(f"[DL] [Window {self.window_id}] Profil {self.username} fermé, thread annulé")
            return

        name = media.get("name")
        url = media.get("url")

        try:
            subdir = self.video_dir if is_video(media) else self.image_dir
        except Exception as e:
            log_error(f"[DIR] [Window {self.window_id}] Erreur détection type pour {name}: {e}")
            subdir = self.local_dir

        dest_path = os.path.join(subdir, name)
        tmp_path = dest_path + ".tmp"
        log_info(f"[DL] [Window {self.window_id}] {name} → Dir: {subdir}")
        media["retry_count"] = media.get("retry_count", 0)
        has_received_data = False
        progress_lock = threading.Lock()
        last_progress_update = 0
        min_progress_interval = 0.5
        thread_timeout = 180
        retries = 0

        def on_progress(downloaded, speed_str, total):
            nonlocal has_received_data, last_progress_update
            if self.is_closing or not self.is_active or not self.check_ui_alive():
                log_info(f"[DL] [Window {self.window_id}] Annulation update GUI pour {name}")
                return
            current_time = time.time()
            with progress_lock:
                if current_time - last_progress_update < min_progress_interval:
                    return
                last_progress_update = current_time
                try:
                    if not media.get("size_http") and total:
                        media["size_http"] = total
                    media["local_size"] = downloaded
                    media["percent"] = int((downloaded / total) * 100) if total else 0
                    media["speed"] = speed_str
                    media["last_downloaded"] = downloaded

                    if downloaded > 0:
                        has_received_data = True
                        if media["status"] != "Downloading":
                            media["status"] = "Downloading"
                            log_info(f"[DL] [Window {self.window_id}] {name} → Téléchargement démarré")

                    if self.check_ui_alive() and not self.is_closing:
                        log_info(f"[DL] [Window {self.window_id}] Mise à jour progression : {name} → {media['percent']}% ({media['local_size']}/{media['size_http']}, speed={media['speed']}, status={media['status']})")
                        self.refresh_media_row(media)
                except Exception as e:
                    log_error(f"[DL] [Window {self.window_id}] Erreur dans on_progress pour {name} : {e}")

        def should_stop():
            return self.is_closing or not self.is_active or not self.queue_processor_running

        def run_download():
            nonlocal retries
            name = media.get("name")
            log_info(f"[DL] [Window {self.window_id}] {name} → 📥 File en queue")
            max_retries = 3000000

            while self.is_active and not self.is_closing and self.queue_processor_running and retries < max_retries:
                try:
                    # Définir le statut initial à "Downloading" pour chaque tentative
                    #media["status"] = "Retrying" if retries > 0 else "Downloading"
                    media["status"] = "Downloading" # Retrait des retryning
                    log_info(f"[DL] [Window {self.window_id}] {name} → Tentative {retries+1}/{max_retries}, status={media['status']}")
                    if self.check_ui_alive() and not self.is_closing:
                        self.refresh_media_row(media)

                    success, err = download_file(
                        url,
                        tmp_path,
                        resume=True,
                        on_progress=on_progress,
                        should_stop=should_stop,
                        window_id=self.window_id
                    )

                    if not success:
                        raise Exception(err or "Téléchargement interrompu")

                    if not os.path.exists(tmp_path):
                        raise Exception("Fichier .tmp manquant après téléchargement")

                    downloaded_size = os.path.getsize(tmp_path)
                    expected_size = media.get("size_http", 0)
                    if expected_size and downloaded_size < expected_size * 0.98:
                        raise Exception(f"Téléchargement incomplet : {downloaded_size} / {expected_size} bytes")

                    if not verify_hash_from_cdn_path(tmp_path, url):
                        raise Exception("Checksum invalide")

                    if is_video(media) and not is_valid_video(tmp_path):
                        raise Exception("Fichier vidéo corrompu ou invalide")
                    elif media.get("type") == "image" and not is_valid_image(tmp_path):
                        raise Exception("Fichier image corrompu ou invalide")

                    os.rename(tmp_path, dest_path)

                    media["status"] = "Completed"
                    media["percent"] = 100
                    media["error"] = ""
                    media["local_size"] = os.path.getsize(dest_path)
                    media["size_http"] = media.get("size_http", downloaded_size)
                    media["hash_check"] = ""
                    media["speed"] = "0 B/s"

                    log_info(f"[DL] [Window {self.window_id}] ✅ Téléchargement terminé pour {name}")
                    log_info(f"[DL] [Window {self.window_id}] Final : {name} → status={media['status']}, percent={media['percent']}%, local_size={media['local_size']}, size_http={media['size_http']}, speed={media['speed']}")

                    if self.check_ui_alive() and not self.is_closing:
                        self.refresh_media_row(media, move_to_completed=True)
                        self.save_json()

                    break

                except Exception as e:
                    media["error"] = str(e)
                    media["status"] = "Failed"
                    media["percent"] = 0
                    media["speed"] = "0 B/s"
                    log_error(f"[DL] [Window {self.window_id}] {name} → ❌ Échec : {e}")

                    if os.path.exists(tmp_path):
                        tmp_size = os.path.getsize(tmp_path)
                        if tmp_size < 1024:
                            try:
                                os.remove(tmp_path)
                                log_info(f"[DL] [Window {self.window_id}] 🗑️ .tmp supprimé (taille trop faible : {tmp_size} bytes)")
                            except Exception as remove_err:
                                log_warning(f"[DL] [Window {self.window_id}] ⚠️ Échec suppression .tmp : {remove_err}")
                        else:
                            log_info(f"[DL] [Window {self.window_id}] ⏸️ .tmp conservé malgré l’échec (taille: {tmp_size} bytes)")

                    retries += 1
                    if retries < max_retries:
                        log_warning(f"[Retry] [Window {self.window_id}] {name} ({retries}/{max_retries}) nouvelle tentative dans 2s")
                        time.sleep(2)
                    else:
                        log_error(f"[DL] [Window {self.window_id}] {name} → ⛔ Échec définitif après {max_retries} tentatives")
                        time.sleep(2)
                        #break

            if media["status"] != "Completed":
                media["status"] = "Failed"
                media["error"] = "Échec après plusieurs tentatives" if retries >= max_retries else media.get("error", "")
                media["percent"] = 0
                media["speed"] = "0 B/s"
                if self.check_ui_alive() and not self.is_closing:
                    self.refresh_media_row(media)

        download_thread = threading.Thread(target=run_download, daemon=True)
        download_thread.start()
        download_thread.join(timeout=thread_timeout)

        if download_thread.is_alive():
            log_error(f"[DL] [Window {self.window_id}] {name} → Thread timed out après {thread_timeout}s")
            media["status"] = "Failed"
            media["error"] = "Thread timed out"
            media["percent"] = 0
            media["speed"] = "0 B/s"
            if self.check_ui_alive() and not self.is_closing:
                self.refresh_media_row(media)

        self.decrement_running_downloads()
        log_info(f"[DL] [Window {self.window_id}] {name} → Thread terminé")

        if self.check_ui_alive() and not self.is_closing:
            self.save_json()

        self.root.after(50, self.start_next_in_queue)

    def start_next_in_queue(self):
        log_info(f"[QUEUE] [Window {self.window_id}] Lancement de start_next_in_queue running_downloads={self.running_downloads}, queue_size={len(self.download_queue)}")
        if self.is_closing or not self.is_active:
            log_info(f"[QUEUE] [Window {self.window_id}] Fermeture active, start_next_in_queue() annulé")
            return

        with queue_lock:
            log_info(f"[QUEUE] [Window {self.window_id}] Appel start_next_in_queue()")
            log_info(f"[QUEUE] État : instance_downloads={self.running_downloads}, queue={len(self.download_queue)}")

            while self.download_queue and self.running_downloads < MAX_CONCURRENT_DOWNLOADS:
                media = self.download_queue.pop(0)

                if not media:
                    log_warning(f"[QUEUE] [Window {self.window_id}] ⚠️ Média None retiré de la queue → skip")
                    continue

                status = media.get("status", "")
                name = media.get("name", "Unknown")
                if status in ["Completed", "Downloading", "Ignored"]:
                    log_info(f"[QUEUE] [Window {self.window_id}] ⏩ {name} déjà en statut {status} → skip")
                    continue

                self.running_downloads += 1
                log_info(f"[QUEUE] [Window {self.window_id}] ✅ Lancement de start_next_in_queue (instance_downloads={self.running_downloads})")
                threading.Thread(target=self.download_file_thread, args=(media,), daemon=True).start()

            if not self.download_queue:
                log_info(f"[QUEUE] [Window {self.window_id}] ✅ Plus rien dans la queue")

    def update_file_size(self, item_id, tree_type, subtab):
        tree = (self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree)
        media_name = tree.item(item_id, "values")[0]
        media = next((m for m in self.medias if m.get("name") == media_name), None)
        if media and media.get("url"):
            size = get_remote_file_size(media["url"])
            if size:
                media["size_http"] = size
                self.refresh_media_row(media)

    def decrement_running_downloads(self):
        with queue_lock:
            self.running_downloads = max(0, self.running_downloads - 1)
            log_info(f"[QUEUE] [Window {self.window_id}] ⬇️ instance_downloads décrémenté → {self.running_downloads}")

    def repair_file(self, item_id, tree_type, subtab):
        tree = (self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree)
        media_name = tree.item(item_id, "values")[0]
        media = next((m for m in self.medias if m.get("name") == media_name), None)
        if not media:
            log_error(f"[Repair] Média non trouvé : {media_name}")
            return

        subdir = self.video_dir if is_video(media) else self.image_dir
        tmp_path = os.path.join(subdir, media_name + ".tmp")
        final_path = os.path.join(subdir, media_name)

        if os.path.exists(tmp_path):
            try:
                os.rename(tmp_path, final_path)
                media["status"] = "Completed"
                media["local_size"] = os.path.getsize(final_path)
                media["percent"] = 100
                self.refresh_media_row(media, move_to_completed=True)
                log_info(f"[Repair] Réparé : {media_name}")
            except Exception as e:
                log_error(f"[Repair] Erreur renommage : {e}")
        else:
            log_warning(f"[Repair] Fichier tmp absent : {tmp_path}")

    def change_download_directory(self):
        new_dir = filedialog.askdirectory(initialdir=self.local_dir)
        if new_dir:
            self.local_dir = new_dir
            self.video_dir = os.path.join(new_dir, "v")
            self.image_dir = os.path.join(new_dir, "p")
            os.makedirs(self.video_dir, exist_ok=True)
            os.makedirs(self.image_dir, exist_ok=True)
            self.global_settings["profile_dirs"] = self.global_settings.get("profile_dirs", {})
            self.global_settings["profile_dirs"][self.profile_key] = new_dir
            with open("settings.json", "w", encoding="utf-8") as f:
                json.dump(self.global_settings, f, indent=4)
            log_info(f"[DIR] Changement de répertoire pour {self.profile_key} vers {new_dir}")

    def retry_failed_downloads_loop(self, interval_minutes=5):
        def loop():
            while self.is_active and not self.is_closing:
                self.retry_failed_downloads()
                time.sleep(interval_minutes * 60)
        threading.Thread(target=loop, daemon=True).start()

    def retry_failed_downloads(self):
        failed_media = [m for m in self.medias if m.get("status") == "Failed"]
        for media in failed_media:
            if self.running_downloads < MAX_CONCURRENT_DOWNLOADS:
                media["status"] = "Retrying"
                self.refresh_media_row(media)
                self.enqueue_media(media)

    def start_queue_processor(self):
        def process_queue():
            while self.queue_processor_running and self.is_active and not self.is_closing:
                if self.download_queue and self.running_downloads < MAX_CONCURRENT_DOWNLOADS:
                    self.start_next_in_queue()
                time.sleep(0.1)
        threading.Thread(target=process_queue, daemon=True).start()

    def check_ui_alive(self):
        return self.root.winfo_exists() and all(getattr(self, attr, None) and getattr(self, attr).winfo_exists()
                                               for attr in ["video_not_downloaded_tree", "video_completed_tree",
                                                            "image_not_downloaded_tree", "image_completed_tree"])

    def save_json(self):
        with self.save_lock:
            try:
                with open(self.json_path, "w", encoding="utf-8") as f:
                    json.dump(self.medias_data, f, indent=4)
                log_info(f"[SAVE] [Window {self.window_id}] JSON sauvegardé à {self.json_path}")
            except Exception as e:
                log_error(f"[SAVE] [Window {self.window_id}] Erreur sauvegarde JSON : {e}")

    def on_event_update(self, event_data):
        if event_data.get("profile_key") == self.profile_key:
            self.refresh_profile()

    def monitor_queue(self):
        def _monitor():
            last_activity = time.time()
            previous_count = 0
            log_info(f"[MONITOR] 🧭 Démarrage du watchdog pour {self.username}")

            while not self.is_closing and self.is_active and self.queue_processor_running:
                time.sleep(10)

                with queue_lock:
                    current_running = running_downloads

                if current_running > 0:
                    last_activity = time.time()
                    previous_count = current_running
                    continue

                elapsed = time.time() - last_activity
                if elapsed > 30:  # bloqué depuis plus de 30 sec
                    log_warning(f"[MONITOR] ⏰ {self.username} semble bloqué (inactif depuis {int(elapsed)}s)")
                    self.start_next_in_queue()
                    last_activity = time.time()

            log_info(f"[MONITOR] 🛑 Watchdog terminé pour {self.username}")

        monitor_thread = threading.Thread(target=_monitor, daemon=True)
        monitor_thread.start()

if __name__ == "__main__":
    root = tk.Tk()
    root.title("Coomer Ultimate v1.0")
    root.geometry("1200x700")  # Ajusté pour plus d'espace
    app = MediaWindow(root, "service", "username", "local_dir", "path/to/json", {"medias": []})
    root.mainloop()