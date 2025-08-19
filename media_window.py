# media_window.py
# === Standard library ===
import os
import sys
import json
import time
import uuid
import threading
from threading import Lock

import subprocess
import tkinter as tk
from tkinter import messagebox, filedialog

# === Retry constants (hard-coded for now) ===
RETRY_DELAY_SECONDS = 10
MAX_FAILED_RETRIES = 10

from collections import defaultdict
from contextlib import contextmanager

# === Third-party libraries ===
import requests

# === Local application imports ===
from core.download_manager import DownloadManager
from core.restore_service import RestoreService
from event_bus import event_bus
from log import log_info, log_error, log_warning
from media_utils import is_valid_image, is_valid_video
from ui.media_window import MediaWindowUI
from core.executor import submit_unique
from core.limits import GLOBAL_SEM, window_sem
from utils.format_utils import format_bytes, render_progress_bar
from utils.network_utils import get_remote_file_size, verify_hash_from_cdn_path, generate_alternative_urls
from utils.media_utils import detect_type_from_name, is_video
from utils.file_utils import sha256_file
from queue import Queue


class DownloadConcurrencyController:
    def __init__(self, max_workers: int, name: str = "dlpool"):
        self.q = Queue()
        self.stop_evt = threading.Event()
        self.workers = []
        self.name = name
        for i in range(max_workers):
            t = threading.Thread(target=self._worker, daemon=True, name=f"worker_{i}")
            t.start()
            self.workers.append(t)

    def _worker(self):
        while not self.stop_evt.is_set():
            try:
                job = self.q.get(timeout=0.5)
            except Exception:
                continue
            try:
                job()
            finally:
                self.q.task_done()

    def enqueue(self, job):
        self.q.put(job)

    def shutdown(self, wait=False):
        self.stop_evt.set()
        if wait:
            # drainer proprement
            for _ in self.workers:
                self.q.put(lambda: None)
            for t in self.workers:
                try:
                    t.join(timeout=2)
                except Exception:
                    pass

# === Retry constants (hard-coded for now) ===
RETRY_DELAY_SECONDS = 10          # délai de base entre tentatives internes
INTERNAL_MAX_RETRIES_DEFAULT = 3  # ← AU LIEU de 1 : ré-essaie 3 fois par défaut
INTERNAL_BACKOFF_FACTOR = 2.0     # backoff expo : 10s, 20s, 40s...

# Quand toutes les tentatives internes échouent, on place le média en Failed,
# puis on le re-enqueue automatiquement jusqu’à cette limite « externe ».
EXTERNAL_RETRY_LIMIT = 10         # 10 relances max par média à travers le temps
EXTERNAL_RETRY_DELAY_SECONDS = 15 # délai avant requeue après un Failed final

MAX_CONCURRENT_DOWNLOADS = 25
queue_lock = threading.Lock()


class MediaWindow:
    def __init__(self, root, service, username, local_dir, json_path, medias_data):
        self.window_id = str(uuid.uuid4())  # Identifiant unique pour la fenêtre
        self.download_queue = []  # File d'attente spécifique à l'instance
        self.running_downloads = 0  # Compteur spécifique à l'instance
        self.queue_processor_running = True  # Contrôle du queue_processor
        self.ctrl = DownloadConcurrencyController(MAX_CONCURRENT_DOWNLOADS, name=f"pool:{self.window_id[:4]}")
        self.tree_item_keys = defaultdict(set)
        self.last_ui_update = {}
        self.root = root
        self.service = service
        self.username = str(username)
        self.json_path = json_path
        self.medias_data = medias_data
        self.medias = medias_data.get("medias", [])

        # --- Boot pipeline & guards ---
        self._booting = True  # tant que True, on ne touche pas l’UI
        self._suppress_events = True  # ignore les events bus pendant le boot
        self._initial_render_done = False  # garantit une seule insertion initiale
        self._restore_done = threading.Event()

        self._after_ids = set()
        self._after_lock = Lock()

        self.is_active = True
        self.is_closing = False
        self.restoring = True
        self.restore_progress_running = True
        self.ui_ready = threading.Event()

        self.profile_key = f"{service}:{self.username}"
        self.last_sorted_column = None
        self.sort_reverse = False
        self.item_id_cache = {}
        self.last_tagged = {}
        # Auto-sort flags
        self._auto_sort_enabled = False
        self._suspend_sorting = False

        # watchdog de queue
        self._monitor_stop = threading.Event()
        self.monitor_queue()

        # Verrou JSON + settings globaux
        self.save_lock = Lock()
        self.load_global_settings()
        self.global_settings = getattr(self, "global_settings", {})

        # Dossiers
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

        # =========================================================
        #                ⬇️  UI DÉLÉGUÉE À MediaWindowUI  ⬇️
        # =========================================================

        # Libellés multilingues
        try:
            with open("lang/en.json", "r", encoding="utf-8") as f:
                self.labels = json.load(f)
        except Exception as e:
            log_error(f"[LANG] Erreur chargement libellés multilangue : {e}")
            self.labels = {"columns": {}}

        # Définition des colonnes modernes
        self.columns = {
            "not_downloaded": ["name", "local_size", "http_size", "speed", "percent",
                               "status", "hash_check", "extension", "error", "url", "retry_count"],
            "completed": ["name", "local_size", "http_size", "percent",
                          "status", "hash_check", "extension", "error", "url", "retry_count"],
        }
        col_widths = {
            "name": 250,
            "local_size": 150,
            "http_size": 150,
            "speed": 150,
            "percent": 250,
            "status": 100,
            "hash_check": 100,
            "extension": 100,
            "error": 200,
            "url": 250,
            "retry_count": 80,
        }

        # Construit toute l’UI (⚠️ ne fait AUCUNE insertion dans les trees)
        MediaWindowUI(
            controller=self,
            root=self.root,
            service=self.service,
            username=self.username,
            labels=self.labels,
            columns=self.columns,
            col_widths=col_widths,
        )

        # Important: au boot, RIEN n’est “déjà chargé”
        self.loaded_treeviews = {}

        self.ui_ready.set()
        log_info(f"[UI] UI prête pour {self.profile_key}")

        # =========================================================
        #                 ⬆️  FIN DU BLOC UI EXTRACTED  ⬆️
        # =========================================================

        # Filtres de statut (restent côté core)
        self.filter_vars = {
            "Missing": tk.BooleanVar(value=False),
            "Completed": tk.BooleanVar(value=False),
            "Error": tk.BooleanVar(value=False),
            "Waiting": tk.BooleanVar(value=False),
            "Downloading": tk.BooleanVar(value=False),
            "Retrying": tk.BooleanVar(value=False),
            "Failed": tk.BooleanVar(value=False),
            "Incomplete": tk.BooleanVar(value=False),
            "Paused": tk.BooleanVar(value=False),
        }

        # Events / hooks fenêtre
        event_bus.subscribe(f"update:{self.profile_key}", self.on_event_update)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        # ===== Pipeline de boot : d'abord RESTORE (sans UI), puis rendu unique =====
        threading.Thread(target=self._do_restore_phase, daemon=True).start()
        self.schedule_after(0, self._wait_and_render_initial)

    def _do_restore_phase(self):
        try:
            # 1) Snapshot des items ignorés AVANT le restore
            self._ignored_keys_before_restore = set()
            for _m in self.medias:
                if (_m.get("status") or "").strip().lower() == "ignored":
                    try:
                        k = self._media_key(_m)
                    except Exception:
                        k = _m.get("name")
                    if k:
                        self._ignored_keys_before_restore.add(k)

            # 2) Restore depuis le disque (peut écraser des champs)
            self.restore_progress_from_files(skip_sha256_verify=True)

            # 3) Ré-applique les "Ignored" (signature tolère after/bind)
            self._reapply_ignored_after_restore()

            # (optionnel) sauvegarde après ré-application
            try:
                self.save_json()
            except Exception as e:
                log_warning(f"[RESTORE] Sauvegarde post-réapply ignorés a échoué : {e}")

        except Exception as e:
            log_error(f"[RESTORE] Erreur Restore phase: {e}")
        finally:
            # libère la suite du pipeline (attendue par _wait_and_render_initial)
            self._restore_done.set()
            # on peut nettoyer le snapshot si tu veux
            try:
                del self._ignored_keys_before_restore
            except Exception:
                pass

    def _wait_and_render_initial(self):
        """Attend la fin du RESTORE, puis fait UN SEUL rendu initial; démarre ensuite les services."""
        # 0) Attente restore (non bloquante : on se replanifie)
        if not getattr(self, "_restore_done", threading.Event()).is_set():
            self.schedule_after(50, self._wait_and_render_initial)
            return

        # 1) Rendu initial (idempotent) sous garde d'events supprimés
        try:
            self._suppress_events = True  # aucun on_tab_changed ne doit s'exécuter ici
            self._initial_render_once()  # ne doit pas clear/reinsérer agressivement
            self.restoring = False  # autorise les actions utilisateur (Download All, etc.)
        except Exception as e:
            log_error(f"[BOOT] initial render failed: {e}")

        # 2) Fin du boot → on autorise les events
        self._booting = False
        self._suppress_events = False

        # 3) Déclenche explicitement la 1ʳᵉ charge des onglets visibles (remplit ND/Completed/Ignored selon le tab actif)
        try:
            # On force un passage par les handlers, maintenant que les events ne sont plus supprimés
            # et que les statuts (dont 'Ignored') ont été ré-appliqués par la phase de restore.
            self.on_video_notebook_tab_changed(None)
        except Exception as e:
            log_warning(f"[BOOT] video tab initial load failed: {e}")

        try:
            self.on_image_notebook_tab_changed(None)
        except Exception as e:
            log_warning(f"[BOOT] image tab initial load failed: {e}")

        # 4) Démarrage services (après boot)
        self.start_queue_processor()
        if not hasattr(self, "_monitor_started"):
            self._monitor_started = True
            threading.Thread(target=self.monitor_threads_background, daemon=True).start()

        # 5) Post UI (tags/stats uniquement — pas d'insertion)
        self.schedule_after(50, self._post_ui_bootstrap)

        # 6) Boucle de retry + stats initiales
        self.retry_failed_downloads_loop(interval_minutes=5)
        self.update_media_stats()

    def _fix_media_types(self):
        """
        Corrige les types incohérents via l'extension de fichier.
        Force 'video'/'image' si l'extension est explicite.
        """
        VIDEO_EXT = {".mp4", ".m4v", ".mov", ".webm", ".avi", ".flv", ".mkv"}
        IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp", ".gif"}

        fixed = 0
        for m in self.medias:
            name = (m.get("name") or "").strip()
            if not name:
                continue
            ext = os.path.splitext(name.lower())[1]
            t = (m.get("type") or "").strip().lower()

            if ext in VIDEO_EXT and t != "video":
                m["type"] = "video"
                fixed += 1
            elif ext in IMAGE_EXT and t != "image":
                m["type"] = "image"
                fixed += 1
            elif t not in ("video", "image"):
                # fallback neutre
                m["type"] = "video" if ext in VIDEO_EXT else "image" if ext in IMAGE_EXT else "autre"

        if fixed:
            log_warning(f"[TYPES] {fixed} type(s) corrigé(s) via extension")

    def debug_snapshot(self, label=""):
        try:
            def count(tree):
                try:
                    return len(tree.get_children())
                except Exception:
                    return -1

            snap = {
                "label": label,
                "loaded_treeviews": dict(getattr(self, "loaded_treeviews", {})),
                "running_downloads": getattr(self, "running_downloads", None),
                "queue_size": len(getattr(self, "download_queue", [])),
                "video": {
                    "ND": count(getattr(self, "video_not_downloaded_tree", None)),
                    "Completed": count(getattr(self, "video_completed_tree", None)),
                    "Ignored": count(getattr(self, "video_ignored_tree", None)),
                },
                "image": {
                    "ND": count(getattr(self, "image_not_downloaded_tree", None)),
                    "Completed": count(getattr(self, "image_completed_tree", None)),
                    "Ignored": count(getattr(self, "image_ignored_tree", None)),
                },
                "medias_counts": {
                    "total": len(self.medias),
                    "video": sum(1 for m in self.medias if (m.get("type") or "").lower() == "video"),
                    "image": sum(1 for m in self.medias if (m.get("type") or "").lower() == "image"),
                    "nd_video": sum(1 for m in self.medias if (m.get("type") or "").lower() == "video" and (
                            m.get("status", "").lower() not in ("completed", "ignored"))),
                }
            }
            log_info(f"[DEBUG-SNAP] {snap}")
        except Exception as e:
            log_warning(f"[DEBUG-SNAP] fail: {e}")

    def _initial_render_once(self):
        if self._initial_render_done:
            return
        self._initial_render_done = True

        # Initialise le cache 'loaded_treeviews' si besoin
        if not hasattr(self, "loaded_treeviews"):
            self.loaded_treeviews = {}

        # Remplit chaque tree UNE fois (pas de clear+reinsert ensuite)
        try:
            self.configure_tree_tags()
        except Exception:
            pass

        # Vidéos
        if "video_not_downloaded" not in self.loaded_treeviews:
            self.insert_media_in_treeview(tree_type="video", status="not_downloaded")
            self.loaded_treeviews["video_not_downloaded"] = True
        if "video_completed" not in self.loaded_treeviews:
            self.insert_media_in_treeview(tree_type="video", status="completed")
            self.loaded_treeviews["video_completed"] = True
        if "video_ignored" not in self.loaded_treeviews:
            self.insert_media_in_treeview(tree_type="video", status="ignored")
            self.loaded_treeviews["video_ignored"] = True

        # Images
        if "image_not_downloaded" not in self.loaded_treeviews:
            self.insert_media_in_treeview(tree_type="image", status="not_downloaded")
            self.loaded_treeviews["image_not_downloaded"] = True
        if "image_completed" not in self.loaded_treeviews:
            self.insert_media_in_treeview(tree_type="image", status="completed")
            self.loaded_treeviews["image_completed"] = True
        if "image_ignored" not in self.loaded_treeviews:
            self.insert_media_in_treeview(tree_type="image", status="ignored")
            self.loaded_treeviews["image_ignored"] = True

        # Stats de header
        try:
            self.update_status_summary()
        except Exception:
            pass

    def _post_ui_bootstrap(self):
        # ⚠️ Ne pas marquer d’onglet comme "déjà chargé" ici.
        try:
            # Initialise le dict s'il n'existe pas, mais le laisser VIDE
            if not hasattr(self, "loaded_treeviews") or not isinstance(getattr(self, "loaded_treeviews"), dict):
                self.loaded_treeviews = {}

            # Config visuelle uniquement (pas d'insertion dans les trees)
            self.configure_tree_tags()

            # Stats & résumé (léger)
            self.update_media_stats()
            self.update_status_summary()

            # NE PAS appeler:
            # - insert_media_in_treeview(...)
            # - start_queue_processor() (déjà lancé ailleurs)
            # - aucun clear/reinsert
        except Exception as e:
            log_error(f"[BOOT] post UI bootstrap failed: {e}")

    def update_status_summary(self):
        try:
            # calcule et set le label déjà existant
            total = len(self.medias)
            completed = sum(1 for m in self.medias if m.get("status") == "Completed")
            percent = round((completed / total) * 100, 1) if total else 0
            if hasattr(self, "media_stats_label"):
                self.media_stats_label.config(text=f"{completed}/{total} — {percent}%")
        except Exception as e:
            log_warning(f"[SUMMARY] Stub summary error: {e}")

    def on_video_notebook_tab_changed(self, event):
        if getattr(self, "_suppress_events", False):
            log_info(f"[NOTEBOOK] (suppressed) video tab change ignoré pendant boot")
            return
        selected_tab = self.video_notebook.select()
        tab_text = self.video_notebook.tab(selected_tab, "text").lower()

        if "completed" in tab_text:
            status = "completed"
            tree_key = "video_completed"
        elif "ignored" in tab_text:
            status = "ignored"
            tree_key = "video_ignored"
        else:
            status = "not_downloaded"
            tree_key = "video_not_downloaded"

        log_info(f"[NOTEBOOK] [Window {self.window_id}] Changement onglet vidéos : {tab_text} → {tree_key}")

        if tree_key not in getattr(self, "loaded_treeviews", {}):
            self.root.config(cursor="wait")
            self.insert_media_in_treeview(tree_type="video", status=status)
            self.loaded_treeviews[tree_key] = True
            self.root.config(cursor="")
            log_info(f"[NOTEBOOK] [Window {self.window_id}] {tree_key} chargé")
        else:
            log_info(f"[NOTEBOOK] [Window {self.window_id}] {tree_key} déjà chargé, aucun rechargement")

    def on_image_notebook_tab_changed(self, event):
        if getattr(self, "_suppress_events", False):
            log_info(f"[NOTEBOOK] (suppressed) image tab change ignoré pendant boot")
            return
        selected_tab = self.image_notebook.select()
        tab_text = self.image_notebook.tab(selected_tab, "text").lower()

        if "completed" in tab_text:
            status = "completed"
            tree_key = "image_completed"
        elif "ignored" in tab_text:
            status = "ignored"
            tree_key = "image_ignored"
        else:
            status = "not_downloaded"
            tree_key = "image_not_downloaded"

        log_info(f"[NOTEBOOK] [Window {self.window_id}] Changement onglet images : {tab_text} → {tree_key}")

        if tree_key not in getattr(self, "loaded_treeviews", {}):
            self.root.config(cursor="wait")
            self.insert_media_in_treeview(tree_type="image", status=status)
            self.loaded_treeviews[tree_key] = True
            self.root.config(cursor="")
            log_info(f"[NOTEBOOK] [Window {self.window_id}] {tree_key} chargé")

    def _snapshot_ignored_keys(self):
        """Capture les clés stables des médias marqués 'Ignored' avant le restore."""
        ignored = set()
        for m in self.medias:
            st = (m.get("status") or "").strip().lower()
            if st == "ignored":
                try:
                    key = self._media_key(m)
                except Exception:
                    key = m.get("name")
                if key:
                    ignored.add(key)
        return ignored

    def _reapply_ignored_after_restore(self, ignored_keys):
        """Ré-applique 'Ignored' sur base du snapshot pris AVANT restore."""
        try:
            for m in self.medias:
                try:
                    key = self._media_key(m)
                except Exception:
                    key = m.get("name")

                if key in ignored_keys:
                    m["status"] = "Ignored"
                    m["error"] = ""
                    m["speed"] = ""
                    m["percent"] = 0
                    subdir = self.video_dir if (m.get("type") or "").lower() == "video" else self.image_dir
                    final_path = os.path.join(subdir, (m.get("name") or ""))
                    if not os.path.exists(final_path):
                        m["local_size"] = 0
                    m["hash_check"] = ""
        except Exception as e:
            log_warning(f"[RESTORE] Ré-application Ignored a échoué : {e}")

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
                self.video_ignored_tree if tree_type == "video" and subtab == "ignored" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree if tree_type == "image" and subtab == "completed" else
                self.image_ignored_tree
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

    def get_current_tree_with_context(self):
        """
        Retourne (tree, tree_type, subtab) en fonction des onglets actuellement sélectionnés.
        tree_type ∈ {"video","image"}
        subtab ∈ {"not_downloaded","completed","ignored"}
        """
        try:
            main_tab = self.notebook.tab(self.notebook.select(), "text").lower()
        except Exception:
            # fallback: suppose vidéos
            main_tab = "vidéos"

        if "vidé" in main_tab or "video" in main_tab:  # gestion FR/EN
            tree_type = "video"
            subtxt = self.video_notebook.tab(self.video_notebook.select(), "text").lower()
            if "completed" in subtxt:
                subtab = "completed"
                tree = self.video_completed_tree
            elif "ignored" in subtxt:
                subtab = "ignored"
                tree = self.video_ignored_tree
            else:
                subtab = "not_downloaded"
                tree = self.video_not_downloaded_tree
        else:
            tree_type = "image"
            subtxt = self.image_notebook.tab(self.image_notebook.select(), "text").lower()
            if "completed" in subtxt:
                subtab = "completed"
                tree = self.image_completed_tree
            elif "ignored" in subtxt:
                subtab = "ignored"
                tree = self.image_ignored_tree
            else:
                subtab = "not_downloaded"
                tree = self.image_not_downloaded_tree

        return tree, tree_type, subtab

    def check_sha256_all_video_not_downloaded(self):
        """Vérifie le SHA256 pour tous les items de l'onglet Vidéos > Not downloaded."""
        try:
            tree = self.video_not_downloaded_tree
            for item_id in tree.get_children():
                self.verify_sha256(item_id, "video", "not_downloaded")
        except Exception as e:
            log_error(f"[SHA256-ALL] Video not_downloaded: {e}")

    def check_sha256_all_image_not_downloaded(self):
        """Vérifie le SHA256 pour tous les items de l'onglet Photos > Not downloaded."""
        try:
            tree = self.image_not_downloaded_tree
            for item_id in tree.get_children():
                self.verify_sha256(item_id, "image", "not_downloaded")
        except Exception as e:
            log_error(f"[SHA256-ALL] Image not_downloaded: {e}")

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

    def _normalize_restored_statuses(self):
        """Force les statuts instables en Paused après restauration"""
        changed = 0
        for media in self.medias:
            status = (media.get("status") or "").lower()
            if status in ("downloading", "retrying", "waiting"):
                media["status"] = "Paused"
                media["speed"] = ""
                media["error"] = ""
                changed += 1
        if changed:
            log_info(f"[RESTORE] {changed} média(s) normalisé(s) → Paused")
            self.save_json()

    def restore_progress_background(self):
        """
        Restaure l'état depuis le disque SANS réinsérer le Tree.
        - Pendant le boot: data-only (aucune écriture UI).
        - Après le boot: MAJ des stats/summary uniquement (update léger).
        """
        try:
            booting = getattr(self, "_booting", False)
            suppress = getattr(self, "_suppress_events", False)

            # ⚠️ Pendant le boot on ne touche pas à l'UI (pas de wait sur ui_ready, pas de curseur)
            if booting or suppress:
                try:
                    self.restore_progress_from_files(skip_sha256_verify=True)
                    log_info(f"[RESTORE] [Window {self.window_id}] Data restore (boot) OK")
                except Exception as e:
                    log_error(f"[RESTORE] Data restore (boot) a échoué : {e}")
                return

            # Ici, le boot est terminé → on peut accéder à l'UI en douceur
            self.ui_ready.wait()  # s'assure que les widgets existent
            if not self.check_ui_alive():
                log_info(f"[RESTORE] [Window {self.window_id}] UI non disponible, restauration data uniquement")
                try:
                    self.restore_progress_from_files(skip_sha256_verify=True)
                except Exception as e:
                    log_error(f"[RESTORE] Data restore (no UI) a échoué : {e}")
                return

            log_info(f"[RESTORE] [Window {self.window_id}] Début restauration (post-boot)")
            try:
                # Curseur "wait" uniquement si UI vivante
                self.root.config(cursor="wait")
            except Exception:
                pass

            try:
                # 1) Restauration DATA uniquement
                try:
                    self.restore_progress_from_files(skip_sha256_verify=True)
                    log_info(f"[RESTORE] [Window {self.window_id}] Progrès restauré depuis les fichiers")
                except Exception as e:
                    log_error(f"[RESTORE] restore_progress_from_files() a échoué : {e}")

                # 2) 🚫 PAS de insert_media_in_treeview() ici
                #    On laisse le rendu initial unique (_initial_render_once) et les updates in-place faire le job.

                # 3) MAJ légère de l'UI (stats/summary), sans toucher aux items
                try:
                    if self.check_ui_alive():
                        self.update_status_summary()
                except Exception as e:
                    log_error(f"[RESTORE] update_status_summary() a échoué : {e}")

                try:
                    if self.check_ui_alive():
                        self.update_media_stats()
                except Exception as e:
                    log_error(f"[RESTORE] update_media_stats() a échoué : {e}")

                log_info(f"[RESTORE] [Window {self.window_id}] Restauration terminée (post-boot)")
            finally:
                try:
                    if self.check_ui_alive():
                        self.root.config(cursor="")
                except Exception:
                    pass

        finally:
            # ✅ Toujours désarmer le flag, quoi qu’il arrive
            self.restoring = False
            # Optionnel: self.restore_progress_running = False

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
        # ========= Guard anti re-entrance / idempotence =========
        # (on autorise shutdown du contrôleur en tout premier si dispo)
        try:
            if hasattr(self, "ctrl") and self.ctrl:
                # Ne pas bloquer l'UI (on a un withdraw juste après)
                self.ctrl.shutdown(wait=False)
        except Exception:
            pass

        if getattr(self, "_closing_already", False):
            log_info(f"[CLOSE] [Window {getattr(self, 'window_id', '?')}] Close déjà en cours → ignore")
            return
        self._closing_already = True
        log_info(
            f"[CLOSE] [Window {getattr(self, 'window_id', '?')}] Fermeture de la fenêtre pour {getattr(self, 'profile_key', '?')}")

        # ========= Garde-fous UI tout de suite =========
        self._suppress_events = True
        try:
            if getattr(self, "root", None):
                self.root.protocol("WM_DELETE_WINDOW", lambda: None)
        except Exception:
            pass

        # ========= Flags d'arrêt immédiats (lus par les threads) =========
        try:
            self.is_closing = True
            self.is_active = False
            self.queue_processor_running = False
            self.restore_progress_running = False

            # Stop watchdog
            if hasattr(self, "_monitor_stop") and self._monitor_stop:
                try:
                    self._monitor_stop.set()
                except Exception:
                    pass

            # Stop retry loop
            if hasattr(self, "_retry_stop") and self._retry_stop:
                try:
                    self._retry_stop.set()
                except Exception:
                    pass

            # Stop éventuels autres événements
            if hasattr(self, "stop_event") and self.stop_event:
                try:
                    self.stop_event.set()
                except Exception:
                    pass
        except Exception:
            pass

        # ========= Retire la fenêtre de l'écran au plus vite (UX) =========
        try:
            if getattr(self, "root", None) and self.root.winfo_exists():
                self.root.withdraw()
                self.root.update_idletasks()
        except Exception:
            pass

        # ========= Annule tous les after() connus (le plus tôt possible) =========
        try:
            if hasattr(self, "_cancel_all_afters") and callable(self._cancel_all_afters):
                self._cancel_all_afters()
            else:
                for aid in list(getattr(self, "_after_ids", [])):
                    try:
                        if getattr(self, "root", None) and self.root.winfo_exists():
                            self.root.after_cancel(aid)
                    except Exception:
                        pass
                try:
                    self._after_ids.clear()
                except Exception:
                    pass
        except Exception:
            pass

        # ========= Désabonnement EventBus (best effort) =========
        try:
            if hasattr(event_bus, "publish"):
                event_bus.publish("profile:update", {
                    "reason": "window_close",
                    "no_sort": True,
                    "profile_key": getattr(self, "profile_key", None),
                    "service": getattr(self, "service", None),
                    "username": getattr(self, "username", None),
                })
        except Exception as e:
            log_warning(f"[CLOSE] Notification profile:update échouée : {e}")

        # ========= Stopper/vider la file & normaliser les statuts =========
        changed = 0
        try:
            for m in getattr(self, "medias", []):
                if m.get("status") in ("Downloading", "Retrying", "Waiting"):
                    m["status"] = "Paused"
                    m["speed"] = ""
                    m["error"] = ""
                    changed += 1

            # vide proprement la queue si elle existe (Queue ou list)
            try:
                if hasattr(self, "download_queue"):
                    if hasattr(self.download_queue, "queue"):  # queue.Queue
                        while True:
                            try:
                                self.download_queue.get_nowait()
                            except Exception:
                                break
                    elif hasattr(self.download_queue, "clear"):
                        self.download_queue.clear()
            except Exception:
                pass

            try:
                self.running_downloads = 0
            except Exception:
                pass
        except Exception as e:
            log_warning(f"[CLOSE] Normalisation/queue a échoué: {e}")

        # ========= Sauvegarde FINALE (toujours) =========
        try:
            # même si 'changed' == 0, on persiste l'état (ex: clic 'Ignore all' juste avant close)
            self.save_json()
            if changed:
                log_info(f"[CLOSE] {changed} média(s) converti(s) en Paused et sauvegardé(s)")
            else:
                log_info(f"[CLOSE] Sauvegarde finale effectuée (aucune normalisation supplémentaire)")
        except Exception as e:
            log_warning(f"[CLOSE] Sauvegarde finale a échoué: {e}")

        # ========= Notifier l’appli (best effort) =========
        try:
            if hasattr(self, "_notify_profile_update"):
                self._notify_profile_update()
        except Exception as e:
            log_warning(f"[CLOSE] Notification profile:update échouée : {e}")

        # ========= Destruction forcée sur thread UI (idempotent) =========
        def _force_destroy():
            try:
                if getattr(self, "root", None) and self.root.winfo_exists():
                    # Détruit d'abord les enfants pour couper les callbacks UI résiduels
                    for w in list(self.root.winfo_children()):
                        try:
                            w.destroy()
                        except Exception:
                            pass
                    self.root.destroy()
                    log_info(f"[CLOSE] Fenêtre détruite")
            except Exception as e:
                log_warning(f"[CLOSE] Erreur destruction fenêtre: {e}")

        try:
            if getattr(self, "root", None) and self.root.winfo_exists():
                # Une seule séquence de backup destroy (sans doublon)
                self.root.after(0, _force_destroy)
                self.root.after(80, _force_destroy)  # backup court
                self.root.after(300, _force_destroy)  # backup long
        except Exception as e:
            log_warning(f"[CLOSE] Planif destroy échouée : {e}")

    def _notify_profile_update(self):
        payload = {"service": self.service, "username": self.username, "profile_key": self.profile_key}
        bus = event_bus
        try:
            if hasattr(bus, "publish"):
                bus.publish("profile:update", payload)
            elif hasattr(bus, "emit"):
                bus.emit("profile:update", payload)
            elif hasattr(bus, "post"):
                bus.post("profile:update", payload)
            elif hasattr(bus, "send"):
                bus.send("profile:update", payload)
            elif hasattr(bus, "dispatch"):
                bus.dispatch("profile:update", payload)
            elif hasattr(bus, "trigger"):
                bus.trigger("profile:update", payload)
            elif callable(bus):
                bus("profile:update", payload)
            else:
                log_warning("[EVENT] Aucune méthode compatible sur event_bus (publish/emit/post/send/dispatch/trigger)")
        except Exception as e:
            log_warning(f"[EVENT] Notification profile:update échouée : {e}")

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
            for tree in [
                self.video_not_downloaded_tree, self.video_completed_tree, self.video_ignored_tree,
                self.image_not_downloaded_tree, self.image_completed_tree, self.image_ignored_tree
            ]:
                for tag, options in tags.items():
                    tree.tag_configure(tag, **options)
        except Exception as e:
            log_warning(f"[TREEVIEW] Erreur configuration tags couleurs : {e}")

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

    def refresh_profile(self):
        if self._booting or self._suppress_events:
            return
        self.restore_progress_from_files(skip_sha256_verify=True)
        self.update_status_summary()
        self.update_media_stats()
        # Pas de insert_media_in_treeview() ici → on évite le flicker
        # Les lignes bougent d’onglet via refresh_media_row/remove_media_from_all_tabs()
        self.apply_filter()

    def remove_media_from_all_tabs(self, name, media_type):
        """Supprime un média de tous les onglets avant réinsertion dans le bon."""
        for subtab in ("not_downloaded", "completed", "ignored"):
            cache_key = (name, media_type, subtab)
            iid = self.item_id_cache.pop(cache_key, None)
            if iid:
                try:
                    tree = (
                        self.video_not_downloaded_tree if media_type == "video" and subtab == "not_downloaded" else
                        self.video_completed_tree if media_type == "video" and subtab == "completed" else
                        self.video_ignored_tree if media_type == "video" and subtab == "ignored" else
                        self.image_not_downloaded_tree if media_type == "image" and subtab == "not_downloaded" else
                        self.image_completed_tree if media_type == "image" and subtab == "completed" else
                        self.image_ignored_tree
                    )
                    if tree.exists(iid):
                        tree.delete(iid)
                except Exception:
                    pass

    def restore_progress_from_files(self, skip_sha256_verify=True):
        log_info(f"[RESTORE] [Window {self.window_id}] Using video_dir: {self.video_dir}")
        log_info(f"[RESTORE] [Window {self.window_id}] Using image_dir: {self.image_dir}")
        log_info(f"[RESTORE] [Window {self.window_id}] Using local_dir: {self.local_dir}")

        touched_types = set()

        for media in self.medias:
            name = media.get("name", "")
            if not name:
                log_warning(f"[RESTORE] [Window {self.window_id}] Média sans nom, ignoré")
                continue

            # — type
            mtype = detect_type_from_name(name)
            media["type"] = mtype
            touched_types.add(mtype)
            log_info(f"[RESTORE] [Window {self.window_id}] {name} → Type détecté : {mtype}")

            if mtype == "video":
                subdir = os.path.join(self.local_dir, "v")
            elif mtype == "image":
                subdir = os.path.join(self.local_dir, "p")
            else:
                subdir = self.local_dir

            dest_path = os.path.join(subdir, name)
            tmp_path = dest_path + ".tmp"

            prev_status = (media.get("status") or "").strip()

            # === Règle d’or : NE JAMAIS ÉCRASER un Ignored pendant le restore ===
            if prev_status == "Ignored":
                # Met à jour uniquement des infos passives (taille locale) sans changer le status
                if os.path.exists(dest_path):
                    size = os.path.getsize(dest_path)
                    media["local_size"] = size
                    media["size_http"] = max(media.get("size_http", 0) or 0, size)
                else:
                    media["local_size"] = 0
                    media["size_http"] = media.get("size_http", 0) or 0
                # ne pas toucher percent/hash/speed/error ici
                log_info(f"[RESTORE] {name} → Ignored (préservé)")
                continue

            # === Fichier temporaire présent → Paused
            if os.path.exists(tmp_path):
                media["local_size"] = os.path.getsize(tmp_path)
                media["status"] = "Paused"
                media["percent"] = 0
                media["hash_check"] = ""
                media.setdefault("size_http", 0)

            # === Fichier final présent
            elif os.path.exists(dest_path):
                size = os.path.getsize(dest_path)
                media["local_size"] = size
                expected_size = media.get("size_http", 0) or 0
                media["size_http"] = max(size, expected_size)

                if not skip_sha256_verify:
                    try:
                        actual_hash = sha256_file(dest_path)
                        expected_hash = name.split("_")[-1].split(".")[0]
                        if expected_hash and actual_hash.startswith(expected_hash):
                            media["status"] = "Completed"
                            media["percent"] = 100
                            media["hash_check"] = ""
                        elif size > 0:
                            media["status"] = "Incomplete"
                            media["percent"] = 0
                            media["hash_check"] = actual_hash
                        else:
                            media["status"] = "Missing"
                            media["local_size"] = 0
                            media["percent"] = 0
                            media["hash_check"] = ""
                    except Exception as e:
                        log_warning(f"[RESTORE] {name} → Erreur SHA256 : {e}")
                        media["status"] = "Incomplete"
                        media["percent"] = 0
                        media["hash_check"] = ""
                else:
                    if size > 0:
                        media["status"] = "Completed"
                        media["percent"] = 100
                        media["hash_check"] = ""
                    else:
                        media["status"] = "Missing"
                        media["local_size"] = 0
                        media["percent"] = 0
                        media["hash_check"] = ""

            # === Rien trouvé
            else:
                media["status"] = "Missing"
                media["local_size"] = 0
                media["percent"] = 0
                media["hash_check"] = ""

        # Normaliser les états transitoires (mais pas Ignored)
        for media in self.medias:
            st = (media.get("status") or "").strip()
            if st in ("Downloading", "Retrying", "Waiting"):
                media["status"] = "Paused"
                media["speed"] = ""
                media["error"] = ""

        try:
            self.save_json()
        except Exception as e:
            log_warning(f"[RESTORE] Sauvegarde post-normalisation échouée : {e}")

        # Refresh visuel ciblé (insertion se fera après, via rendu initial)
        for m_type in touched_types:
            try:
                self.refresh_tabs_for_type(m_type)
            except Exception:
                pass

    # ---- Auto-sort controls ----
    def enable_auto_sort(self, enabled: bool):
        """Toggle auto-sorting of treeviews. Disabled by default."""
        self._auto_sort_enabled = bool(enabled)

    @contextmanager
    def suspend_sorting(self):
        """Temporarily suspend any sorting while updating/moving items."""
        prev = self._suspend_sorting
        self._suspend_sorting = True
        try:
            yield
        finally:
            self._suspend_sorting = prev


    def resort_treeview_if_needed(self, tree):
        # Respect global sorting guards
        if getattr(self, "_suspend_sorting", False):
            return
        if not getattr(self, "_auto_sort_enabled", False):
            return
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


    def _media_key(self, media: dict) -> str:
        """Construit une clé unique et stable pour un média."""
        return (
                str(media.get("id"))
                or media.get("cdn_path")
                or media.get("url")
                or media.get("name")
                or ""
        )


    def insert_media_in_treeview(self, tree_type="video", status="not_downloaded"):
        start_time = time.time()
        tree_attr = f"{tree_type}_{status}_tree"

        # ---- Existence & vie du widget ----
        if not hasattr(self, tree_attr):
            log_warning(f"[TREEVIEW] [Window {self.window_id}] {tree_attr} n'existe pas sur l'instance → abort")
            return
        if not self.check_ui_alive([tree_attr]):
            log_warning(f"[TREEVIEW] [Window {self.window_id}] {tree_attr} pas prêt, insertion annulée")
            return
        if self.is_closing or not self.is_active or not self.restore_progress_running:
            log_warning(f"[TREEVIEW] [Window {self.window_id}] Fenêtre inactive, insertion annulée")
            return

        tree = getattr(self, tree_attr)
        if not self.check_ui_alive(tree):
            log_warning(f"[TREEVIEW] [Window {self.window_id}] {tree_attr} widget mort, insertion annulée")
            return

        # ---- DIAG global avant filtrage ----
        total = len(self.medias)
        videos = sum(1 for m in self.medias if (m.get("type") or "").strip().lower() == "video")
        images = sum(1 for m in self.medias if (m.get("type") or "").strip().lower() == "image")
        log_info(f"[TREEVIEW] [{self.window_id}] {tree_attr} DIAG: total={total} videos={videos} images={images}")

        # ---- Clear + reset anti-doublon (seulement à l'initial render) ----
        try:
            tree.delete(*tree.get_children())
            if not hasattr(self, "tree_item_keys"):
                from collections import defaultdict
                self.tree_item_keys = defaultdict(set)
            self.tree_item_keys[tree_attr].clear()
            log_info(f"[TREEVIEW] [Window {self.window_id}] {tree_attr} vidé + reset anti-doublons")
        except Exception as e:
            log_warning(f"[TREEVIEW] [Window {self.window_id}] Erreur vidage {tree_attr} : {e}")
            return

        # ---- Comptage rapides pour vidéos à l'onglet ND ----
        if tree_type == "video" and status == "not_downloaded":
            self.stats_videos = self.stats_images = self.stats_autres = 0

        VIDEO_EXT = {".mp4", ".m4v", ".mov", ".webm", ".avi", ".flv", ".mkv"}
        IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp", ".gif"}

        # ---- Prépare les rows pour batch insert ----
        prepared_rows = []
        inserted = 0

        # normalise statut attendu
        wanted = status.strip().lower()

        for media in self.medias:
            # gardes de boucle
            if self.is_closing or not self.is_active or not self.check_ui_alive() or not self.restore_progress_running:
                log_warning(f"[TREEVIEW] [Window {self.window_id}] Arrêt insertion (fenêtre fermée)")
                return
            if not isinstance(media, dict):
                continue

            name = (media.get("name") or "").strip()
            if not name:
                continue

            # ---- Normalisation TYPE robuste via extension ----
            type_ = (media.get("type") or "").strip().lower()
            if type_ not in ("video", "image"):
                ext = os.path.splitext(name.lower())[1]
                if ext in VIDEO_EXT:
                    type_ = "video"
                elif ext in IMAGE_EXT:
                    type_ = "image"
                else:
                    type_ = "autre"
                media["type"] = type_

            # Stats rapides (vidéo ND)
            if tree_type == "video" and status == "not_downloaded":
                if type_ == "video":
                    self.stats_videos += 1
                elif type_ == "image":
                    self.stats_images += 1
                else:
                    self.stats_autres += 1

            # ---- Filtre par tab (case-insensitive, trim) ----
            media_status_raw = media.get("status", "Missing")
            media_status = (media_status_raw or "Missing").strip()
            media_status_l = media_status.lower()

            if type_ != tree_type:
                continue

            match = (
                    (wanted == "completed" and media_status_l == "completed") or
                    (wanted == "ignored" and media_status_l == "ignored") or
                    (wanted == "not_downloaded" and media_status_l not in ("completed", "ignored"))
            )
            if not match:
                continue

            # ---- anti-doublon (clé stable) ----
            key = self._media_key(media) or name
            if key in self.tree_item_keys[tree_attr]:
                continue
            self.tree_item_keys[tree_attr].add(key)

            # ---- compute values + tags ----
            values = self._prepare_row_values(media, tree)
            combined_tag = f"{media_status_l}.{tree_type}"
            tags = (combined_tag, media_status_l, tree_type, "missing")

            cache_key = (name, tree_type, status)
            prepared_rows.append((values, tags, cache_key))
            inserted += 1

        # ---- Batch insert non-bloquant UI ----
        self._bulk_insert_start(f"{tree_type}_{status}", prepared_rows, chunk_size=250, delay_ms=1)

        # DIAG final
        log_info(f"[TREEVIEW] [Window {self.window_id}] {tree_attr} → candidats={inserted} "
                 f"en {time.time() - start_time:.2f}s (prep)")

        # ---- Post actions légères ----
        if self.is_closing or not self.is_active or not self.check_ui_alive() or not self.restore_progress_running:
            return
        try:
            self.configure_tree_tags()
        except Exception:
            pass
        try:
            self.update_status_summary()
        except Exception:
            pass


    def insert_single_media(self, media, tree_type, subtab):
        if self.is_closing or not self.is_active or not self.check_ui_alive():
            return
        if not isinstance(media, dict):
            return

        try:
            tree = (
                self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.video_ignored_tree if tree_type == "video" and subtab == "ignored" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree if tree_type == "image" and subtab == "completed" else
                self.image_ignored_tree
            )
        except Exception:
            return
        if not tree.winfo_exists():
            return

        media.setdefault("name", "???")
        media.setdefault("speed", "0 B/s")
        media.setdefault("status", "Missing")
        media.setdefault("error", "")
        media.setdefault("url", "")
        media.setdefault("hash_check", "")
        media.setdefault("local_size", 0)
        media.setdefault("size_http", 0)

        name = media["name"]
        if not name:
            return

        # 🔹 Retirer avant réinsertion (évite doublons)
        self.remove_media_from_all_tabs(name, tree_type)

        cache_key = (name, tree_type, subtab)
        if cache_key in self.item_id_cache and tree.exists(self.item_id_cache[cache_key]):
            return

        downloaded = int(media.get("local_size", 0) or 0)
        total = int(media.get("size_http", 0) or 0)
        percent = int((downloaded / total) * 100) if total else 0
        percent_str = render_progress_bar(percent)
        ext = os.path.splitext(name)[1][1:].lower() or "unknown"

        values = (
            (name, format_bytes(downloaded), format_bytes(total),
             media.get("speed", "0 B/s"), percent_str, media["status"], media["hash_check"],
             ext, media["error"], media["url"], str(media.get("retry_count", 0)))
            if tree in [self.video_not_downloaded_tree, self.image_not_downloaded_tree]
            else
            (name, format_bytes(downloaded), format_bytes(total),
             percent_str, media["status"], media["hash_check"],
             ext, media["error"], media["url"], str(media.get("retry_count", 0)))
        )

        status = (media.get("status") or "Missing").lower()
        tags = (f"{status}.{tree_type}", status, tree_type, "missing")

        now = time.time()
        last = self.last_ui_update.get(name, 0)
        if now - last < 0.05:
            return
        self.last_ui_update[name] = now

        def _do_insert():
            if self.is_closing or not self.is_active:
                return
            if not self.check_ui_alive(tree):
                return
            try:
                iid = tree.insert("", tk.END, values=values, tags=tags)
                self.item_id_cache[cache_key] = iid
                try:
                    self.safe_update_tree(iid, tree_type, subtab, tags=(f"{status}.{tree_type}",))
                    self.resort_treeview_if_needed(tree)
                except Exception:
                    pass
            except Exception as e:
                log_error(f"[INSERT] [{self.window_id}] insert_single_media fail {name} → {e}")

        self.root.after_idle(_do_insert)


    def _reapply_ignored_after_restore(self, *args, **kwargs):
        """
        Ré-applique 'Ignored' aux médias qui l'étaient avant le restore.
        Doit être appelée juste après restore_progress_from_files().
        Supporte un appel via after()/bind (args ignorés).
        """
        try:
            # 1) Snapshot pris avant le restore (préféré)
            ignored_keys = getattr(self, "_ignored_keys_before_restore", None)

            # 2) Fallback safe: si pas de snapshot, on prend ceux déjà 'Ignored' (idempotent)
            if not ignored_keys:
                ignored_keys = set()
                for m in self.medias:
                    st = (m.get("status") or "").strip().lower()
                    if st == "ignored":
                        try:
                            key = self._media_key(m)
                        except Exception:
                            key = m.get("name")
                        if key:
                            ignored_keys.add(key)

            if not ignored_keys:
                return  # rien à faire

            # 3) Ré-application
            for m in self.medias:
                try:
                    key = self._media_key(m)
                except Exception:
                    key = m.get("name")

                if not key or key not in ignored_keys:
                    continue

                m["status"] = "Ignored"
                m["error"] = ""
                m["speed"] = ""
                m["percent"] = 0
                m["hash_check"] = ""

                # remet local_size à 0 si le fichier final n'existe pas
                mtype = (m.get("type") or "").lower()
                subdir = self.video_dir if mtype == "video" else self.image_dir
                final_path = os.path.join(subdir, m.get("name", "") or "")
                if not os.path.exists(final_path):
                    m["local_size"] = 0

        except Exception as e:
            log_warning(f"[RESTORE] Ré-application Ignored a échoué : {e}")


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
            menu.add_command(label="Ouvrir dossier destination",
                             command=lambda: self.open_media_dir(iid, tree_type, subtab))
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
        def _runner():
            # Attendre proprement la fin de restore (max 10s) si l’event existe
            evt = getattr(self, "_restore_done", None)
            if hasattr(evt, "wait"):
                try:
                    evt.wait(timeout=10)
                except Exception:
                    pass
            else:
                # Fallback ultra court si pas d'event
                time.sleep(0.2)

            self._download_all_not_downloaded_thread(tree_type)

        threading.Thread(target=_runner, daemon=True).start()


    def _download_all_not_downloaded_thread(self, tree_type):
        log_info(
            f"[Download All {tree_type}] [Window {self.window_id}] Début lancement, running_downloads={self.running_downloads}, queue_size={len(self.download_queue)}")
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
            self.enqueue_media(media, override=True)

            # Essayer de lancer immédiatement si possible
            if self.running_downloads < MAX_CONCURRENT_DOWNLOADS:
                self.start_next_in_queue()
            else:
                log_info(
                    f"[Download All {tree_type}] [Window {self.window_id}] Limite atteinte ({self.running_downloads}/{MAX_CONCURRENT_DOWNLOADS}), en attente")
                while self.running_downloads >= MAX_CONCURRENT_DOWNLOADS and self.is_active and not self.is_closing:
                    time.sleep(0.1)  # Attente plus courte pour réactivité

        log_info(
            f"[Download All {tree_type}] [Window {self.window_id}] Tous les médias éligibles en file (queue_size={len(self.download_queue)})")


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
                log_warning(
                    f"[ForceComplete] Fichier tmp absent, mais {media_name} existe déjà → marquage forcé en Completed")

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
        media["hash_check"] = "" if ok else "Mismatch"
        try:
            media["local_size"] = os.path.getsize(path)
        except Exception:
            pass

        if ok:
            # si c’était un .tmp → rename et finalise
            if path.endswith(".tmp"):
                try:
                    os.rename(path, final_path)
                    media["name"] = os.path.basename(final_path)
                except Exception as e:
                    log_error(f"[SHA256] Rename .tmp échoué : {e}")
            media["status"] = "Completed"
            media["percent"] = 100
            media["speed"] = "0 B/s"
        else:
            # ❗ ne pas marquer Failed ici : un check n'est pas un échec réseau
            if media.get("status") not in ("Downloading", "Retrying"):
                media["status"] = "Incomplete"
            # option: met à jour percent si la taille HTTP est connue
            try:
                total = int(media.get("size_http") or 0)
                if total > 0:
                    media["percent"] = int((media.get("local_size", 0) * 100) / total)
            except Exception:
                pass

        log_info(f"[SHA256] {media['name']} → {media.get('hash_check', '')}")
        self.refresh_media_row(media, move_to_completed=ok)
        self.save_json()


    def download_all(self):
        # tout: vidéos + images
        self.download_all_not_downloaded("video")
        self.download_all_not_downloaded("image")


    def download_all_videos(self):
        self.download_all_not_downloaded("video")


    def download_all_pictures(self):
        self.download_all_not_downloaded("image")


    def enqueue_media(self, media, override: bool = False):
        media_name = media.get("name")

        # 🔧 Ne bloque pas les actions utilisateur
        if getattr(self, "restoring", False) and not override:
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


    def monitor_threads_background(self):
        while self.is_active:
            time.sleep(60)
            active_threads = threading.enumerate()
            log_info(f"[THREAD-MONITOR] {len(active_threads)} threads actifs: {[t.name for t in active_threads]}")


    def enqueue_download(self, item_id, tree_type, subtab):
        tree = (self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree)
        media_name = tree.item(item_id, "values")[0]
        media = next((m for m in self.medias if m.get("name") == media_name), None)
        log_info(
            f"[Queue] [Window {self.window_id}] Ajout de {media_name} à la file (queue_size={len(self.download_queue)})")

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
        log_info(
            f"[Queue] [Window {self.window_id}] {media_name} → Ajouté à la file de téléchargement (queue_size={len(self.download_queue)})")


    def refresh_media_row(self, media, move_to_completed=False):
        # Bloquer si fermeture ou UI morte
        if self.is_closing or not self.is_active or not self.check_ui_alive():
            return

        media_name = media.get("name")
        if not media_name:
            return

        now = time.time()
        last_update = self.last_ui_update.get(media_name, 0)
        if (now - last_update < 0.2) and not move_to_completed and media.get("status") != "Completed":
            return
        self.last_ui_update[media_name] = now

        tree_type = "video" if is_video(media) else "image"
        status = media.get("status", "Missing")
        downloaded = media.get("local_size", 0)
        total = media.get("size_http", 0)
        percent_val = int((downloaded / total) * 100) if total else 0

        # Si complet, on force move_to_completed
        if percent_val >= 100 and status == "Completed":
            move_to_completed = True

        subtab = "completed" if move_to_completed else "not_downloaded"

        target_tree = (
            self.video_completed_tree if tree_type == "video" and subtab == "completed" else
            self.video_not_downloaded_tree if tree_type == "video" else
            self.image_completed_tree if subtab == "completed" else
            self.image_not_downloaded_tree
        )

        old_subtab = "not_downloaded" if subtab == "completed" else "completed"
        old_tree = (
            self.video_not_downloaded_tree if tree_type == "video" and old_subtab == "not_downloaded" else
            self.video_completed_tree if tree_type == "video" else
            self.image_not_downloaded_tree if old_subtab == "not_downloaded" else
            self.image_completed_tree
        )

        # Supprimer de l'ancien tree si nécessaire
        old_key = (media_name, tree_type, old_subtab)
        old_item_id = self.item_id_cache.get(old_key)
        if old_item_id and old_tree.exists(old_item_id):
            try:
                old_tree.delete(old_item_id)
            except Exception:
                pass
            self.item_id_cache.pop(old_key, None)

        # Valeurs à afficher
        if subtab == "not_downloaded":
            values = (
                media_name,
                format_bytes(downloaded),
                format_bytes(total),
                media.get("speed", "0 B/s"),
                render_progress_bar(percent_val),
                status,
                media.get("hash_check", ""),
                os.path.splitext(media_name)[1][1:].lower(),
                media.get("error", ""),
                media.get("url", ""),
                str(media.get("retry_count", 0))
            )
        else:  # completed
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

        # Insertion / mise à jour
        tree_key = (media_name, tree_type, subtab)
        item_id = self.item_id_cache.get(tree_key)

        def _do_update():
            if self.is_closing or not self.is_active or not self.check_ui_alive():
                return
            try:
                if not item_id or not target_tree.exists(item_id):
                    new_id = target_tree.insert("", "end", values=values, tags=(f"{status.lower()}.{tree_type}",))
                    self.item_id_cache[tree_key] = new_id
                else:
                    self.safe_update_tree(item_id, tree_type, subtab, values=values,
                                          tags=(f"{status.lower()}.{tree_type}",))
            except Exception as e:
                log_warning(f"[REFRESH] Erreur update {media_name} : {e}")

        self.root.after_idle(_do_update)


    def download_selected_file(self):
        tree = self.get_current_video_tree()
        item_id = self.get_selected_item_id(tree)
        if item_id:
            self.enqueue_download(item_id, "video", "not_downloaded")
        else:
            messagebox.showwarning("Aucun fichier", "Veuillez sélectionner un fichier dans la liste.")


    def download_media(self, item_id, tree_type, subtab):
        # 1) Garde-fous UI
        if self.is_closing:
            log_info(f"[DL] 🚫 Téléchargement annulé car {self.username} fermé")
            return
        if not all(hasattr(self, tree) and getattr(self, tree).winfo_exists() for tree in [
            "video_not_downloaded_tree", "video_completed_tree",
            "image_not_downloaded_tree", "image_completed_tree"
        ]):
            log_warning("[Download] Treeview indisponible")
            return

        # 2) Résolution du tree ciblé
        tree = (self.video_not_downloaded_tree if tree_type == "video" and subtab == "not_downloaded" else
                self.video_completed_tree if tree_type == "video" and subtab == "completed" else
                self.image_not_downloaded_tree if tree_type == "image" and subtab == "not_downloaded" else
                self.image_completed_tree)

        if not tree.exists(item_id):
            log_warning(f"[DL] Item {item_id} non trouvé dans {tree_type}/{subtab}")
            return

        # 3) Retrouver le média
        media_name = tree.item(item_id, "values")[0]
        media = next((m for m in self.medias if m.get("name") == media_name), None)
        if not media:
            log_error(f"[DL] Média non trouvé pour {media_name}")
            return
        if media.get("status") == "Completed":
            log_info(f"[DL] Ignoré : {media_name} déjà Completed")
            return
        if media.get("status") in ("Downloading", "Retrying"):
            log_info(f"[DL] Ignoré : {media_name} déjà en cours ({media.get('status')})")
            return

        # 4) Préparer les infos de reprise (affichage)
        subdir = self.video_dir if is_video(media) else self.image_dir
        tmp_path = os.path.join(subdir, media_name + ".tmp")
        if os.path.exists(tmp_path):
            current_size = os.path.getsize(tmp_path)
            media["local_size"] = current_size
            total_size = media.get("size_http", 0)
            media["percent"] = int((current_size / total_size) * 100) if total_size else 0
        else:
            media["local_size"] = 0
            media["percent"] = 0

        # 5) Mettre en "Waiting", pousser dans la file, puis lancer le processeur
        media["status"] = "Waiting"
        media["error"] = ""
        media["speed"] = ""
        media.setdefault("retry_count", 0)
        self.refresh_media_row(media)

        # éviter doublons dans la file
        if media not in self.download_queue:
            self.download_queue.append(media)
            log_info(
                f"[Queue] [Window {self.window_id}] {media_name} → Ajouté à la file (queue_size={len(self.download_queue)})")
        else:
            log_info(f"[Queue] [Window {self.window_id}] {media_name} déjà en file")

        # 6) Démarrer si des slots sont dispo (respecte MAX_CONCURRENT_DOWNLOADS + submit_unique)
        self.start_next_in_queue()


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

        try:
            media["local_size"] = os.path.getsize(final_path)
        except Exception:
            pass

        if ok:
            media["status"] = "Completed"
            media["percent"] = 100
            media["speed"] = "0 B/s"
        else:
            # ❗ pas de Failed ici non plus
            if media.get("status") not in ("Downloading", "Retrying"):
                media["status"] = "Incomplete"
            try:
                total = int(media.get("size_http") or 0)
                if total > 0:
                    media["percent"] = int((media.get("local_size", 0) * 100) / total)
            except Exception:
                pass

        self.refresh_media_row(media)
        self.save_json()

    def ignore_selected_file(self):
        tree, tree_type, subtab = self.get_current_tree_with_context()
        item_id = self.get_selected_item_id(tree)
        if not item_id:
            messagebox.showwarning("Aucun fichier", "Veuillez sélectionner un fichier dans la liste.")
            return

        media_name = tree.item(item_id, "values")[0]
        media = next((m for m in self.medias if m.get("name") == media_name), None)
        if not media:
            log_warning("[Ignore] Média non trouvé")
            return

        with self.suspend_sorting():
            # Marque comme ignoré
            media["status"] = "Ignored"
            media["error"] = ""
            media["percent"] = 0
            media["local_size"] = 0
            media["speed"] = ""
            media["hash_check"] = ""

        # Supprime éventuels fichiers locaux
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

        # Refresh minimal (il passera dans l'onglet Ignored)
        self.refresh_media_row(media, move_to_completed=False)
        self.save_json()

        # rafraîchir les 3 onglets du type courant
        self.refresh_tabs_for_type(tree_type)


    def unignore_selected_file(self):
        tree, tree_type, subtab = self.get_current_tree_with_context()
        item_id = self.get_selected_item_id(tree)
        if not item_id:
            messagebox.showwarning("Aucun fichier", "Veuillez sélectionner un fichier dans la liste.")
            return

        media_name = tree.item(item_id, "values")[0]
        media = next((m for m in self.medias if m.get("name") == media_name), None)
        if not media:
            log_warning("[Unignore] Média non trouvé")
            return

        # Si le fichier existe localement → Completed, sinon Missing
        subdir = self.video_dir if is_video(media) else self.image_dir
        final_path = os.path.join(subdir, media_name)
        if os.path.exists(final_path):
            media["status"] = "Completed"
            media["local_size"] = os.path.getsize(final_path)
            media["percent"] = 100
            media["error"] = ""
            media["speed"] = ""
            media["hash_check"] = ""
            move_to_completed = True
        else:
            media["status"] = "Missing"
            media["local_size"] = 0
            media["percent"] = 0
            media["error"] = ""
            media["speed"] = ""
            media["hash_check"] = ""
            move_to_completed = False

        self.refresh_media_row(media, move_to_completed=move_to_completed)
        self.save_json()

        # rafraîchir les 3 onglets du type courant
        self.refresh_tabs_for_type(tree_type)


    def ignore_all_missing(self, media_type: str):
        count = 0
        for media in self.medias:
            if media.get("type") != media_type: continue
            if media.get("status") == "Completed": continue

            name = media.get("name", "")
            if not name: continue

            subdir = self.video_dir if media_type == "video" else self.image_dir
            final_path = os.path.join(subdir, name)
            tmp_path = final_path + ".tmp"
            if os.path.exists(final_path) or os.path.exists(tmp_path):
                continue

            media["status"] = "Ignored"
            media["error"] = ""
            media["percent"] = 0
            media["local_size"] = 0
            media["speed"] = ""
            media["hash_check"] = ""
            self.remove_media_from_all_tabs(name, media_type)
            self.insert_single_media(media, media_type, "ignored")
            count += 1

        # ✅ Sauvegarde toujours si on a modifié des choses
        if count > 0:
            # sync de la structure + flush/replace garanti par save_json
            self.save_json()
            log_info(f"[IGNORE-ALL] {count} éléments '{media_type}' marqués Ignored (missing)")
        else:
            log_info(f"[IGNORE-ALL] Aucun élément éligible trouvé pour '{media_type}'")


    def refresh_tabs_for_type(self, media_type: str):
        try:
            self.insert_media_in_treeview(media_type, "not_downloaded")
        except Exception as e:
            log_warning(f"[REFRESH-TABS] {media_type}/not_downloaded: {e}")
        try:
            self.insert_media_in_treeview(media_type, "completed")
        except Exception as e:
            log_warning(f"[REFRESH-TABS] {media_type}/completed: {e}")
        try:
            self.insert_media_in_treeview(media_type, "ignored")
        except Exception as e:
            log_warning(f"[REFRESH-TABS] {media_type}/ignored: {e}")


    def restart_selected_file(self):
        tree = self.get_current_video_tree()
        item_id = self.get_selected_item_id(tree)
        if item_id:
            media_name = tree.item(item_id, "values")[0]
            media = next((m for m in self.medias if m.get("name") == media_name), None)
            if media:
                media["status"] = "Waiting"
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
                            log_info(f"[Restart] 🗑️ Fichier supprimé : {path}")
                        except Exception as e:
                            log_warning(f"[Restart] ⚠️ Erreur suppression {path} : {e}")

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


    def download_file_thread(self, media):
        """Téléchargement basé sur la queue, autonome (résout le chemin, gère .tmp, décrémente les compteurs)."""
        import time, os, platform
        from utils.network_utils import generate_alternative_urls
        import requests

        def should_stop():
            return self.is_closing or not self.is_active or not self.queue_processor_running

        # Résolution URL
        url = media.get("url") or ""
        if not url:
            for alt in generate_alternative_urls(media.get("name", "")):
                try:
                    r = requests.head(alt, timeout=8)
                    if r.status_code == 200:
                        url = alt
                        media["url"] = url
                        break
                except Exception:
                    pass
        if not url:
            media["status"] = "Failed"
            media["error"] = "Aucune URL valide"
            self.refresh_media_row(media)
            # décrément via finally si on a pris les sémaphores; ici on ne les a pas encore → décrémente direct
            self.decrement_running_downloads()
            return

        # Chemins
        subdir = self.video_dir if is_video(media) else self.image_dir
        os.makedirs(subdir, exist_ok=True)
        final_path = os.path.join(subdir, media["name"])
        tmp_path = final_path + ".tmp"

        # Marque en cours (premier affichage)
        media["status"] = "Downloading"
        media["error"] = ""
        if os.path.exists(tmp_path):
            media["local_size"] = os.path.getsize(tmp_path)
        else:
            media["local_size"] = media.get("local_size", 0) or 0
        self.refresh_media_row(media)

        # Progress callback
        last_ui = 0.0

        def on_progress(downloaded, speed_str, total):
            nonlocal last_ui
            if should_stop():
                return
            media["local_size"] = downloaded
            if total and not media.get("size_http"):
                media["size_http"] = total
            media["speed"] = speed_str or "0 B/s"
            if time.time() - last_ui > 0.2:
                last_ui = time.time()
                self.refresh_media_row(media)

        # helper local pour classifier les erreurs transitoires
        def is_transient_error(err_msg: str) -> bool:
            if not err_msg:
                return True
            em = err_msg.lower()
            transient_keys = [
                "timeout", "timed out", "connection", "reset", "temporar", "unreachable",
                "403", "429", "502", "503", "504", "chunked", "throttle", "broken pipe"
            ]
            return any(k in em for k in transient_keys)

        max_retries = int(media.get("max_retries", 15))
        delay = float(RETRY_DELAY_SECONDS)
        backoff = 1.0

        # Sémaphores globaux & fenêtre (⚠️ conserve ces objets tels qu’ils existent chez toi)
        wsem = window_sem(self.window_id, per_window_max=MAX_CONCURRENT_DOWNLOADS)
        with GLOBAL_SEM, wsem:
            try:
                attempt = 1
                while attempt <= max_retries:
                    if should_stop():
                        media["status"] = "Paused"
                        media["speed"] = "0 B/s"
                        self.refresh_media_row(media)
                        return

                    media["status"] = "Downloading" if attempt == 1 else "Retrying"
                    # Reset external retry counter when a new download starts
                    if attempt == 1 and media.get("retry_count", 0):
                        media["retry_count"] = 0
                        try:
                            self.refresh_media_row(media)
                        except Exception:
                            pass
                    if attempt > 1 and not media.get("error"):
                        media["error"] = "retry"
                    self.refresh_media_row(media)

                    try:
                        ok, err = DownloadManager.download_file(
                            url,
                            tmp_path,
                            resume=True,
                            on_progress=on_progress,
                            should_stop=should_stop,
                            window_id=self.window_id
                        )

                        if should_stop():
                            media["status"] = "Paused"
                            media["speed"] = "0 B/s"
                            self.refresh_media_row(media)
                            return

                        if ok:
                            try:
                                os.rename(tmp_path, final_path)
                            except Exception:
                                pass
                            try:
                                media["local_size"] = os.path.getsize(final_path)
                            except Exception:
                                pass
                            media["status"] = "Completed"
                            media["percent"] = 100
                            media["speed"] = "0 B/s"
                            self.refresh_media_row(media, move_to_completed=True)
                            self.save_json()
                            return

                        # ok == False → gestion spéciale range/416 si détecté
                        if err and any(k in err.lower() for k in ("range", "range not satisfiable", "416")):
                            try:
                                if os.path.exists(tmp_path):
                                    os.remove(tmp_path)  # repart propre
                            except Exception:
                                pass
                            if attempt < max_retries:
                                media["status"] = "Retrying"
                                media["error"] = err
                                self.refresh_media_row(media)
                                time.sleep(delay)
                                delay *= backoff
                                attempt += 1
                                continue

                        # sinon logique transitoire générique
                        if attempt < max_retries and is_transient_error(err or ""):
                            media["status"] = "Retrying"
                            media["error"] = (err or "retry")
                            self.refresh_media_row(media)
                            time.sleep(delay)
                            delay *= backoff
                            attempt += 1
                            continue
                        else:
                            media["status"] = "Failed"
                            media["error"] = err or "Téléchargement interrompu"
                            media["speed"] = "0 B/s"
                            self.refresh_media_row(media)
                            return

                    except Exception as e:
                        msg = str(e)
                        if attempt < max_retries and is_transient_error(msg):
                            media["status"] = "Retrying"
                            media["error"] = msg
                            self.refresh_media_row(media)
                            time.sleep(delay)
                            delay *= backoff
                            attempt += 1
                            continue
                        else:
                            media["status"] = "Failed"
                            media["error"] = msg
                            media["speed"] = "0 B/s"
                            self.refresh_media_row(media)
                            return

            finally:
                # ✅ décrémente TOUJOURS & relance la queue si possible
                self.decrement_running_downloads()
                if not self.is_closing and self.is_active:
                    self.start_next_in_queue()


    def start_next_in_queue(self):
        log_info(
            f"[QUEUE] [Window {self.window_id}] Lancement de start_next_in_queue running_downloads={self.running_downloads}, queue_size={len(self.download_queue)}")
        if self.is_closing or not self.is_active or not self.queue_processor_running:
            return

        with queue_lock:
            log_info(f"[QUEUE] [Window {self.window_id}] Appel start_next_in_queue()")
            log_info(f"[QUEUE] État : running_downloads={self.running_downloads}, queue={len(self.download_queue)}")

            # Tant qu'il y a de la place dans le *pool* et des jobs en file, on pousse des jobs.
            while self.download_queue and self.running_downloads < MAX_CONCURRENT_DOWNLOADS:
                media = self.download_queue.pop(0)
                if not media:
                    continue

                status = (media.get("status") or "").capitalize()
                name = media.get("name", "Unknown")
                if status in ("Completed", "Downloading", "Ignored"):
                    log_info(f"[QUEUE] [Window {self.window_id}] ⏩ {name} déjà {status} → skip")
                    continue

                # Envoi au pool : on incrémente *à l'exécution*, pas ici.
                def job(m=media):
                    if self.is_closing or not self.is_active:
                        return
                    with queue_lock:
                        self.running_downloads += 1
                    try:
                        self.download_file_thread(m)
                    finally:
                        # download_file_thread fait déjà un decrement, mais on garde une cohérence au cas où
                        pass

                try:
                    self.ctrl.enqueue(job)
                except Exception as e:
                    log_warning(f"[QUEUE] Enqueue pool a échoué ({name}) : {e}")
                    # On remet l'item en tête si échec d’enqueue
                    self.download_queue.insert(0, media)
                    break  # on sort, le pool est peut-être en arrêt

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
                if self.is_closing or not self.check_ui_alive():
                    return
                self.refresh_media_row(media)


    def decrement_running_downloads(self):
        with queue_lock:
            self.running_downloads = max(0, self.running_downloads - 1)
            log_info(f"[QUEUE] [Window {self.window_id}] ⬇️ running_downloads → {self.running_downloads}")


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
        # Single loop guard
        if getattr(self, "_retry_loop_started", False):
            return
        self._retry_loop_started = True

        # Stop event
        if not hasattr(self, "_retry_stop"):
            import threading
            self._retry_stop = threading.Event()

        import time

        # Per-media meta: { key: {"attempts": int, "next_ts": float} }
        if not hasattr(self, "_retry_meta"):
            self._retry_meta = {}

        def _media_key_for_retry(m):
            try:
                return self._media_key(m) or m.get("name") or id(m)
            except Exception:
                return m.get("name") or id(m)

        def _eligible_failed_medias():
            now = time.time()
            out = []
            for m in [x for x in self.medias if (x.get("status") == "Failed")]:
                key = _media_key_for_retry(m)
                meta = self._retry_meta.get(key, {})
                attempts = int(meta.get("attempts", 0))
                next_ts = float(meta.get("next_ts", 0))
                if attempts >= int(MAX_FAILED_RETRIES):
                    continue
                if now >= next_ts:
                    out.append((key, m, meta))
            return out

        def _schedule_next_retry(key, meta):
            attempts = int(meta.get("attempts", 0)) + 1
            meta["attempts"] = attempts
            meta["next_ts"] = time.time() + float(RETRY_DELAY_SECONDS)
            self._retry_meta[key] = meta

        def _reset_retry_meta_on_success(m):
            key = _media_key_for_retry(m)
            if key in self._retry_meta:
                self._retry_meta.pop(key, None)

        def _retry_pass():
            if self.is_closing or not self.is_active or not self.queue_processor_running:
                return

            candidates = _eligible_failed_medias()
            if not candidates:
                return

            pushed = 0
            # Push a small batch per pass to avoid spikes
            for key, media, meta in candidates[:5]:
                # If status changed meanwhile: reset meta
                st = media.get("status")
                if st in ("Completed", "Downloading", "Waiting", "Retrying", "Paused", "Ignored"):
                    _reset_retry_meta_on_success(media)
                    continue

                # Prepare for requeue
                media["status"] = "Waiting"
                media["error"] = ""
                media["speed"] = "0 B/s"
                media["retry_count"] = int(media.get("retry_count", 0)) + 1
                try:
                    self.refresh_media_row(media)
                except Exception:
                    pass

                # Enqueue
                try:
                    self.enqueue_media(media, override=True)
                except Exception:
                    continue

                _schedule_next_retry(key, meta)
                pushed += 1

            if pushed:
                try:
                    self.start_next_in_queue()
                except Exception:
                    pass

        def loop():
            log_info("[RETRY] ♻️ Démarrage du retry_failed_downloads_loop (10s constant, 10 max)")
            while not self._retry_stop.is_set():
                try:
                    _retry_pass()
                except Exception as e:
                    log_warning(f"[RETRY] Erreur dans la passe de retry: {e}")
                # Sleep small ticks to react to stop event
                woke = self._retry_stop.wait(float(RETRY_DELAY_SECONDS))
                if woke:
                    break
            log_info("[RETRY] ⏹️ Arrêt du retry_failed_downloads_loop")

        threading.Thread(target=loop, daemon=True, name="retry_failed_loop").start()


    def retry_failed_downloads(self):
        if self.is_closing or not self.is_active or not self.queue_processor_running:
            return
        for media in [m for m in self.medias if m.get("status") == "Failed"]:
            media["status"] = "Waiting"
            self.refresh_media_row(media)
            self.enqueue_media(media, override=True)


    def start_queue_processor(self):
        if getattr(self, "_queue_thread_started", False):
            return
        self._queue_thread_started = True

        def process_queue():
            while self.queue_processor_running and self.is_active and not self.is_closing:
                if self.download_queue and self.running_downloads < MAX_CONCURRENT_DOWNLOADS:
                    self.start_next_in_queue()
                time.sleep(0.1)

        threading.Thread(target=process_queue, daemon=True, name=f"process_queue:{self.window_id[:4]}").start()


    def check_ui_alive(self, target=None):
        """
        Vérifie si l'UI est prête et si les widgets demandés existent encore.
        target peut être:
          - None : vérifie juste la fenêtre
          - un widget Tk
          - un nom d'attribut (str)
          - une liste/tuple de widgets et/ou noms d'attributs
        """
        # UI prête ?
        if not getattr(self, "ui_ready", None) or not self.ui_ready.is_set():
            log_warning(f"[TREEVIEW] [Window {self.window_id}] UI non prête, opération annulée")
            return False

        # Fenêtre vivante ?
        if self.is_closing or not self.is_active:
            log_warning(f"[TREEVIEW] [Window {self.window_id}] Fenêtre fermée/inactive, opération annulée")
            return False

        try:
            if not (self.root and self.root.winfo_exists()):
                log_warning(f"[TREEVIEW] [Window {self.window_id}] Root inexistant, opération annulée")
                return False
        except tk.TclError:
            log_warning(f"[TREEVIEW] [Window {self.window_id}] Erreur accès root, opération annulée")
            return False

        # Normalise target en liste
        if target is None:
            return True
        if not isinstance(target, (list, tuple)):
            target = [target]

        # Résout les noms -> widgets et vérifie winfo_exists
        for t in target:
            widget = t
            if isinstance(t, str):
                widget = getattr(self, t, None)

            if widget is None:
                log_warning(f"[TREEVIEW] [Window {self.window_id}] Widget '{t}' introuvable")
                return False

            try:
                if not widget.winfo_exists():
                    log_warning(f"[TREEVIEW] [Window {self.window_id}] Widget '{t}' détruit")
                    return False
            except tk.TclError:
                log_warning(f"[TREEVIEW] [Window {self.window_id}] Widget '{t}' inaccessible")
                return False

        return True


    def save_json(self):
        with self.save_lock:
            try:
                # 🔒 s'assurer que medias_data pointe bien sur la liste courante
                try:
                    if self.medias_data.get("medias") is not self.medias:
                        self.medias_data["medias"] = self.medias
                except Exception:
                    self.medias_data["medias"] = self.medias

                # 💾 écriture avec flush + fsync pour garantir la persistance immédiate
                tmp_path = self.json_path + ".tmp"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(self.medias_data, f, indent=4)
                    f.flush()
                    os.fsync(f.fileno())

                # swap atomique
                try:
                    os.replace(tmp_path, self.json_path)
                except Exception:
                    # fallback mac/win
                    if os.path.exists(self.json_path):
                        os.remove(self.json_path)
                    os.rename(tmp_path, self.json_path)

                log_info(f"[SAVE] [Window {self.window_id}] JSON sauvegardé à {self.json_path}")

                # Notifie (best effort)
                self._notify_profile_update()
                try:
                    event_bus.emit("profile:update", {
                        "service": self.service,
                        "username": self.username,
                        "profile_key": self.profile_key
                    })
                except Exception as e:
                    log_warning(f"[SAVE] emit profile:update failed: {e}")
            except Exception as e:
                log_error(f"[SAVE] [Window {self.window_id}] Erreur sauvegarde JSON : {e}")


    def on_event_update(self, event_data):
        if self._suppress_events or self._booting or self.is_closing:
            return
        if event_data.get("profile_key") == self.profile_key:
            self.refresh_profile()

        # ---- Batching Treeview inserts ----


    def _prepare_row_values(self, media, tree):
        """Construit les 'values' pour un media donné et le tree ciblé."""
        name = media.get("name", "???")
        downloaded = media.get("local_size", 0)
        total = media.get("size_http", 0)
        status = media.get("status", "Missing")
        hash_check = media.get("hash_check", "")
        ext = os.path.splitext(name)[1][1:].lower() or "unknown"
        error = media.get("error", "")
        url = media.get("url", "")
        speed = media.get("speed", "0 B/s")
        percent_val = int((downloaded / total) * 100) if total else 0
        percent_str = render_progress_bar(percent_val)

        if tree in [self.video_not_downloaded_tree, self.image_not_downloaded_tree]:
            return (
                name, format_bytes(downloaded), format_bytes(total),
                speed, percent_str, status, hash_check, ext, error, url,
                str(media.get("retry_count", 0))
            )
        else:
            return (
                name, format_bytes(downloaded), format_bytes(total),
                percent_str, status, hash_check, ext, error, url,
                str(media.get("retry_count", 0))
            )


    def _bulk_insert_start(self, tree_key, rows, chunk_size=200, delay_ms=1):
        """
        rows: liste de tuples (values, tags, cache_key)
        On insère par paquets en utilisant after() pour ne pas bloquer l’UI.
        """
        if not hasattr(self, "_bulk_state"):
            self._bulk_state = {}
        self._bulk_state[tree_key] = {
            "rows": rows,
            "index": 0,
            "chunk": chunk_size,
            "delay": delay_ms,
        }
        self._bulk_insert_step(tree_key)


    def _bulk_insert_step(self, tree_key):
        st = self._bulk_state.get(tree_key)
        if not st or self.is_closing or not self.check_ui_alive():
            return
        rows, idx, chunk = st["rows"], st["index"], st["chunk"]

        # resolve tree
        try:
            tree = getattr(self, tree_key + "_tree")
        except Exception:
            return
        if not tree.winfo_exists():
            return

        end = min(idx + chunk, len(rows))
        for i in range(idx, end):
            values, tags, cache_key = rows[i]
            try:
                item_id = tree.insert("", tk.END, values=values, tags=tags)
                if cache_key:
                    self.item_id_cache[cache_key] = item_id
            except Exception as e:
                log_warning(f"[BULK] insert fail on {tree_key} : {e}")

        st["index"] = end

        if end >= len(rows):
            # terminé
            self._bulk_state.pop(tree_key, None)
            return
        else:
            # planifie le prochain paquet
            self.schedule_after(st["delay"], lambda k=tree_key: self._bulk_insert_step(k))


    def schedule_after(self, delay_ms, fn):
        if self.is_closing or not self.root or not self.root.winfo_exists():
            return None

        def wrapper():
            # Dès qu'on entre dans le callback, on le retire du registre
            with self._after_lock:
                self._after_ids.discard(aid)
            if self.is_closing:
                return
            try:
                fn()
            except Exception as e:
                log_warning(f"[AFTER] Callback error: {e}")

        try:
            aid = self.root.after(delay_ms, wrapper)
            with self._after_lock:
                self._after_ids.add(aid)
            return aid
        except Exception as e:
            log_warning(f"[AFTER] schedule_after failed: {e}")
            return None


    def schedule_after_idle(self, fn):
        if self.is_closing or not self.root or not self.root.winfo_exists():
            return None

        def wrapper():
            with self._after_lock:
                self._after_ids.discard(aid)
            if self.is_closing:
                return
            try:
                fn()
            except Exception as e:
                log_warning(f"[AFTER-IDLE] Callback error: {e}")

        try:
            aid = self.root.after_idle(wrapper)
            with self._after_lock:
                self._after_ids.add(aid)
            return aid
        except Exception as e:
            log_warning(f"[AFTER-IDLE] schedule_after_idle failed: {e}")
            return None


    def _cancel_all_afters(self):
        with self._after_lock:
            ids = list(self._after_ids)
            self._after_ids.clear()
        for aid in ids:
            try:
                self.root.after_cancel(aid)
            except Exception:
                pass


    def monitor_queue(self):
        # Empêche les multiples watchdogs
        if getattr(self, "_watchdog_started", False):
            return
        self._watchdog_started = True

        # Timestamps pour mesurer l'activité (MAJ dans enqueue/decrement si possible)
        self._last_queue_activity = time.time()
        self._last_watchdog_warn = 0.0

        def _mark_activity():
            self._last_queue_activity = time.time()

        # Hook simples (si pas déjà faits ailleurs)
        if not hasattr(self, "_orig_enqueue_media_for_watchdog"):
            self._orig_enqueue_media_for_watchdog = getattr(self, "enqueue_media", None)

            def _enqueue_media_wrapped(media, override=False):
                try:
                    _mark_activity()
                finally:
                    return self._orig_enqueue_media_for_watchdog(media, override=override)

            if self._orig_enqueue_media_for_watchdog:
                self.enqueue_media = _enqueue_media_wrapped

        if not hasattr(self, "_orig_decrement_running_for_watchdog"):
            self._orig_decrement_running_for_watchdog = getattr(self, "decrement_running_downloads", None)

            def _decrement_running_wrapped():
                try:
                    _mark_activity()
                finally:
                    return self._orig_decrement_running_for_watchdog()

            if self._orig_decrement_running_for_watchdog:
                self.decrement_running_downloads = _decrement_running_wrapped

        def _monitor():
            import random
            log_info(f"[MONITOR] 🧭 Démarrage du watchdog pour {self.username}")

            # Jitter initial (évite tempête si plusieurs fenêtres bootent ensemble)
            self._monitor_stop.wait(random.uniform(0.5, 1.5))

            STALL_SECONDS = 30
            SLEEP_SECONDS = 5  # cadence de check plus réactive mais non agressive

            while (not self._monitor_stop.is_set()
                   and not self.is_closing
                   and self.is_active
                   and self.queue_processor_running):

                # Attente interruptible
                if self._monitor_stop.wait(SLEEP_SECONDS):
                    break
                if self.is_closing or not self.is_active or not self.queue_processor_running:
                    break

                try:
                    running = int(self.running_downloads)
                    qsize = len(self.download_queue)
                except Exception:
                    # valeurs incohérentes : on repart au prochain tour
                    continue

                # Si ça tourne, on note l'activité et on continue
                if running > 0:
                    self._last_queue_activity = time.time()
                    continue

                # Rien ne tourne : seulement s'il reste des éléments en file
                if qsize > 0:
                    elapsed = time.time() - self._last_queue_activity
                    if elapsed >= STALL_SECONDS:
                        # Anti-spam logs: max un warning toutes 30s
                        if time.time() - self._last_watchdog_warn >= STALL_SECONDS:
                            log_warning(f"[MONITOR] ⏰ {self.username} semble bloqué "
                                        f"(queue={qsize}, running=0, inactif {int(elapsed)}s) → relance")
                            self._last_watchdog_warn = time.time()

                        # Petite relance contrôlée
                        try:
                            self.start_next_in_queue()
                            # petite sieste aléatoire pour éviter collisions inter-fenêtres
                            self._monitor_stop.wait(random.uniform(0.2, 0.6))
                        except Exception as e:
                            log_warning(f"[MONITOR] Relance start_next_in_queue() a échoué: {e}")
                else:
                    # queue vide + rien ne tourne → RAS
                    self._last_queue_activity = time.time()

            log_info(f"[MONITOR] 🛑 Watchdog terminé pour {self.username}")

        threading.Thread(target=_monitor, daemon=True, name=f"watchdog:{self.window_id[:4]}").start()


if __name__ == "__main__":
    root = tk.Tk()
    root.title("Coomer Ultimate v1.0")
    app = MediaWindow(root, "service", "username", "local_dir", "path/to/json", {"medias": []})
    root.mainloop()