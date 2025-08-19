# ui/media_window.py

import tkinter as tk
from tkinter import ttk

class MediaWindowUI:
    """
    Construit toute l'UI (thème, frames, notebooks, treeviews, colonnes, scrollbars, bindings)
    et attache les widgets sur le contrôleur passé (MediaWindow existant).
    Le contrôleur doit exposer:
      - on_video_notebook_tab_changed(event)
      - on_image_notebook_tab_changed(event)
      - on_right_click(event, tree_type, subtab)
      - sort_column(col, tree)
      - ignore_selected_file()
      - unignore_selected_file()
      - ignore_all_missing(media_type: str)
    Et il doit fournir (avant l'appel) :
      - self.labels : dict multilingue (self.labels["columns"]...)
      - self.columns : dict {"not_downloaded": [...], "completed": [...]} (+ optionnellement "ignored")
      - col_widths : dict {col_name: width}
    """
    def __init__(self, controller, root, service, username, labels, columns, col_widths):
        self.c = controller
        self.root = root
        self.service = service
        self.username = username
        self.labels = labels or {"columns": {}}
        self.columns = columns
        self.col_widths = col_widths

        # ---- Fenêtre / Style ----
        self.root.title(f"Coomer Ultimate v1.0 – {username} ({service})")
        self.root.geometry("2000x1000")

        style = ttk.Style()
        # thème "clam" afin de permettre les customisations (ttk)
        try:
            style.theme_use("clam")
        except Exception:
            pass

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

        # ---- Main frame ----
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # ---- Notebook principal (Vidéos / Photos) ----
        self.c.notebook = ttk.Notebook(main_frame)
        self.c.notebook.pack(fill=tk.BOTH, expand=True)

        # ========== ONGLET VIDÉOS ==========
        self.c.video_frame = ttk.Frame(self.c.notebook)
        self.c.notebook.add(self.c.video_frame, text="Vidéos")

        self.c.video_notebook = ttk.Notebook(self.c.video_frame)
        self.c.video_notebook.pack(fill=tk.BOTH, expand=True)

        # --- Vidéos: Not Downloaded ---
        self.c.video_not_downloaded_frame = ttk.Frame(self.c.video_notebook)
        self.c.video_notebook.add(self.c.video_not_downloaded_frame, text="Not Downloaded")

        video_nd_button_frame = ttk.Frame(self.c.video_not_downloaded_frame)
        video_nd_button_frame.pack(fill=tk.X, pady=5)

        ttk.Button(video_nd_button_frame, text="DOWNLOAD ALL",
                   command=lambda: self.c.download_all_not_downloaded("video")).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="DOWNLOAD",
                   command=self.c.download_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="PAUSE",
                   command=lambda: self.c.pause_downloads("video")).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="IGNORE",
                   command=self.c.ignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="UNIGNORE",
                   command=self.c.unignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="IGNORE ALL MISSING",
                   command=lambda: self.c.ignore_all_missing("video")).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="OPEN FOLDER",
                   command=self.c.open_video_folder).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="CHECKSUM",
                   command=self.c.check_sha256_all_video_not_downloaded).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_nd_button_frame, text="GET SIZES",
                   command=lambda: self.c.get_all_sizes_thread(media_type="video")).pack(side=tk.LEFT, padx=5)

        self.c.video_not_downloaded_tree = self._make_tree(self.c.video_not_downloaded_frame, subtab="not_downloaded")
        self._pack_tree(self.c.video_not_downloaded_frame, self.c.video_not_downloaded_tree)

        # --- Vidéos: Completed ---
        self.c.video_completed_frame = ttk.Frame(self.c.video_notebook)
        self.c.video_notebook.add(self.c.video_completed_frame, text="Completed")

        video_c_button_frame = ttk.Frame(self.c.video_completed_frame)
        video_c_button_frame.pack(fill=tk.X, pady=5)

        ttk.Button(video_c_button_frame, text="CHECK FILES",
                   command=lambda: self.c.check_all_completed_files("video")).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_c_button_frame, text="IGNORE",
                   command=self.c.ignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_c_button_frame, text="UNIGNORE",
                   command=self.c.unignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_c_button_frame, text="OPEN FOLDER",
                   command=self.c.open_video_folder).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_c_button_frame, text="CHECKSUM",
                   command=self.c.check_sha256_all_video_not_downloaded).pack(side=tk.LEFT, padx=5)

        self.c.video_completed_tree = self._make_tree(self.c.video_completed_frame, subtab="completed")
        self._pack_tree(self.c.video_completed_frame, self.c.video_completed_tree)

        # --- Vidéos: Ignored ---
        self.c.video_ignored_frame = ttk.Frame(self.c.video_notebook)
        self.c.video_notebook.add(self.c.video_ignored_frame, text="Ignored")

        video_i_button_frame = ttk.Frame(self.c.video_ignored_frame)
        video_i_button_frame.pack(fill=tk.X, pady=5)

        ttk.Button(video_i_button_frame, text="UNIGNORE",
                   command=self.c.unignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(video_i_button_frame, text="OPEN FOLDER",
                   command=self.c.open_video_folder).pack(side=tk.LEFT, padx=5)

        self.c.video_ignored_tree = self._make_tree(self.c.video_ignored_frame, subtab="ignored")
        self._pack_tree(self.c.video_ignored_frame, self.c.video_ignored_tree)

        # Bind tab change (vidéos)
        self.c.video_notebook.bind("<<NotebookTabChanged>>", self.c.on_video_notebook_tab_changed)

        # ========== ONGLET PHOTOS ==========
        self.c.image_frame = ttk.Frame(self.c.notebook)
        self.c.notebook.add(self.c.image_frame, text="Photos")

        self.c.image_notebook = ttk.Notebook(self.c.image_frame)
        self.c.image_notebook.pack(fill=tk.BOTH, expand=True)

        # --- Photos: Not Downloaded ---
        self.c.image_not_downloaded_frame = ttk.Frame(self.c.image_notebook)
        self.c.image_notebook.add(self.c.image_not_downloaded_frame, text="Not Downloaded")

        image_nd_button_frame = ttk.Frame(self.c.image_not_downloaded_frame)
        image_nd_button_frame.pack(fill=tk.X, pady=5)

        ttk.Button(image_nd_button_frame, text="DOWNLOAD ALL",
                   command=lambda: self.c.download_all_not_downloaded("image")).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_nd_button_frame, text="PAUSE",
                   command=lambda: self.c.pause_downloads("image")).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_nd_button_frame, text="IGNORE",
                   command=self.c.ignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_nd_button_frame, text="UNIGNORE",
                   command=self.c.unignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_nd_button_frame, text="IGNORE ALL MISSING",
                   command=lambda: self.c.ignore_all_missing("image")).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_nd_button_frame, text="OPEN FOLDER",
                   command=self.c.open_image_folder).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_nd_button_frame, text="CHECKSUM",
                   command=self.c.check_sha256_all_image_not_downloaded).pack(side=tk.LEFT, padx=5)

        self.c.image_not_downloaded_tree = self._make_tree(self.c.image_not_downloaded_frame, subtab="not_downloaded")
        self._pack_tree(self.c.image_not_downloaded_frame, self.c.image_not_downloaded_tree)

        # --- Photos: Completed ---
        self.c.image_completed_frame = ttk.Frame(self.c.image_notebook)
        self.c.image_notebook.add(self.c.image_completed_frame, text="Completed")

        image_c_button_frame = ttk.Frame(self.c.image_completed_frame)
        image_c_button_frame.pack(fill=tk.X, pady=5)

        ttk.Button(image_c_button_frame, text="IGNORE",
                   command=self.c.ignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_c_button_frame, text="UNIGNORE",
                   command=self.c.unignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_c_button_frame, text="OPEN FOLDER",
                   command=self.c.open_image_folder).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_c_button_frame, text="CHECKSUM",
                   command=lambda: self.c.check_all_completed_files("image")).pack(side=tk.LEFT, padx=5)

        self.c.image_completed_tree = self._make_tree(self.c.image_completed_frame, subtab="completed")
        self._pack_tree(self.c.image_completed_frame, self.c.image_completed_tree)

        # --- Photos: Ignored ---
        self.c.image_ignored_frame = ttk.Frame(self.c.image_notebook)
        self.c.image_notebook.add(self.c.image_ignored_frame, text="Ignored")

        image_i_button_frame = ttk.Frame(self.c.image_ignored_frame)
        image_i_button_frame.pack(fill=tk.X, pady=5)

        ttk.Button(image_i_button_frame, text="UNIGNORE",
                   command=self.c.unignore_selected_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(image_i_button_frame, text="OPEN FOLDER",
                   command=self.c.open_image_folder).pack(side=tk.LEFT, padx=5)

        self.c.image_ignored_tree = self._make_tree(self.c.image_ignored_frame, subtab="ignored")
        self._pack_tree(self.c.image_ignored_frame, self.c.image_ignored_tree)

        # Bind tab change (images)
        self.c.image_notebook.bind("<<NotebookTabChanged>>", self.c.on_image_notebook_tab_changed)

        # ---- Filter frame (placeholder) + stats ----
        filter_frame = ttk.Frame(main_frame)
        filter_frame.pack(fill=tk.X, pady=5)

        self.c.media_stats_label = ttk.Label(main_frame, text="")
        self.c.media_stats_label.pack(fill=tk.X, pady=5)

        # ---- Right-click bindings (les menus restent gérés par le contrôleur) ----
        for tree, tree_type, subtab in [
            (self.c.video_not_downloaded_tree, "video", "not_downloaded"),
            (self.c.video_completed_tree, "video", "completed"),
            (self.c.video_ignored_tree, "video", "ignored"),
            (self.c.image_not_downloaded_tree, "image", "not_downloaded"),
            (self.c.image_completed_tree, "image", "completed"),
            (self.c.image_ignored_tree, "image", "ignored"),
        ]:
            tree.bind("<Button-3>",
                      lambda e, tt=tree_type, st=subtab: self.c.on_right_click(e, tt, st))

        # Marquer comme préchargé l’onglet vidéo ND
        if not hasattr(self.c, "loaded_treeviews"):
            self.c.loaded_treeviews = {}
        self.c.loaded_treeviews["video_not_downloaded"] = True

    # -----------------------
    # Helpers internes UI
    # -----------------------
    def _make_tree(self, parent, subtab: str):
        """
        Crée un Treeview selon la config columns/labels/col_widths.
        subtab ∈ {"not_downloaded", "completed", "ignored"}
        """
        tree = ttk.Treeview(parent, show="headings", style="Treeview")

        # Colonnes : si pas de config spécifique pour "ignored", on réutilise celles de "not_downloaded"
        cols = self.columns.get(subtab) or self.columns.get("not_downloaded") or []
        tree["columns"] = cols

        for col in cols:
            label = self.labels.get("columns", {}).get(col, col.title())
            width = self.col_widths.get(col, 100)
            # tri délégué au contrôleur
            tree.heading(col, text=label, command=lambda c=col, t=tree: self.c.sort_column(c, t))
            tree.column(col, width=width, stretch=True)

        return tree

    def _pack_tree(self, parent, tree):
        vsb = ttk.Scrollbar(parent, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(parent, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        tree.pack(fill=tk.BOTH, expand=True)
