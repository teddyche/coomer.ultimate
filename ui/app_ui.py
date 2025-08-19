# ui/app_ui.py
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

class AppUI:
    """
    Rôle: construire toute l'UI (thème, toolbar, treeview, menus)
    et déléguer les actions au contrôleur fourni.
    """
    def __init__(self, root: tk.Tk, controller):
        self.root = root
        self.c = controller  # référence vers App (contrôleur)
        self.tree = None
        self.stats_label = None
        self._context_menu = None

        self._setup_theme()
        self._build_ui()

    # ---------- THEME ----------
    def _setup_theme(self):
        style = ttk.Style(self.root)
        self.root.configure(bg="#2e2e2e")
        style.theme_use("clam")

        style.configure("TEntry", fieldbackground="#3a3a3a", foreground="#ffffff",
                        insertcolor="#ffffff", padding=5)
        style.configure("TButton", padding=6, relief="flat", background="#444444",
                        foreground="#ffffff", font=("Segoe UI", 10))
        style.map("TButton", background=[("active", "#5a5a5a")],
                  foreground=[("active", "#ffffff")])
        style.configure("TLabel", background="#2e2e2e", foreground="#dddddd",
                        font=("Segoe UI", 10))
        style.configure("TCheckbutton", background="#2e2e2e", foreground="#dddddd")
        style.configure("TRadiobutton", background="#2e2e2e", foreground="#dddddd")
        style.configure("TCombobox", fieldbackground="#3a3a3a", background="#3a3a3a",
                        foreground="#ffffff", padding=5)
        style.map("TCombobox", background=[("active", "#5a5a5a")])
        style.configure("Horizontal.TProgressbar", troughcolor="#444444",
                        background="#888888", thickness=20)
        style.configure("Treeview", background="#2e2e2e", foreground="#ffffff",
                        fieldbackground="#2e2e2e", rowheight=25, font=("Segoe UI", 10))
        style.map("Treeview", background=[("selected", "#444444")],
                  foreground=[("selected", "#ffffff")])
        style.configure("Treeview.Heading", background="#3a3a3a", foreground="#ffffff",
                        font=("Segoe UI", 10, "bold"))

    # ---------- BUILD ----------
    def _build_ui(self):
        # Toolbar
        toolbar = tk.Frame(self.root, bg="#2e2e2e")
        toolbar.pack(fill=tk.X, padx=10, pady=5)

        table_frame = ttk.Frame(self.root)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Button(toolbar, text="🔄 Rafraîchir", command=self.c.load_profiles).pack(side=tk.LEFT, padx=5)
        ttk.Button(toolbar, text="⚙️ Settings", command=self.c.change_download_dir).pack(side=tk.LEFT, padx=5)

        ttk.Label(toolbar, text="➕ Ajouter profil (URL)").pack(side=tk.LEFT, padx=5)
        self.add_entry = ttk.Entry(toolbar, width=40)
        self.add_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(toolbar, text="Ajouter", command=self.c.add_profile_threaded).pack(side=tk.LEFT, padx=5)

        # Stats
        self.stats_label = ttk.Label(self.root, text="Stats globales: 0 profils, 0 médias", anchor="w")
        self.stats_label.pack(fill=tk.X, padx=10, pady=5)

        # Action Frame
        action = ttk.Frame(self.root)
        action.pack(fill=tk.X, padx=10, pady=(0, 5))

        self.btn_update = ttk.Button(action, text="🔁 UPDATE", command=self.c.handle_update_selected)
        self.btn_open   = ttk.Button(action, text="📂 OPEN DIR", command=self.c.handle_open_dir_selected)
        self.btn_dl     = ttk.Button(action, text="📥 DOWNLOAD", command=self.c.handle_download_selected)
        self.btn_chdir  = ttk.Button(action, text="✂️ CHANGE DIR", command=self.c.handle_change_dir_selected)
        self.btn_add_existing = ttk.Button(action, text="ADD EXISTINGS", command=self.c.handle_add_already_downloaded)

        for b in (self.btn_update, self.btn_open, self.btn_dl, self.btn_chdir, self.btn_add_existing):
            b.pack(side=tk.LEFT, padx=5, pady=3)

        # Treeview
        self.tree = ttk.Treeview(
            table_frame,
            columns=("service", "profile", "status", "videos_dl_total", "photos_dl_total",
                     "video_size", "photo_size", "completed", "last_update", "download_path"),
            show="headings",
            selectmode="browse"
        )

        # Scrollbars
        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        # Headings
        self.tree.heading("service", text="Service", command=lambda: self.c.treeview_sort_column("service", False))
        self.tree.heading("profile", text="Profil", command=lambda: self.c.treeview_sort_column("profile", False))
        self.tree.heading("status", text="Statut", command=lambda: self.c.treeview_sort_column("status", False))
        self.tree.heading("videos_dl_total", text="Vidéos (dl/total)", command=lambda: self.c.treeview_sort_column("videos_dl_total", False))
        self.tree.heading("photos_dl_total", text="Photos (dl/total)", command=lambda: self.c.treeview_sort_column("photos_dl_total", False))
        self.tree.heading("video_size", text="Taille Vidéo (Mo)", command=lambda: self.c.treeview_sort_column("video_size", False))
        self.tree.heading("photo_size", text="Taille Photo (Mo)", command=lambda: self.c.treeview_sort_column("photo_size", False))
        self.tree.heading("completed", text="% Complété", command=lambda: self.c.treeview_sort_column("completed", False))
        self.tree.heading("last_update", text="Dernière maj", command=lambda: self.c.treeview_sort_column("last_update", False))
        self.tree.heading("download_path", text="Chemin", command=lambda: self.c.treeview_sort_column("download_path", False))

        # Colonnes
        self.tree.column("service", width=80, stretch=False)
        self.tree.column("profile", width=180, stretch=False)
        self.tree.column("status", width=100, stretch=False)
        self.tree.column("videos_dl_total", width=120, stretch=False)
        self.tree.column("photos_dl_total", width=120, stretch=False)
        self.tree.column("video_size", width=120, stretch=False)
        self.tree.column("photo_size", width=120, stretch=False)
        self.tree.column("completed", width=100, stretch=False)
        self.tree.column("last_update", width=160, stretch=False)
        self.tree.column("download_path", width=600, stretch=True)  # large + stretch pour activer hsb

        # Grid propre (remplace .pack pour le bloc table)
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        # Bindings
        self.tree.bind("<Double-Button-1>", self.c.on_profile_double_click)
        self.tree.bind("<Button-3>", self.c.on_right_click)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        self._bind_tree_scrollwheel(self.tree)  # ← molette cross‑platform
        self._on_tree_select(None)

        # Tag "clean"
        self.tree.tag_configure("clean", background="#2e2e2e", foreground="#ffffff")

    # ---------- Helpers exposés au contrôleur ----------
    def read_add_url(self) -> str:
        return self.add_entry.get().strip()

    def set_stats(self, text: str):
        self.stats_label.config(text=text)

    def enable_profile_buttons(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for b in (self.btn_update, self.btn_open, self.btn_dl, self.btn_chdir):
            b.config(state=state)

    def popup_menu(self, x_root: int, y_root: int, items: list[tuple[str, callable]]):
        if self._context_menu:
            self._context_menu.destroy()
        self._context_menu = tk.Menu(self.root, tearoff=0, bg="#2f2f2f", fg="#ffffff")
        for label, cmd in items:
            self._context_menu.add_command(label=f" {label}", command=cmd)
        self._context_menu.post(x_root, y_root)

    # ---------- internes ----------
    def _on_tree_select(self, _event):
        selected = self.tree.selection()
        self.enable_profile_buttons(bool(selected))

    def _bind_tree_scrollwheel(self, widget: ttk.Treeview):
        # Windows & macOS
        widget.bind("<MouseWheel>", lambda e: widget.yview_scroll(-1 * (e.delta // 120), "units"))
        # Linux (X11)
        widget.bind("<Button-4>",   lambda e: widget.yview_scroll(-1, "units"))
        widget.bind("<Button-5>",   lambda e: widget.yview_scroll(+1, "units"))