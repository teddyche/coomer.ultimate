# coomer.ultimate
project_root/
├── main.py               # Point d'entrée principal (initialisation et lancement de l'app)
├── app.py                # Classe principale App (logique générale de l'application)
├── ui.py                 # Configuration de l'interface graphique (setup_ui, setup_theme, etc.)
├── profile_manager.py    # Gestion des profils (ajout, suppression, mise à jour, etc.)
├── media_manager.py      # Gestion des médias (téléchargement, enrichissement, etc.)
├── file_utils.py         # Fonctions utilitaires pour la gestion des fichiers (move, calculate_folder_size, etc.)
├── api_utils.py          # Appels API et traitement des données (fetch_medias_from_api, etc.)
├── settings.py           # Gestion des paramètres (load_settings, save_settings, etc.)
├── log.py                # Gestion des logs (log_info, log_error, etc.)
├── downloader.py         # Download des médias (téléchargement, vérification de l'existence, etc.)
├── event_bus.py          # 
├── media_window.py       # Fenêtre des médias
├── utils.py              # Fonctions utilitaires générales (format_bytes, sha256_file, etc.)
├── data/                 # Dossier pour les fichiers JSON des profils
└── settings.json         # Fichier de configuration