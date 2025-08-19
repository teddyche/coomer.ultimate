import os
import threading

# valeur par défaut plus haute + override possible par env
GLOBAL_MAX = int(os.getenv("CU_GLOBAL_MAX", "50"))
GLOBAL_SEM = threading.BoundedSemaphore(GLOBAL_MAX)

# inchangé : sémaphore par fenêtre
_sems_by_window = {}
def window_sem(window_id: str, per_window_max: int = 25):
    if window_id not in _sems_by_window:
        _sems_by_window[window_id] = threading.BoundedSemaphore(per_window_max)
    return _sems_by_window[window_id]