# core/executor.py
import atexit
import builtins
from concurrent.futures import ThreadPoolExecutor, Future
from threading import Lock

# ---- Singleton d'executor (unique au process), quel que soit le nombre d'import ----
if not hasattr(builtins, "_GLOBAL_EXECUTOR"):
    builtins._GLOBAL_EXECUTOR = ThreadPoolExecutor(
        max_workers=128, thread_name_prefix="worker"
    )
    builtins._GLOBAL_EXECUTOR_LOCK = Lock()
    builtins._GLOBAL_SUBMITTED = {}  # key -> Future
    atexit.register(builtins._GLOBAL_EXECUTOR.shutdown, wait=False)

EXECUTOR: ThreadPoolExecutor = builtins._GLOBAL_EXECUTOR
_EXEC_LOCK: Lock = builtins._GLOBAL_EXECUTOR_LOCK
_SUBMITTED: dict = builtins._GLOBAL_SUBMITTED

def submit_unique(key: str, fn, *args, **kwargs) -> Future | None:
    """Ne soumet la tâche que si 'key' n'est pas déjà en cours ou terminée récemment."""
    if not key:
        key = f"anon:{id(fn)}"
    with _EXEC_LOCK:
        fut = _SUBMITTED.get(key)
        if fut and not fut.done():
            return None
        fut = EXECUTOR.submit(fn, *args, **kwargs)
        _SUBMITTED[key] = fut
        def _cleanup(_f: Future, _k=key):
            with _EXEC_LOCK:
                # on laisse l'entrée mais pas de resoumission tant que done() est False
                pass
        fut.add_done_callback(_cleanup)
        return fut
