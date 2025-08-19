"""
Refactor simple et robuste pour piloter le parallélisme de téléchargements.
- Une seule boucle planificatrice (scheduler) par fenêtre
- Un ThreadPoolExecutor borné (max_concurrent)
- AUCUNE création de thread ailleurs que dans l'executor et la boucle scheduler
- Sémaphore pour ne jamais dépasser max_concurrent
- Callbacks propres pour brancher l'UI (progress / status)

Dépendances externes :
- core.download_manager.DownloadManager (conservé tel quel)
- log.log_info / log_warning / log_error

Intégration rapide :
1) Instancie le controller dans ta classe fenêtre (self.ctrl = DownloadConcurrencyController(...))
2) self.ctrl.enqueue(media_dict) pour ajouter un job
3) self.ctrl.start() au moment d'activer la fenêtre ; self.ctrl.stop() à la fermeture
4) Retire toute logique de comptage running_downloads / start_next_in_queue dispersée
"""
from __future__ import annotations

import threading
import queue
import time
from dataclasses import dataclass, field
from typing import Callable, Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, Future

from log import log_info, log_warning, log_error
from core.download_manager import DownloadManager


@dataclass
class DownloadJob:
    media: Dict[str, Any]
    # chemins et infos calculées par l'appelant (déjà existantes chez toi)
    final_path: str
    url: str
    window_id: Optional[str] = None

    # callbacks UI – tous sont optionnels
    on_progress: Optional[Callable[[Dict[str, Any]], None]] = None
    on_status: Optional[Callable[[Dict[str, Any]], None]] = None

    # interne
    id: str = field(default_factory=lambda: str(time.time()))


class DownloadConcurrencyController:
    def __init__(
        self,
        username: str,
        max_concurrent: int = 10,
        scheduler_tick: float = 0.05,
    ) -> None:
        self.username = username
        self.max_concurrent = max(1, max_concurrent)
        self.scheduler_tick = max(0.01, scheduler_tick)

        # État
        self._pending: "queue.Queue[DownloadJob]" = queue.Queue()
        self._active: Dict[str, Future] = {}

        # bornage strict
        self._slots = threading.BoundedSemaphore(self.max_concurrent)

        # Executor unique
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_concurrent,
            thread_name_prefix=f"dlw_{username[:4]}"
        )

        # Watch/stop
        self._scheduler_thread: Optional[threading.Thread] = None
        self._stop_evt = threading.Event()
        self._started = False

    # --- API publique -----------------------------------------------------
    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._stop_evt.clear()
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            name=f"scheduler:{self.username[:4]}",
            daemon=True,
        )
        self._scheduler_thread.start()
        log_info(f"[CTRL] ▶️ Scheduler démarré ({self.username})")

    def stop(self, wait: bool = False) -> None:
        self._stop_evt.set()
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=2.0)
        # On ne tue pas brutalement les téléchargements en cours ; on laisse finir
        if wait:
            for f in list(self._active.values()):
                try:
                    f.result(timeout=10)
                except Exception:
                    pass
        self._executor.shutdown(wait=False, cancel_futures=False)
        log_info(f"[CTRL] 🛑 Scheduler arrêté ({self.username})")

    def set_max_concurrent(self, n: int) -> None:
        n = max(1, int(n))
        if n == self.max_concurrent:
            return
        log_info(f"[CTRL] 🔧 Changement de parallélisme: {self.max_concurrent} → {n}")
        self.max_concurrent = n
        # remplace les structures bornées
        self._slots = threading.BoundedSemaphore(self.max_concurrent)
        # recréer l'executor avec le nouveau pool – sans interrompre les jobs actifs
        old = self._executor
        self._executor = ThreadPoolExecutor(max_workers=self.max_concurrent, thread_name_prefix=f"dlw_{self.username[:4]}")
        # on laisse l'ancien se vider tout seul
        old.shutdown(wait=False, cancel_futures=False)

    def enqueue(self, job: DownloadJob) -> None:
        # Nettoie statut initial coté UI
        job.media.setdefault("status", "Waiting")
        job.media.setdefault("percent", 0)
        job.media.setdefault("local_size", 0)
        job.media.setdefault("speed", "0 B/s")
        if job.on_status:
            try:
                job.on_status(job.media)
            except Exception:
                pass
        self._pending.put(job)
        log_info(f"[CTRL] ➕ Enqueue: {job.media.get('name', job.final_path)} (pending={self._pending.qsize()})")

    def stats(self) -> Dict[str, int]:
        return {
            "pending": self._pending.qsize(),
            "active": len(self._active),
            "slots_free": getattr(self._slots, "_value", 0),
            "max": self.max_concurrent,
        }

    # --- Interne ----------------------------------------------------------
    def _scheduler_loop(self) -> None:
        idle_count = 0
        while not self._stop_evt.is_set():
            # 1) collecter les futures terminées
            done_ids: List[str] = []
            for jid, fut in list(self._active.items()):
                if fut.done():
                    done_ids.append(jid)
                    try:
                        fut.result()  # propage les exceptions au log dans le worker
                    except Exception as e:
                        log_warning(f"[CTRL] future err: {e}")
                    finally:
                        self._release_slot()
            for jid in done_ids:
                self._active.pop(jid, None)

            # 2) démarrer de nouveaux jobs si slots disponibles
            started = 0
            while self._try_acquire_slot():
                try:
                    job = self._pending.get_nowait()
                except queue.Empty:
                    self._release_slot()  # rien à lancer → libère le slot
                    break
                fut = self._executor.submit(self._run_job, job)
                self._active[job.id] = fut
                started += 1

            if started == 0 and len(self._active) == 0 and self._pending.qsize() == 0:
                idle_count += 1
                if idle_count % 100 == 0:
                    s = self.stats()
                    log_info(f"[CTRL] ✅ Idle (active={s['active']} / pending={s['pending']})")
            else:
                idle_count = 0

            time.sleep(self.scheduler_tick)

    def _try_acquire_slot(self) -> bool:
        try:
            return self._slots.acquire(blocking=False)
        except Exception:
            return False

    def _release_slot(self) -> None:
        try:
            self._slots.release()
        except Exception:
            pass

    # --- Worker -----------------------------------------------------------
    def _run_job(self, job: DownloadJob) -> None:
        media = job.media
        media["status"] = "Downloading"
        if job.on_status:
            try:
                job.on_status(media)
            except Exception:
                pass

        def _on_progress(bytes_done: int, speed_str: str, total_size: int) -> None:
            media["local_size"] = bytes_done
            media["size_http"] = total_size
            media["percent"] = int((bytes_done / total_size) * 100) if total_size else 0
            media["speed"] = speed_str
            if job.on_progress:
                try:
                    job.on_progress(media)
                except Exception:
                    pass

        try:
            ok, err = DownloadManager.download_file(
                url=job.url,
                final_path=job.final_path,
                on_progress=_on_progress,
                resume=True,
                retry_delay=10,
                should_stop=None,
                window_id=job.window_id,
            )
            if ok:
                media["status"] = "Completed"
            else:
                media["status"] = "Failed"
                media["error"] = err or "Unknown"
        except Exception as e:
            media["status"] = "Failed"
            media["error"] = str(e)
            log_error(f"[CTRL] job failed: {media.get('name')}: {e}")
        finally:
            if job.on_status:
                try:
                    job.on_status(media)
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# Exemple d'intégration minimal (dans ta classe principale)
# ---------------------------------------------------------------------------
# self.ctrl = DownloadConcurrencyController(self.username, max_concurrent=40)
# self.ctrl.start()
# self.ctrl.enqueue(DownloadJob(
#     media=media_dict,
#     final_path=final_path,
#     url=media_url,
#     window_id=self.window_id,
#     on_progress=lambda m: self.refresh_media_row(m),
#     on_status=lambda m: self.refresh_media_row(m),
# ))
# ... à la fermeture: self.ctrl.stop(wait=False)
