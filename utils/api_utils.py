from log import log_info, log_error, log_warning
import os, time, requests
import random  # ajout pour jitter

from .network_utils import build_media_url, get_remote_file_size

def fetch_medias_paginated(service, username, on_media_callback):
    log_info(f"[API] Streaming medias for {service}/{username}")
    seen_ids = set()
    offset = 0
    limit = 50
    while True:
        url = f"https://coomer.st/api/v1/{service}/user/{username}?o={offset}"
        log_info(f"[API] Requête : {url}")
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                log_error(f"[API] Erreur HTTP {resp.status_code} pour {url}")
                break
            data = resp.json()
            if not isinstance(data, list) or not data:
                break

            log_info(f"[API] Page : {len(data)} posts")
            for item in data:
                media_id = item.get("id")
                if media_id in seen_ids:
                    continue
                seen_ids.add(media_id)

                file = item.get("file", {})
                path = file.get("path")
                name = file.get("name")
                if not path or not name:
                    continue

                url_cdn = build_media_url(path)
                size_http = get_remote_file_size(url_cdn)
                log_info(f"[MEDIA] URL CDN : {url_cdn}")
                log_info(f"[MEDIA] Taille HTTP : {size_http} octets")

                media = {
                    "id": media_id,
                    "name": name,
                    "path": path,
                    "title": item.get("title", ""),
                    "added": item.get("added"),
                    "url": url_cdn,
                    "size_http": size_http,
                    "percent": "0",
                    "downloaded": False,
                    "retry_count": 0,
                    "error": "",
                }
                on_media_callback(media)
            offset += limit
            time.sleep(0.3)
        except Exception as e:
            log_error(f"[API] fetch_medias_paginated error: {e}")
            break

UA = "CoomerUltimate/0.4 (+https://github.com/you/yourapp)"

def fetch_medias_from_api(service, username, session_cookie=None, extra_cookies=None, check_cdn=False):
    import os, time, requests
    from urllib.parse import urlencode

    log_info(f"[API] Fetching medias for {service}/{username}")

    # --- Session HTTP ---
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,                       # tu peux mettre ton UA navigateur ici si besoin
        "Accept": "text/css",                   # <— clé pour Coomer/DDG en ce moment
        "Referer": f"https://coomer.st/{service}/user/{username}",
        "Origin": "https://coomer.st",
    })
    # Cookies d’auth éventuels
    if session_cookie:
        s.cookies.set("session", session_cookie, domain="coomer.st", path="/")
    if extra_cookies:
        for k, v in extra_cookies.items():
            s.cookies.set(k, v, domain="coomer.st", path="/")

    def build_url(url, params: dict | None = None):
        q = dict(params or {})
        q["_"] = int(time.time())              # cache-buster
        return f"{url}?{urlencode(q)}"

    def http_get(url):
        """GET no-redirect avec petit backoff sur 429/5xx"""
        backoff = 2.0
        for _ in range(4):
            try:
                r = s.get(url, timeout=15, allow_redirects=False)
            except Exception as e:
                log_error(f"[API] Exception réseau : {e} → retry {backoff:.1f}s")
                time.sleep(backoff); backoff = min(backoff * 1.5, 30); continue

            if r.status_code in (429, 502, 503, 504):
                retry_in = r.headers.get("Retry-After")
                try: retry_in = float(retry_in) if retry_in else backoff
                except Exception: retry_in = backoff
                retry_in = min(retry_in, 30)
                log_warning(f"[API] {r.status_code} sur {url} → retry dans {retry_in:.1f}s")
                time.sleep(retry_in); backoff = min(backoff * 1.5, 30); continue
            return r
        return r  # dernier essai

    def parse_posts(resp):
        try:
            data = resp.json()
        except Exception:
            log_error("[API] Réponse invalide (pas un JSON)")
            return None
        if isinstance(data, dict) and "posts" in data:
            data = data["posts"]
        if not isinstance(data, list):
            log_error("[API] Erreur : réponse inattendue (liste attendue)")
            return None
        return data

    seen_ids = set()
    seen_hashes = set()
    seen_hashes_map = {}

    def extract_cdn_hash(url):
        try:
            return os.path.splitext(os.path.basename(url))[0]
        except Exception:
            return None

    base = f"https://coomer.st/api/v1/{service}/user/{username}/posts"

    # ---- Page 1 (sans ?o=0)
    url1 = build_url(base)
    r = http_get(url1)
    if r is None:
        log_error("[API] Échec réseau sur page 1"); return
    if r.status_code in (401, 403):
        log_error("[API] 401/403 — cookies requis (session/DDG) ou UA/IP différents."); return
    if r.status_code in (301, 302, 303, 307, 308):
        log_warning(f"[API] Redirection {r.status_code} — blocage DDG probable."); return
    if r.status_code != 200:
        log_error(f"[API] HTTP {r.status_code} sur {base}"); return

    data = parse_posts(r)
    if data is None:
        return
    if not data:
        log_info("[API] Aucune donnée (page 1)."); return

    page_index = 1
    last_id, last_ts = None, None

    def extract_page_medias(items):
        """Transforme une page de posts en médias plats + maj last_id/last_ts"""
        nonlocal last_id, last_ts
        page_medias, added_names, skipped_names = [], 0, 0
        for item in items:
            media_id = item.get("id") or item.get("post_id")
            if not media_id:
                continue
            # curseurs de pagination
            last_id = str(media_id)
            last_ts = item.get("published") or item.get("added") or item.get("created_at")

            if media_id in seen_ids:
                continue
            seen_ids.add(media_id)

            title = item.get("title", "")
            added = item.get("added") or item.get("published") or item.get("created_at")

            # 1) fichier principal
            file = item.get("file")
            if file and file.get("path") and file.get("name"):
                name = file["name"]
                url_cdn = build_media_url(file["path"])
                cdn_hash = extract_cdn_hash(url_cdn)
                duplicate_key = cdn_hash or name
                if duplicate_key in seen_hashes:
                    origin_id = seen_hashes_map.get(duplicate_key, "inconnu")
                    skipped_names += 1
                    log_info(f"[DUPLICATE] {duplicate_key} — {name} — Post: {media_id} / Origine: {origin_id}")
                else:
                    ext = os.path.splitext(name)[1].lower()
                    type_detected = (
                        "video" if ext in [".mp4", ".m4v", ".mov", ".webm", ".avi", ".mkv", ".flv"]
                        else "image" if ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]
                        else "autre"
                    )
                    media = {
                        "id": str(media_id),
                        "name": name,
                        "path": file["path"],
                        "title": title,
                        "added": added,
                        "url": url_cdn,
                        "size_http": None,
                        "cdn_checked": False,
                        "percent": "0",
                        "downloaded": False,
                        "retry_count": 0,
                        "error": "",
                        "status": "",
                        "type": type_detected
                    }
                    page_medias.append(media)
                    seen_hashes.add(duplicate_key)
                    seen_hashes_map[duplicate_key] = media["id"]
                    added_names += 1

            # 2) attachments
            for i, att in enumerate(item.get("attachments", [])):
                name = att.get("name"); path = att.get("path")
                if not path or not name:
                    continue
                url_cdn = build_media_url(path)
                cdn_hash = extract_cdn_hash(url_cdn)
                duplicate_key = cdn_hash or name
                att_id = f"{media_id}_att{i}"
                if duplicate_key in seen_hashes:
                    origin_id = seen_hashes_map.get(duplicate_key, "inconnu")
                    skipped_names += 1
                    log_info(f"[DUPLICATE] {duplicate_key} — {name} — Post: {att_id} / Origine: {origin_id}")
                    continue

                ext = os.path.splitext(name)[1].lower()
                type_detected = (
                    "video" if ext in [".mp4", ".m4v", ".mov", ".webm", ".avi", ".mkv", ".flv"]
                    else "image" if ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]
                    else "autre"
                )
                media = {
                    "id": att_id,
                    "name": name,
                    "path": path,
                    "title": title,
                    "added": added,
                    "url": url_cdn,
                    "size_http": None,
                    "cdn_checked": False,
                    "percent": "0",
                    "downloaded": False,
                    "retry_count": 0,
                    "error": "",
                    "status": "",
                    "type": type_detected
                }
                page_medias.append(media)
                seen_hashes.add(duplicate_key)
                seen_hashes_map[duplicate_key] = media["id"]
                added_names += 1

        log_info(f"[PAGE] {added_names} ajoutés, {skipped_names} doublons ignorés (page={page_index})")
        return page_medias

    # Page 1 -> yield
    log_info(f"[API] Page {page_index} : {len(data)} posts")
    yield extract_page_medias(data)

    # ---- Détection de la pagination (sans ?o), avec offset en fallback
    page_mode = None   # 'before_id' | 'max_id' | 'before' | 'page' | 'until' | 'offset'
    page_no = 2

    def try_next():
        """Essaie les modes de pagination connus, choisit le 1er qui renvoie des IDs nouveaux."""
        nonlocal page_mode, page_no
        candidates = []

        # ordre le plus robuste
        if last_id:
            candidates.append( (build_url(base, {"before_id": last_id}), "before_id") )
            candidates.append( (build_url(base, {"max_id":    last_id}), "max_id") )
        if last_ts:
            candidates.append( (build_url(base, {"before": last_ts}),    "before") )

        # certains profils veulent page=1 pour la 2e page (indexation 0/1 incohérente)
        candidates.append( (build_url(base, {"page": 2}), "page") )
        candidates.append( (build_url(base, {"page": 1}), "page") )

        # dernier recours : offset
        candidates.append( (build_url(base, {"o": len(seen_ids)}), "offset") )

        for url, mode in candidates:
            r = http_get(url)
            if not r or r.status_code != 200:
                continue
            items = parse_posts(r)
            if not items:
                continue
            ids = [(it.get("id") or it.get("post_id")) for it in items]
            if any(i not in seen_ids for i in ids):
                page_mode = mode
                log_info(f"[API] Mode candidat OK: {mode} ({len(items)} posts)")
                return items
        return []

    items = try_next()
    if not page_mode:
        log_info("[API] Aucune page suivante détectée (before_id/max_id/before/page/offset). Fin normale.")
        return

    log_info(f"[API] Pagination détectée : {page_mode}")

    # ---- Boucle des pages suivantes
    while items:
        page_index += 1
        log_info(f"[API] Page {page_index} : {len(items)} posts")
        yield extract_page_medias(items)

        # Construire l'URL suivante selon le mode sélectionné
        if page_mode == "before_id":
            next_url = build_url(base, {"before_id": last_id})
        elif page_mode == "max_id":
            next_url = build_url(base, {"max_id": last_id})
        elif page_mode == "before":
            next_url = build_url(base, {"before": last_ts})
        elif page_mode == "page":
            page_no += 1
            next_url = build_url(base, {"page": page_no})
        elif page_mode == "until":
            next_url = build_url(base, {"until": last_ts})
        elif page_mode == "offset":
            next_url = build_url(base, {"o": len(seen_ids)})
        else:
            break

        nxt = fetch_page_resilient(http_get, next_url, parse_posts)

        if not nxt:
            break
        if all((it.get("id") or it.get("post_id")) in seen_ids for it in nxt):
            break

        items = nxt
        time.sleep(0.2 + random.random() * 0.25)  # petit jitter anti-DDG

def fetch_page_resilient(get_fn, url, parse_fn, max_retry=6, base_delay=2.0):
    """GET + parse avec retries agressifs sur 5xx/429. Renvoie [] si vide/échec."""
    delay = base_delay
    for i in range(max_retry):
        r = get_fn(url)
        if r and r.status_code == 200:
            items = parse_fn(r)
            if items is not None:
                return items
            log_warning(f"[API] JSON invalide sur {url} → retry {i+1}/{max_retry} dans {delay:.1f}s")
        elif r and r.status_code in (500, 502, 503, 504, 429):
            log_warning(f"[API] {r.status_code} sur {url} → retry {i+1}/{max_retry} dans {delay:.1f}s")
        else:
            break
        time.sleep(delay)
        delay = min(delay * 1.6, 20.0)
    return []