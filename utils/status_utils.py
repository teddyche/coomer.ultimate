import os
from .file_utils import sha256_file, rename_if_tmp_match

def enrich_media_status(medias, download_path):
    subdir_v = os.path.join(download_path, "v")
    subdir_p = os.path.join(download_path, "p")
    for media in medias:
        media_name = media.get("name")
        url = media.get("url", "")
        if not media_name or not url:
            continue

        ext = os.path.splitext(media_name)[1].lower()
        subdir = subdir_v if ext in [".mp4", ".m4v", ".mov", ".webm", ".avi", ".mkv", ".flv"] else subdir_p
        final_path = os.path.join(subdir, media_name)

        tmp_path = final_path + ".tmp"
        if os.path.exists(tmp_path) and not os.path.exists(final_path):
            rename_if_tmp_match(tmp_path, final_path, url)

        if os.path.exists(final_path):
            local_sha = sha256_file(final_path)
            if local_sha and local_sha in url:
                media["percent"] = "100"
                media["status"] = "Completed"
                media["hash_check"] = ""
    return medias
