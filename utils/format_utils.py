def format_file_size(size_bytes):
    if size_bytes is None:
        return ""
    try:
        size_bytes = int(size_bytes)
        if size_bytes >= 1_000_000_000:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} Go"
        elif size_bytes >= 1_000_000:
            return f"{size_bytes / (1024 * 1024):.2f} Mo"
        elif size_bytes >= 1_000:
            return f"{size_bytes / 1024:.2f} Ko"
        else:
            return f"{size_bytes} o"
    except Exception:
        return "?"

def format_bytes(size):
    if size is None:
        return "?"
    for unit in ['o', 'Ko', 'Mo', 'Go']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} To"

def render_progress_bar(percent: int) -> str:
    filled = int(percent / 10)
    empty = 10 - filled
    return f"{'█' * filled}{'░' * empty} {percent:>3}%"
