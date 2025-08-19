import re, json, os, requests
from typing import Optional

def extract_profile_info(url: str):
    pattern = r"https?://(?:www\.)?coomer\.st/([^/]+)/user/([^/?#]+)"
    match = re.match(pattern, url.strip())
    if match:
        service, username = match.groups()
        return service.lower(), username
    return None, None


def get_fansly_username_from_id(user_id: str) -> Optional[str]:
    url = f"https://apiv3.fansly.com/api/v1/account?ids={user_id}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://fansly.com",
        "Referer": "https://fansly.com/"
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            json_data = r.json()
            users = json_data.get("response", [])
            if users and "username" in users[0]:
                return users[0]["username"]
        else:
            print(f"[get_fansly_username_from_id] HTTP {r.status_code} - fallback to user_id")
    except Exception as e:
        print(f"[get_fansly_username_from_id] Error: {e}")

    return user_id  # 🔁 failover propre

def get_settings(filepath="settings.json"):
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return json.load(f)
    return {}

def save_settings(settings, filepath="settings.json"):
    with open(filepath, "w") as f:
        json.dump(settings, f, indent=2)