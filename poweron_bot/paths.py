from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
LOGS_DIR = BASE_DIR / "logs"
TMP_DIR = BASE_DIR / "tmp"
DATA_DIR = BASE_DIR / "data"

BOT_TOKEN_FILE = BASE_DIR / "poweron_bot_token.txt"
ADMIN_ID_FILE = BASE_DIR / "poweron_admin_user_id.txt"
