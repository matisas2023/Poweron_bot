import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict


class UserStateStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_state (
                    chat_id TEXT PRIMARY KEY,
                    seen INTEGER NOT NULL,
                    history_json TEXT NOT NULL,
                    pinned_json TEXT NOT NULL,
                    auto_update_json TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.commit()

    def load_all(self) -> Dict[str, dict]:
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT chat_id, seen, history_json, pinned_json, auto_update_json FROM user_state"
                ).fetchall()
        payload = {}
        for chat_id, seen, history_json, pinned_json, auto_update_json in rows:
            try:
                payload[str(chat_id)] = {
                    "seen": bool(seen),
                    "history": json.loads(history_json or "[]"),
                    "pinned": json.loads(pinned_json or "[]"),
                    "auto_update": json.loads(auto_update_json or "{}"),
                }
            except json.JSONDecodeError:
                continue
        return payload

    def upsert_chat(self, chat_id: int, user_payload: dict):
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO user_state(chat_id, seen, history_json, pinned_json, auto_update_json, updated_at)
                    VALUES(?,?,?,?,?,?)
                    ON CONFLICT(chat_id) DO UPDATE SET
                        seen=excluded.seen,
                        history_json=excluded.history_json,
                        pinned_json=excluded.pinned_json,
                        auto_update_json=excluded.auto_update_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        str(chat_id),
                        1 if user_payload.get("seen") else 0,
                        json.dumps(user_payload.get("history") or [], ensure_ascii=False),
                        json.dumps(user_payload.get("pinned") or [], ensure_ascii=False),
                        json.dumps(user_payload.get("auto_update") or {}, ensure_ascii=False),
                        time.time(),
                    ),
                )
                conn.commit()

    def replace_all(self, payload: Dict[str, dict]):
        with self._lock:
            with self._connect() as conn:
                conn.execute("DELETE FROM user_state")
                for chat_id, user_payload in payload.items():
                    conn.execute(
                        """
                        INSERT INTO user_state(chat_id, seen, history_json, pinned_json, auto_update_json, updated_at)
                        VALUES(?,?,?,?,?,?)
                        """,
                        (
                            str(chat_id),
                            1 if user_payload.get("seen") else 0,
                            json.dumps(user_payload.get("history") or [], ensure_ascii=False),
                            json.dumps(user_payload.get("pinned") or [], ensure_ascii=False),
                            json.dumps(user_payload.get("auto_update") or {}, ensure_ascii=False),
                            time.time(),
                        ),
                    )
                conn.commit()
