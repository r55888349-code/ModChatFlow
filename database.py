import sqlite3
import os
from datetime import datetime
from typing import List, Dict, Any
import json

DB_PATH = "moderation.db"

def get_connection():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return None

def init_db():
    conn = get_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT UNIQUE NOT NULL,
                remove_links BOOLEAN DEFAULT 1,
                remove_caps BOOLEAN DEFAULT 1,
                remove_long BOOLEAN DEFAULT 1,
                remove_repeats BOOLEAN DEFAULT 1,
                ignore_broadcaster BOOLEAN DEFAULT 1,
                ignore_mods BOOLEAN DEFAULT 1,
                caps_percent INTEGER DEFAULT 80,
                max_length INTEGER DEFAULT 500,
                repeat_count INTEGER DEFAULT 3,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS blacklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                word TEXT NOT NULL,
                added_by TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel, word)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS banned_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                username TEXT NOT NULL,
                reason TEXT,
                banned_by TEXT,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel, username)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS warns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                username TEXT NOT NULL,
                count INTEGER DEFAULT 0,
                reason TEXT,
                moderator TEXT,
                last_warn TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                username TEXT NOT NULL,
                message TEXT NOT NULL,
                message_id TEXT,
                action TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("✅ Базовые таблицы инициализированы")
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")
    finally:
        conn.close()

def init_stats_table():
    conn = get_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS automod_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                links INTEGER DEFAULT 0,
                caps INTEGER DEFAULT 0,
                long INTEGER DEFAULT 0,
                repeats INTEGER DEFAULT 0,
                blacklist INTEGER DEFAULT 0,
                total INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel)
            )
        """)
        cursor.execute("PRAGMA table_info(automod_stats)")
        columns = [col[1] for col in cursor.fetchall()]
        if "total" not in columns:
            cursor.execute("ALTER TABLE automod_stats ADD COLUMN total INTEGER DEFAULT 0")
            print("✅ Добавлена колонка total в automod_stats")
        conn.commit()
        print("✅ Таблица автостатистики инициализирована")
    except Exception as e:
        print(f"❌ Ошибка инициализации таблицы статистики: {e}")
    finally:
        conn.close()

def init_slow_mode_table():
    conn = get_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS slow_mode (
                channel TEXT PRIMARY KEY,
                enabled BOOLEAN DEFAULT 0,
                interval_sec INTEGER DEFAULT 5,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("✅ Таблица slow_mode инициализирована")
    except Exception as e:
        print(f"❌ Ошибка инициализации slow_mode: {e}")
    finally:
        conn.close()

def get_slow_mode(channel: str) -> dict:
    conn = get_connection()
    if not conn:
        return {"enabled": False, "interval_sec": 5}
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT enabled, interval_sec FROM slow_mode WHERE channel = ?", (channel.lower(),))
        row = cursor.fetchone()
        if row:
            return {"enabled": bool(row["enabled"]), "interval_sec": row["interval_sec"]}
        else:
            cursor.execute("INSERT INTO slow_mode (channel) VALUES (?)", (channel.lower(),))
            conn.commit()
            return {"enabled": False, "interval_sec": 5}
    except Exception as e:
        print(f"❌ Ошибка получения slow_mode: {e}")
        return {"enabled": False, "interval_sec": 5}
    finally:
        conn.close()

def update_slow_mode(channel: str, enabled: bool, interval_sec: int) -> bool:
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO slow_mode (channel, enabled, interval_sec) 
            VALUES (?, ?, ?) 
            ON CONFLICT(channel) DO UPDATE SET enabled = excluded.enabled, interval_sec = excluded.interval_sec, updated_at = CURRENT_TIMESTAMP
        """, (channel.lower(), enabled, interval_sec))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка обновления slow_mode: {e}")
        return False
    finally:
        conn.close()

def init_manual_stats_table():
    conn = get_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS manual_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                bans INTEGER DEFAULT 0,
                deletions INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel)
            )
        """)
        conn.commit()
        print("✅ Таблица ручной статистики инициализирована")
    except Exception as e:
        print(f"❌ Ошибка инициализации таблицы ручной статистики: {e}")
    finally:
        conn.close()

# ============= ПЕРСОНАЛЬНАЯ СТАТИСТИКА МОДЕРАТОРОВ =============
def init_moderator_stats_table():
    conn = get_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS moderator_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                moderator_id TEXT NOT NULL,
                bans INTEGER DEFAULT 0,
                deletions INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel, moderator_id)
            )
        """)
        conn.commit()
        print("✅ Таблица moderator_stats инициализирована")
    except Exception as e:
        print(f"❌ Ошибка инициализации moderator_stats: {e}")
    finally:
        conn.close()

def get_moderator_stats(channel: str, moderator_id: str) -> Dict[str, int]:
    conn = get_connection()
    if not conn:
        return {"bans": 0, "deletions": 0}
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT bans, deletions FROM moderator_stats WHERE channel = ? AND moderator_id = ?",
                       (channel.lower(), moderator_id))
        row = cursor.fetchone()
        if row:
            return {"bans": row["bans"], "deletions": row["deletions"]}
        else:
            cursor.execute("INSERT INTO moderator_stats (channel, moderator_id) VALUES (?, ?)",
                           (channel.lower(), moderator_id))
            conn.commit()
            return {"bans": 0, "deletions": 0}
    except Exception as e:
        print(f"❌ Ошибка получения персональной статистики: {e}")
        return {"bans": 0, "deletions": 0}
    finally:
        conn.close()

def increment_moderator_stat(channel: str, moderator_id: str, stat_type: str) -> bool:
    if stat_type not in ["bans", "deletions"]:
        return False
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM moderator_stats WHERE channel = ? AND moderator_id = ?",
                       (channel.lower(), moderator_id))
        if not cursor.fetchone():
            cursor.execute("INSERT INTO moderator_stats (channel, moderator_id) VALUES (?, ?)",
                           (channel.lower(), moderator_id))
            conn.commit()
        cursor.execute(f"UPDATE moderator_stats SET {stat_type} = {stat_type} + 1, updated_at = CURRENT_TIMESTAMP WHERE channel = ? AND moderator_id = ?",
                       (channel.lower(), moderator_id))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка увеличения персональной статистики: {e}")
        return False
    finally:
        conn.close()

# ============= НАСТРОЙКИ =============
def get_settings(channel: str) -> Dict[str, Any]:
    conn = get_connection()
    if not conn:
        return {}
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM settings WHERE channel = ?", (channel.lower(),))
        row = cursor.fetchone()
        if row:
            return dict(row)
        else:
            cursor.execute("INSERT INTO settings (channel) VALUES (?)", (channel.lower(),))
            conn.commit()
            cursor.execute("SELECT * FROM settings WHERE channel = ?", (channel.lower(),))
            return dict(cursor.fetchone())
    except Exception as e:
        print(f"❌ Ошибка получения настроек: {e}")
        return {}
    finally:
        conn.close()

def update_settings(channel: str, settings: Dict[str, Any]) -> bool:
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        allowed_fields = ["remove_links", "remove_caps", "remove_long", "remove_repeats",
                          "ignore_broadcaster", "ignore_mods", "caps_percent", "max_length", "repeat_count"]
        updates = []
        values = []
        for field in allowed_fields:
            if field in settings:
                updates.append(f"{field} = ?")
                values.append(settings[field])
        if not updates:
            return True
        values.append(channel.lower())
        cursor.execute(f"UPDATE settings SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE channel = ?", values)
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка обновления настроек: {e}")
        return False
    finally:
        conn.close()

# ============= ЧЁРНЫЙ СПИСОК =============
def get_blacklist(channel: str) -> List[str]:
    conn = get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT word FROM blacklist WHERE channel = ? ORDER BY created_at DESC", (channel.lower(),))
        return [row["word"] for row in cursor.fetchall()]
    except Exception as e:
        print(f"❌ Ошибка получения чёрного списка: {e}")
        return []
    finally:
        conn.close()

def add_blacklist_word(channel: str, word: str, added_by: str = "system") -> bool:
    if not word or not word.strip():
        return False
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO blacklist (channel, word, added_by) VALUES (?, ?, ?)",
                       (channel.lower(), word.lower().strip(), added_by))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка добавления в чёрный список: {e}")
        return False
    finally:
        conn.close()

def remove_blacklist_word(channel: str, word: str) -> bool:
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM blacklist WHERE channel = ? AND word = ?",
                       (channel.lower(), word.lower().strip()))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"❌ Ошибка удаления из чёрного списка: {e}")
        return False
    finally:
        conn.close()

# ============= ЗАБАНЕННЫЕ ПОЛЬЗОВАТЕЛИ =============
def get_banned(channel: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, reason, banned_by, banned_at FROM banned_users WHERE channel = ? ORDER BY banned_at DESC", (channel.lower(),))
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        print(f"❌ Ошибка получения списка забаненных: {e}")
        return []
    finally:
        conn.close()

def add_banned(channel: str, username: str, reason: str = "", banned_by: str = "system") -> bool:
    if not username:
        return False
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO banned_users (channel, username, reason, banned_by) VALUES (?, ?, ?, ?)",
                       (channel.lower(), username.lower().strip(), reason, banned_by))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка добавления в список забаненных: {e}")
        return False
    finally:
        conn.close()

def remove_banned(channel: str, username: str) -> bool:
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM banned_users WHERE channel = ? AND username = ?",
                       (channel.lower(), username.lower().strip()))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"❌ Ошибка удаления из списка забаненных: {e}")
        return False
    finally:
        conn.close()

# ============= ПРЕДУПРЕЖДЕНИЯ (ВАРНЫ) =============
def get_warns_count(channel: str, username: str) -> int:
    conn = get_connection()
    if not conn:
        return 0
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT count FROM warns WHERE channel = ? AND username = ?",
                       (channel.lower(), username.lower().strip()))
        row = cursor.fetchone()
        return row["count"] if row else 0
    except Exception as e:
        print(f"❌ Ошибка получения количества предупреждений: {e}")
        return 0
    finally:
        conn.close()

def add_warn(channel: str, username: str, reason: str = "", moderator: str = "") -> int:
    conn = get_connection()
    if not conn:
        return 0
    try:
        cursor = conn.cursor()
        current = get_warns_count(channel, username)
        new_count = current + 1
        cursor.execute("""
            INSERT INTO warns (channel, username, count, reason, moderator)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(channel, username) DO UPDATE SET
                count = count + 1,
                reason = excluded.reason,
                moderator = excluded.moderator,
                last_warn = CURRENT_TIMESTAMP
        """, (channel.lower(), username.lower().strip(), new_count, reason, moderator))
        conn.commit()
        return new_count
    except Exception as e:
        print(f"❌ Ошибка добавления предупреждения: {e}")
        return 0
    finally:
        conn.close()

def clear_warns(channel: str, username: str) -> bool:
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE warns SET count = 0 WHERE channel = ? AND username = ?",
                       (channel.lower(), username.lower().strip()))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка сброса предупреждений: {e}")
        return False
    finally:
        conn.close()

# ============= СТАТИСТИКА АВТОМОДА =============
def get_stats(channel: str) -> Dict[str, int]:
    conn = get_connection()
    if not conn:
        return {"links": 0, "caps": 0, "long": 0, "repeats": 0, "blacklist": 0, "total": 0}
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT links, caps, long, repeats, blacklist, total FROM automod_stats WHERE channel = ?",
                       (channel.lower(),))
        row = cursor.fetchone()
        if row:
            return {
                "links": row["links"],
                "caps": row["caps"],
                "long": row["long"],
                "repeats": row["repeats"],
                "blacklist": row["blacklist"],
                "total": row["total"]
            }
        else:
            cursor.execute("INSERT INTO automod_stats (channel) VALUES (?)", (channel.lower(),))
            conn.commit()
            return {"links": 0, "caps": 0, "long": 0, "repeats": 0, "blacklist": 0, "total": 0}
    except Exception as e:
        print(f"❌ Ошибка получения статистики: {e}")
        return {"links": 0, "caps": 0, "long": 0, "repeats": 0, "blacklist": 0, "total": 0}
    finally:
        conn.close()

def increment_stat(channel: str, stat_type: str) -> bool:
    if stat_type not in ["links", "caps", "long", "repeats", "blacklist"]:
        return False
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM automod_stats WHERE channel = ?", (channel.lower(),))
        if not cursor.fetchone():
            cursor.execute("INSERT INTO automod_stats (channel) VALUES (?)", (channel.lower(),))
            conn.commit()
        cursor.execute(f"""
            UPDATE automod_stats 
            SET {stat_type} = {stat_type} + 1, 
                total = total + 1,
                updated_at = CURRENT_TIMESTAMP 
            WHERE channel = ?
        """, (channel.lower(),))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка увеличения статистики: {e}")
        return False
    finally:
        conn.close()

def reset_stats(channel: str) -> bool:
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE automod_stats 
            SET links = 0, caps = 0, long = 0, repeats = 0, blacklist = 0, total = 0
            WHERE channel = ?
        """, (channel.lower(),))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка сброса статистики: {e}")
        return False
    finally:
        conn.close()

# ============= СТАТИСТИКА РУЧНОЙ МОДЕРАЦИИ (ОБЩАЯ) =============
def get_manual_stats(channel: str) -> Dict[str, int]:
    conn = get_connection()
    if not conn:
        return {"bans": 0, "deletions": 0}
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT bans, deletions FROM manual_stats WHERE channel = ?", (channel.lower(),))
        row = cursor.fetchone()
        if row:
            return {"bans": row["bans"], "deletions": row["deletions"]}
        else:
            cursor.execute("INSERT INTO manual_stats (channel) VALUES (?)", (channel.lower(),))
            conn.commit()
            return {"bans": 0, "deletions": 0}
    except Exception as e:
        print(f"❌ Ошибка получения ручной статистики: {e}")
        return {"bans": 0, "deletions": 0}
    finally:
        conn.close()

def increment_manual_stat(channel: str, stat_type: str) -> bool:
    if stat_type not in ["bans", "deletions"]:
        return False
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM manual_stats WHERE channel = ?", (channel.lower(),))
        if not cursor.fetchone():
            cursor.execute("INSERT INTO manual_stats (channel) VALUES (?)", (channel.lower(),))
            conn.commit()
        cursor.execute(f"UPDATE manual_stats SET {stat_type} = {stat_type} + 1, updated_at = CURRENT_TIMESTAMP WHERE channel = ?", (channel.lower(),))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка увеличения ручной статистики: {e}")
        return False
    finally:
        conn.close()

def reset_manual_stats(channel: str) -> bool:
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE manual_stats SET bans = 0, deletions = 0 WHERE channel = ?", (channel.lower(),))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка сброса ручной статистики: {e}")
        return False
    finally:
        conn.close()

# ============= ЛОГИ =============
def add_log(channel: str, username: str, message: str, message_id: str = None, action: str = None) -> bool:
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO logs (channel, username, message, message_id, action) VALUES (?, ?, ?, ?, ?)",
                       (channel.lower(), username.lower().strip(), message, message_id, action))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка добавления лога: {e}")
        return False
    finally:
        conn.close()

def get_logs(channel: str, limit: int = 100) -> List[Dict[str, Any]]:
    conn = get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, channel, username, message, message_id, action, created_at FROM logs WHERE channel = ? ORDER BY created_at DESC LIMIT ?", (channel.lower(), limit))
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        print(f"❌ Ошибка получения логов: {e}")
        return []
    finally:
        conn.close()

def clear_old_logs(days: int = 30) -> bool:
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM logs WHERE created_at < datetime('now', '-' || ? || ' days')", (days,))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка очистки логов: {e}")
        return False
    finally:
        conn.close()

# ============= ЗАМЕТКИ =============
def add_note(user_id: str, content: str) -> bool:
    if not content or not content.strip():
        return False
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO notes (user_id, content) VALUES (?, ?)", (str(user_id), content.strip()))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка добавления заметки: {e}")
        return False
    finally:
        conn.close()

def get_notes(user_id: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, content, created_at FROM notes WHERE user_id = ? ORDER BY created_at DESC", (str(user_id),))
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        print(f"❌ Ошибка получения заметок: {e}")
        return []
    finally:
        conn.close()

def delete_note(note_id: int) -> bool:
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM notes WHERE id = ?", (note_id,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"❌ Ошибка удаления заметки: {e}")
        return False
    finally:
        conn.close()

# ============= УТИЛИТЫ =============
def get_channel_stats_full(channel: str) -> Dict[str, Any]:
    auto = get_stats(channel)
    manual = get_manual_stats(channel)
    settings = get_settings(channel)
    blacklist_count = len(get_blacklist(channel))
    banned_count = len(get_banned(channel))
    return {
        "channel": channel,
        "auto_stats": auto,
        "manual_stats": manual,
        "blacklist_count": blacklist_count,
        "banned_count": banned_count,
        "settings": {
            "remove_links": settings.get("remove_links", True),
            "remove_caps": settings.get("remove_caps", True),
            "remove_long": settings.get("remove_long", True),
            "remove_repeats": settings.get("remove_repeats", True),
            "ignore_broadcaster": settings.get("ignore_broadcaster", True),
            "ignore_mods": settings.get("ignore_mods", True),
            "caps_percent": settings.get("caps_percent", 80),
            "max_length": settings.get("max_length", 500),
            "repeat_count": settings.get("repeat_count", 3)
        }
    }

def export_stats(channel: str, format: str = "json") -> str:
    try:
        stats = get_channel_stats_full(channel)
        if format == "json":
            return json.dumps(stats, indent=2, default=str)
        else:
            text = f"=== Статистика канала #{channel} ===\n\n"
            text += "Автомодерация:\n"
            for k, v in stats["auto_stats"].items():
                text += f"  {k}: {v}\n"
            text += "\nРучная модерация:\n"
            for k, v in stats["manual_stats"].items():
                text += f"  {k}: {v}\n"
            text += f"\nЧёрный список: {stats['blacklist_count']} слов\n"
            text += f"Забаненных: {stats['banned_count']} пользователей\n"
            return text
    except Exception as e:
        print(f"❌ Ошибка экспорта статистики: {e}")
        return ""

def vacuum_db() -> bool:
    conn = get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("VACUUM")
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Ошибка оптимизации БД: {e}")
        return False
    finally:
        conn.close()
