from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, RedirectResponse
import asyncio
import aiohttp
import secrets
import re
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from twitchio import Client
from database import *
from collections import defaultdict

load_dotenv()

app = FastAPI()
sessions = {}
init_db()
init_stats_table()
init_manual_stats_table()
init_slow_mode_table()
init_moderator_stats_table()

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8000/auth/callback")
CHAT_BOT_TOKEN = os.getenv("CHAT_BOT_TOKEN")
CHAT_BOT_NICK = os.getenv("CHAT_BOT_NICK", "fellinthoughst")
ALLOWED_TWITCH_ID = os.getenv("ALLOWED_TWITCH_ID")

if not TWITCH_CLIENT_ID:
    print("❌ ОШИБКА: TWITCH_CLIENT_ID не задан в переменных окружения")
if not TWITCH_CLIENT_SECRET:
    print("❌ ОШИБКА: TWITCH_CLIENT_SECRET не задан в переменных окружения")
if not CHAT_BOT_TOKEN:
    print("⚠️ ВНИМАНИЕ: CHAT_BOT_TOKEN не задан, бот не сможет читать чат")

channel_websockets = {}
readers = {}
user_id_cache = {}
CACHE_TTL = 3600
reader_creation_locks = defaultdict(asyncio.Lock)
http_session = None


def log_chat_message(channel, username, message, msg_id):
    try:
        os.makedirs("logs", exist_ok=True)
        filename = f"logs/chat_{datetime.now().strftime('%Y-%m-%d')}.log"
        with open(filename, "a", encoding="utf-8") as f:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{timestamp}] #{channel} | {username}: {message} (id={msg_id})\n")
    except Exception as e:
        print(f"❌ Ошибка записи лога: {e}")


def analyze_message(text):
    warnings = []
    letters = [c for c in text if c.isalpha()]
    if letters:
        upper_percent = sum(1 for c in letters if c.isupper()) / len(letters) * 100
        if upper_percent >= 80:
            warnings.append("CAPS")
    if re.search(r'https?://\S+|www\.\S+', text):
        warnings.append("LINK")
    if len(text) > 500:
        warnings.append("LONG")
    return warnings


async def get_user_id(username, access_token):
    if not access_token or not username:
        return None
    cache_key = username.lower()
    if cache_key in user_id_cache:
        cached_id, timestamp = user_id_cache[cache_key]
        if time.time() - timestamp < CACHE_TTL:
            return cached_id
    try:
        url = f"https://api.twitch.tv/helix/users?login={username}"
        headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {access_token}"}
        async with http_session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("data"):
                    user_id = data["data"][0]["id"]
                    user_id_cache[cache_key] = (user_id, time.time())
                    return user_id
            else:
                print(f"⚠️ Ошибка получения ID {username}: {resp.status}")
    except Exception as e:
        print(f"❌ Ошибка get_user_id: {e}")
    return None


async def get_user_id_from_token(access_token):
    try:
        url = "https://api.twitch.tv/helix/users"
        headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {access_token}"}
        async with http_session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("data"):
                    return data["data"][0]["id"]
    except Exception as e:
        print(f"❌ Ошибка get_user_id_from_token: {e}")
    return None


async def send_message_as_user(channel_name, user_token, message_text):
    if not user_token or not message_text or not channel_name:
        return False, "Missing parameters"
    try:
        broadcaster_id = await get_user_id(channel_name, user_token)
        if not broadcaster_id:
            return False, "Broadcaster not found"
        sender_id = await get_user_id_from_token(user_token)
        if not sender_id:
            return False, "Sender ID not found"
        url = "https://api.twitch.tv/helix/chat/messages"
        headers = {
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {user_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "broadcaster_id": broadcaster_id,
            "sender_id": sender_id,
            "message": message_text[:500]
        }
        async with http_session.post(url, headers=headers, json=payload,
                                     timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                return True, None
            else:
                text = await resp.text()
                return False, f"Error {resp.status}: {text}"
    except Exception as e:
        return False, str(e)


async def delete_message(channel_name, message_id, moderator_token):
    if not message_id or not moderator_token:
        return False, "Нет ID сообщения или токена"
    try:
        broadcaster_id = await get_user_id(channel_name, moderator_token)
        moderator_id = await get_user_id_from_token(moderator_token)
        if not broadcaster_id or not moderator_id:
            return False, "Не удалось определить ID канала или модератора"
        url = "https://api.twitch.tv/helix/moderation/chat"
        headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {moderator_token}"}
        params = {
            "broadcaster_id": broadcaster_id,
            "moderator_id": moderator_id,
            "message_id": str(message_id)
        }
        async with http_session.delete(url, headers=headers, params=params,
                                       timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status in (200, 204):
                return True, None
            else:
                text = await resp.text()
                return False, f"Ошибка {resp.status}: {text}"
    except Exception as e:
        return False, str(e)


async def send_real_ban(channel_name, user_name, moderator_token, reason=""):
    if not moderator_token or not user_name:
        return False, "Нет токена или имени пользователя"
    try:
        broadcaster_id = await get_user_id(channel_name, moderator_token)
        moderator_id = await get_user_id_from_token(moderator_token)
        user_id = await get_user_id(user_name, moderator_token)
        if not broadcaster_id or not moderator_id or not user_id:
            return False, "Не удалось определить ID"
        url = "https://api.twitch.tv/helix/moderation/bans"
        headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {moderator_token}",
                   "Content-Type": "application/json"}
        params = {
            "broadcaster_id": broadcaster_id,
            "moderator_id": moderator_id
        }
        payload = {"data": {"user_id": user_id, "reason": reason[:120]}}
        async with http_session.post(url, headers=headers, params=params, json=payload,
                                     timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                return True, None
            else:
                text = await resp.text()
                return False, f"Ошибка {resp.status}: {text}"
    except Exception as e:
        return False, str(e)


async def send_real_unban(channel_name, user_name, moderator_token):
    if not moderator_token or not user_name:
        return False, "Нет токена или имени пользователя"
    try:
        broadcaster_id = await get_user_id(channel_name, moderator_token)
        moderator_id = await get_user_id_from_token(moderator_token)
        user_id = await get_user_id(user_name, moderator_token)
        if not broadcaster_id or not moderator_id or not user_id:
            return False, "Не удалось определить ID"
        url = "https://api.twitch.tv/helix/moderation/bans"
        headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {moderator_token}"}
        params = {
            "broadcaster_id": broadcaster_id,
            "moderator_id": moderator_id,
            "user_id": user_id
        }
        async with http_session.delete(url, headers=headers, params=params,
                                       timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status in (200, 204):
                return True, None
            else:
                text = await resp.text()
                return False, f"Ошибка {resp.status}: {text}"
    except Exception as e:
        return False, str(e)


class ChatReader(Client):
    def __init__(self, channel_name, bot_token, broadcaster_id, moderator_token, moderator_id):
        super().__init__(token=bot_token, initial_channels=[channel_name])
        self.channel_name = channel_name.lower()
        self._channel = None
        self._ready = asyncio.Event()
        self.last_messages = {}  # user -> {"text": "", "count": 0, "timestamp": 0, "last_msg_time": 0}
        self.user_last_message_time = defaultdict(float)
        self.bot_token = bot_token
        self.moderator_token = moderator_token
        self.broadcaster_id = broadcaster_id
        self.moderator_id = moderator_id

    async def event_ready(self):
        for channel in self.connected_channels:
            if channel.name == self.channel_name:
                self._channel = channel
                self._ready.set()
                print(f"✅ Бот в чате #{self.channel_name}")
                return
        print(f"❌ Не удалось подключиться к #{self.channel_name}")

    async def _delete_message_api(self, message_id):
        if not self.broadcaster_id or not message_id or not self.moderator_id:
            print("[DELETE] Нет broadcaster_id, moderator_id или message_id")
            return False
        try:
            url = "https://api.twitch.tv/helix/moderation/chat"
            headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {self.moderator_token}"}
            params = {
                "broadcaster_id": self.broadcaster_id,
                "moderator_id": self.moderator_id,
                "message_id": str(message_id)
            }
            async with http_session.delete(url, headers=headers, params=params,
                                           timeout=aiohttp.ClientTimeout(total=10)) as resp:
                return resp.status in (200, 204)
        except Exception as e:
            print(f"[DELETE] Ошибка: {e}")
            return False

    async def event_message(self, message):
        if message.echo:
            return
        msg_id = message.id
        log_chat_message(self.channel_name, message.author.name, message.content, msg_id)

        settings = get_settings(self.channel_name)
        print(
            f"🔧 Настройки #{self.channel_name}: caps={settings.get('remove_caps')}({settings.get('caps_percent')}%), repeats={settings.get('remove_repeats')}({settings.get('repeat_count')}), long={settings.get('remove_long')}({settings.get('max_length')})")

        is_ignored = (settings.get("ignore_broadcaster") and message.author.is_broadcaster) or \
                     (settings.get("ignore_mods") and message.author.is_mod)

        # Slow mode
        if not is_ignored:
            sm = get_slow_mode(self.channel_name)
            if sm["enabled"]:
                current_time = time.time()
                last_time = self.user_last_message_time.get(message.author.name, 0)
                if current_time - last_time < sm["interval_sec"]:
                    await self._delete_message_api(msg_id)
                    print(
                        f"🚫 Авто-удалено (slow mode, {sm['interval_sec']} сек): {message.author.name}: {message.content}")
                    if self.channel_name in channel_websockets:
                        for ws in channel_websockets[self.channel_name]:
                            try:
                                await ws.send_json({
                                    "type": "message_deleted",
                                    "message_id": str(msg_id),
                                    "user": message.author.name,
                                    "reason": f"slow_mode ({sm['interval_sec']} сек)"
                                })
                            except:
                                pass
                    return
                else:
                    self.user_last_message_time[message.author.name] = current_time

        # Чёрный список
        blacklist = get_blacklist(self.channel_name)
        if blacklist and any(word in message.content.lower() for word in blacklist):
            success = await self._delete_message_api(msg_id)
            if success:
                increment_stat(self.channel_name, "blacklist")
                print(f"🚫 Авто-удалено (чёрный список): {message.author.name}: {message.content}")
                if self.channel_name in channel_websockets:
                    for ws in channel_websockets[self.channel_name]:
                        try:
                            await ws.send_json(
                                {"type": "message_deleted", "message_id": str(msg_id), "user": message.author.name})
                        except:
                            pass
            return

        # ========= ОСНОВНЫЕ ПРАВИЛА АВТОУДАЛЕНИЯ =========
        should_delete = False
        reason = ""
        stat_name = ""

        # 1. Ссылки
        if settings.get("remove_links") and re.search(r'https?://\S+|www\.\S+', message.content):
            should_delete = True
            reason = "ссылка"
            stat_name = "links"
            print(f"[LINK] {message.author.name}: обнаружена ссылка -> УДАЛЕНИЕ")

        # 2. Капс
        elif settings.get("remove_caps"):
            letters = [c for c in message.content if c.isalpha()]
            if letters:
                upper_count = sum(1 for c in letters if c.isupper())
                percent = (upper_count / len(letters)) * 100
                caps_threshold = settings.get("caps_percent", 80)
                if percent >= caps_threshold:
                    should_delete = True
                    reason = f"капс ({percent:.0f}% > {caps_threshold}%)"
                    stat_name = "caps"
                    print(f"[CAPS] {message.author.name}: {percent:.0f}% заглавных -> УДАЛЕНИЕ")
                else:
                    print(f"[CAPS] {message.author.name}: процент {percent:.0f}% < {caps_threshold}% -> НЕ УДАЛЕНО")

        # 3. Длинные сообщения
        elif settings.get("remove_long") and len(message.content) > settings.get("max_length", 500):
            should_delete = True
            reason = f"длина {len(message.content)} > {settings['max_length']}"
            stat_name = "long"
            print(f"[LONG] {message.author.name}: длина {len(message.content)} превышает лимит -> УДАЛЕНИЕ")

        # 4. Повторы (ОТДЕЛЬНЫЙ if, НЕ elif, чтобы проверялось даже если ничего не сработало выше)
        if not should_delete and settings.get("remove_repeats"):
            user = message.author.name
            current = message.content.strip()  # убираем пробелы в начале/конце
            current_time = time.time()
            prev = self.last_messages.get(user)
            timeout = 60  # секунд, можно будет вынести в настройки БД
            if prev and (current_time - prev.get("last_msg_time", 0)) > timeout:
                print(f"[REPEAT] Сброс счётчика для {user} (таймаут {timeout} сек)")
                prev = None
            if prev and current == prev["text"]:
                count = prev["count"] + 1
                print(f"[REPEAT] {user}: повтор #{count}, текст='{current}'")
            else:
                count = 1
                print(f"[REPEAT] {user}: новое сообщение или текст изменился, сброс счётчика")
            self.last_messages[user] = {
                "text": current,
                "count": count,
                "timestamp": current_time,
                "last_msg_time": current_time
            }
            repeat_threshold = settings.get("repeat_count", 3)
            if count >= repeat_threshold:
                should_delete = True
                reason = f"повтор ({count} раз)"
                stat_name = "repeats"
                print(f"[REPEAT] ✅ УДАЛЯЕМ сообщение от {user}, счётчик {count} >= {repeat_threshold}")

        # Применяем удаление
        if should_delete:
            # Для повторов удаляем всегда; для остальных проверяем is_ignored
            if is_ignored and stat_name in ["links", "caps", "long"]:
                print(f"⏩ Игнорируем удаление {stat_name} для {message.author.name} (мод/стример)")
            else:
                success = await self._delete_message_api(msg_id)
                if success:
                    increment_stat(self.channel_name, stat_name)
                    print(f"🚫 Авто-удалено ({reason}): {message.author.name}: {message.content}")
                    if self.channel_name in channel_websockets:
                        for ws in channel_websockets[self.channel_name]:
                            try:
                                await ws.send_json(
                                    {"type": "message_deleted", "message_id": str(msg_id), "user": message.author.name})
                            except:
                                pass
                    return  # сообщение удалено, не отправляем в вебсокет

        # Если не удалено – отправляем в вебсокет
        warns = get_warns_count(self.channel_name, message.author.name)
        if self.channel_name in channel_websockets:
            for ws in channel_websockets[self.channel_name]:
                try:
                    await ws.send_json({
                        "type": "chat",
                        "user": message.author.name,
                        "text": message.content,
                        "message_id": str(msg_id),
                        "warns": warns,
                        "is_broadcaster": message.author.is_broadcaster,
                        "is_mod": message.author.is_mod,
                        "warnings": analyze_message(message.content)
                    })
                except:
                    pass

    async def send_message(self, text):
        await self._ready.wait()
        if not self._channel:
            return False
        try:
            await self._channel.send(text)
            return True
        except Exception as e:
            err_msg = str(e).lower()
            if "closing transport" not in err_msg:
                print(f"❌ Ошибка отправки сообщения: {e}")
            return False

    async def close(self):
        try:
            await super().close()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"⚠️ Ошибка при закрытии читателя: {e}")
        self._ready.clear()


async def get_user_avatar(access_token):
    try:
        url = "https://api.twitch.tv/helix/users"
        headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {access_token}"}
        async with http_session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("data"):
                    return data["data"][0].get("profile_image_url", "")
    except:
        pass
    return ""


def force_all_settings():
    """Устанавливает для всех существующих каналов значения автомодерации по умолчанию (включены)"""
    conn = get_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE settings 
            SET remove_links = 1, remove_caps = 1, remove_long = 1, remove_repeats = 1,
                ignore_broadcaster = 0, ignore_mods = 0,
                caps_percent = 80, max_length = 500, repeat_count = 3
        ''')
        cursor.execute('''
            INSERT OR IGNORE INTO settings (channel, remove_links, remove_caps, remove_long, remove_repeats,
                                            ignore_broadcaster, ignore_mods, caps_percent, max_length, repeat_count)
            SELECT DISTINCT channel, 1, 1, 1, 1, 0, 0, 80, 500, 3 FROM automod_stats
            WHERE channel NOT IN (SELECT channel FROM settings)
        ''')
        conn.commit()
        print("✅ Принудительно обновлены настройки автомодерации для ВСЕХ каналов (правила включены)")
    except Exception as e:
        print(f"❌ Ошибка при обновлении настроек: {e}")
    finally:
        conn.close()


@app.on_event("startup")
async def startup_event():
    global http_session
    http_session = aiohttp.ClientSession()
    print("✅ HTTP сессия создана")
    if not CHAT_BOT_TOKEN or not CHAT_BOT_NICK:
        print("❌ БОТ НЕ БУДЕТ РАБОТАТЬ: CHAT_BOT_TOKEN или CHAT_BOT_NICK не заданы в .env")
    else:
        print(f"🤖 Бот настроен: {CHAT_BOT_NICK}")
    force_all_settings()
    print(f"TWITCH_CLIENT_ID: {TWITCH_CLIENT_ID[:5] if TWITCH_CLIENT_ID else 'Не задан'}...")
    print(f"REDIRECT_URI: {REDIRECT_URI}")


@app.on_event("shutdown")
async def shutdown_event():
    for channel, reader in list(readers.items()):
        try:
            await reader.close()
        except:
            pass
    readers.clear()
    if http_session:
        await http_session.close()
        print("✅ HTTP сессия закрыта")
    for channel, socks in channel_websockets.items():
        for ws in socks:
            try:
                await ws.close()
            except:
                pass
    channel_websockets.clear()


@app.get("/")
async def home(request: Request):
    token = request.cookies.get("session_token")
    if token in sessions:
        try:
            with open("index.html", "r", encoding="utf-8") as f:
                return HTMLResponse(f.read())
        except FileNotFoundError:
            return HTMLResponse("<h1>index.html не найден</h1>", status_code=404)
    return RedirectResponse(url="/login")


@app.get("/login")
async def login_page():
    import urllib.parse
    scope = "chat:read chat:edit channel:moderate moderator:manage:chat_messages moderator:manage:banned_users user:read:email user:write:chat user:bot"
    url = f"https://id.twitch.tv/oauth2/authorize?client_id={TWITCH_CLIENT_ID}&redirect_uri={urllib.parse.quote(REDIRECT_URI)}&response_type=code&scope={urllib.parse.quote(scope)}"
    try:
        with open("login.html", "r", encoding="utf-8") as f:
            html = f.read()
        return HTMLResponse(html.replace("{{twitch_auth_url}}", url))
    except FileNotFoundError:
        return HTMLResponse("<h1>login.html не найден</h1>", status_code=404)


@app.get("/auth/callback")
async def auth_callback(code: str):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    "https://id.twitch.tv/oauth2/token",
                    params={
                        "client_id": TWITCH_CLIENT_ID,
                        "client_secret": TWITCH_CLIENT_SECRET,
                        "code": code,
                        "grant_type": "authorization_code",
                        "redirect_uri": REDIRECT_URI
                    },
                    timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return HTMLResponse("<h1>Ошибка авторизации</h1>", status_code=400)
                data = await resp.json()
                access_token = data.get("access_token")
                if not access_token:
                    return HTMLResponse("<h1>Токен не получен</h1>", status_code=400)

        async with aiohttp.ClientSession() as session:
            async with session.get(
                    "https://api.twitch.tv/helix/users",
                    headers={"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {access_token}"},
                    timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return HTMLResponse("<h1>Ошибка получения данных пользователя</h1>", status_code=400)
                user_data = await resp.json()
                user = user_data["data"][0]

        if ALLOWED_TWITCH_ID and user["id"] != ALLOWED_TWITCH_ID:
            return HTMLResponse(
                "<h1>⛔ Доступ запрещён</h1>"
                "<p>Этот сайт предназначен только для определённого Twitch‑аккаунта.</p>"
                "<a href='/login'>Попробовать другой аккаунт</a>",
                status_code=403
            )

        token = secrets.token_urlsafe(32)
        avatar = await get_user_avatar(access_token)
        sessions[token] = {
            "id": user["id"],
            "username": user["login"],
            "display_name": user["display_name"],
            "access_token": access_token,
            "avatar": avatar
        }
        print(f"✅ Создана сессия для модератора ID: {user['id']} (ник: {user['login']})")
        response = RedirectResponse(url="/", status_code=303)
        response.set_cookie(key="session_token", value=token, httponly=True, samesite="Lax")
        return response
    except Exception as e:
        print(f"❌ Ошибка в auth_callback: {e}")
        return HTMLResponse(f"<h1>Ошибка: {e}</h1>", status_code=500)


@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie("session_token")
    return response


@app.get("/auth/me")
async def auth_me(request: Request):
    token = request.cookies.get("session_token")
    if token in sessions:
        user = sessions[token]
        return {
            "display_name": user["display_name"],
            "avatar": user.get("avatar", ""),
            "id": user["id"]
        }
    return {"error": "Unauthorized"}, 401


@app.get("/api/channels")
async def get_user_channels(request: Request):
    return {"channels": []}


@app.get("/api/stream/{channel}")
async def get_stream_status(channel: str, request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"is_live": False}
    try:
        access_token = sessions[token]["access_token"]
        url = f"https://api.twitch.tv/helix/streams?user_login={channel}"
        headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {access_token}"}
        async with http_session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("data"):
                    stream = data["data"][0]
                    return {
                        "is_live": True,
                        "started_at": stream["started_at"],
                        "viewers": stream["viewer_count"],
                        "title": stream["title"]
                    }
        return {"is_live": False, "started_at": None}
    except:
        return {"is_live": False}


@app.get("/api/settings/{channel}")
async def get_settings_api(channel: str, request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"error": "Unauthorized"}, 401
    return get_settings(channel)


@app.post("/api/settings/{channel}")
async def update_settings_api(channel: str, request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"error": "Unauthorized"}, 401
    data = await request.json()
    update_settings(channel, data)
    return {"success": True}


@app.post("/api/blacklist/add")
async def add_blacklist(request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"error": "Unauthorized"}, 401
    data = await request.json()
    add_blacklist_word(data["channel"], data["word"], sessions[token]["display_name"])
    return {"success": True}


@app.post("/api/blacklist/remove")
async def remove_blacklist(request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"error": "Unauthorized"}, 401
    data = await request.json()
    remove_blacklist_word(data["channel"], data["word"])
    return {"success": True}


@app.get("/api/blacklist/{channel}")
async def get_blacklist_api(channel: str, request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"words": []}
    return {"words": get_blacklist(channel)}


@app.get("/api/banned/{channel}")
async def get_banned_api(channel: str, request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"banned": []}
    banned_list = get_banned(channel)
    formatted = [[b["username"], b["reason"] or "", b["banned_by"] or "", b["banned_at"]] for b in banned_list]
    return {"banned": formatted}


@app.get("/api/automod_stats/{channel}")
async def get_automod_stats(channel: str, request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"error": "Unauthorized"}, 401
    return get_stats(channel)


@app.get("/api/manual_stats/{channel}")
async def get_manual_stats_api(channel: str, request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"error": "Unauthorized"}, 401
    moderator_id = sessions[token]["id"]
    stats = get_moderator_stats(channel, moderator_id)
    return {
        "bans": stats["bans"],
        "deletions": stats["deletions"],
        "moderator_id": moderator_id
    }


@app.post("/api/reset_stats/{channel}")
async def reset_my_stats(channel: str, request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"error": "Unauthorized"}, 401
    moderator_id = sessions[token]["id"]
    conn = get_connection()
    if not conn:
        return {"error": "DB error"}, 500
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM moderator_stats WHERE channel = ? AND moderator_id = ?",
                       (channel.lower(), moderator_id))
        conn.commit()
        return {"success": True, "message": "Статистика сброшена"}
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        conn.close()


@app.post("/api/reset_all_stats/{channel}")
async def reset_all_stats(channel: str, request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"error": "Unauthorized"}, 401
    conn = get_connection()
    if not conn:
        return {"error": "DB error"}, 500
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE automod_stats SET links = 0, caps = 0, long = 0, repeats = 0, blacklist = 0, total = 0 WHERE channel = ?",
            (channel.lower(),)
        )
        cursor.execute("DELETE FROM moderator_stats WHERE channel = ?", (channel.lower(),))
        conn.commit()
        return {"success": True, "message": "Вся статистика канала сброшена"}
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        conn.close()


@app.post("/api/notes/add")
async def add_note_api(request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"error": "Unauthorized"}, 401
    try:
        data = await request.json()
        user_id = sessions[token]["id"]
        content = data.get("content", "").strip()
        if not content:
            return {"error": "Empty content"}, 400
        success = add_note(user_id, content)
        return {"success": success}
    except Exception as e:
        print(f"❌ Ошибка add_note_api: {e}")
        return {"error": str(e)}, 500


@app.get("/api/notes")
async def get_notes_api(request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"notes": []}
    try:
        user_id = sessions[token]["id"]
        notes = get_notes(user_id)
        formatted = [[note["id"], note["content"], note["created_at"]] for note in notes]
        return {"notes": formatted}
    except Exception as e:
        print(f"❌ Ошибка get_notes_api: {e}")
        return {"notes": []}


@app.post("/api/notes/delete")
async def delete_note_api(request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"error": "Unauthorized"}, 401
    try:
        data = await request.json()
        note_id = data.get("note_id")
        if not note_id:
            return {"error": "No note_id"}, 400
        success = delete_note(note_id)
        return {"success": success}
    except Exception as e:
        print(f"❌ Ошибка delete_note_api: {e}")
        return {"error": str(e)}, 500


@app.get("/api/slow_mode/{channel}")
async def get_slow_mode_api(channel: str, request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"error": "Unauthorized"}, 401
    return get_slow_mode(channel)


@app.post("/api/slow_mode/{channel}")
async def update_slow_mode_api(channel: str, request: Request):
    token = request.cookies.get("session_token")
    if token not in sessions:
        return {"error": "Unauthorized"}, 401
    data = await request.json()
    enabled = data.get("enabled", False)
    interval_sec = data.get("interval_sec", 5)
    if interval_sec < 1:
        interval_sec = 1
    if interval_sec > 300:
        interval_sec = 300
    success = update_slow_mode(channel, enabled, interval_sec)
    return {"success": success}


@app.websocket("/ws/{channel}")
async def websocket_endpoint(websocket: WebSocket, channel: str):
    await websocket.accept()
    channel = channel.lower()
    print(f"🔌 Подключён к #{channel}")

    cookie_header = websocket.headers.get("cookie", "")
    session_token = None
    for cookie in cookie_header.split(";"):
        if "session_token=" in cookie:
            session_token = cookie.split("=")[1].strip()
            break

    if not session_token or session_token not in sessions:
        await websocket.close(code=1008, reason="Unauthorized")
        return

    moderator = sessions[session_token]
    moderator_token = moderator["access_token"]
    broadcaster_id = await get_user_id(channel, moderator_token)
    if not broadcaster_id:
        print(f"❌ Не удалось получить broadcaster_id для #{channel}")
        await websocket.close(code=1008, reason="Broadcaster not found")
        return

    moderator_id = await get_user_id_from_token(moderator_token)
    if not moderator_id:
        print(f"❌ Не удалось получить moderator_id для #{channel}")
        await websocket.close(code=1008, reason="Cannot determine moderator ID")
        return

    if channel not in channel_websockets:
        channel_websockets[channel] = []
    channel_websockets[channel].append(websocket)

    if channel not in readers:
        async with reader_creation_locks[channel]:
            if channel not in readers:
                readers[channel] = ChatReader(channel, CHAT_BOT_TOKEN, broadcaster_id, moderator_token, moderator_id)
                asyncio.create_task(readers[channel].start())
                await asyncio.sleep(2)

    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")

            if action == "auth":
                await websocket.send_json({"type": "auth_ok"})
            elif action == "send_message":
                text = data.get("text", "").strip()
                if not text:
                    await websocket.send_json({"type": "error", "message": "Сообщение пусто"})
                    continue
                if channel in readers:
                    success = await readers[channel].send_message(text)
                    await websocket.send_json({"type": "success" if success else "error",
                                               "message": "Сообщение отправлено" if success else "Ошибка отправки"})
            elif action == "send_own_message":
                text = data.get("text", "").strip()
                if not text:
                    await websocket.send_json({"type": "error", "message": "Сообщение пусто"})
                    continue
                success, err_msg = await send_message_as_user(channel, moderator_token, text)
                if success:
                    await websocket.send_json({"type": "success", "message": "Сообщение отправлено"})
                else:
                    await websocket.send_json({"type": "error", "message": err_msg})
            elif action == "delete":
                message_id = data.get("message_id")
                if not message_id:
                    await websocket.send_json({"type": "error", "message": "Нет ID сообщения"})
                    continue
                success, err_msg = await delete_message(channel, message_id, moderator_token)
                if success:
                    increment_manual_stat(channel, "deletions")
                    increment_moderator_stat(channel, moderator_id, "deletions")
                    await websocket.send_json({"type": "success", "deleted": True})
                else:
                    await websocket.send_json({"type": "error", "message": err_msg})
            elif action == "ban":
                user = data.get("user", "").strip()
                reason = data.get("reason", "").strip()
                if not user:
                    await websocket.send_json({"type": "error", "message": "Нет пользователя"})
                    continue
                success, err_msg = await send_real_ban(channel, user, moderator_token, reason)
                if success:
                    increment_manual_stat(channel, "bans")
                    increment_moderator_stat(channel, moderator_id, "bans")
                    add_banned(channel, user, reason, moderator["display_name"])
                    await websocket.send_json({"type": "success", "message": f"Бан {user}"})
                else:
                    await websocket.send_json({"type": "error", "message": err_msg})
            elif action == "unban":
                user = data.get("user", "").strip()
                if not user:
                    await websocket.send_json({"type": "error", "message": "Нет пользователя"})
                    continue
                success, err_msg = await send_real_unban(channel, user, moderator_token)
                if success:
                    remove_banned(channel, user)
                    await websocket.send_json({"type": "success", "message": f"Разбан {user}"})
                else:
                    if "not banned" in err_msg.lower():
                        remove_banned(channel, user)
                        await websocket.send_json({"type": "success",
                                                   "message": f"Пользователь {user} не был забанен в Twitch, запись удалена из списка"})
                    else:
                        await websocket.send_json({"type": "error", "message": err_msg})
    except WebSocketDisconnect:
        print(f"❌ Отключён от #{channel}")
        if channel in channel_websockets and websocket in channel_websockets[channel]:
            channel_websockets[channel].remove(websocket)
        if channel in channel_websockets and not channel_websockets[channel]:
            reader = readers.get(channel)
            if reader:
                await reader.close()
                del readers[channel]
            if channel in reader_creation_locks:
                del reader_creation_locks[channel]
    except Exception as e:
        print(f"❌ Ошибка WebSocket #{channel}: {e}")
        if channel in channel_websockets and websocket in channel_websockets[channel]:
            channel_websockets[channel].remove(websocket)
        if channel in channel_websockets and not channel_websockets[channel]:
            reader = readers.get(channel)
            if reader:
                await reader.close()
                del readers[channel]
