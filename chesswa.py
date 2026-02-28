import os
import json
import time
import hmac
import secrets
import sqlite3
import asyncio
from typing import Any, Dict, Optional, Set, List, Tuple

import chess
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Depends, Header
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# =========================================================
# ENV
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEB_SECRET_KEY = os.getenv("WEB_SECRET_KEY", "")
DB_PATH = os.getenv("DB_PATH", "./database.db")
HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("HTTP_PORT", "8000"))
WEBAPP_URL = os.getenv("WEBAPP_URL", "")

if not WEB_SECRET_KEY:
    raise RuntimeError("WEB_SECRET_KEY is empty")

# =========================================================
# FastAPI
# =========================================================
app = FastAPI()

# CORS: GitHub Pages + Telegram WebView
# (Ты можешь ужесточить список, но сейчас важнее чтобы OPTIONS не падал)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# DB helpers
# =========================================================
def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def now_ts() -> int:
    return int(time.time())

def init_db() -> None:
    with db() as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS tg_users (
            tag TEXT PRIMARY KEY,
            tg_id INTEGER,
            tg_username TEXT,
            created_ts INTEGER
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            tag TEXT PRIMARY KEY,
            nickname TEXT NOT NULL,
            gender TEXT NOT NULL,
            skill INTEGER NOT NULL,
            banned INTEGER NOT NULL DEFAULT 0,
            created_ts INTEGER NOT NULL
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            tag TEXT NOT NULL,
            created_ts INTEGER NOT NULL,
            expires_ts INTEGER NOT NULL
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS pending_codes (
            tag TEXT PRIMARY KEY,
            code TEXT NOT NULL,
            purpose TEXT NOT NULL, -- 'login' / 'register'
            created_ts INTEGER NOT NULL,
            expires_ts INTEGER NOT NULL
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS lobby_online (
            tag TEXT PRIMARY KEY,
            last_ts INTEGER NOT NULL
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS invites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_tag TEXT NOT NULL,
            to_tag TEXT NOT NULL,
            minutes INTEGER NOT NULL,
            created_ts INTEGER NOT NULL
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS games (
            game_id TEXT PRIMARY KEY,
            w_tag TEXT NOT NULL,
            b_tag TEXT NOT NULL,
            minutes INTEGER NOT NULL,
            status TEXT NOT NULL, -- 'active'/'finished'
            fen TEXT NOT NULL,
            turn TEXT NOT NULL, -- 'w'/'b'

            coins2_w INTEGER NOT NULL,
            coins2_b INTEGER NOT NULL,

            clock_w_rem INTEGER NOT NULL,
            clock_b_rem INTEGER NOT NULL,
            server_ts INTEGER NOT NULL,

            post_buy_extra2_w INTEGER NOT NULL,
            post_buy_extra2_b INTEGER NOT NULL,

            moved_this_turn INTEGER NOT NULL,
            bought_this_turn INTEGER NOT NULL,
            bought_after_move INTEGER NOT NULL,

            consecutive_pass_w INTEGER NOT NULL,
            consecutive_pass_b INTEGER NOT NULL,

            result_json TEXT NOT NULL
        )
        """)
        con.commit()

@app.on_event("startup")
def _startup():
    init_db()

# =========================================================
# Security: API key + session token
# =========================================================
def api_key_dep(x_api_key: Optional[str] = Header(default=None)) -> None:
    if not x_api_key:
        raise JSONResponse({"ok": False, "reason": "no_api_key"}, status_code=401)
    if not hmac.compare_digest(x_api_key, WEB_SECRET_KEY):
        raise JSONResponse({"ok": False, "reason": "bad_api_key"}, status_code=401)

def load_session_tag(token: str) -> Optional[str]:
    if not token:
        return None
    with db() as con:
        row = con.execute("SELECT tag, expires_ts FROM sessions WHERE token=?", (token,)).fetchone()
    if not row:
        return None
    if int(row["expires_ts"]) < now_ts():
        return None
    return str(row["tag"])

def session_dep(
    _: None = Depends(api_key_dep),
    authorization: Optional[str] = Header(default=None),
):
    # Authorization: Bearer <token>
    token = ""
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
    tag = load_session_tag(token)
    if not tag:
        raise JSONResponse({"ok": False, "reason": "no_session"}, status_code=401)
    return tag

# =========================================================
# Users helpers
# =========================================================
def norm_tag(tag: str) -> str:
    t = (tag or '').strip()
    if not t.startswith('@'):
        t = '@' + t
    return t.lower()


def get_user(tag: str) -> Optional[sqlite3.Row]:
    with db() as con:
        return con.execute("SELECT * FROM users WHERE tag=?", (tag,)).fetchone()

def has_tg_user(tag: str) -> bool:
    with db() as con:
        r = con.execute("SELECT tag FROM tg_users WHERE tag=?", (tag,)).fetchone()
    return bool(r)


def get_tg_id(tag: str) -> Optional[int]:
    with db() as con:
        r = con.execute('SELECT tg_id FROM tg_users WHERE tag=?', (tag,)).fetchone()
    if not r:
        return None
    try:
        return int(r['tg_id'])
    except Exception:
        return None

def set_pending_code(tag: str, purpose: str) -> str:
    code = f"{secrets.randbelow(1000000):06d}"
    now = now_ts()
    exp = now + 10 * 60
    with db() as con:
        con.execute("""
        INSERT INTO pending_codes(tag, code, purpose, created_ts, expires_ts)
        VALUES (?,?,?,?,?)
        ON CONFLICT(tag) DO UPDATE SET
            code=excluded.code,
            purpose=excluded.purpose,
            created_ts=excluded.created_ts,
            expires_ts=excluded.expires_ts
        """, (tag, code, purpose, now, exp))
        con.commit()
    return code

def check_pending_code(tag: str, code: str, purpose: str) -> bool:
    with db() as con:
        row = con.execute("SELECT * FROM pending_codes WHERE tag=?", (tag,)).fetchone()
    if not row:
        return False
    if str(row["purpose"]) != purpose:
        return False
    if str(row["code"]) != str(code):
        return False
    if int(row["expires_ts"]) < now_ts():
        return False
    return True

def create_session(tag: str) -> str:
    token = secrets.token_urlsafe(24)
    now = now_ts()
    exp = now + 14 * 24 * 3600
    with db() as con:
        con.execute("INSERT INTO sessions(token, tag, created_ts, expires_ts) VALUES (?,?,?,?)",
                    (token, tag, now, exp))
        con.commit()
    return token

# =========================================================
# Telegram integration (минимально)
# Ты уже пишешь tg_users через бота /start. Здесь это не трогаем.
# =========================================================

# =========================================================
# Game rules (Economy Chess)
# =========================================================
START_FEN_KINGS_ONLY = "4k3/8/8/8/8/8/8/4K3 w - - 0 1"

# costs in half-coins (2 = 1.0 coin)
BASE_COSTS2 = {
    "p": 2,   # 1.0
    "n": 5,   # 2.5
    "b": 7,   # 3.5
    "r": 9,   # 4.5
    "q": 18,  # 9.0
}

def piece_type_from_char(ch: str) -> Optional[int]:
    m = {"p": chess.PAWN, "n": chess.KNIGHT, "b": chess.BISHOP, "r": chess.ROOK, "q": chess.QUEEN, "k": chess.KING}
    return m.get(ch.lower())

def update_clocks_inplace(row: Dict[str, Any]) -> None:
    if row["status"] != "active":
        return
    now = now_ts()
    last = int(row["server_ts"])
    if now <= last:
        return
    dt = now - last
    if row["turn"] == "w":
        row["clock_w_rem"] = max(0, int(row["clock_w_rem"]) - dt)
    else:
        row["clock_b_rem"] = max(0, int(row["clock_b_rem"]) - dt)
    row["server_ts"] = now

def finish_if_needed_inplace(row: Dict[str, Any]) -> None:
    if row["status"] != "active":
        return
    # timeouts
    if int(row["clock_w_rem"]) <= 0:
        row["status"] = "finished"
        row["result_json"] = json.dumps({"winner": "b", "reason": "timeout"})
        return
    if int(row["clock_b_rem"]) <= 0:
        row["status"] = "finished"
        row["result_json"] = json.dumps({"winner": "w", "reason": "timeout"})
        return

    board = chess.Board(row["fen"])
    if board.is_checkmate():
        winner = "b" if board.turn == chess.WHITE else "w"
        row["status"] = "finished"
        row["result_json"] = json.dumps({"winner": winner, "reason": "checkmate"})
        return
    if board.is_stalemate():
        row["status"] = "finished"
        row["result_json"] = json.dumps({"winner": "", "reason": "stalemate"})
        return

def load_game_row(con: sqlite3.Connection, gid: str) -> Optional[sqlite3.Row]:
    return con.execute("SELECT * FROM games WHERE game_id=?", (gid,)).fetchone()

def row_to_game(row: sqlite3.Row, viewer_tag: str) -> Dict[str, Any]:
    you = "w" if row["w_tag"] == viewer_tag else ("b" if row["b_tag"] == viewer_tag else "")
    return {
        "game_id": row["game_id"],
        "w_tag": row["w_tag"],
        "b_tag": row["b_tag"],
        "you": you,
        "minutes": int(row["minutes"]),
        "status": row["status"],
        "fen": row["fen"],
        "turn": row["turn"],
        "coins2_w": int(row["coins2_w"]),
        "coins2_b": int(row["coins2_b"]),
        "clock_w_rem": int(row["clock_w_rem"]),
        "clock_b_rem": int(row["clock_b_rem"]),
        "server_ts": int(row["server_ts"]),
        "post_buy_extra2": {"w": int(row["post_buy_extra2_w"]), "b": int(row["post_buy_extra2_b"])},
        "result": json.loads(row["result_json"]) if row["result_json"] else {},
    }

def create_game(w_tag: str, b_tag: str, minutes: int) -> str:
    gid = secrets.token_urlsafe(12)
    total = max(60, int(minutes) * 60)
    with db() as con:
        con.execute("""
        INSERT INTO games(
            game_id, w_tag, b_tag, minutes, status, fen, turn,
            coins2_w, coins2_b,
            clock_w_rem, clock_b_rem, server_ts,
            post_buy_extra2_w, post_buy_extra2_b,
            moved_this_turn, bought_this_turn, bought_after_move,
            consecutive_pass_w, consecutive_pass_b,
            result_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            gid, w_tag, b_tag, int(minutes), "active",
            START_FEN_KINGS_ONLY, "w",
            10, 10,  # старт: 5 монет каждому
            total, total, now_ts(),
            0, 0,
            0, 0, 0,
            0, 0,
            ""
        ))
        con.commit()
    return gid

def behind_pawn_square(board: chess.Board, color: bool, file_idx: int) -> Optional[chess.Square]:
    # найдём пешку данного цвета на этом файле (берём самую "ближнюю" к противнику)
    pawns = board.pieces(chess.PAWN, color)
    candidates: List[chess.Square] = [sq for sq in pawns if chess.square_file(sq) == file_idx]
    if not candidates:
        return None
    # для белых "вперёд" = рост ранга. Берём пешку с максимальным рангом (самая продвинутая)
    # для чёрных берём с минимальным рангом
    if color == chess.WHITE:
        pawn_sq = max(candidates, key=lambda s: chess.square_rank(s))
        back_rank = chess.square_rank(pawn_sq) - 1
    else:
        pawn_sq = min(candidates, key=lambda s: chess.square_rank(s))
        back_rank = chess.square_rank(pawn_sq) + 1

    if back_rank < 0 or back_rank > 7:
        return None
    return chess.square(file_idx, back_rank)

def legal_drop_targets(board: chess.Board, you_color: bool, piece_ch: str) -> Set[str]:
    piece_ch = piece_ch.lower().strip()
    targets: Set[str] = set()

    # если шах — запрещаем покупку, которая "закроет" шах (т.е. после покупки шах исчезнет)
    in_check = board.is_check()

    if piece_ch == "p":
        # пешки только на стартовую линию (2 для белых / 7 для чёрных)
        rank = 1 if you_color == chess.WHITE else 6
        for f in range(8):
            sq = chess.square(f, rank)
            if board.piece_at(sq) is not None:
                continue
            if in_check:
                b2 = board.copy(stack=False)
                b2.set_piece_at(sq, chess.Piece(chess.PAWN, you_color))
                # если шах исчез — запрещаем
                if not b2.is_check():
                    continue
            targets.add(chess.square_name(sq))
        return targets

    if piece_ch in ("n", "b", "r", "q"):
        # обычные фигуры: только "за каждой пешкой" (клетка позади пешки), динамически
        for f in range(8):
            sq = behind_pawn_square(board, you_color, f)
            if sq is None:
                continue
            if board.piece_at(sq) is not None:
                continue
            if in_check:
                b2 = board.copy(stack=False)
                ptype = piece_type_from_char(piece_ch)
                b2.set_piece_at(sq, chess.Piece(ptype, you_color))
                if not b2.is_check():
                    continue
            targets.add(chess.square_name(sq))
        return targets

    return targets

def capture_bonus2(mover: chess.Piece, captured: chess.Piece) -> int:
    # half-coins
    mover_t = mover.piece_type
    cap_t = captured.piece_type

    # captured pawn
    if cap_t == chess.PAWN:
        if mover_t == chess.PAWN:
            return 1   # +0.5
        if mover_t == chess.KING:
            return 1   # +0.5 (итого за ход будет +1 +0.5 = +1.5)
        return 0       # остальные: без бонуса за пешку

    # captured not pawn
    if mover_t == chess.PAWN:
        return 3       # +1.5
    if mover_t == chess.KING:
        return 4       # +2.0 (итого за ход +1 +2 = +3)
    return 2           # +1.0 (остальные фигуры)

# =========================================================
# WebSocket hubs
# =========================================================
class LobbyHub:
    def __init__(self):
        self.conns: Dict[str, Set[WebSocket]] = {}
        self.lock = asyncio.Lock()

    async def add(self, tag: str, ws: WebSocket):
        async with self.lock:
            self.conns.setdefault(tag, set()).add(ws)

    async def remove(self, tag: str, ws: WebSocket):
        async with self.lock:
            if tag in self.conns and ws in self.conns[tag]:
                self.conns[tag].remove(ws)
            if tag in self.conns and not self.conns[tag]:
                del self.conns[tag]

    async def send_to(self, tag: str, payload: Dict[str, Any]):
        async with self.lock:
            sockets = list(self.conns.get(tag, set()))
        for ws in sockets:
            try:
                await ws.send_text(json.dumps(payload, ensure_ascii=False))
            except Exception:
                pass

    async def broadcast_online(self):
        # онлайн = кто держит ws и шлёт heartbeat
        with db() as con:
            rows = con.execute("SELECT tag FROM lobby_online WHERE last_ts>=?", (now_ts() - 40,)).fetchall()
        tags = [r["tag"] for r in rows]
        online = []
        for t in tags:
            u = get_user(t)
            if u and int(u["banned"]) == 0:
                online.append({"tag": t, "nickname": u["nickname"]})
        # всем, кто в хабе
        async with self.lock:
            all_tags = list(self.conns.keys())
        for t in all_tags:
            await self.send_to(t, {"type": "online", "data": {"online": online}})

class GameHub:
    def __init__(self):
        self.conns: Dict[str, Set[WebSocket]] = {}
        self.lock = asyncio.Lock()

    async def add(self, gid: str, ws: WebSocket):
        async with self.lock:
            self.conns.setdefault(gid, set()).add(ws)

    async def remove(self, gid: str, ws: WebSocket):
        async with self.lock:
            if gid in self.conns and ws in self.conns[gid]:
                self.conns[gid].remove(ws)
            if gid in self.conns and not self.conns[gid]:
                del self.conns[gid]

    async def push(self, gid: str, payload: Dict[str, Any]):
        async with self.lock:
            sockets = list(self.conns.get(gid, set()))
        for ws in sockets:
            try:
                await ws.send_text(json.dumps(payload, ensure_ascii=False))
            except Exception:
                pass

lobby_hub = LobbyHub()
game_hub = GameHub()

# =========================================================
# AUTH API
# =========================================================
@app.post("/api/auth/register_start")
async def register_start(req: Request, _: None = Depends(api_key_dep)):
    body = await req.json()
    tag = norm_tag(body.get("tag", ""))
    gender = (body.get("gender") or "").strip().lower()
    skill = int(body.get("skill") or 0)

    if not tag or gender not in ("m", "f") or skill not in (1, 2, 3, 4, 5):
        return JSONResponse({"ok": False, "reason": "bad_fields"}, status_code=400)

    if not has_tg_user(tag):
        return JSONResponse({"ok": False, "reason": "tg_not_started"}, status_code=400)

    u = get_user(tag)
    if u:
        return JSONResponse({"ok": False, "reason": "already_registered"}, status_code=400)

    code = set_pending_code(tag, "register")

    # отправляем код в Telegram
    with db() as con:
        tg_row = con.execute("SELECT tg_id FROM tg_users WHERE tag=?", (tag,)).fetchone()

    if tg_row and tg_row["tg_id"]:
        await tg_send(int(tg_row["tg_id"]), f"Код регистрации: {code}")

    return JSONResponse({"ok": True, "sent": True})

@app.post("/api/auth/login_start")
async def login_start(req: Request, _: None = Depends(api_key_dep)):
    body = await req.json()
    tag = norm_tag(body.get("tag", ""))

    if not has_tg_user(tag):
        return JSONResponse({"ok": False, "reason": "tg_not_started"}, status_code=400)

    u = get_user(tag)
    if not u:
        return JSONResponse({"ok": False, "reason": "not_registered"}, status_code=400)

    if int(u["banned"]) == 1:
        return JSONResponse({"ok": False, "reason": "banned"}, status_code=403)

    code = set_pending_code(tag, "login")

    with db() as con:
        tg_row = con.execute("SELECT tg_id FROM tg_users WHERE tag=?", (tag,)).fetchone()

    if tg_row and tg_row["tg_id"]:
        await tg_send(int(tg_row["tg_id"]), f"Код входа: {code}")

    return JSONResponse({"ok": True, "sent": True})

@app.post("/api/auth/verify")
async def auth_verify(req: Request, _: None = Depends(api_key_dep)):
    body = await req.json()

    tag = norm_tag(body.get("tag", ""))
    code = (body.get("code") or "").strip()
    raw_mode = (body.get("mode") or "").strip().lower()

    # --- НОРМАЛИЗАЦИЯ MODE ---
    if raw_mode in ("login", "login_start"):
        mode = "login"
    elif raw_mode in ("register", "register_start"):
        mode = "register"
    else:
        return JSONResponse({"ok": False, "reason": "bad_mode"}, status_code=400)

    if not tag or not code:
        return JSONResponse({"ok": False, "reason": "bad_input"}, status_code=400)

    # --- ПРОВЕРКА КОДА ---
    # проверяем код независимо от мелких расхождений
    if not check_pending_code(tag, code, mode):
        return JSONResponse({"ok": False, "reason": "bad_code"}, status_code=400)

    # --- РЕГИСТРАЦИЯ ---
    if mode == "register":
        nickname = (body.get("nickname") or "").strip()
        gender = (body.get("gender") or "").strip().lower()
        skill = int(body.get("skill") or 0)

        if not nickname or gender not in ("m", "f") or skill not in (1, 2, 3, 4, 5):
            return JSONResponse({"ok": False, "reason": "bad_fields"}, status_code=400)

        # если уже зарегистрирован
        if get_user(tag):
            return JSONResponse({"ok": False, "reason": "already_registered"}, status_code=400)

        with db() as con:
            con.execute(
                "INSERT INTO users(tag, nickname, gender, skill, banned, created_ts) VALUES (?,?,?,?,?,?)",
                (tag, nickname, gender, skill, 0, now_ts())
            )
            con.commit()

        # бан сразу после создания
        if gender == "f":
            with db() as con:
                con.execute("UPDATE users SET banned=1 WHERE tag=?", (tag,))
                con.commit()

            return JSONResponse({
                "ok": False,
                "reason": "banned_now",
                "ban_text": "Ты выбрала женский пол — теперь ты в бане. По разбану пиши @Kain_cr."
            }, status_code=403)

    # --- ЛОГИН / ФИНАЛ ---
    u = get_user(tag)

    if not u:
        return JSONResponse({"ok": False, "reason": "no_user"}, status_code=400)

    if int(u["banned"]) == 1:
        return JSONResponse({"ok": False, "reason": "banned"}, status_code=403)

    token = create_session(tag)

    return JSONResponse({
        "ok": True,
        "token": token,
        "user": {
            "tag": tag,
            "nickname": u["nickname"]
        }
    })
# =========================================================
# LOBBY API
# =========================================================
@app.post("/api/lobby/online")
async def lobby_online(tag: str = Depends(session_dep)):
    # обновим last_ts (heartbeat)
    with db() as con:
        con.execute("""
        INSERT INTO lobby_online(tag, last_ts) VALUES (?,?)
        ON CONFLICT(tag) DO UPDATE SET last_ts=excluded.last_ts
        """, (tag, now_ts()))
        con.commit()

    with db() as con:
        rows = con.execute("SELECT tag FROM lobby_online WHERE last_ts>=?", (now_ts() - 40,)).fetchall()

    online = []
    for r in rows:
        t = r["tag"]
        u = get_user(t)
        if u and int(u["banned"]) == 0:
            online.append({"tag": t, "nickname": u["nickname"]})
    return JSONResponse({"ok": True, "online": online})

@app.post("/api/lobby/invite")
async def lobby_invite(req: Request, tag: str = Depends(session_dep)):
    body = await req.json()
    to_tag = norm_tag(body.get("to_tag", ""))
    minutes = int(body.get("minutes") or 10)
    if minutes not in (10, 30, 60):
        minutes = 10

    if to_tag == tag:
        return JSONResponse({"ok": False, "reason": "self"}, status_code=400)

    u = get_user(to_tag)
    if not u:
        return JSONResponse({"ok": False, "reason": "no_user"}, status_code=400)
    if int(u["banned"]) == 1:
        return JSONResponse({"ok": False, "reason": "banned_user"}, status_code=400)

    with db() as con:
        con.execute("INSERT INTO invites(from_tag, to_tag, minutes, created_ts) VALUES (?,?,?,?)",
                    (tag, to_tag, minutes, now_ts()))
        con.commit()

    me = get_user(tag)
    from_nick = me["nickname"] if me else tag

    await lobby_hub.send_to(to_tag, {"type": "invite", "data": {"from_tag": tag, "from_nick": from_nick, "minutes": minutes}})
    return JSONResponse({"ok": True})

@app.post("/api/lobby/invite_accept")
async def lobby_invite_accept(req: Request, tag: str = Depends(session_dep)):
    body = await req.json()
    from_tag = norm_tag(body.get("from_tag", ""))
    minutes = int(body.get("minutes") or 10)
    if minutes not in (10, 30, 60):
        minutes = 10

    # создаём игру
    gid = create_game(from_tag, tag, minutes)

    await lobby_hub.send_to(from_tag, {"type": "game_created", "data": {"game_id": gid}})
    await lobby_hub.send_to(tag, {"type": "game_created", "data": {"game_id": gid}})
    return JSONResponse({"ok": True, "game_id": gid})

@app.post("/api/lobby/invite_decline")
async def lobby_invite_decline(req: Request, tag: str = Depends(session_dep)):
    body = await req.json()
    from_tag = norm_tag(body.get("from_tag", ""))
    await lobby_hub.send_to(from_tag, {"type": "invite_declined", "data": {"to_tag": tag}})
    return JSONResponse({"ok": True})

# =========================================================
# GAME API
# =========================================================
@app.post("/api/game/get")
async def game_get(req: Request, tag: str = Depends(session_dep)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()

    with db() as con:
        row = load_game_row(con, gid)
    if not row:
        return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
    if tag not in (row["w_tag"], row["b_tag"]):
        return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

    return JSONResponse({"ok": True, "game": row_to_game(row, tag)})

@app.post("/api/game/legals")
async def game_legals(req: Request, tag: str = Depends(session_dep)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()
    frm = (body.get("from") or "").strip()

    with db() as con:
        row = load_game_row(con, gid)
    if not row:
        return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)

    you = "w" if row["w_tag"] == tag else "b"
    if row["status"] != "active" or row["turn"] != you:
        return JSONResponse({"ok": True, "dests": []})

    board = chess.Board(row["fen"])
    # проверка принадлежности фигуры
    try:
        from_sq = chess.parse_square(frm)
    except Exception:
        return JSONResponse({"ok": True, "dests": []})

    pc = board.piece_at(from_sq)
    if not pc:
        return JSONResponse({"ok": True, "dests": []})
    if pc.color != (you == "w"):
        return JSONResponse({"ok": True, "dests": []})

    dests = []
    for mv in board.legal_moves:
        if mv.from_square == from_sq:
            dests.append(chess.square_name(mv.to_square))
    return JSONResponse({"ok": True, "dests": dests})

@app.post("/api/game/move")
async def game_move(req: Request, tag: str = Depends(session_dep)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()
    uci = (body.get("uci") or "").strip()

    with db() as con:
        row_db = load_game_row(con, gid)
        if not row_db:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row_db["w_tag"], row_db["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        row = dict(row_db)
        you = "w" if row["w_tag"] == tag else "b"
        if row["status"] != "active" or row["turn"] != you:
            return JSONResponse({"ok": False, "reason": "not_your_turn"}, status_code=400)

        update_clocks_inplace(row)
        finish_if_needed_inplace(row)
        if row["status"] != "active":
            con.execute("UPDATE games SET status=?, clock_w_rem=?, clock_b_rem=?, server_ts=?, result_json=? WHERE game_id=?",
                        (row["status"], row["clock_w_rem"], row["clock_b_rem"], row["server_ts"], row["result_json"], gid))
            con.commit()
            row2 = load_game_row(con, gid)
            gpub = row_to_game(row2, tag)
            await game_hub.push(gid, {"type": "finished", "data": gpub})
            return JSONResponse({"ok": True, "game": gpub})

        board = chess.Board(row["fen"])

        try:
            mv = chess.Move.from_uci(uci)
        except Exception:
            return JSONResponse({"ok": False, "reason": "bad_uci"}, status_code=400)

        if mv not in board.legal_moves:
            return JSONResponse({"ok": False, "reason": "illegal"}, status_code=400)

        mover = board.piece_at(mv.from_square)
        captured = None

        # корректно определим взятую фигуру (включая en-passant)
        if board.is_en_passant(mv):
            cap_sq = chess.square(chess.square_file(mv.to_square), chess.square_rank(mv.from_square))
            captured = board.piece_at(cap_sq)
        else:
            captured = board.piece_at(mv.to_square)

        is_capture = board.is_capture(mv)
        board.push(mv)
        row["fen"] = board.fen()

        # экономика: за сам ход (+1 coin) выдаём сразу,
        # бонус за взятие тоже сразу.
        gain2 = 2
        if is_capture and mover and captured:
            gain2 += capture_bonus2(mover, captured)

        if you == "w":
            row["coins2_w"] = int(row["coins2_w"]) + gain2
        else:
            row["coins2_b"] = int(row["coins2_b"]) + gain2

        row["moved_this_turn"] = 1
        # любое действие сбрасывает пасс
        if you == "w":
            row["consecutive_pass_w"] = 0
        else:
            row["consecutive_pass_b"] = 0

        finish_if_needed_inplace(row)

        con.execute("""
            UPDATE games SET
              status=?, fen=?,
              coins2_w=?, coins2_b=?,
              clock_w_rem=?, clock_b_rem=?, server_ts=?,
              moved_this_turn=?, result_json=?
            WHERE game_id=?
        """, (
            row["status"], row["fen"],
            row["coins2_w"], row["coins2_b"],
            row["clock_w_rem"], row["clock_b_rem"], row["server_ts"],
            row["moved_this_turn"], row.get("result_json", ""), gid
        ))
        con.commit()

        row2 = load_game_row(con, gid)
        gpub = row_to_game(row2, tag)

    await game_hub.push(gid, {"type": "state" if gpub["status"] == "active" else "finished", "data": gpub})
    return JSONResponse({"ok": True, "game": gpub})

@app.post("/api/game/drop_targets")
async def game_drop_targets(req: Request, tag: str = Depends(session_dep)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()
    piece = (body.get("piece") or "").lower().strip()

    with db() as con:
        row = load_game_row(con, gid)
    if not row:
        return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
    if tag not in (row["w_tag"], row["b_tag"]):
        return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

    you = "w" if row["w_tag"] == tag else "b"
    if row["status"] != "active" or row["turn"] != you:
        return JSONResponse({"ok": True, "targets": []})

    if piece not in BASE_COSTS2:
        return JSONResponse({"ok": True, "targets": []})

    board = chess.Board(row["fen"])
    targets = legal_drop_targets(board, you == "w", piece)
    return JSONResponse({"ok": True, "targets": sorted(list(targets))})

@app.post("/api/game/drop")
async def game_drop(req: Request, tag: str = Depends(session_dep)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()
    piece = (body.get("piece") or "").lower().strip()
    sqname = (body.get("square") or "").strip()

    if piece not in BASE_COSTS2:
        return JSONResponse({"ok": False, "reason": "bad_piece"}, status_code=400)

    with db() as con:
        row_db = load_game_row(con, gid)
        if not row_db:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row_db["w_tag"], row_db["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        row = dict(row_db)
        you = "w" if row["w_tag"] == tag else "b"
        if row["status"] != "active" or row["turn"] != you:
            return JSONResponse({"ok": False, "reason": "not_your_turn"}, status_code=400)

        update_clocks_inplace(row)
        finish_if_needed_inplace(row)
        if row["status"] != "active":
            con.execute("UPDATE games SET status=?, clock_w_rem=?, clock_b_rem=?, server_ts=?, result_json=? WHERE game_id=?",
                        (row["status"], row["clock_w_rem"], row["clock_b_rem"], row["server_ts"], row["result_json"], gid))
            con.commit()
            row2 = load_game_row(con, gid)
            gpub = row_to_game(row2, tag)
            await game_hub.push(gid, {"type": "finished", "data": gpub})
            return JSONResponse({"ok": True, "game": gpub})

        board = chess.Board(row["fen"])

        # проверка цели по правилам режима + анти-шах-бай
        targets = legal_drop_targets(board, you == "w", piece)
        if sqname not in targets:
            return JSONResponse({"ok": False, "reason": "bad_square"}, status_code=400)

        # стоимость (с учётом удорожания покупок после хода)
        base2 = BASE_COSTS2[piece]
        extra2 = int(row["post_buy_extra2_w"] if you == "w" else row["post_buy_extra2_b"])
        price2 = base2 + extra2

        coins2 = int(row["coins2_w"] if you == "w" else row["coins2_b"])
        if coins2 < price2:
            return JSONResponse({"ok": False, "reason": "no_money"}, status_code=400)

        # ставим фигуру
        try:
            sq = chess.parse_square(sqname)
        except Exception:
            return JSONResponse({"ok": False, "reason": "bad_square"}, status_code=400)

        ptype = piece_type_from_char(piece)
        board.set_piece_at(sq, chess.Piece(ptype, you == "w"))
        row["fen"] = board.fen()

        # списываем деньги
        if you == "w":
            row["coins2_w"] = coins2 - price2
        else:
            row["coins2_b"] = coins2 - price2

        row["bought_this_turn"] = 1

        # если уже ходил в этот ход => покупка после хода => удорожаем каждую следующую покупку на +0.5
        if int(row["moved_this_turn"]) == 1:
            row["bought_after_move"] = 1
            if you == "w":
                row["post_buy_extra2_w"] = int(row["post_buy_extra2_w"]) + 1
            else:
                row["post_buy_extra2_b"] = int(row["post_buy_extra2_b"]) + 1

        # любое действие сбрасывает пасс
        if you == "w":
            row["consecutive_pass_w"] = 0
        else:
            row["consecutive_pass_b"] = 0

        con.execute("""
            UPDATE games SET
              fen=?,
              coins2_w=?, coins2_b=?,
              server_ts=?,
              post_buy_extra2_w=?, post_buy_extra2_b=?,
              bought_this_turn=?, bought_after_move=?,
              consecutive_pass_w=?, consecutive_pass_b=?
            WHERE game_id=?
        """, (
            row["fen"],
            row["coins2_w"], row["coins2_b"],
            now_ts(),
            row["post_buy_extra2_w"], row["post_buy_extra2_b"],
            row["bought_this_turn"], row["bought_after_move"],
            row["consecutive_pass_w"], row["consecutive_pass_b"],
            gid
        ))
        con.commit()

        row2 = load_game_row(con, gid)
        gpub = row_to_game(row2, tag)

    await game_hub.push(gid, {"type": "state", "data": gpub})
    return JSONResponse({"ok": True, "game": gpub})

@app.post("/api/game/end_turn")
async def game_end_turn(req: Request, tag: str = Depends(session_dep)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()

    with db() as con:
        row_db = load_game_row(con, gid)
        if not row_db:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row_db["w_tag"], row_db["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        row = dict(row_db)
        you = "w" if row["w_tag"] == tag else "b"
        if row["status"] != "active" or row["turn"] != you:
            return JSONResponse({"ok": False, "reason": "not_your_turn"}, status_code=400)

        update_clocks_inplace(row)
        finish_if_needed_inplace(row)
        if row["status"] != "active":
            con.execute("UPDATE games SET status=?, clock_w_rem=?, clock_b_rem=?, server_ts=?, result_json=? WHERE game_id=?",
                        (row["status"], row["clock_w_rem"], row["clock_b_rem"], row["server_ts"], row["result_json"], gid))
            con.commit()
            row2 = load_game_row(con, gid)
            gpub = row_to_game(row2, tag)
            await game_hub.push(gid, {"type": "finished", "data": gpub})
            return JSONResponse({"ok": True, "game": gpub})

        moved = int(row["moved_this_turn"])
        bought = int(row["bought_this_turn"])
        bought_after_move = int(row["bought_after_move"])

        # Правило: если купил и завершил ход => за ход +0.5 вместо +1.
        # Но ход уже начисляет +1 в game_move. Поэтому:
        # - если ты ходил и потом покупал => штраф -0.5 (т.е. -1 half-coin) при end_turn.
        if moved == 1 and bought_after_move == 1:
            if you == "w":
                row["coins2_w"] = max(0, int(row["coins2_w"]) - 1)
            else:
                row["coins2_b"] = max(0, int(row["coins2_b"]) - 1)

        # Если НЕ ходил, но что-то купил и завершил => +0.5
        if moved == 0 and bought == 1:
            if you == "w":
                row["coins2_w"] = int(row["coins2_w"]) + 1
            else:
                row["coins2_b"] = int(row["coins2_b"]) + 1

        # Если ничего не сделал и завершил => 0 монет, пасс++
        if moved == 0 and bought == 0:
            if you == "w":
                row["consecutive_pass_w"] = int(row["consecutive_pass_w"]) + 1
                if row["consecutive_pass_w"] >= 3:
                    row["status"] = "finished"
                    row["result_json"] = json.dumps({"winner": "b", "reason": "3_passes"})
            else:
                row["consecutive_pass_b"] = int(row["consecutive_pass_b"]) + 1
                if row["consecutive_pass_b"] >= 3:
                    row["status"] = "finished"
                    row["result_json"] = json.dumps({"winner": "w", "reason": "3_passes"})

        # переключаем ход, сбрасываем флаги
        row["turn"] = "b" if you == "w" else "w"
        row["moved_this_turn"] = 0
        row["bought_this_turn"] = 0
        row["bought_after_move"] = 0
        # удорожание покупок после хода сбрасываем при окончании хода
        if you == "w":
            row["post_buy_extra2_w"] = 0
        else:
            row["post_buy_extra2_b"] = 0

        finish_if_needed_inplace(row)

        con.execute("""
            UPDATE games SET
              status=?, turn=?,
              coins2_w=?, coins2_b=?,
              clock_w_rem=?, clock_b_rem=?, server_ts=?,
              post_buy_extra2_w=?, post_buy_extra2_b=?,
              moved_this_turn=?, bought_this_turn=?, bought_after_move=?,
              consecutive_pass_w=?, consecutive_pass_b=?,
              result_json=?
            WHERE game_id=?
        """, (
            row["status"], row["turn"],
            row["coins2_w"], row["coins2_b"],
            row["clock_w_rem"], row["clock_b_rem"], now_ts(),
            row["post_buy_extra2_w"], row["post_buy_extra2_b"],
            row["moved_this_turn"], row["bought_this_turn"], row["bought_after_move"],
            row["consecutive_pass_w"], row["consecutive_pass_b"],
            row.get("result_json", ""),
            gid
        ))
        con.commit()

        row2 = load_game_row(con, gid)
        gpub = row_to_game(row2, tag)

    await game_hub.push(gid, {"type": "state" if gpub["status"] == "active" else "finished", "data": gpub})
    return JSONResponse({"ok": True, "game": gpub})

@app.post("/api/game/resign")
async def game_resign(req: Request, tag: str = Depends(session_dep)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()

    with db() as con:
        row = load_game_row(con, gid)
        if not row:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row["w_tag"], row["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        winner = "b" if row["w_tag"] == tag else "w"
        con.execute("UPDATE games SET status='finished', result_json=? WHERE game_id=?",
                    (json.dumps({"winner": winner, "reason": "resign"}), gid))
        con.commit()
        row2 = load_game_row(con, gid)
        gpub = row_to_game(row2, tag)

    await game_hub.push(gid, {"type": "finished", "data": gpub})
    return JSONResponse({"ok": True, "game": gpub})

# =========================================================
# WebSocket endpoints
# =========================================================
def ws_key_ok(key: str) -> bool:
    return bool(key) and hmac.compare_digest(key, WEB_SECRET_KEY)

@app.websocket("/ws/lobby")
async def ws_lobby(ws: WebSocket):
    await ws.accept()

    key = ws.query_params.get("key", "")
    token = ws.query_params.get("token", "")

    if not ws_key_ok(key):
        await ws.close(code=4401)
        return

    tag = load_session_tag(token)
    if not tag:
        await ws.close(code=4401)
        return

    await lobby_hub.add(tag, ws)

    try:
        while True:
            await ws.receive_text()
            with db() as con:
                con.execute("""
                INSERT INTO lobby_online(tag, last_ts)
                VALUES (?,?)
                ON CONFLICT(tag) DO UPDATE SET last_ts=excluded.last_ts
                """, (tag, now_ts()))
                con.commit()
            await lobby_hub.broadcast_online()
    except WebSocketDisconnect:
        pass
    finally:
        await lobby_hub.remove(tag, ws)

@app.websocket("/ws/game/{gid}")
async def ws_game(ws: WebSocket, gid: str):
    key = ws.query_params.get("key", "")
    token = ws.query_params.get("token", "")
    if not ws_key_ok(key):
        await ws.close(code=4401)
        return
    tag = load_session_tag(token)
    if not tag:
        await ws.close(code=4401)
        return

    # проверим что он игрок
    with db() as con:
        row = load_game_row(con, gid)
    if not row or tag not in (row["w_tag"], row["b_tag"]):
        await ws.close(code=4403)
        return

    await ws.accept()
    await game_hub.add(gid, ws)

    # сразу отдадим снапшот
    await ws.send_text(json.dumps({"type": "state", "data": row_to_game(row, tag)}, ensure_ascii=False))

    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await game_hub.remove(gid, ws)

# =========================================================
# TELEGRAM WEBHOOK
# =========================================================
import httpx

async def tg_send(chat_id: int, text: str):
    if not BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        await client.post(url, json={
            "chat_id": chat_id,
            "text": text
        })

@app.post("/webhook")
async def telegram_webhook(req: Request):
    data = await req.json()

    message = data.get("message")
    if not message:
        return {"ok": True}

    text = message.get("text", "")
    chat = message.get("chat", {})
    user = message.get("from", {})

    tg_id = user.get("id")
    tg_username = user.get("username")

    if not tg_id or not tg_username:
        return {"ok": True}

    tag = norm_tag(tg_username)

    # сохраняем tg_users
    with db() as con:
        con.execute("""
        INSERT INTO tg_users(tag, tg_id, tg_username, created_ts)
        VALUES (?,?,?,?)
        ON CONFLICT(tag) DO UPDATE SET
            tg_id=excluded.tg_id,
            tg_username=excluded.tg_username
        """, (tag, tg_id, tg_username, now_ts()))
        con.commit()

    if text.startswith("/start"):
        await tg_send(chat["id"], "Бот подключён. Теперь можешь авторизоваться в игре.")

    return {"ok": True}