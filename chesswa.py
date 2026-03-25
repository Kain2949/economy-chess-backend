import os
import json
import time
import hmac
import secrets
import sqlite3
import asyncio
from urllib.parse import urlparse
from typing import Any, Dict, Optional, Set, List

import chess
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Depends, Header, HTTPException
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
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
INVITE_TTL_SECONDS = int(os.getenv("INVITE_TTL_SECONDS", "120"))
CORS_ALLOW_ORIGINS_RAW = os.getenv("CORS_ALLOW_ORIGINS", "")

# =========================================================
# Admin / Ban debug (MVP)
# =========================================================
ADMIN_TG_ID_RAW = (os.getenv("ADMIN_TG_ID", "") or "").strip()
try:
    ADMIN_TG_ID_INT: Optional[int] = int(ADMIN_TG_ID_RAW) if ADMIN_TG_ID_RAW else None
except Exception:
    ADMIN_TG_ID_INT = None

# Safe test mechanism: if enabled, user registering with gender "f" gets a 10-year ban.
DEBUG_BAN_WOMEN_ON_REGISTER = (os.getenv("DEBUG_BAN_WOMEN_ON_REGISTER", "") or "").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

DEBUG_BAN_WOMEN_SECONDS = int(os.getenv("DEBUG_BAN_WOMEN_SECONDS", "315360000"))  # 10 years
SUPPORT_CHAT_HANDLE = os.getenv("SUPPORT_CHAT_HANDLE", "@Kain_cr").strip() or "@Kain_cr"

# =========================================================
# Matchmaking params (ELO widening)
# =========================================================
MM_INITIAL_DELTA_ELO = int(os.getenv("MM_INITIAL_DELTA_ELO", "60"))
MM_STEP_SECONDS = int(os.getenv("MM_STEP_SECONDS", "20"))
MM_STEP_DELTA_ELO = int(os.getenv("MM_STEP_DELTA_ELO", "20"))
MM_MAX_DELTA_ELO = int(os.getenv("MM_MAX_DELTA_ELO", "300"))
MM_QUEUE_TIMEOUT_SECONDS = int(os.getenv("MM_QUEUE_TIMEOUT_SECONDS", "900"))

if not WEB_SECRET_KEY:
    raise RuntimeError("WEB_SECRET_KEY is empty")

SKILL_TO_ELO = {
    1: 100,
    2: 400,
    3: 800,
    4: 1200,
    5: 1600,
}

# =========================================================
# FastAPI
# =========================================================
app = FastAPI()


def _origin_from_url(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return ""


def _cors_allowed_origins() -> List[str]:
    origins: List[str] = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]
    for chunk in CORS_ALLOW_ORIGINS_RAW.split(","):
        origin = _origin_from_url(chunk)
        if origin:
            origins.append(origin)
    webapp_origin = _origin_from_url(WEBAPP_URL)
    if webapp_origin:
        origins.append(webapp_origin)
    deduped: List[str] = []
    seen: Set[str] = set()
    for origin in origins:
        if origin in seen:
            continue
        seen.add(origin)
        deduped.append(origin)
    return deduped


app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_allowed_origins(),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException):
    detail = exc.detail
    if isinstance(detail, dict):
        payload = {"ok": False, **detail}
    else:
        payload = {"ok": False, "reason": str(detail)}
    return JSONResponse(payload, status_code=exc.status_code)


# =========================================================
# DB helpers
# =========================================================
def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def now_ts() -> int:
    return int(time.time())


def fail(status_code: int, reason: str, **extra: Any) -> None:
    raise HTTPException(status_code=status_code, detail={"reason": reason, **extra})


def has_column(con: sqlite3.Connection, table: str, column: str) -> bool:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return any(str(r["name"]) == column for r in rows)


def ensure_column(con: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    if not has_column(con, table, column):
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def init_db() -> None:
    with db() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS tg_users (
                tag TEXT PRIMARY KEY,
                tg_id INTEGER,
                tg_username TEXT,
                created_ts INTEGER
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                tag TEXT PRIMARY KEY,
                nickname TEXT NOT NULL,
                gender TEXT NOT NULL,
                skill INTEGER NOT NULL,
                elo INTEGER NOT NULL DEFAULT 100,
                banned INTEGER NOT NULL DEFAULT 0,
                created_ts INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                tag TEXT NOT NULL,
                created_ts INTEGER NOT NULL,
                expires_ts INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_codes (
                tag TEXT PRIMARY KEY,
                code TEXT NOT NULL,
                purpose TEXT NOT NULL,
                created_ts INTEGER NOT NULL,
                expires_ts INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS lobby_online (
                tag TEXT PRIMARY KEY,
                last_ts INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS invites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_tag TEXT NOT NULL,
                to_tag TEXT NOT NULL,
                minutes INTEGER NOT NULL,
                created_ts INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS matchmaking_queue (
                tag TEXT PRIMARY KEY,
                minutes INTEGER NOT NULL,
                elo_at_join INTEGER NOT NULL,
                created_ts INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS user_bans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tag TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_ts INTEGER NOT NULL,
                expires_ts INTEGER NOT NULL, -- 0 => permanent
                revoked_ts INTEGER,
                created_by_tag TEXT,
                created_by_tg_id INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS support_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_tag TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open', -- MVP: open/closed
                created_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS support_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER NOT NULL,
                from_tag TEXT NOT NULL,
                body TEXT NOT NULL,
                created_ts INTEGER NOT NULL
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_user_bans_tag_active ON user_bans(tag, revoked_ts, expires_ts, created_ts)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_support_tickets_from_tag_updated ON support_tickets(from_tag, updated_ts)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_support_messages_ticket_created ON support_messages(ticket_id, created_ts)")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS games (
                game_id TEXT PRIMARY KEY,
                w_tag TEXT NOT NULL,
                b_tag TEXT NOT NULL,
                minutes INTEGER NOT NULL,
                status TEXT NOT NULL,
                fen TEXT NOT NULL,
                turn TEXT NOT NULL,

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

                draw_offer_by TEXT NOT NULL DEFAULT '',
                rematch_offer_by TEXT NOT NULL DEFAULT '',
                last_move_from TEXT NOT NULL DEFAULT '',
                last_move_to TEXT NOT NULL DEFAULT '',

                result_json TEXT NOT NULL
            )
            """
        )

        ensure_column(con, "users", "elo", "INTEGER NOT NULL DEFAULT 100")
        ensure_column(con, "games", "draw_offer_by", "TEXT NOT NULL DEFAULT ''")
        ensure_column(con, "games", "rematch_offer_by", "TEXT NOT NULL DEFAULT ''")
        ensure_column(con, "games", "last_move_from", "TEXT NOT NULL DEFAULT ''")
        ensure_column(con, "games", "last_move_to", "TEXT NOT NULL DEFAULT ''")
        ensure_column(con, "invites", "status", "TEXT NOT NULL DEFAULT 'pending'")
        ensure_column(con, "invites", "expires_ts", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(con, "invites", "responded_ts", "INTEGER")
        con.execute("CREATE INDEX IF NOT EXISTS idx_invites_to_status_created ON invites(to_tag, status, created_ts)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_invites_from_to_status ON invites(from_tag, to_tag, status)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_matchmaking_minutes_created ON matchmaking_queue(minutes, created_ts)")

        for skill, elo in SKILL_TO_ELO.items():
            con.execute("UPDATE users SET elo=? WHERE (elo IS NULL OR elo<=0) AND skill=?", (elo, skill))

        con.commit()


@app.on_event("startup")
def _startup():
    init_db()


# =========================================================
# Security: API key + session token
# =========================================================
def api_key_dep(x_api_key: Optional[str] = Header(default=None)) -> None:
    if not x_api_key:
        return
    if not hmac.compare_digest(x_api_key, WEB_SECRET_KEY):
        fail(401, "bad_api_key")


def load_session_tag(token: str) -> Optional[str]:
    if not token:
        return None
    with db() as con:
        row = con.execute("SELECT tag, expires_ts FROM sessions WHERE token=?", (token,)).fetchone()
        if row and int(row["expires_ts"]) < now_ts():
            con.execute("DELETE FROM sessions WHERE token=?", (token,))
            con.commit()
            return None
    if not row:
        return None
    return str(row["tag"])


def session_dep(
    _: None = Depends(api_key_dep),
    authorization: Optional[str] = Header(default=None),
) -> str:
    token = ""
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
    tag = load_session_tag(token)
    if not tag:
        fail(401, "no_session")
    return tag


# =========================================================
# Ban / Admin helpers (MVP)
# =========================================================
BANNED_ALLOWED_PATHS: Set[str] = {
    "/api/app/ban_status",
    "/api/support/ticket_create",
    "/api/support/ticket_list",
    "/api/support/ticket_messages",
}


def _norm_admin_reason(reason: str) -> str:
    r = (reason or "").strip()
    return r if r else "no_reason"


def _is_tag_admin(con: sqlite3.Connection, tag: str) -> bool:
    if ADMIN_TG_ID_INT is None:
        return False
    row = con.execute("SELECT tg_id FROM tg_users WHERE tag=?", (tag,)).fetchone()
    if not row:
        return False
    try:
        return int(row["tg_id"]) == int(ADMIN_TG_ID_INT)
    except Exception:
        return False


def _get_active_ban_row(con: sqlite3.Connection, tag: str, now: int) -> Optional[sqlite3.Row]:
    return con.execute(
        """
        SELECT *
        FROM user_bans
        WHERE tag=?
          AND revoked_ts IS NULL
          AND (expires_ts=0 OR expires_ts>?)
        ORDER BY created_ts DESC, id DESC
        LIMIT 1
        """,
        (tag, int(now)),
    ).fetchone()


def _ban_payload_from_row(row: sqlite3.Row, now: int) -> Dict[str, Any]:
    expires_ts = int(row["expires_ts"] or 0)
    is_permanent = expires_ts == 0
    remaining_seconds = 0
    if not is_permanent:
        remaining_seconds = max(0, expires_ts - int(now))
    return {
        "banned": True,
        "is_permanent": bool(is_permanent),
        "reason": str(row["reason"] or ""),
        "remaining_seconds": int(remaining_seconds),
        "expires_ts": int(expires_ts),
    }


def _sync_users_banned_from_user_bans(con: sqlite3.Connection, tag: str, now: int) -> None:
    # Lazy sync: when user accesses app, keep users.banned consistent with user_bans.
    row = con.execute("SELECT banned FROM users WHERE tag=?", (tag,)).fetchone()
    if not row:
        return
    active = bool(_get_active_ban_row(con, tag, now))
    want = 1 if active else 0
    cur = int(row["banned"] or 0)
    if cur != want:
        con.execute("UPDATE users SET banned=? WHERE tag=?", (want, tag))
        con.commit()


async def session_dep_ban_guard(request: Request, tag: str = Depends(session_dep)) -> str:
    if request.url.path in BANNED_ALLOWED_PATHS:
        return tag
    now = now_ts()
    with db() as con:
        active_row = _get_active_ban_row(con, tag, now)
        _sync_users_banned_from_user_bans(con, tag, now)
        if active_row:
            fail(403, "banned")
    return tag


async def admin_only_dep(tag: str = Depends(session_dep_ban_guard)) -> str:
    with db() as con:
        if not _is_tag_admin(con, tag):
            fail(403, "not_admin")
    return tag


# =========================================================
# Users helpers
# =========================================================
def norm_tag(tag: str) -> str:
    t = (tag or "").strip()
    if not t.startswith("@"):
        t = "@" + t
    return t.lower()


def get_user(tag: str) -> Optional[sqlite3.Row]:
    with db() as con:
        return con.execute("SELECT * FROM users WHERE tag=?", (tag,)).fetchone()


def has_tg_user(tag: str) -> bool:
    with db() as con:
        row = con.execute("SELECT tag FROM tg_users WHERE tag=?", (tag,)).fetchone()
    return bool(row)


def get_tg_id(tag: str) -> Optional[int]:
    with db() as con:
        row = con.execute("SELECT tg_id FROM tg_users WHERE tag=?", (tag,)).fetchone()
    if not row:
        return None
    try:
        return int(row["tg_id"])
    except Exception:
        return None


def set_pending_code(tag: str, purpose: str) -> str:
    code = f"{secrets.randbelow(1000000):06d}"
    now = now_ts()
    exp = now + 10 * 60
    with db() as con:
        con.execute(
            """
            INSERT INTO pending_codes(tag, code, purpose, created_ts, expires_ts)
            VALUES (?,?,?,?,?)
            ON CONFLICT(tag) DO UPDATE SET
                code=excluded.code,
                purpose=excluded.purpose,
                created_ts=excluded.created_ts,
                expires_ts=excluded.expires_ts
            """,
            (tag, code, purpose, now, exp),
        )
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


def clear_pending_code(tag: str) -> None:
    with db() as con:
        con.execute("DELETE FROM pending_codes WHERE tag=?", (tag,))
        con.commit()


def create_session(tag: str) -> str:
    token = secrets.token_urlsafe(24)
    now = now_ts()
    exp = now + 14 * 24 * 3600
    with db() as con:
        con.execute(
            "INSERT INTO sessions(token, tag, created_ts, expires_ts) VALUES (?,?,?,?)",
            (token, tag, now, exp),
        )
        con.commit()
    return token


# =========================================================
# Game rules (Economy Chess)
# =========================================================
START_FEN_KINGS_ONLY = "4k3/8/8/8/8/8/8/4K3 w - - 0 1"

BASE_COSTS2 = {
    "p": 2,
    "n": 5,
    "b": 7,
    "r": 9,
    "q": 18,
}


def piece_type_from_char(ch: str) -> Optional[int]:
    m = {
        "p": chess.PAWN,
        "n": chess.KNIGHT,
        "b": chess.BISHOP,
        "r": chess.ROOK,
        "q": chess.QUEEN,
        "k": chess.KING,
    }
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

    if int(row["clock_w_rem"]) <= 0:
        row["status"] = "finished"
        row["result_json"] = json.dumps({"winner": "b", "reason": "timeout"})
        row["draw_offer_by"] = ""
        return
    if int(row["clock_b_rem"]) <= 0:
        row["status"] = "finished"
        row["result_json"] = json.dumps({"winner": "w", "reason": "timeout"})
        row["draw_offer_by"] = ""
        return

    board = chess.Board(row["fen"])
    if board.is_checkmate():
        winner = "b" if board.turn == chess.WHITE else "w"
        row["status"] = "finished"
        row["result_json"] = json.dumps({"winner": winner, "reason": "checkmate"})
        row["draw_offer_by"] = ""
        return
    if board.is_stalemate():
        row["status"] = "finished"
        row["result_json"] = json.dumps({"winner": "", "reason": "stalemate"})
        row["draw_offer_by"] = ""
        return


def load_game_row(con: sqlite3.Connection, gid: str) -> Optional[sqlite3.Row]:
    return con.execute("SELECT * FROM games WHERE game_id=?", (gid,)).fetchone()


def get_user_public(tag: str) -> Dict[str, Any]:
    user = get_user(tag)
    is_admin = False
    if ADMIN_TG_ID_INT is not None:
        with db() as con:
            is_admin = _is_tag_admin(con, tag)
    admin_label = "admin" if is_admin else ""
    if not user:
        return {"tag": tag, "nickname": tag, "elo": 0, "is_admin": is_admin, "admin": admin_label}
    return {
        "tag": tag,
        "nickname": str(user["nickname"]),
        "elo": int(user["elo"] or 0),
        "is_admin": is_admin,
        "admin": admin_label,
    }


def row_to_game(row: sqlite3.Row, viewer_tag: str) -> Dict[str, Any]:
    you = "w" if row["w_tag"] == viewer_tag else ("b" if row["b_tag"] == viewer_tag else "")
    w_user = get_user_public(str(row["w_tag"]))
    b_user = get_user_public(str(row["b_tag"]))
    return {
        "game_id": row["game_id"],
        "w_tag": row["w_tag"],
        "b_tag": row["b_tag"],
        "w_nickname": w_user["nickname"],
        "b_nickname": b_user["nickname"],
        "w_is_admin": bool(w_user.get("is_admin")),
        "b_is_admin": bool(b_user.get("is_admin")),
        "w_admin": str(w_user.get("admin") or ""),
        "b_admin": str(b_user.get("admin") or ""),
        "w_elo": w_user["elo"],
        "b_elo": b_user["elo"],
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
        "post_buy_extra2": {
            "w": int(row["post_buy_extra2_w"]),
            "b": int(row["post_buy_extra2_b"]),
        },
        "draw_offer_by": str(row["draw_offer_by"] or ""),
        "rematch_offer_by": str(row["rematch_offer_by"] or ""),
        "last_move": {
            "from": str(row["last_move_from"] or ""),
            "to": str(row["last_move_to"] or ""),
        },
        "result": json.loads(row["result_json"]) if row["result_json"] else {},
    }


def create_game_on_con(con: sqlite3.Connection, w_tag: str, b_tag: str, minutes: int) -> str:
    gid = secrets.token_urlsafe(12)
    total = max(60, int(minutes) * 60)
    con.execute(
        """
        INSERT INTO games(
            game_id, w_tag, b_tag, minutes, status, fen, turn,
            coins2_w, coins2_b,
            clock_w_rem, clock_b_rem, server_ts,
            post_buy_extra2_w, post_buy_extra2_b,
            moved_this_turn, bought_this_turn, bought_after_move,
            consecutive_pass_w, consecutive_pass_b,
            draw_offer_by, rematch_offer_by,
            last_move_from, last_move_to,
            result_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            gid,
            w_tag,
            b_tag,
            int(minutes),
            "active",
            START_FEN_KINGS_ONLY,
            "w",
            10,
            10,
            total,
            total,
            now_ts(),
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            "",
            "",
            "",
            "",
            "",
        ),
    )
    return gid


def create_game(w_tag: str, b_tag: str, minutes: int) -> str:
    gid = secrets.token_urlsafe(12)
    total = max(60, int(minutes) * 60)
    with db() as con:
        gid2 = create_game_on_con(con, w_tag, b_tag, minutes)
        con.commit()
    return gid2


def behind_pawn_square(board: chess.Board, color: bool, file_idx: int) -> Optional[chess.Square]:
    pawns = board.pieces(chess.PAWN, color)
    candidates: List[chess.Square] = [sq for sq in pawns if chess.square_file(sq) == file_idx]
    if not candidates:
        return None

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
    in_check = board.is_check()

    if piece_ch == "p":
        rank = 1 if you_color == chess.WHITE else 6
        for file_idx in range(8):
            sq = chess.square(file_idx, rank)
            if board.piece_at(sq) is not None:
                continue
            if in_check:
                b2 = board.copy(stack=False)
                b2.set_piece_at(sq, chess.Piece(chess.PAWN, you_color))
                if not b2.is_check():
                    continue
            targets.add(chess.square_name(sq))
        return targets

    if piece_ch in ("n", "b", "r", "q"):
        for file_idx in range(8):
            sq = behind_pawn_square(board, you_color, file_idx)
            if sq is None:
                continue
            if board.piece_at(sq) is not None:
                continue
            if in_check:
                b2 = board.copy(stack=False)
                ptype = piece_type_from_char(piece_ch)
                if ptype is None:
                    continue
                b2.set_piece_at(sq, chess.Piece(ptype, you_color))
                if not b2.is_check():
                    continue
            targets.add(chess.square_name(sq))
        return targets

    return targets


def capture_bonus2(mover: chess.Piece, captured: chess.Piece) -> int:
    mover_t = mover.piece_type
    cap_t = captured.piece_type

    if cap_t == chess.PAWN:
        if mover_t == chess.PAWN:
            return 1
        if mover_t == chess.KING:
            return 1
        return 0

    if mover_t == chess.PAWN:
        return 3
    if mover_t == chess.KING:
        return 4
    return 2


def save_game_state(con: sqlite3.Connection, row: Dict[str, Any]) -> None:
    con.execute(
        """
        UPDATE games SET
            status=?, fen=?, turn=?,
            coins2_w=?, coins2_b=?,
            clock_w_rem=?, clock_b_rem=?, server_ts=?,
            post_buy_extra2_w=?, post_buy_extra2_b=?,
            moved_this_turn=?, bought_this_turn=?, bought_after_move=?,
            consecutive_pass_w=?, consecutive_pass_b=?,
            draw_offer_by=?, rematch_offer_by=?,
            last_move_from=?, last_move_to=?,
            result_json=?
        WHERE game_id=?
        """,
        (
            row["status"],
            row["fen"],
            row["turn"],
            row["coins2_w"],
            row["coins2_b"],
            row["clock_w_rem"],
            row["clock_b_rem"],
            row["server_ts"],
            row["post_buy_extra2_w"],
            row["post_buy_extra2_b"],
            row["moved_this_turn"],
            row["bought_this_turn"],
            row["bought_after_move"],
            row["consecutive_pass_w"],
            row["consecutive_pass_b"],
            row.get("draw_offer_by", ""),
            row.get("rematch_offer_by", ""),
            row.get("last_move_from", ""),
            row.get("last_move_to", ""),
            row.get("result_json", ""),
            row["game_id"],
        ),
    )


def refresh_game_row(con: sqlite3.Connection, row_db: sqlite3.Row) -> sqlite3.Row:
    row = dict(row_db)
    before = (
        row["status"],
        row["clock_w_rem"],
        row["clock_b_rem"],
        row["server_ts"],
        row["result_json"],
    )
    update_clocks_inplace(row)
    finish_if_needed_inplace(row)
    after = (
        row["status"],
        row["clock_w_rem"],
        row["clock_b_rem"],
        row["server_ts"],
        row.get("result_json", ""),
    )
    if after != before:
        save_game_state(con, row)
        con.commit()
        return load_game_row(con, str(row["game_id"])) or row_db
    return row_db


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
        await self.broadcast_online()

    async def remove(self, tag: str, ws: WebSocket):
        async with self.lock:
            if tag in self.conns and ws in self.conns[tag]:
                self.conns[tag].remove(ws)
            if tag in self.conns and not self.conns[tag]:
                del self.conns[tag]
        await self.broadcast_online()

    async def send_to(self, tag: str, payload: Dict[str, Any]):
        async with self.lock:
            sockets = list(self.conns.get(tag, set()))
        stale: List[WebSocket] = []
        for ws in sockets:
            try:
                await ws.send_text(json.dumps(payload, ensure_ascii=False))
            except Exception:
                stale.append(ws)
        if stale:
            async with self.lock:
                for ws in stale:
                    if tag in self.conns and ws in self.conns[tag]:
                        self.conns[tag].remove(ws)
                if tag in self.conns and not self.conns[tag]:
                    del self.conns[tag]

    async def broadcast_online(self):
        async with self.lock:
            tags = list(self.conns.keys())

        now = now_ts()
        online = []
        with db() as con:
            for tag in tags:
                user = con.execute("SELECT tag, nickname, elo FROM users WHERE tag=?", (tag,)).fetchone()
                if not user:
                    continue
                if _get_active_ban_row(con, str(tag), now):
                    continue
                online.append(
                    {
                        "tag": tag,
                        "nickname": str(user["nickname"]),
                        "elo": int(user["elo"] or 0),
                        "is_admin": _is_tag_admin(con, str(tag)),
                        "admin": "admin" if _is_tag_admin(con, str(tag)) else "",
                    }
                )
        online.sort(key=lambda item: (-int(item["elo"]), str(item["nickname"]).lower()))

        for tag in tags:
            await self.send_to(tag, {"type": "online", "data": {"online": online}})


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
        stale: List[WebSocket] = []
        for ws in sockets:
            try:
                await ws.send_text(json.dumps(payload, ensure_ascii=False))
            except Exception:
                stale.append(ws)
        if stale:
            async with self.lock:
                for ws in stale:
                    if gid in self.conns and ws in self.conns[gid]:
                        self.conns[gid].remove(ws)
                if gid in self.conns and not self.conns[gid]:
                    del self.conns[gid]


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

    if not tag or gender not in ("m", "f") or skill not in SKILL_TO_ELO:
        return JSONResponse({"ok": False, "reason": "bad_fields"}, status_code=400)
    if not has_tg_user(tag):
        return JSONResponse({"ok": False, "reason": "tg_not_started"}, status_code=400)
    if get_user(tag):
        return JSONResponse({"ok": False, "reason": "already_registered"}, status_code=400)

    code = set_pending_code(tag, "register")
    tg_id = get_tg_id(tag)
    if tg_id:
        await tg_send(tg_id, f"Код регистрации: {code}")

    return JSONResponse({"ok": True, "sent": True})


@app.post("/api/auth/login_start")
async def login_start(req: Request, _: None = Depends(api_key_dep)):
    body = await req.json()
    tag = norm_tag(body.get("tag", ""))

    if not has_tg_user(tag):
        return JSONResponse({"ok": False, "reason": "tg_not_started"}, status_code=400)

    user = get_user(tag)
    if not user:
        return JSONResponse({"ok": False, "reason": "not_registered"}, status_code=400)

    code = set_pending_code(tag, "login")
    tg_id = get_tg_id(tag)
    if tg_id:
        await tg_send(tg_id, f"Код входа: {code}")

    return JSONResponse({"ok": True, "sent": True})


@app.post("/api/auth/verify")
async def auth_verify(req: Request, _: None = Depends(api_key_dep)):
    body = await req.json()

    tag = norm_tag(body.get("tag", ""))
    code = (body.get("code") or "").strip()
    raw_mode = (body.get("mode") or "").strip().lower()

    if raw_mode in ("login", "login_start"):
        mode = "login"
    elif raw_mode in ("register", "register_start"):
        mode = "register"
    else:
        return JSONResponse({"ok": False, "reason": "bad_mode"}, status_code=400)

    if not tag or not code:
        return JSONResponse({"ok": False, "reason": "bad_input"}, status_code=400)
    if not check_pending_code(tag, code, mode):
        return JSONResponse({"ok": False, "reason": "bad_code"}, status_code=400)

    if mode == "register":
        nickname = (body.get("nickname") or "").strip()
        gender = (body.get("gender") or "").strip().lower()
        skill = int(body.get("skill") or 0)

        if not nickname or gender not in ("m", "f") or skill not in SKILL_TO_ELO:
            return JSONResponse({"ok": False, "reason": "bad_fields"}, status_code=400)
        if get_user(tag):
            return JSONResponse({"ok": False, "reason": "already_registered"}, status_code=400)

        elo = SKILL_TO_ELO[skill]
        with db() as con:
            con.execute(
                "INSERT INTO users(tag, nickname, gender, skill, elo, banned, created_ts) VALUES (?,?,?,?,?,?,?)",
                (tag, nickname, gender, skill, elo, 0, now_ts()),
            )
            con.commit()

        # Safe debug autoban (server-side env flag only).
        if DEBUG_BAN_WOMEN_ON_REGISTER and gender == "f":
            now = now_ts()
            expires_ts = now + int(DEBUG_BAN_WOMEN_SECONDS)
            created_by_tg_id = int(ADMIN_TG_ID_INT or 0)
            reason = "debug_autoban_women_10y"
            with db() as con:
                con.execute(
                    """
                    INSERT INTO user_bans(tag, reason, created_ts, expires_ts, revoked_ts, created_by_tag, created_by_tg_id)
                    VALUES (?,?,?,?,?,?,?)
                    """,
                    (tag, reason, now, expires_ts, None, None, created_by_tg_id),
                )
                con.execute("UPDATE users SET banned=1 WHERE tag=?", (tag,))
                con.commit()

    user = get_user(tag)
    if not user:
        return JSONResponse({"ok": False, "reason": "no_user"}, status_code=400)

    clear_pending_code(tag)
    token = create_session(tag)

    is_admin = False
    with db() as con:
        is_admin = _is_tag_admin(con, tag)

    return JSONResponse(
        {
            "ok": True,
            "token": token,
            "user": {
                "tag": tag,
                "nickname": str(user["nickname"]),
                "elo": int(user["elo"] or 0),
                "skill": int(user["skill"] or 0),
                "is_admin": bool(is_admin),
            },
        }
    )


# =========================================================
# Ban / Support / Admin API (MVP)
# =========================================================
@app.post("/api/app/ban_status")
async def app_ban_status(tag: str = Depends(session_dep)):
    now = now_ts()
    with db() as con:
        active = _get_active_ban_row(con, tag, now)
        if active:
            _sync_users_banned_from_user_bans(con, tag, now)
            payload = _ban_payload_from_row(active, now)
            return JSONResponse(
                {
                    "ok": True,
                    **payload,
                    "support": {
                        "chat_handle": SUPPORT_CHAT_HANDLE,
                        "message": f"Ты заблокирован. Причина: {payload['reason']}. Обжалуй/задай вопрос в поддержку: {SUPPORT_CHAT_HANDLE}",
                    },
                }
            )

        # Keep response shape stable for the frontend.
        _sync_users_banned_from_user_bans(con, tag, now)
        return JSONResponse(
            {
                "ok": True,
                "banned": False,
                "is_permanent": False,
                "reason": "",
                "remaining_seconds": 0,
                "expires_ts": 0,
                "support": {"chat_handle": SUPPORT_CHAT_HANDLE, "message": ""},
            }
        )


@app.post("/api/support/ticket_create")
async def support_ticket_create(req: Request, tag: str = Depends(session_dep)):
    body = await req.json()
    msg = (body.get("message") or "").strip()
    if not msg:
        return JSONResponse({"ok": False, "reason": "bad_message"}, status_code=400)

    now = now_ts()
    with db() as con:
        cur = con.execute(
            """
            INSERT INTO support_tickets(from_tag, status, created_ts, updated_ts)
            VALUES (?,?,?,?)
            """,
            (tag, "open", now, now),
        )
        ticket_id = int(cur.lastrowid or 0)
        con.execute(
            """
            INSERT INTO support_messages(ticket_id, from_tag, body, created_ts)
            VALUES (?,?,?,?)
            """,
            (ticket_id, tag, msg, now),
        )
        con.commit()

    return JSONResponse({"ok": True, "ticket_id": ticket_id})


@app.post("/api/support/ticket_list")
async def support_ticket_list(tag: str = Depends(session_dep)):
    with db() as con:
        rows = con.execute(
            """
            SELECT id, status, created_ts, updated_ts
            FROM support_tickets
            WHERE from_tag=?
            ORDER BY updated_ts DESC, id DESC
            """,
            (tag,),
        ).fetchall()

    tickets = []
    for r in rows:
        tickets.append(
            {
                "ticket_id": int(r["id"]),
                "status": str(r["status"] or "open"),
                "created_ts": int(r["created_ts"] or 0),
                "updated_ts": int(r["updated_ts"] or 0),
            }
        )
    return JSONResponse({"ok": True, "tickets": tickets})


@app.post("/api/support/ticket_messages")
async def support_ticket_messages(req: Request, tag: str = Depends(session_dep)):
    body = await req.json()
    ticket_id = body.get("ticket_id")
    try:
        ticket_id_i = int(ticket_id)
    except Exception:
        return JSONResponse({"ok": False, "reason": "bad_ticket_id"}, status_code=400)

    now = now_ts()
    with db() as con:
        ticket = con.execute(
            "SELECT id, from_tag, status FROM support_tickets WHERE id=?",
            (ticket_id_i,),
        ).fetchone()
        if not ticket:
            return JSONResponse({"ok": False, "reason": "ticket_not_found"}, status_code=400)
        if str(ticket["from_tag"]) != tag:
            return JSONResponse({"ok": False, "reason": "not_allowed"}, status_code=403)
        msgs = con.execute(
            """
            SELECT id, from_tag, body, created_ts
            FROM support_messages
            WHERE ticket_id=?
            ORDER BY created_ts ASC, id ASC
            """,
            (ticket_id_i,),
        ).fetchall()

    messages = []
    for m in msgs:
        messages.append(
            {
                "id": int(m["id"]),
                "from_tag": str(m["from_tag"]),
                "body": str(m["body"]),
                "created_ts": int(m["created_ts"] or 0),
            }
        )

    return JSONResponse(
        {
            "ok": True,
            "ticket_id": ticket_id_i,
            "status": str(ticket["status"] or "open"),
            "messages": messages,
        }
    )


@app.post("/api/admin/users_search")
async def admin_users_search(req: Request, admin_tag: str = Depends(admin_only_dep)):
    body = await req.json()
    q = (body.get("query") or "").strip()
    if not q:
        return JSONResponse({"ok": False, "reason": "bad_query"}, status_code=400)

    q_low = q.lower()
    tag_exact = None
    if q_low.startswith("@"):
        tag_exact = norm_tag(q_low)

    limit = 50
    with db() as con:
        if tag_exact:
            row = con.execute("SELECT tag, nickname, elo, banned FROM users WHERE tag=?", (tag_exact,)).fetchone()
            if not row:
                return JSONResponse({"ok": True, "users": []})
            users = [
                {
                    "tag": str(row["tag"]),
                    "nickname": str(row["nickname"]),
                    "elo": int(row["elo"] or 0),
                    "is_admin": _is_tag_admin(con, str(row["tag"])),
                }
            ]
            return JSONResponse({"ok": True, "users": users})

        rows = con.execute(
            """
            SELECT tag, nickname, elo, banned
            FROM users
            WHERE LOWER(nickname) LIKE ?
               OR LOWER(tag) LIKE ?
            ORDER BY elo DESC, nickname ASC
            LIMIT ?
            """,
            (f"%{q_low}%", f"%{q_low}%", limit),
        ).fetchall()

        users = []
        for r in rows:
            users.append(
                {
                    "tag": str(r["tag"]),
                    "nickname": str(r["nickname"]),
                    "elo": int(r["elo"] or 0),
                    "is_admin": _is_tag_admin(con, str(r["tag"])),
                }
            )
    return JSONResponse({"ok": True, "users": users})


@app.post("/api/admin/ban_quick")
async def admin_ban_quick(req: Request, admin_tag: str = Depends(admin_only_dep)):
    body = await req.json()
    raw_tag = (body.get("tag") or "").strip()
    if not raw_tag:
        return JSONResponse({"ok": False, "reason": "bad_tag"}, status_code=400)
    tag = norm_tag(raw_tag)

    duration_type = (body.get("duration_type") or "").strip().lower()
    reason = _norm_admin_reason(body.get("reason") or "")
    now = now_ts()

    if duration_type not in ("seconds", "permanent"):
        return JSONResponse({"ok": False, "reason": "bad_duration_type"}, status_code=400)

    expires_ts = 0
    if duration_type == "seconds":
        try:
            duration_seconds = int(body.get("duration_seconds"))
        except Exception:
            return JSONResponse({"ok": False, "reason": "bad_duration_seconds"}, status_code=400)
        if duration_seconds <= 0:
            return JSONResponse({"ok": False, "reason": "bad_duration_seconds"}, status_code=400)
        expires_ts = now + duration_seconds

    with db() as con:
        user = con.execute("SELECT tag FROM users WHERE tag=?", (tag,)).fetchone()
        if not user:
            return JSONResponse({"ok": False, "reason": "no_user"}, status_code=400)

        admin_tg_id = con.execute("SELECT tg_id FROM tg_users WHERE tag=?", (admin_tag,)).fetchone()
        created_by_tg_id = int(admin_tg_id["tg_id"]) if admin_tg_id else int(ADMIN_TG_ID_INT or 0)

        # Revoke existing active bans for this tag.
        con.execute(
            """
            UPDATE user_bans
            SET revoked_ts=?
            WHERE tag=?
              AND revoked_ts IS NULL
              AND (expires_ts=0 OR expires_ts>?)
            """,
            (now, tag, now),
        )

        con.execute(
            """
            INSERT INTO user_bans(tag, reason, created_ts, expires_ts, revoked_ts, created_by_tag, created_by_tg_id)
            VALUES (?,?,?,?,?,?,?)
            """,
            (tag, reason, now, int(expires_ts), None, admin_tag, created_by_tg_id),
        )
        con.execute("UPDATE users SET banned=1 WHERE tag=?", (tag,))
        con.commit()

    return JSONResponse(
        {
            "ok": True,
            "banned": True,
            "is_permanent": bool(expires_ts == 0),
            "expires_ts": int(expires_ts),
            "reason": reason,
        }
    )


@app.post("/api/admin/unban")
async def admin_unban(req: Request, admin_tag: str = Depends(admin_only_dep)):
    body = await req.json()
    raw_tag = (body.get("tag") or "").strip()
    if not raw_tag:
        return JSONResponse({"ok": False, "reason": "bad_tag"}, status_code=400)
    tag = norm_tag(raw_tag)
    now = now_ts()

    with db() as con:
        user = con.execute("SELECT tag FROM users WHERE tag=?", (tag,)).fetchone()
        if not user:
            return JSONResponse({"ok": False, "reason": "no_user"}, status_code=400)

        con.execute(
            """
            UPDATE user_bans
            SET revoked_ts=?
            WHERE tag=?
              AND revoked_ts IS NULL
            """,
            (now, tag),
        )
        con.execute("UPDATE users SET banned=0 WHERE tag=?", (tag,))
        con.commit()

    return JSONResponse({"ok": True, "unbanned_tag": tag})


@app.post("/api/admin/support_ticket_list")
async def admin_support_ticket_list(admin_tag: str = Depends(admin_only_dep)):
    with db() as con:
        rows = con.execute(
            """
            SELECT id, from_tag, status, created_ts, updated_ts
            FROM support_tickets
            ORDER BY updated_ts DESC, id DESC
            LIMIT 200
            """
        ).fetchall()

    tickets = []
    for r in rows:
        tickets.append(
            {
                "ticket_id": int(r["id"]),
                "from_tag": str(r["from_tag"]),
                "status": str(r["status"] or "open"),
                "created_ts": int(r["created_ts"] or 0),
                "updated_ts": int(r["updated_ts"] or 0),
            }
        )
    return JSONResponse({"ok": True, "tickets": tickets})


@app.post("/api/admin/support_ticket_messages")
async def admin_support_ticket_messages(req: Request, admin_tag: str = Depends(admin_only_dep)):
    body = await req.json()
    ticket_id = body.get("ticket_id")
    try:
        ticket_id_i = int(ticket_id)
    except Exception:
        return JSONResponse({"ok": False, "reason": "bad_ticket_id"}, status_code=400)

    with db() as con:
        ticket = con.execute(
            "SELECT id, from_tag, status FROM support_tickets WHERE id=?",
            (ticket_id_i,),
        ).fetchone()
        if not ticket:
            return JSONResponse({"ok": False, "reason": "ticket_not_found"}, status_code=400)
        msgs = con.execute(
            """
            SELECT id, from_tag, body, created_ts
            FROM support_messages
            WHERE ticket_id=?
            ORDER BY created_ts ASC, id ASC
            """,
            (ticket_id_i,),
        ).fetchall()

    messages = []
    for m in msgs:
        messages.append(
            {
                "id": int(m["id"]),
                "from_tag": str(m["from_tag"]),
                "body": str(m["body"]),
                "created_ts": int(m["created_ts"] or 0),
            }
        )

    return JSONResponse(
        {
            "ok": True,
            "ticket_id": ticket_id_i,
            "status": str(ticket["status"] or "open"),
            "messages": messages,
        }
    )


@app.post("/api/admin/support_reply")
async def admin_support_reply(req: Request, admin_tag: str = Depends(admin_only_dep)):
    body = await req.json()
    ticket_id = body.get("ticket_id")
    msg = (body.get("message") or "").strip()
    if not msg:
        return JSONResponse({"ok": False, "reason": "bad_message"}, status_code=400)
    try:
        ticket_id_i = int(ticket_id)
    except Exception:
        return JSONResponse({"ok": False, "reason": "bad_ticket_id"}, status_code=400)

    now = now_ts()
    with db() as con:
        ticket = con.execute("SELECT id FROM support_tickets WHERE id=?", (ticket_id_i,)).fetchone()
        if not ticket:
            return JSONResponse({"ok": False, "reason": "ticket_not_found"}, status_code=400)

        con.execute(
            """
            INSERT INTO support_messages(ticket_id, from_tag, body, created_ts)
            VALUES (?,?,?,?)
            """,
            (ticket_id_i, admin_tag, msg, now),
        )
        con.execute("UPDATE support_tickets SET updated_ts=? WHERE id=?", (now, ticket_id_i))
        con.commit()

    return JSONResponse({"ok": True})


# =========================================================
# LOBBY API
# =========================================================
@app.post("/api/lobby/online")
async def lobby_online(tag: str = Depends(session_dep_ban_guard)):
    with db() as con:
        now = now_ts()
        con.execute(
            """
            INSERT INTO lobby_online(tag, last_ts) VALUES (?,?)
            ON CONFLICT(tag) DO UPDATE SET last_ts=excluded.last_ts
            """,
            (tag, now),
        )
        con.commit()

        rows = con.execute("SELECT tag FROM lobby_online WHERE last_ts>=?", (now - 40,)).fetchall()

        online = []
        for row in rows:
            u_tag = str(row["tag"])
            user = con.execute("SELECT nickname, elo FROM users WHERE tag=?", (u_tag,)).fetchone()
            if not user:
                continue
            if _get_active_ban_row(con, u_tag, now):
                continue
            online.append(
                {
                    "tag": u_tag,
                    "nickname": str(user["nickname"]),
                    "elo": int(user["elo"] or 0),
                    "is_admin": _is_tag_admin(con, u_tag),
                    "admin": "admin" if _is_tag_admin(con, u_tag) else "",
                }
            )
    online.sort(key=lambda item: (-int(item["elo"]), str(item["nickname"]).lower()))
    return JSONResponse({"ok": True, "online": online})


def _cleanup_expired_invites(con: sqlite3.Connection) -> None:
    now = now_ts()
    con.execute(
        """
        UPDATE invites
        SET status='expired', responded_ts=?
        WHERE status='pending' AND expires_ts>0 AND expires_ts<?
        """,
        (now, now),
    )


def _invite_row_for_response(con: sqlite3.Connection, to_tag: str, body: Dict[str, Any]) -> Optional[sqlite3.Row]:
    _cleanup_expired_invites(con)
    invite_id_raw = body.get("invite_id")
    if invite_id_raw not in (None, ""):
        try:
            invite_id = int(invite_id_raw)
        except Exception:
            return None
        return con.execute(
            """
            SELECT * FROM invites
            WHERE id=? AND to_tag=? AND status='pending'
            ORDER BY id DESC
            LIMIT 1
            """,
            (invite_id, to_tag),
        ).fetchone()

    from_tag = norm_tag(body.get("from_tag", ""))
    minutes = int(body.get("minutes") or 10)
    if minutes not in (10, 30, 60):
        minutes = 10
    if not from_tag:
        return None
    return con.execute(
        """
        SELECT * FROM invites
        WHERE from_tag=? AND to_tag=? AND minutes=? AND status='pending'
        ORDER BY id DESC
        LIMIT 1
        """,
        (from_tag, to_tag, minutes),
    ).fetchone()


def _invite_missing_reason(con: sqlite3.Connection, to_tag: str, body: Dict[str, Any]) -> str:
    invite_id_raw = body.get("invite_id")
    if invite_id_raw not in (None, ""):
        try:
            invite_id = int(invite_id_raw)
        except Exception:
            return "bad_invite"
        row = con.execute("SELECT status FROM invites WHERE id=? AND to_tag=?", (invite_id, to_tag)).fetchone()
        if not row:
            return "invite_not_found"
        status = str(row["status"] or "")
        if status == "expired":
            return "invite_expired"
        if status in ("accepted", "declined"):
            return "invite_already_handled"
        return "invite_not_found"

    from_tag = norm_tag(body.get("from_tag", ""))
    if not from_tag:
        return "bad_invite"
    row = con.execute(
        "SELECT status FROM invites WHERE from_tag=? AND to_tag=? ORDER BY id DESC LIMIT 1",
        (from_tag, to_tag),
    ).fetchone()
    if not row:
        return "invite_not_found"
    status = str(row["status"] or "")
    if status == "expired":
        return "invite_expired"
    if status in ("accepted", "declined"):
        return "invite_already_handled"
    return "invite_not_found"


def _mm_delta_elo(wait_seconds: int) -> int:
    if wait_seconds < 0:
        wait_seconds = 0
    steps = wait_seconds // max(1, MM_STEP_SECONDS)
    delta = MM_INITIAL_DELTA_ELO + int(steps) * MM_STEP_DELTA_ELO
    return min(MM_MAX_DELTA_ELO, int(delta))


def _mm_build_searching_payload(
    elo_at_join: int,
    created_ts: int,
    minutes: int,
    now: int,
) -> Dict[str, Any]:
    wait_seconds = max(0, int(now) - int(created_ts))
    delta = _mm_delta_elo(wait_seconds)
    return {
        "in_queue": True,
        "minutes": int(minutes),
        "elo": int(elo_at_join),
        "wait_seconds": int(wait_seconds),
        "delta_elo": int(delta),
    }


def _mm_tag_has_active_game(con: sqlite3.Connection, tag: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM games
        WHERE status='active' AND (w_tag=? OR b_tag=?)
        LIMIT 1
        """,
        (tag, tag),
    ).fetchone()
    return bool(row)


def _mm_tag_has_pending_invite(con: sqlite3.Connection, tag: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM invites
        WHERE status='pending' AND (from_tag=? OR to_tag=?)
        LIMIT 1
        """,
        (tag, tag),
    ).fetchone()
    return bool(row)


def _mm_cleanup_expired_queue(con: sqlite3.Connection, now: int) -> None:
    threshold = now - MM_QUEUE_TIMEOUT_SECONDS
    con.execute(
        "DELETE FROM matchmaking_queue WHERE created_ts<?",
        (int(threshold),),
    )


def _mm_pick_candidate(
    self_tag: str,
    self_minutes: int,
    self_elo_at_join: int,
    self_created_ts: int,
    now: int,
    candidates: List[sqlite3.Row],
) -> Optional[sqlite3.Row]:
    self_wait = max(0, int(now) - int(self_created_ts))
    self_delta = _mm_delta_elo(self_wait)
    self_low = int(self_elo_at_join) - self_delta
    self_high = int(self_elo_at_join) + self_delta

    best: Optional[sqlite3.Row] = None
    best_diff: Optional[int] = None

    for c in candidates:
        cand_tag = str(c["tag"])
        if cand_tag == self_tag:
            continue

        cand_created_ts = int(c["created_ts"])
        cand_wait = max(0, int(now) - cand_created_ts)
        cand_delta = _mm_delta_elo(cand_wait)
        cand_elo = int(c["elo_at_join"])
        cand_low = cand_elo - cand_delta
        cand_high = cand_elo + cand_delta

        overlap = max(self_low, cand_low) <= min(self_high, cand_high)
        if not overlap:
            continue

        diff = abs(int(self_elo_at_join) - cand_elo)
        if best is None or diff < int(best_diff or diff):
            best = c
            best_diff = diff
        elif best is not None and diff == int(best_diff or diff):
            # Tie-breaker: prefer the older one (created_ts smaller)
            if int(c["created_ts"]) < int(best["created_ts"]):
                best = c
                best_diff = diff

    return best


def _mm_norm_minutes(raw: Any) -> int:
    try:
        minutes = int(raw or 10)
    except Exception:
        minutes = 10
    if minutes not in (10, 30, 60):
        minutes = 10
    return minutes


def _mm_get_queue_row(con: sqlite3.Connection, tag: str) -> Optional[sqlite3.Row]:
    return con.execute("SELECT * FROM matchmaking_queue WHERE tag=?", (tag,)).fetchone()


def _mm_searching_from_queue_row(row: sqlite3.Row, now: int, *, in_queue: bool) -> Dict[str, Any]:
    payload = _mm_build_searching_payload(
        elo_at_join=int(row["elo_at_join"]),
        created_ts=int(row["created_ts"]),
        minutes=int(row["minutes"]),
        now=int(now),
    )
    payload["in_queue"] = bool(in_queue)
    return payload


def _mm_upsert_queue_row(con: sqlite3.Connection, tag: str, minutes: int, elo_now: int, now: int) -> sqlite3.Row:
    existing = _mm_get_queue_row(con, tag)
    if existing and int(existing["minutes"]) == int(minutes):
        return existing
    con.execute(
        """
        INSERT INTO matchmaking_queue(tag, minutes, elo_at_join, created_ts)
        VALUES (?,?,?,?)
        ON CONFLICT(tag) DO UPDATE SET
            minutes=excluded.minutes,
            elo_at_join=excluded.elo_at_join,
            created_ts=excluded.created_ts
        """,
        (tag, int(minutes), int(elo_now), int(now)),
    )
    row = _mm_get_queue_row(con, tag)
    if not row:
        fail(500, "mm_failed")
    return row


def _mm_list_candidates(con: sqlite3.Connection, minutes: int, self_tag: str) -> List[sqlite3.Row]:
    return con.execute(
        """
        SELECT *
        FROM matchmaking_queue
        WHERE minutes=? AND tag<>?
        ORDER BY created_ts ASC
        """,
        (int(minutes), self_tag),
    ).fetchall()


def _mm_try_match_on_con(con: sqlite3.Connection, self_tag: str, now: int) -> Optional[Dict[str, Any]]:
    self_row = _mm_get_queue_row(con, self_tag)
    if not self_row:
        return None

    candidates = _mm_list_candidates(con, int(self_row["minutes"]), self_tag)
    best = _mm_pick_candidate(
        self_tag=self_tag,
        self_minutes=int(self_row["minutes"]),
        self_elo_at_join=int(self_row["elo_at_join"]),
        self_created_ts=int(self_row["created_ts"]),
        now=int(now),
        candidates=candidates,
    )
    if not best:
        return None

    other_tag = str(best["tag"])

    # Re-check constraints for both sides right before creating a game.
    if _mm_tag_has_active_game(con, self_tag) or _mm_tag_has_active_game(con, other_tag):
        return None
    if _mm_tag_has_pending_invite(con, self_tag) or _mm_tag_has_pending_invite(con, other_tag):
        return None

    cur = con.execute("DELETE FROM matchmaking_queue WHERE tag IN (?,?)", (self_tag, other_tag))
    if int(cur.rowcount or 0) != 2:
        return None

    minutes = int(self_row["minutes"])
    gid = create_game_on_con(con, w_tag=self_tag, b_tag=other_tag, minutes=minutes)

    self_searching = _mm_searching_from_queue_row(self_row, now, in_queue=False)
    other_searching = _mm_searching_from_queue_row(best, now, in_queue=False)

    return {
        "game_id": gid,
        "w_tag": self_tag,
        "b_tag": other_tag,
        "minutes": minutes,
        "searching_self": self_searching,
        "searching_other": other_searching,
    }


@app.post("/api/lobby/invite")
async def lobby_invite(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    to_tag = norm_tag(body.get("to_tag", ""))
    minutes = int(body.get("minutes") or 10)
    if minutes not in (10, 30, 60):
        minutes = 10

    if to_tag == tag:
        return JSONResponse({"ok": False, "reason": "self"}, status_code=400)

    user = get_user(to_tag)
    if not user:
        return JSONResponse({"ok": False, "reason": "no_user"}, status_code=400)
    if int(user["banned"]) == 1:
        return JSONResponse({"ok": False, "reason": "banned_user"}, status_code=400)

    invite_id = 0
    with db() as con:
        created = now_ts()
        expires = created + max(30, INVITE_TTL_SECONDS)
        cur = con.execute(
            """
            INSERT INTO invites(from_tag, to_tag, minutes, created_ts, status, expires_ts, responded_ts)
            VALUES (?,?,?,?,?,?,?)
            """,
            (tag, to_tag, minutes, created, "pending", expires, None),
        )
        invite_id = int(cur.lastrowid or 0)
        con.commit()

    me = get_user(tag)
    await lobby_hub.send_to(
        to_tag,
        {
            "type": "invite",
            "data": {
                "from_tag": tag,
                "from_nick": str(me["nickname"]) if me else tag,
                "from_elo": int(me["elo"] or 0) if me else 0,
                "minutes": minutes,
                "invite_id": invite_id,
            },
        },
    )
    return JSONResponse({"ok": True, "invite_id": invite_id})


@app.post("/api/lobby/invite_accept")
async def lobby_invite_accept(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    with db() as con:
        invite = _invite_row_for_response(con, tag, body)
        if not invite:
            return JSONResponse({"ok": False, "reason": _invite_missing_reason(con, tag, body)}, status_code=400)
        if int(invite["expires_ts"] or 0) < now_ts():
            con.execute("UPDATE invites SET status='expired', responded_ts=? WHERE id=?", (now_ts(), int(invite["id"])))
            con.commit()
            return JSONResponse({"ok": False, "reason": "invite_expired"}, status_code=400)

        from_tag = str(invite["from_tag"])
        minutes = int(invite["minutes"])
        if not get_user(from_tag):
            return JSONResponse({"ok": False, "reason": "no_user"}, status_code=400)
        cur = con.execute(
            "UPDATE invites SET status='accepted', responded_ts=? WHERE id=? AND status='pending'",
            (now_ts(), int(invite["id"])),
        )
        if int(cur.rowcount or 0) != 1:
            con.commit()
            return JSONResponse({"ok": False, "reason": "invite_already_handled"}, status_code=400)
        con.commit()

    gid = create_game(from_tag, tag, minutes)
    payload = {"type": "game_created", "data": {"game_id": gid, "w_tag": from_tag, "b_tag": tag}}
    await lobby_hub.send_to(from_tag, payload)
    await lobby_hub.send_to(tag, payload)
    return JSONResponse({"ok": True, "game_id": gid})


@app.post("/api/lobby/invite_decline")
async def lobby_invite_decline(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    with db() as con:
        invite = _invite_row_for_response(con, tag, body)
        if not invite:
            return JSONResponse({"ok": False, "reason": _invite_missing_reason(con, tag, body)}, status_code=400)
        from_tag = str(invite["from_tag"])
        cur = con.execute(
            "UPDATE invites SET status='declined', responded_ts=? WHERE id=? AND status='pending'",
            (now_ts(), int(invite["id"])),
        )
        if int(cur.rowcount or 0) != 1:
            con.commit()
            return JSONResponse({"ok": False, "reason": "invite_already_handled"}, status_code=400)
        con.commit()
    await lobby_hub.send_to(from_tag, {"type": "invite_declined", "data": {"to_tag": tag}})
    return JSONResponse({"ok": True})


# =========================================================
# MATCHMAKING API
# =========================================================
@app.post("/api/lobby/matchmaking_start")
async def lobby_matchmaking_start(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    minutes = _mm_norm_minutes(body.get("minutes"))
    now = now_ts()

    user = get_user(tag)
    if not user:
        return JSONResponse({"ok": False, "reason": "no_user"}, status_code=400)
    elo_now = int(user["elo"] or 0)

    with db() as con:
        con.execute("BEGIN IMMEDIATE")
        _mm_cleanup_expired_queue(con, now)

        if _mm_tag_has_active_game(con, tag):
            con.commit()
            return JSONResponse({"ok": False, "reason": "in_game"}, status_code=400)
        if _mm_tag_has_pending_invite(con, tag):
            con.commit()
            return JSONResponse({"ok": False, "reason": "invite_in_progress"}, status_code=400)

        self_row = _mm_upsert_queue_row(con, tag, minutes, elo_now, now)
        if not isinstance(self_row, sqlite3.Row):
            self_row = _mm_get_queue_row(con, tag)
        if not self_row:
            con.commit()
            return JSONResponse({"ok": False, "reason": "mm_failed"}, status_code=500)

        matched = _mm_try_match_on_con(con, tag, now)
        if matched:
            con.commit()
        else:
            con.commit()

    if matched:
        payload_game = {"type": "game_created", "data": {"game_id": matched["game_id"], "w_tag": matched["w_tag"], "b_tag": matched["b_tag"]}}
        await lobby_hub.send_to(matched["w_tag"], payload_game)
        await lobby_hub.send_to(matched["b_tag"], payload_game)

        # Ensure both players are informed matchmaking ended.
        payload_mm_w = {"type": "matchmaking", "data": {"searching": matched["searching_self"]}}
        payload_mm_b = {"type": "matchmaking", "data": {"searching": matched["searching_other"]}}
        await lobby_hub.send_to(matched["w_tag"], payload_mm_w)
        await lobby_hub.send_to(matched["b_tag"], payload_mm_b)

        return JSONResponse({"ok": True, "searching": matched["searching_self"]})

    return JSONResponse({"ok": True, "searching": _mm_searching_from_queue_row(self_row, now, in_queue=True)})


@app.post("/api/lobby/matchmaking_cancel")
async def lobby_matchmaking_cancel(req: Request, tag: str = Depends(session_dep_ban_guard)):
    _ = await req.json()
    now = now_ts()

    with db() as con:
        con.execute("BEGIN IMMEDIATE")
        row = _mm_get_queue_row(con, tag)
        con.execute("DELETE FROM matchmaking_queue WHERE tag=?", (tag,))
        con.commit()

    if row:
        searching = _mm_searching_from_queue_row(row, now, in_queue=False)
    else:
        user = get_user(tag)
        elo_now = int(user["elo"] or 0) if user else 0
        searching = {
            "in_queue": False,
            "minutes": 10,
            "elo": elo_now,
            "wait_seconds": 0,
            "delta_elo": _mm_delta_elo(0),
        }

    await lobby_hub.send_to(tag, {"type": "matchmaking", "data": {"searching": searching}})
    return JSONResponse({"ok": True, "searching": searching})


# =========================================================
# GAME API
# =========================================================
@app.post("/api/game/get")
async def game_get(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()

    with db() as con:
        row = load_game_row(con, gid)
        if not row:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row["w_tag"], row["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)
        row = refresh_game_row(con, row)
        return JSONResponse({"ok": True, "game": row_to_game(row, tag)})


@app.post("/api/game/legals")
async def game_legals(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()
    frm = (body.get("from") or "").strip()

    with db() as con:
        row = load_game_row(con, gid)
        if not row:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row["w_tag"], row["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)
        row = refresh_game_row(con, row)

    you = "w" if row["w_tag"] == tag else "b"
    if row["status"] != "active" or row["turn"] != you:
        return JSONResponse({"ok": True, "dests": []})
    if int(row["moved_this_turn"]) == 1:
        return JSONResponse({"ok": True, "dests": []})

    board = chess.Board(str(row["fen"]))
    try:
        from_sq = chess.parse_square(frm)
    except Exception:
        return JSONResponse({"ok": True, "dests": []})

    piece = board.piece_at(from_sq)
    if not piece or piece.color != (you == "w"):
        return JSONResponse({"ok": True, "dests": []})

    dests = []
    for mv in board.legal_moves:
        if mv.from_square == from_sq:
            dests.append(chess.square_name(mv.to_square))
    return JSONResponse({"ok": True, "dests": dests})


@app.post("/api/game/move")
async def game_move(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()
    uci = (body.get("uci") or "").strip().lower()

    with db() as con:
        row_db = load_game_row(con, gid)
        if not row_db:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row_db["w_tag"], row_db["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        row = dict(refresh_game_row(con, row_db))
        you = "w" if row["w_tag"] == tag else "b"
        if row["status"] != "active" or row["turn"] != you:
            return JSONResponse({"ok": False, "reason": "not_your_turn"}, status_code=400)
        if int(row["moved_this_turn"]) == 1:
            return JSONResponse({"ok": False, "reason": "already_moved"}, status_code=400)

        board = chess.Board(str(row["fen"]))

        try:
            mv = chess.Move.from_uci(uci)
        except Exception:
            return JSONResponse({"ok": False, "reason": "bad_uci"}, status_code=400)
        if mv not in board.legal_moves:
            return JSONResponse({"ok": False, "reason": "illegal"}, status_code=400)

        mover = board.piece_at(mv.from_square)
        if not mover or mover.color != (you == "w"):
            return JSONResponse({"ok": False, "reason": "wrong_piece"}, status_code=400)

        captured = None
        if board.is_en_passant(mv):
            cap_sq = chess.square(chess.square_file(mv.to_square), chess.square_rank(mv.from_square))
            captured = board.piece_at(cap_sq)
        else:
            captured = board.piece_at(mv.to_square)

        gain2 = 2
        if board.is_capture(mv) and mover and captured:
            gain2 += capture_bonus2(mover, captured)

        board.push(mv)
        row["fen"] = board.fen()
        row["draw_offer_by"] = ""
        row["last_move_from"] = chess.square_name(mv.from_square)
        row["last_move_to"] = chess.square_name(mv.to_square)

        if you == "w":
            row["coins2_w"] = int(row["coins2_w"]) + gain2
            row["consecutive_pass_w"] = 0
        else:
            row["coins2_b"] = int(row["coins2_b"]) + gain2
            row["consecutive_pass_b"] = 0

        row["moved_this_turn"] = 1
        finish_if_needed_inplace(row)
        save_game_state(con, row)
        con.commit()

        row2 = load_game_row(con, gid)
        gpub = row_to_game(row2, tag) if row2 else row_to_game(row, tag)

    await game_hub.push(gid, {"type": "state" if gpub["status"] == "active" else "finished", "data": gpub})
    return JSONResponse({"ok": True, "game": gpub})


@app.post("/api/game/drop_targets")
async def game_drop_targets(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()
    piece = (body.get("piece") or "").lower().strip()

    with db() as con:
        row = load_game_row(con, gid)
        if not row:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row["w_tag"], row["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)
        row = refresh_game_row(con, row)

    you = "w" if row["w_tag"] == tag else "b"
    if row["status"] != "active" or row["turn"] != you:
        return JSONResponse({"ok": True, "targets": []})
    if piece not in BASE_COSTS2:
        return JSONResponse({"ok": True, "targets": []})

    board = chess.Board(str(row["fen"]))
    targets = legal_drop_targets(board, you == "w", piece)
    return JSONResponse({"ok": True, "targets": sorted(list(targets))})


@app.post("/api/game/drop")
async def game_drop(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()
    piece = (body.get("piece") or "").lower().strip()
    sqname = (body.get("square") or "").strip().lower()

    if piece not in BASE_COSTS2:
        return JSONResponse({"ok": False, "reason": "bad_piece"}, status_code=400)

    with db() as con:
        row_db = load_game_row(con, gid)
        if not row_db:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row_db["w_tag"], row_db["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        row = dict(refresh_game_row(con, row_db))
        you = "w" if row["w_tag"] == tag else "b"
        if row["status"] != "active" or row["turn"] != you:
            return JSONResponse({"ok": False, "reason": "not_your_turn"}, status_code=400)

        board = chess.Board(str(row["fen"]))
        targets = legal_drop_targets(board, you == "w", piece)
        if sqname not in targets:
            return JSONResponse({"ok": False, "reason": "bad_square"}, status_code=400)

        base2 = BASE_COSTS2[piece]
        extra2 = int(row["post_buy_extra2_w"] if you == "w" else row["post_buy_extra2_b"])
        price2 = base2 + extra2
        coins2 = int(row["coins2_w"] if you == "w" else row["coins2_b"])
        if coins2 < price2:
            return JSONResponse({"ok": False, "reason": "no_money"}, status_code=400)

        try:
            sq = chess.parse_square(sqname)
        except Exception:
            return JSONResponse({"ok": False, "reason": "bad_square"}, status_code=400)

        ptype = piece_type_from_char(piece)
        if ptype is None:
            return JSONResponse({"ok": False, "reason": "bad_piece"}, status_code=400)

        board.set_piece_at(sq, chess.Piece(ptype, you == "w"))
        row["fen"] = board.fen()
        row["draw_offer_by"] = ""
        row["last_move_from"] = ""
        row["last_move_to"] = sqname

        if you == "w":
            row["coins2_w"] = coins2 - price2
            row["consecutive_pass_w"] = 0
        else:
            row["coins2_b"] = coins2 - price2
            row["consecutive_pass_b"] = 0

        row["bought_this_turn"] = 1
        if int(row["moved_this_turn"]) == 1:
            row["bought_after_move"] = 1
            if you == "w":
                row["post_buy_extra2_w"] = int(row["post_buy_extra2_w"]) + 1
            else:
                row["post_buy_extra2_b"] = int(row["post_buy_extra2_b"]) + 1

        save_game_state(con, row)
        con.commit()
        row2 = load_game_row(con, gid)
        gpub = row_to_game(row2, tag) if row2 else row_to_game(row, tag)

    await game_hub.push(gid, {"type": "state", "data": gpub})
    return JSONResponse({"ok": True, "game": gpub})


@app.post("/api/game/end_turn")
async def game_end_turn(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()

    with db() as con:
        row_db = load_game_row(con, gid)
        if not row_db:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row_db["w_tag"], row_db["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        row = dict(refresh_game_row(con, row_db))
        you = "w" if row["w_tag"] == tag else "b"
        if row["status"] != "active" or row["turn"] != you:
            return JSONResponse({"ok": False, "reason": "not_your_turn"}, status_code=400)

        moved = int(row["moved_this_turn"])
        bought = int(row["bought_this_turn"])
        bought_after_move = int(row["bought_after_move"])
        row["draw_offer_by"] = ""

        if moved == 1 and bought_after_move == 1:
            if you == "w":
                row["coins2_w"] = max(0, int(row["coins2_w"]) - 1)
            else:
                row["coins2_b"] = max(0, int(row["coins2_b"]) - 1)

        if moved == 0 and bought == 1:
            if you == "w":
                row["coins2_w"] = int(row["coins2_w"]) + 1
            else:
                row["coins2_b"] = int(row["coins2_b"]) + 1

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

        if row["status"] == "active":
            row["turn"] = "b" if you == "w" else "w"
            if moved == 0:
                board = chess.Board(str(row["fen"]))
                board.turn = not board.turn
                board.clear_stack()
                row["fen"] = board.fen()

        row["moved_this_turn"] = 0
        row["bought_this_turn"] = 0
        row["bought_after_move"] = 0
        if you == "w":
            row["post_buy_extra2_w"] = 0
        else:
            row["post_buy_extra2_b"] = 0

        finish_if_needed_inplace(row)
        save_game_state(con, row)
        con.commit()
        row2 = load_game_row(con, gid)
        gpub = row_to_game(row2, tag) if row2 else row_to_game(row, tag)

    await game_hub.push(gid, {"type": "state" if gpub["status"] == "active" else "finished", "data": gpub})
    return JSONResponse({"ok": True, "game": gpub})


@app.post("/api/game/resign")
async def game_resign(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()

    with db() as con:
        row_db = load_game_row(con, gid)
        if not row_db:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row_db["w_tag"], row_db["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        row = dict(refresh_game_row(con, row_db))
        if row["status"] != "active":
            gpub = row_to_game(load_game_row(con, gid), tag)
            return JSONResponse({"ok": True, "game": gpub})

        winner = "b" if row["w_tag"] == tag else "w"
        row["status"] = "finished"
        row["draw_offer_by"] = ""
        row["result_json"] = json.dumps({"winner": winner, "reason": "resign"})
        save_game_state(con, row)
        con.commit()
        row2 = load_game_row(con, gid)
        gpub = row_to_game(row2, tag) if row2 else row_to_game(row, tag)

    await game_hub.push(gid, {"type": "finished", "data": gpub})
    return JSONResponse({"ok": True, "game": gpub})


@app.post("/api/game/draw_offer")
async def game_draw_offer(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()

    with db() as con:
        row_db = load_game_row(con, gid)
        if not row_db:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row_db["w_tag"], row_db["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        row = dict(refresh_game_row(con, row_db))
        if row["status"] != "active":
            return JSONResponse({"ok": False, "reason": "not_active"}, status_code=400)

        you = "w" if row["w_tag"] == tag else "b"
        row["draw_offer_by"] = you
        save_game_state(con, row)
        con.commit()
        row2 = load_game_row(con, gid)
        gpub = row_to_game(row2, tag) if row2 else row_to_game(row, tag)

    await game_hub.push(gid, {"type": "state", "data": gpub})
    return JSONResponse({"ok": True, "game": gpub})


@app.post("/api/game/draw_accept")
async def game_draw_accept(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()

    with db() as con:
        row_db = load_game_row(con, gid)
        if not row_db:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row_db["w_tag"], row_db["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        row = dict(refresh_game_row(con, row_db))
        if row["status"] != "active":
            return JSONResponse({"ok": False, "reason": "not_active"}, status_code=400)

        you = "w" if row["w_tag"] == tag else "b"
        offer_by = str(row.get("draw_offer_by") or "")
        if not offer_by or offer_by == you:
            return JSONResponse({"ok": False, "reason": "no_offer"}, status_code=400)

        row["status"] = "finished"
        row["draw_offer_by"] = ""
        row["result_json"] = json.dumps({"winner": "", "reason": "draw_agreed"})
        save_game_state(con, row)
        con.commit()
        row2 = load_game_row(con, gid)
        gpub = row_to_game(row2, tag) if row2 else row_to_game(row, tag)

    await game_hub.push(gid, {"type": "finished", "data": gpub})
    return JSONResponse({"ok": True, "game": gpub})


@app.post("/api/game/draw_decline")
async def game_draw_decline(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()

    with db() as con:
        row_db = load_game_row(con, gid)
        if not row_db:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row_db["w_tag"], row_db["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        row = dict(refresh_game_row(con, row_db))
        if row["status"] != "active":
            return JSONResponse({"ok": False, "reason": "not_active"}, status_code=400)

        you = "w" if row["w_tag"] == tag else "b"
        offer_by = str(row.get("draw_offer_by") or "")
        if not offer_by or offer_by == you:
            return JSONResponse({"ok": False, "reason": "no_offer"}, status_code=400)

        row["draw_offer_by"] = ""
        save_game_state(con, row)
        con.commit()
        row2 = load_game_row(con, gid)
        gpub = row_to_game(row2, tag) if row2 else row_to_game(row, tag)

    await game_hub.push(gid, {"type": "state", "data": gpub})
    return JSONResponse({"ok": True, "game": gpub})


@app.post("/api/game/rematch_offer")
async def game_rematch_offer(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()

    with db() as con:
        row = load_game_row(con, gid)
        if not row:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row["w_tag"], row["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        row_dict = dict(row)
        if row_dict["status"] != "finished":
            return JSONResponse({"ok": False, "reason": "not_finished"}, status_code=400)

        you = "w" if row_dict["w_tag"] == tag else "b"
        row_dict["rematch_offer_by"] = you
        save_game_state(con, row_dict)
        con.commit()
        row2 = load_game_row(con, gid)
        gpub = row_to_game(row2, tag) if row2 else row_to_game(row, tag)

    await game_hub.push(gid, {"type": "state", "data": gpub})
    return JSONResponse({"ok": True, "game": gpub})


@app.post("/api/game/rematch_accept")
async def game_rematch_accept(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()

    with db() as con:
        row = load_game_row(con, gid)
        if not row:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row["w_tag"], row["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        row_dict = dict(row)
        if row_dict["status"] != "finished":
            return JSONResponse({"ok": False, "reason": "not_finished"}, status_code=400)

        you = "w" if row_dict["w_tag"] == tag else "b"
        offer_by = str(row_dict.get("rematch_offer_by") or "")
        if not offer_by or offer_by == you:
            return JSONResponse({"ok": False, "reason": "no_offer"}, status_code=400)

        new_gid = create_game(str(row_dict["w_tag"]), str(row_dict["b_tag"]), int(row_dict["minutes"]))
        row_dict["rematch_offer_by"] = ""
        save_game_state(con, row_dict)
        con.commit()

        payload = {
            "type": "rematch_game_created",
            "data": {
                "game_id": new_gid,
                "w_tag": row_dict["w_tag"],
                "b_tag": row_dict["b_tag"],
            },
        }

    await game_hub.push(gid, payload)
    await lobby_hub.send_to(str(row_dict["w_tag"]), payload)
    await lobby_hub.send_to(str(row_dict["b_tag"]), payload)
    return JSONResponse({"ok": True, "game_id": new_gid})


@app.post("/api/game/rematch_decline")
async def game_rematch_decline(req: Request, tag: str = Depends(session_dep_ban_guard)):
    body = await req.json()
    gid = (body.get("game_id") or "").strip()

    with db() as con:
        row = load_game_row(con, gid)
        if not row:
            return JSONResponse({"ok": False, "reason": "no_game"}, status_code=400)
        if tag not in (row["w_tag"], row["b_tag"]):
            return JSONResponse({"ok": False, "reason": "not_player"}, status_code=403)

        row_dict = dict(row)
        if row_dict["status"] != "finished":
            return JSONResponse({"ok": False, "reason": "not_finished"}, status_code=400)

        you = "w" if row_dict["w_tag"] == tag else "b"
        offer_by = str(row_dict.get("rematch_offer_by") or "")
        if not offer_by or offer_by == you:
            return JSONResponse({"ok": False, "reason": "no_offer"}, status_code=400)

        row_dict["rematch_offer_by"] = ""
        save_game_state(con, row_dict)
        con.commit()
        row2 = load_game_row(con, gid)
        gpub = row_to_game(row2, tag) if row2 else row_to_game(row, tag)

    await game_hub.push(gid, {"type": "state", "data": gpub})
    return JSONResponse({"ok": True, "game": gpub})


# =========================================================
# WebSocket endpoints
# =========================================================
@app.websocket("/ws/lobby")
async def ws_lobby(ws: WebSocket):
    token = ws.query_params.get("token", "")

    tag = load_session_tag(token)
    if not tag:
        await ws.close(code=4401)
        return

    with db() as con:
        if _get_active_ban_row(con, tag, now_ts()):
            await ws.close(code=4402)
            return

    await ws.accept()
    await lobby_hub.add(tag, ws)

    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await lobby_hub.remove(tag, ws)


@app.websocket("/ws/game/{gid}")
async def ws_game(ws: WebSocket, gid: str):
    token = ws.query_params.get("token", "")

    tag = load_session_tag(token)
    if not tag:
        await ws.close(code=4401)
        return

    with db() as con:
        row = load_game_row(con, gid)
        if not row or tag not in (row["w_tag"], row["b_tag"]):
            await ws.close(code=4403)
            return
        if _get_active_ban_row(con, tag, now_ts()):
            await ws.close(code=4402)
            return
        row = refresh_game_row(con, row)

    await ws.accept()
    await game_hub.add(gid, ws)
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
async def tg_send(chat_id: int, text: str, reply_markup: Optional[Dict[str, Any]] = None) -> None:
    if not BOT_TOKEN:
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup

    async with httpx.AsyncClient() as client:
        await client.post(url, json=payload)


@app.post("/webhook")
async def telegram_webhook(req: Request):
    if TELEGRAM_WEBHOOK_SECRET:
        got = req.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if not got or not hmac.compare_digest(got, TELEGRAM_WEBHOOK_SECRET):
            return JSONResponse({"ok": False, "reason": "bad_webhook_secret"}, status_code=401)
    data = await req.json()
    message = data.get("message")
    if not message:
        return {"ok": True}

    text = message.get("text", "")
    chat = message.get("chat", {})
    user = message.get("from", {})

    tg_id = user.get("id")
    tg_username = user.get("username")

    if not tg_id:
        return {"ok": True}

    if not tg_username:
        if str(text).startswith("/start"):
            await tg_send(int(chat.get("id", tg_id)), "Сначала задай себе Telegram @username в настройках, потом снова нажми /start.")
        return {"ok": True}

    tag = norm_tag(str(tg_username))

    with db() as con:
        con.execute(
            """
            INSERT INTO tg_users(tag, tg_id, tg_username, created_ts)
            VALUES (?,?,?,?)
            ON CONFLICT(tag) DO UPDATE SET
                tg_id=excluded.tg_id,
                tg_username=excluded.tg_username
            """,
            (tag, tg_id, tg_username, now_ts()),
        )
        con.commit()

    if str(text).startswith("/start"):
        if WEBAPP_URL:
            await tg_send(
                int(chat.get("id", tg_id)),
                "Бот подключён. Открывай игру кнопкой ниже.",
                reply_markup={
                    "inline_keyboard": [
                        [
                            {
                                "text": "Открыть игру",
                                "web_app": {"url": WEBAPP_URL},
                            }
                        ]
                    ]
                },
            )
        else:
            await tg_send(int(chat.get("id", tg_id)), "Бот подключён. Теперь можешь авторизоваться в игре.")

    return {"ok": True}
