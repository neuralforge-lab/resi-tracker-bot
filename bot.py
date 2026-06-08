#!/usr/bin/env python3
"""
Shopee Resi Tracker Bot v3 - Secure Edition
- Credentials in .env
- Max 5 resi per user
- Cache (no re-query within 5 min)
- Rate limit: 10 manual checks/min per user
"""

import os
import json
import sqlite3
import logging
import asyncio
import time
from collections import defaultdict
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters, ConversationHandler
)
import urllib.request

# ═══════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
BITESHIP_API_KEY = os.getenv("BITESHIP_API_KEY")
BITESHIP_BASE = "https://api.biteship.com/v1"
TRACK17_TOKEN = os.getenv("TRACK17_TOKEN")
TRACK17_BASE = "https://api.17track.net/track/v2"
CHECK_INTERVAL = 1800   # 30 min auto-check
DB_PATH = "resi_tracker.db"
MAX_RESI_PER_USER = 5
CACHE_TTL = 300         # 5 min cache
RATE_LIMIT_WINDOW = 60  # 1 minute
RATE_LIMIT_MAX = 10     # max manual checks per minute

WAITING_WAYBILL = 1

# Couriers
COURIERS = {
    "jnt": {"name": "J&T Express", "emoji": "🔴"},
    "jne": {"name": "JNE", "emoji": "🟡"},
    "sicepat": {"name": "SiCepat", "emoji": "🔵"},
    "anteraja": {"name": "Anteraja", "emoji": "🟢"},
    "pos": {"name": "Pos Indonesia", "emoji": "📮"},
    "tiki": {"name": "TIKI", "emoji": "🟠"},
    "ninja": {"name": "Ninja Express", "emoji": "🥷"},
    "idexpress": {"name": "ID Express", "emoji": "📦"},
    "wahana": {"name": "Wahana", "emoji": "🚚"},
    "lion": {"name": "Lion Parcel", "emoji": "🦁"},
    "paxel": {"name": "Paxel", "emoji": "✈️"},
    "grab": {"name": "Grab Express", "emoji": "🟢"},
    "gojek": {"name": "GoSend", "emoji": "🏍️"},
    "borzo": {"name": "Borzo", "emoji": "⚡"},
    "spx": {"name": "Shopee Express", "emoji": "🟣"},
    "rpx": {"name": "RPX", "emoji": "📦"},
}

AUTO_DETECT_ORDER = [
    "jnt", "jne", "sicepat", "anteraja",
    "pos", "ninja", "idexpress", "tiki", "wahana",
    "lion", "paxel", "borzo", "grab", "gojek", "rpx"
]

# State
user_state = {}
# Cache: {f"{waybill}_{courier_code}": {"data": ..., "timestamp": ...}}
track_cache = {}
# Rate limit: {chat_id: [timestamps]}
rate_tracker = defaultdict(list)

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════
# RATE LIMITER
# ═══════════════════════════════════════════
def check_rate_limit(chat_id):
    """Returns (allowed, remaining)"""
    now = time.time()
    # Clean old entries
    rate_tracker[chat_id] = [t for t in rate_tracker[chat_id] if now - t < RATE_LIMIT_WINDOW]
    if len(rate_tracker[chat_id]) >= RATE_LIMIT_MAX:
        return False, 0
    rate_tracker[chat_id].append(now)
    return True, RATE_LIMIT_MAX - len(rate_tracker[chat_id])

# ═══════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS resi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            waybill TEXT NOT NULL,
            courier_code TEXT NOT NULL,
            courier_name TEXT NOT NULL,
            label TEXT DEFAULT '',
            last_status TEXT DEFAULT '',
            last_checkpoint TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(chat_id, waybill)
        )
    """)
    conn.commit()
    conn.close()

def db_add(chat_id, waybill, courier_code, courier_name, label=""):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute(
            "INSERT INTO resi (chat_id, waybill, courier_code, courier_name, label) VALUES (?, ?, ?, ?, ?)",
            (chat_id, waybill.upper(), courier_code, courier_name, label)
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def db_list(chat_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, waybill, courier_code, courier_name, label, last_status FROM resi WHERE chat_id = ?", (chat_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def db_count(chat_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM resi WHERE chat_id = ?", (chat_id,))
    count = c.fetchone()[0]
    conn.close()
    return count

def db_delete(chat_id, waybill):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM resi WHERE chat_id = ? AND waybill = ?", (chat_id, waybill.upper()))
    deleted = c.rowcount
    conn.commit()
    conn.close()
    return deleted > 0

def db_get_all():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, chat_id, waybill, courier_code, last_status, last_checkpoint FROM resi")
    rows = c.fetchall()
    conn.close()
    return rows

def db_update_status(resi_id, status, checkpoint):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE resi SET last_status = ?, last_checkpoint = ? WHERE id = ?", (status, checkpoint, resi_id))
    conn.commit()
    conn.close()

# ═══════════════════════════════════════════
# BITESHIP API + CACHE
# ═══════════════════════════════════════════
def track_resi(waybill, courier_code, use_cache=True):
    cache_key = f"{waybill}_{courier_code}"

    # Check cache
    if use_cache and cache_key in track_cache:
        cached = track_cache[cache_key]
        if time.time() - cached["timestamp"] < CACHE_TTL:
            logger.debug(f"Cache hit: {cache_key}")
            return cached["data"]

    url = f"{BITESHIP_BASE}/trackings/{waybill}?courier_code={courier_code}"
    req = urllib.request.Request(url, headers={
        "Authorization": BITESHIP_API_KEY,
        "Content-Type": "application/json"
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            # Save to cache
            track_cache[cache_key] = {"data": data, "timestamp": time.time()}
            return data
    except Exception as e:
        logger.debug(f"Track {waybill}/{courier_code}: {e}")
        return None

def track_17track(waybill, use_cache=True):
    """Track via 17track API (for SPX)"""
    cache_key = f"17t_{waybill}"

    # Check cache
    if use_cache and cache_key in track_cache:
        cached = track_cache[cache_key]
        if time.time() - cached["timestamp"] < CACHE_TTL:
            logger.debug(f"Cache hit 17track: {cache_key}")
            return cached["data"]

    try:
        # Step 1: Register tracking
        reg_data = json.dumps([{"number": waybill}]).encode()
        req = urllib.request.Request(
            f"{TRACK17_BASE}/register",
            data=reg_data,
            headers={
                "17token": TRACK17_TOKEN,
                "Content-Type": "application/json"
            },
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            reg_resp = json.loads(resp.read().decode())

        # code 0 = OK, even if rejected (already registered)
        if reg_resp.get("code") != 0:
            return None

        accepted = reg_resp.get("data", {}).get("accepted", [])
        rejected = reg_resp.get("data", {}).get("rejected", [])

        # If rejected because already registered, that's fine - continue to gettrackinfo
        if not accepted and not rejected:
            return None

        # Wait a bit for 17track to fetch data (only if newly registered)
        if accepted:
            import time as t
            t.sleep(2)

        get_data = json.dumps([{"number": waybill}]).encode()
        req2 = urllib.request.Request(
            f"{TRACK17_BASE}/gettrackinfo",
            data=get_data,
            headers={
                "17token": TRACK17_TOKEN,
                "Content-Type": "application/json"
            },
            method="POST"
        )
        with urllib.request.urlopen(req2, timeout=15) as resp2:
            result = json.loads(resp2.read().decode())

        if result.get("code") != 0:
            return None

        track_info = (result.get("data", {}).get("accepted", [{}]) or [{}])[0].get("track_info", {})
        if not track_info:
            return None

        # Convert to our format
        providers = track_info.get("tracking", {}).get("providers", [])
        provider_name = providers[0].get("provider", {}).get("name", "Shopee Express") if providers else "Shopee Express"

        events = []
        if providers:
            for evt in providers[0].get("events", []):
                events.append({
                    "status": evt.get("stage") or "InTransit",
                    "description": evt.get("description") or "",
                    "updated_at": evt.get("time_iso") or "",
                    "location": ""
                })

        latest = track_info.get("latest_status", {})
        latest_event = track_info.get("latest_event", {})

        data = {
            "success": True,
            "courier_name": provider_name or "Shopee Express",
            "history": events if events else [{
                "status": latest.get("status") or "InTransit",
                "description": latest_event.get("description") or "",
                "updated_at": latest_event.get("time_iso") or "",
                "location": ""
            }]
        }

        # Save to cache
        track_cache[cache_key] = {"data": data, "timestamp": time.time()}
        return data

    except Exception as e:
        logger.error(f"17track error {waybill}: {e}")
        return None

def format_tracking_17track(data, waybill):
    """Format 17track tracking data for display"""
    if not data or not data.get("success"):
        return None

    history = data.get("history", [])
    courier_name = data.get("courier_name") or "Shopee Express"

    if not history:
        return f"📦 **{waybill}** ({courier_name})\nBelum ada riwayat pengiriman."

    latest = history[0]
    status = latest.get("status") or "Unknown"
    desc = latest.get("description") or ""
    date = latest.get("updated_at") or ""

    status_emoji = {
        "InfoReceived": "📝", "PickedUp": "✅", "InTransit": "🚚",
        "Delivered": "📬", "AvailableForPickup": "📦", "OutForDelivery": "🏃",
        "Returning": "↩️", "Returned": "↩️", "FailedDelivery": "❌"
    }
    emoji = status_emoji.get(status, "📦")

    msg = f"{emoji} **{waybill}** ({courier_name})\n"
    msg += f"Status: **{status.upper()}**\n"
    if desc:
        msg += f"Detail: {desc}\n"
    if date:
        msg += f"Update: {date}\n"

    if len(history) > 1:
        msg += f"\n📋 **Riwayat:**\n"
        for h in history[:3]:
            h_status = h.get("status", "")
            h_desc = h.get("description", "")
            h_date = h.get("updated_at", "")
            h_emoji = status_emoji.get(h_status, "•")
            msg += f"  {h_emoji} {h_desc} ({h_date})\n"

    return msg

def format_tracking(data, waybill, courier_name):
    if not data or not data.get("success"):
        return None

    history = data.get("history", [])
    if not history:
        return f"📦 **{waybill}** ({courier_name})\nBelum ada riwayat pengiriman."

    latest = history[0]
    status = latest.get("status", "Unknown")
    desc = latest.get("description", "")
    date = latest.get("updated_at", "")
    location = latest.get("location", "")

    status_emoji = {
        "pending": "⏳", "picking_up": "🏃", "picked": "✅",
        "in_transit": "🚚", "on_hold": "⚠️", "delivered": "📬",
        "returned": "↩️", "failed_delivery": "❌",
    }
    emoji = status_emoji.get(status.lower(), "📦")

    msg = f"{emoji} **{waybill}** ({courier_name})\n"
    msg += f"Status: **{status.upper()}**\n"
    if desc:
        msg += f"Detail: {desc}\n"
    if location:
        msg += f"Lokasi: {location}\n"
    if date:
        msg += f"Update: {date}\n"

    if len(history) > 1:
        msg += f"\n📋 **Riwayat:**\n"
        for h in history[:3]:
            h_status = h.get("status", "")
            h_desc = h.get("description", "")
            h_date = h.get("updated_at", "")
            h_emoji = status_emoji.get(h_status.lower(), "•")
            msg += f"  {h_emoji} {h_desc} ({h_date})\n"

    return msg

# ═══════════════════════════════════════════
# INLINE KEYBOARDS
# ═══════════════════════════════════════════
def main_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("🔍 Auto-Detect Resi", callback_data="add_auto")],
        [
            InlineKeyboardButton("🔴 J&T", callback_data="add_jnt"),
            InlineKeyboardButton("🟡 JNE", callback_data="add_jne"),
        ],
        [InlineKeyboardButton("🔵 SiCepat", callback_data="add_sicepat"),
            InlineKeyboardButton("🟢 Anteraja", callback_data="add_anteraja")],
        [InlineKeyboardButton("📮 Pos", callback_data="add_pos"),
            InlineKeyboardButton("🥷 Ninja", callback_data="add_ninja")],
        [InlineKeyboardButton("🟣 SPX (Shopee)", callback_data="add_spx")],
        [InlineKeyboardButton("🟠 TIKI", callback_data="add_tiki")],
        [
            InlineKeyboardButton("📋 Lihat Resi", callback_data="list"),
            InlineKeyboardButton("❓ Bantuan", callback_data="help"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)

def resi_list_keyboard(rows):
    keyboard = []
    for (id_, waybill, courier_code, courier_name, label, last_status) in rows:
        status_icon = "📦"
        if last_status:
            sl = last_status.lower()
            if sl == "delivered": status_icon = "✅"
            elif sl == "in_transit": status_icon = "🚚"
            elif sl == "on_hold": status_icon = "⚠️"
            elif sl in ("pending", "picking_up"): status_icon = "⏳"
        label_text = f" - {label}" if label else ""
        btn_text = f"{status_icon} {waybill}{label_text}"
        keyboard.append([
            InlineKeyboardButton(btn_text, callback_data=f"status_{waybill}"),
            InlineKeyboardButton("🗑️", callback_data=f"delete_{waybill}"),
        ])
    keyboard.append([InlineKeyboardButton("◀️ Kembali", callback_data="back_menu")])
    return InlineKeyboardMarkup(keyboard)

def status_keyboard(waybill):
    keyboard = [
        [
            InlineKeyboardButton("🔄 Refresh", callback_data=f"status_{waybill}"),
            InlineKeyboardButton("🗑️ Hapus", callback_data=f"delete_{waybill}"),
        ],
        [InlineKeyboardButton("◀️ Kembali", callback_data="list")],
    ]
    return InlineKeyboardMarkup(keyboard)

# ═══════════════════════════════════════════
# HANDLERS
# ═══════════════════════════════════════════
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    count = db_count(chat_id)
    slot_info = f"\n📊 Resi ditrack: **{count}/{MAX_RESI_PER_USER}**" if count else ""

    welcome = f"""
🤖 **Resi Tracker Bot**

Lacak paket Shopee & marketplace lainnya!{slot_info}

🔍 **Auto-Detect** — Bot cari kurir otomatis
📦 **Pilih Kurir** — Langsung pilih kurir

💡 Atau tinggal paste nomor resi langsung!
📋 Untuk melihat daftar resi yang loe simpan, klik **"Lihat Resi"**

⚠️ _Bot masih dalam tahap pengembangan_
✅ _Sekarang support tracking SPX (Shopee Express)!_
👨‍💻 Dibuat oleh Rahman Hidayat
    """
    if update.message:
        await update.message.reply_text(welcome, parse_mode="Markdown", reply_markup=main_menu_keyboard())
    elif update.callback_query:
        await update.callback_query.edit_message_text(welcome, parse_mode="Markdown", reply_markup=main_menu_keyboard())

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    data = query.data

    # Auto-detect
    if data == "add_auto":
        count = db_count(chat_id)
        if count >= MAX_RESI_PER_USER:
            await query.edit_message_text(
                f"⚠️ Resi lo udah penuh ({count}/{MAX_RESI_PER_USER})!\nHapus dulu yang udah sampai.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 Lihat Resi", callback_data="list")],
                    [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
                ])
            )
            return ConversationHandler.END
        user_state[chat_id] = {"action": "auto"}
        await query.edit_message_text(
            f"🔍 Kirim nomor resi:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Batal", callback_data="back_menu")]])
        )
        return WAITING_WAYBILL

    # Courier
    if data.startswith("add_"):
        courier_code = data[4:]
        if courier_code in COURIERS:
            count = db_count(chat_id)
            if count >= MAX_RESI_PER_USER:
                await query.edit_message_text(
                    f"⚠️ Resi lo udah penuh ({count}/{MAX_RESI_PER_USER})!\nHapus dulu yang udah sampai.",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 Lihat Resi", callback_data="list")],
                        [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
                    ])
                )
                return ConversationHandler.END
            user_state[chat_id] = {"action": "courier", "courier_code": courier_code}
            c_info = COURIERS[courier_code]
            await query.edit_message_text(
                f"{c_info['emoji']} {c_info['name']}\nKirim nomor resi:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Batal", callback_data="back_menu")]])
            )
            return WAITING_WAYBILL

    # List
    if data == "list":
        rows = db_list(chat_id)
        if not rows:
            await query.edit_message_text(
                "📭 **Belum ada resi yang ditrack.**",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔍 Tambah Resi", callback_data="add_auto")],
                    [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
                ])
            )
            return ConversationHandler.END
        msg = f"📦 **Daftar Resi** ({len(rows)}/{MAX_RESI_PER_USER})\n\n"
        for i, (_, waybill, _, courier_name, label, last_status) in enumerate(rows, 1):
            label_str = f" — {label}" if label else ""
            status_str = f"\n   {last_status}" if last_status else ""
            msg += f"{i}. `{waybill}` ({courier_name}){label_str}{status_str}\n"
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=resi_list_keyboard(rows))
        return ConversationHandler.END

    # Status
    if data.startswith("status_"):
        waybill = data[7:].upper()

        allowed, remaining = check_rate_limit(chat_id)
        if not allowed:
            await query.answer("⏳ Kebanyakan request! Tunggu 1 menit.", show_alert=True)
            return ConversationHandler.END

        rows = db_list(chat_id)
        found = None
        for row in rows:
            if row[1] == waybill:
                found = row
                break
        if not found:
            await query.edit_message_text("❌ Resi gak ditemukan.", reply_markup=main_menu_keyboard())
            return ConversationHandler.END

        _, wb, courier_code, courier_name, label, _ = found

        # Use 17track for SPX
        if courier_code == "spx":
            track_data = track_17track(wb)
            msg = format_tracking_17track(track_data, wb)
        else:
            track_data = track_resi(wb, courier_code)
            msg = format_tracking(track_data, wb, courier_name)
        if msg:
            if label:
                msg = f"🏷️ **{label}**\n\n{msg}"
            await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=status_keyboard(wb))
        else:
            await query.edit_message_text(f"❌ Gagal cek `{wb}`.", parse_mode="Markdown", reply_markup=status_keyboard(wb))
        return ConversationHandler.END

    # Delete
    if data.startswith("delete_"):
        waybill = data[7:].upper()
        db_delete(chat_id, waybill)
        count = db_count(chat_id)
        await query.edit_message_text(
            f"🗑️ Resi `{waybill}` dihapus!\n📊 Sisa: {count}/{MAX_RESI_PER_USER}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Lihat Resi", callback_data="list")],
                [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
            ])
        )
        return ConversationHandler.END

    # Help
    if data == "help":
        help_text = f"""
❓ **Bantuan Resi Tracker**

**Cara pakai:**
1️⃣ Klik tombol kurir / Auto-Detect
2️⃣ Ketik/paste nomor resi
3️⃣ Bot langsung lacak!

**Atau:** tinggal paste nomor resi ke chat

**Limits:**
• Max {MAX_RESI_PER_USER} resi per user
• Cache 5 menit (hemat API)
• Auto-check tiap 30 menit
• Rate limit: 10 cek/menit

🔄 Auto-notif kalo status berubah!
        """
        await query.edit_message_text(
            help_text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]])
        )
        return ConversationHandler.END

    if data == "back_menu":
        user_state.pop(chat_id, None)
        await cmd_start(update, context)
        return ConversationHandler.END

    return ConversationHandler.END

async def handle_waybill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    waybill = update.message.text.strip().upper()

    if len(waybill) < 5 or len(waybill) > 30:
        await update.message.reply_text("❌ Nomor resi gak valid. Coba lagi:")
        return WAITING_WAYBILL

    # Rate limit check
    allowed, remaining = check_rate_limit(chat_id)
    if not allowed:
        await update.message.reply_text(
            "⏳ Kebanyakan request! Tunggu 1 menit.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]])
        )
        return ConversationHandler.END

    state = user_state.get(chat_id, {})
    action = state.get("action", "auto")

    # Manual courier
    if action == "courier":
        courier_code = state.get("courier_code")
        c_info = COURIERS[courier_code]

        # Check if SPX - use 17track
        if courier_code == "spx":
            loading = await update.message.reply_text(f"{c_info['emoji']} Mengecek `{waybill}` via 17track...", parse_mode="Markdown")
            data = track_17track(waybill)

            if data and data.get("success"):
                added = db_add(chat_id, waybill, "spx", "Shopee Express")
                if not added:
                    await loading.edit_text(
                        f"⚠️ Resi `{waybill}` udah ditrack.", parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("📋 Lihat Semua", callback_data="list")],
                            [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
                        ])
                    )
                else:
                    msg = format_tracking_17track(data, waybill)
                    if msg:
                        msg = f"✅ **Resi ditambahkan!**\n\n{msg}"
                        await loading.edit_text(msg, parse_mode="Markdown",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("🔄 Refresh", callback_data=f"status_{waybill}")],
                                [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
                            ]))
                    else:
                        await loading.edit_text(
                            f"✅ Resi `{waybill}` ditambahkan (Shopee Express)",
                            parse_mode="Markdown",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]])
                        )
            else:
                await loading.edit_text(
                    f"❌ Gak bisa track `{waybill}` di SPX.\nPastikan nomor resi benar.",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]])
                )

            user_state.pop(chat_id, None)
            return ConversationHandler.END

        # Non-SPX couriers - use Biteship
        loading = await update.message.reply_text(f"{c_info['emoji']} Mengecek `{waybill}`...", parse_mode="Markdown")
        data = track_resi(waybill, courier_code)

        if data and data.get("success"):
            added = db_add(chat_id, waybill, courier_code, c_info["name"])
            if not added:
                await loading.edit_text(
                    f"⚠️ Resi `{waybill}` udah ditrack.", parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 Lihat Semua", callback_data="list")],
                        [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
                    ])
                )
            else:
                msg = format_tracking(data, waybill, c_info["name"])
                if msg:
                    msg = f"✅ **Resi ditambahkan!**\n\n{msg}"
                    await loading.edit_text(msg, parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔄 Refresh", callback_data=f"status_{waybill}")],
                            [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
                        ]))
                else:
                    await loading.edit_text(
                        f"✅ Resi `{waybill}` ditambahkan ({c_info['name']})",
                        parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]])
                    )
        else:
            added = db_add(chat_id, waybill, courier_code, c_info["name"])
            await loading.edit_text(
                f"{'✅' if added else '⚠️'} Resi `{waybill}` {'ditambahkan' if added else 'udah ditrack'} ({c_info['name']})\n{'⚠️ Belum ada data. Akan dicek otomatis.' if added else ''}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]])
            )

        user_state.pop(chat_id, None)
        return ConversationHandler.END

    # Auto-detect
    loading = await update.message.reply_text(f"🔍 Auto-detecting `{waybill}`...", parse_mode="Markdown")

    # Check if it's SPX format first
    if waybill.upper().startswith("SPX"):
        data = track_17track(waybill)
        if data and data.get("success"):
            added = db_add(chat_id, waybill, "spx", "Shopee Express")
            if not added:
                await loading.edit_text(
                    f"⚠️ Resi `{waybill}` udah ditrack.", parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 Lihat Semua", callback_data="list")],
                        [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
                    ])
                )
            else:
                msg = format_tracking_17track(data, waybill)
                if msg:
                    msg = f"✅ Kurir: **🟣 Shopee Express**\n\n{msg}"
                    await loading.edit_text(msg, parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔄 Refresh", callback_data=f"status_{waybill}")],
                            [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
                        ]))
                else:
                    await loading.edit_text(
                        f"✅ Kurir: **🟣 Shopee Express**\nResi ditambahkan!",
                        parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]])
                    )
            user_state.pop(chat_id, None)
            return ConversationHandler.END

    # Try Biteship couriers
    found_code = None
    found_data = None
    for code in AUTO_DETECT_ORDER:
        data = track_resi(waybill, code)
        if data and data.get("success") and data.get("history"):
            found_code = code
            found_data = data
            break
        await asyncio.sleep(0.3)

    if found_code:
        c_info = COURIERS[found_code]
        added = db_add(chat_id, waybill, found_code, c_info["name"])
        if not added:
            await loading.edit_text(
                f"⚠️ Resi `{waybill}` udah ditrack.", parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 Lihat Semua", callback_data="list")],
                    [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
                ])
            )
        else:
            msg = format_tracking(found_data, waybill, c_info["name"])
            if msg:
                msg = f"✅ Kurir: **{c_info['emoji']} {c_info['name']}**\n\n{msg}"
                await loading.edit_text(msg, parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Refresh", callback_data=f"status_{waybill}")],
                        [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
                    ]))
            else:
                await loading.edit_text(
                    f"✅ Kurir: **{c_info['emoji']} {c_info['name']}**\nResi ditambahkan!",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]])
                )
    else:
        await loading.edit_text(
            f"❌ Gak detect kurir `{waybill}`.\nPilih kurir manual:",
            parse_mode="Markdown", reply_markup=main_menu_keyboard()
        )

    user_state.pop(chat_id, None)
    return ConversationHandler.END

async def quick_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.replace("-", "").replace(" ", "").isalnum():
        return
    if len(text) < 5 or len(text) > 30:
        return

    chat_id = update.effective_chat.id
    waybill = text.upper()

    # Rate limit
    allowed, _ = check_rate_limit(chat_id)
    if not allowed:
        await update.message.reply_text("⏳ Kebanyakan request! Tunggu 1 menit.")
        return

    # Already tracked? Show status
    rows = db_list(chat_id)
    for row in rows:
        if row[1] == waybill:
            _, wb, courier_code, courier_name, label, _ = row
            track_data = track_resi(wb, courier_code)
            msg = format_tracking(track_data, wb, courier_name)
            if msg:
                if label:
                    msg = f"🏷️ **{label}**\n\n{msg}"
                await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=status_keyboard(wb))
            return

    # Check limit
    count = db_count(chat_id)
    if count >= MAX_RESI_PER_USER:
        await update.message.reply_text(
            f"⚠️ Resi penuh ({count}/{MAX_RESI_PER_USER})! Hapus dulu.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📋 Lihat Resi", callback_data="list")]])
        )
        return

    # Check if SPX format
    if waybill.startswith("SPX"):
        loading = await update.message.reply_text(f"🔍 Auto-detecting `{waybill}`...", parse_mode="Markdown")
        data = track_17track(waybill)
        if data and data.get("success"):
            db_add(chat_id, waybill, "spx", "Shopee Express")
            msg = format_tracking_17track(data, waybill)
            if msg:
                msg = f"✅ Kurir: **🟣 Shopee Express**\n\n{msg}"
                await loading.edit_text(msg, parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Refresh", callback_data=f"status_{waybill}")],
                        [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
                    ]))
            return
        await loading.edit_text(
            f"❌ Gak detect kurir `{waybill}`.\nPilih kurir manual:",
            parse_mode="Markdown", reply_markup=main_menu_keyboard()
        )
        return

    # Auto-detect
    loading = await update.message.reply_text(f"🔍 Auto-detecting `{waybill}`...", parse_mode="Markdown")
    for code in AUTO_DETECT_ORDER:
        data = track_resi(waybill, code)
        if data and data.get("success") and data.get("history"):
            c_info = COURIERS[code]
            db_add(chat_id, waybill, code, c_info["name"])
            msg = format_tracking(data, waybill, c_info["name"])
            if msg:
                msg = f"✅ Kurir: **{c_info['emoji']} {c_info['name']}**\n\n{msg}"
                await loading.edit_text(msg, parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Refresh", callback_data=f"status_{waybill}")],
                        [InlineKeyboardButton("◀️ Menu", callback_data="back_menu")]
                    ]))
            return
        await asyncio.sleep(0.3)

    await loading.edit_text(
        f"❌ Gak detect kurir `{waybill}`.\nPilih kurir manual:",
        parse_mode="Markdown", reply_markup=main_menu_keyboard()
    )

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_state.pop(chat_id, None)
    await update.message.reply_text("❌ Dibatalin.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END

# ═══════════════════════════════════════════
# AUTO CHECK
# ═══════════════════════════════════════════
async def auto_check(context: ContextTypes.DEFAULT_TYPE):
    logger.info("🔄 Auto-check running...")
    all_resi = db_get_all()
    notified = 0

    for resi_id, chat_id, waybill, courier_code, last_status, last_checkpoint in all_resi:
        # Use 17track for SPX, Biteship for others
        if courier_code == "spx":
            data_17 = track_17track(waybill, use_cache=False)
            if data_17 and data_17.get("success"):
                history = data_17.get("history", [])
                if history:
                    latest = history[0]
                    new_status = latest.get("status", "")
                    new_desc = latest.get("description", "")
                    new_date = latest.get("updated_at", "")

                    status_changed = (new_status != last_status) if last_status else False
                    checkpoint_key = f"{new_status}|{new_desc}"
                    checkpoint_changed = (checkpoint_key != last_checkpoint) if last_checkpoint else False

                    if status_changed or checkpoint_changed or not last_status:
                        db_update_status(resi_id, new_status, checkpoint_key)

                        if last_status and (status_changed or checkpoint_changed):
                            status_emoji = {
                                "InfoReceived": "📝", "PickedUp": "✅", "InTransit": "🚚",
                                "Delivered": "📬", "OutForDelivery": "🏃", "FailedDelivery": "❌"
                            }
                            emoji = status_emoji.get(new_status, "📦")

                            conn = sqlite3.connect(DB_PATH)
                            c = conn.cursor()
                            c.execute("SELECT label FROM resi WHERE id = ?", (resi_id,))
                            r = c.fetchone()
                            conn.close()
                            label = r[0] if r else ""

                            notif = f"🔔 **Update Resi!**\n\n"
                            if label:
                                notif += f"🏷️ {label}\n"
                            notif += f"📦 `{waybill}`\n"
                            notif += f"{emoji} **{new_status}**\n"
                            if new_desc:
                                notif += f"📝 {new_desc}\n"
                            if new_date:
                                notif += f"🕐 {new_date}\n"
                            if new_status == "Delivered":
                                notif += "\n🎉 **Paket udah sampai!**"

                            keyboard = InlineKeyboardMarkup([
                                [InlineKeyboardButton("📋 Lihat Detail", callback_data=f"status_{waybill}")],
                            ])

                            try:
                                await context.bot.send_message(chat_id=chat_id, text=notif, parse_mode="Markdown", reply_markup=keyboard)
                                notified += 1
                            except Exception as e:
                                logger.error(f"Notify fail {chat_id}: {e}")
            await asyncio.sleep(1)
            continue

        # Biteship for other couriers
        data = track_resi(waybill, courier_code, use_cache=False)  # Bypass cache for auto-check
        if not data or not data.get("success"):
            continue

        history = data.get("history", [])
        if not history:
            continue

        latest = history[0]
        new_status = latest.get("status", "")
        new_desc = latest.get("description", "")
        new_location = latest.get("location", "")
        new_date = latest.get("updated_at", "")

        status_changed = (new_status != last_status) if last_status else False
        checkpoint_key = f"{new_status}|{new_desc}"
        checkpoint_changed = (checkpoint_key != last_checkpoint) if last_checkpoint else False

        if status_changed or checkpoint_changed or not last_status:
            db_update_status(resi_id, new_status, checkpoint_key)

            if last_status and (status_changed or checkpoint_changed):
                status_emoji = {
                    "pending": "⏳", "picking_up": "🏃", "picked": "✅",
                    "in_transit": "🚚", "on_hold": "⚠️", "delivered": "📬",
                    "returned": "↩️", "failed_delivery": "❌",
                }
                emoji = status_emoji.get(new_status.lower(), "📦")

                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT label FROM resi WHERE id = ?", (resi_id,))
                r = c.fetchone()
                conn.close()
                label = r[0] if r else ""

                notif = f"🔔 **Update Resi!**\n\n"
                if label:
                    notif += f"🏷️ {label}\n"
                notif += f"📦 `{waybill}`\n"
                notif += f"{emoji} **{new_status.upper()}**\n"
                if new_desc:
                    notif += f"📝 {new_desc}\n"
                if new_location:
                    notif += f"📍 {new_location}\n"
                if new_date:
                    notif += f"🕐 {new_date}\n"
                if new_status.lower() == "delivered":
                    notif += "\n🎉 **Paket udah sampai!**"

                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 Lihat Detail", callback_data=f"status_{waybill}")],
                ])

                try:
                    await context.bot.send_message(chat_id=chat_id, text=notif, parse_mode="Markdown", reply_markup=keyboard)
                    notified += 1
                except Exception as e:
                    logger.error(f"Notify fail {chat_id}: {e}")

        await asyncio.sleep(1)

    logger.info(f"✅ Auto-check done. Notified: {notified}")

# ═══════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════
def main():
    init_db()
    logger.info("🚀 Starting Resi Tracker Bot v3 (Secure)...")

    app = Application.builder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(callback_handler)],
        states={
            WAITING_WAYBILL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_waybill),
                CallbackQueryHandler(callback_handler),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("start", cmd_start),
        ],
        per_message=False,
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(conv_handler)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, quick_add))

    job_queue = app.job_queue
    job_queue.run_repeating(auto_check, interval=CHECK_INTERVAL, first=60)

    logger.info("✅ Bot v3 running! Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
