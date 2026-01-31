from __future__ import annotations

import os
import random
import time
from datetime import datetime, date
from pathlib import Path
from io import BytesIO

import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

import ctypes
from ctypes import wintypes

import requests
from PIL import Image

load_dotenv()

# ==========================
# Config
# ==========================
PICKS_FILE = Path(os.getenv("WA_PICKS_FILE", r"outputs\picks_refinados_com_links.csv"))
GROUP_NAME = os.getenv("WA_GROUP_NAME", "").strip()

WINDOW_START = os.getenv("WA_WINDOW_START", "09:00").strip()
WINDOW_END = os.getenv("WA_WINDOW_END", "23:50").strip()
INTERVALS = [int(x.strip()) for x in os.getenv("WA_INTERVALS", "8,10,12").split(",") if x.strip()]
JITTER_SECONDS = int(os.getenv("WA_JITTER_SECONDS", "25"))

DAILY_SENDS = int(os.getenv("WA_DAILY_SENDS", "75"))
TEST_MODE = os.getenv("WA_TEST_MODE", "0").strip().lower() in ("1", "true", "yes", "y")
TEST_PICK_ITEMID = os.getenv("WA_TEST_PICK_ITEMID", "").strip()

PROFILE_DIR = os.getenv("WA_PROFILE_DIR", ".wa_chrome_profile").strip()
HEADLESS = os.getenv("WA_HEADLESS", "0").strip().lower() in ("1", "true", "yes", "y")

# Ledger diário (arquivo por data)
TODAY = date.today().isoformat()
LEDGER_FILE = Path(os.getenv("WA_SENT_LEDGER", f"outputs/sent_ledger_media_{TODAY}.csv"))

# CTA
CTA_LINE = os.getenv("WA_CTA_LINE", "👀 Olha o preço!").strip() or "👀 Olha o preço!"


# ==========================
# Windows Clipboard (CF_DIB)
# ==========================
user32 = ctypes.WinDLL("user32", use_last_error=True)
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

CF_DIB = 8
GMEM_MOVEABLE = 0x0002

user32.OpenClipboard.argtypes = [wintypes.HWND]
user32.OpenClipboard.restype = wintypes.BOOL
user32.CloseClipboard.argtypes = []
user32.CloseClipboard.restype = wintypes.BOOL
user32.EmptyClipboard.argtypes = []
user32.EmptyClipboard.restype = wintypes.BOOL
user32.SetClipboardData.argtypes = [wintypes.UINT, wintypes.HANDLE]
user32.SetClipboardData.restype = wintypes.HANDLE

kernel32.GlobalAlloc.argtypes = [wintypes.UINT, ctypes.c_size_t]
kernel32.GlobalAlloc.restype = wintypes.HGLOBAL
kernel32.GlobalLock.argtypes = [wintypes.HGLOBAL]
kernel32.GlobalLock.restype = ctypes.c_void_p
kernel32.GlobalUnlock.argtypes = [wintypes.HGLOBAL]
kernel32.GlobalUnlock.restype = wintypes.BOOL


def _safe_str(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def _to_float(x):
    """
    Converte preço/rating vindo como:
    - float/int (pandas)
    - "77,89"
    - "77.89"
    - "R$ 77,89"
    - ""
    """
    if x is None:
        return None

    # Já é número?
    try:
        if isinstance(x, (int, float)) and not (isinstance(x, float) and pd.isna(x)):
            return float(x)
    except Exception:
        pass

    s = _safe_str(x)
    if not s:
        return None

    s = s.replace("R$", "").replace(" ", "")

    # Caso 1: tem vírgula -> vírgula é decimal, ponto é milhar
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        # Caso 2: sem vírgula -> ponto pode ser decimal
        # remove apenas separadores “óbvios” de milhar (raros aqui)
        # Mantém '.' como decimal.
        pass

    try:
        return float(s)
    except Exception:
        return None


def _fix_cents_if_needed(v: float | None) -> float | None:
    """
    Heurística anti-"centavos":
    Se vier 7789 (deveria ser 77.89), converte para 77.89.
    Regra: valores muito altos e divisíveis por 1 centavo comum.
    """
    if v is None:
        return None
    # Se vier preço absurdo para Shopee (ex: > 5000), tenta dividir por 100
    if v >= 5000:
        vv = v / 100.0
        # Só aceita se o valor virar algo plausível (>0 e <5000)
        if 0 < vv < 5000:
            return vv
    return v


def _format_brl(v) -> str:
    f = _to_float(v)
    if f is None:
        return ""
    f = _fix_cents_if_needed(f)
    return f"{f:.2f}".replace(".", ",")


def _winerr(msg: str) -> RuntimeError:
    err = ctypes.get_last_error()
    return RuntimeError(f"{msg} (winerr={err})")


def set_clipboard_image_from_pil(img: Image.Image) -> None:
    """
    Coloca uma imagem (PIL) no clipboard como CF_DIB (Windows).
    """
    if img.mode != "RGB":
        img = img.convert("RGB")

    with BytesIO() as output:
        img.save(output, "BMP")
        data = output.getvalue()[14:]  # remove BMP file header, mantém DIB

    if not user32.OpenClipboard(None):
        raise _winerr("OpenClipboard falhou")
    try:
        if not user32.EmptyClipboard():
            raise _winerr("EmptyClipboard falhou")

        hglob = kernel32.GlobalAlloc(GMEM_MOVEABLE, len(data))
        if not hglob:
            raise _winerr("GlobalAlloc falhou")

        ptr = kernel32.GlobalLock(hglob)
        if not ptr:
            raise _winerr("GlobalLock falhou")

        try:
            ctypes.memmove(ptr, data, len(data))
        finally:
            kernel32.GlobalUnlock(hglob)

        if not user32.SetClipboardData(CF_DIB, hglob):
            raise _winerr("SetClipboardData falhou")
    finally:
        user32.CloseClipboard()


def download_image_force_rgb(image_url: str) -> Image.Image:
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(image_url, headers=headers, timeout=30)
    r.raise_for_status()
    return Image.open(BytesIO(r.content)).convert("RGB")


# ==========================
# Ledger
# ==========================
def _ensure_outputs_dir():
    LEDGER_FILE.parent.mkdir(parents=True, exist_ok=True)


def _load_ledger_today() -> set[str]:
    _ensure_outputs_dir()
    if not LEDGER_FILE.exists():
        return set()
    try:
        df = pd.read_csv(LEDGER_FILE)
        if "itemid" not in df.columns:
            return set()
        return set(df["itemid"].astype(str).fillna("").str.strip().tolist())
    except Exception:
        return set()


def _append_ledger(itemid: str):
    _ensure_outputs_dir()
    row = {
        "day": TODAY,
        "ts": datetime.now().isoformat(timespec="seconds"),
        "itemid": str(itemid).strip(),
    }
    if LEDGER_FILE.exists():
        df = pd.read_csv(LEDGER_FILE)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])
    df.to_csv(LEDGER_FILE, index=False, encoding="utf-8")


# ==========================
# Window / timing
# ==========================
def _parse_hhmm(t: str) -> datetime:
    h, m = [int(x) for x in t.split(":")]
    now = datetime.now()
    return now.replace(hour=h, minute=m, second=0, microsecond=0)


def _in_window(now: datetime) -> bool:
    start = _parse_hhmm(WINDOW_START)
    end = _parse_hhmm(WINDOW_END)
    return start <= now <= end


def _sleep_to_window_start():
    now = datetime.now()
    start = _parse_hhmm(WINDOW_START)
    if now < start:
        secs = (start - now).total_seconds()
        print(f"[WAIT] Fora da janela. Dormindo até {WINDOW_START} ({int(secs)}s).", flush=True)
        time.sleep(secs)


# ==========================
# Caption (formato do exemplo)
# ==========================
def build_caption(row: dict) -> str:
    title = _safe_str(row.get("title"))

    link = _safe_str(row.get("product_short_link") or row.get("product_link"))

    sale_val = _fix_cents_if_needed(_to_float(row.get("sale_price")))
    orig_val = _fix_cents_if_needed(_to_float(row.get("original_price")))
    disc_in = _to_float(row.get("discount_pct"))

    rating_val = _to_float(row.get("rating"))
    if rating_val is None:
        rating_val = _to_float(row.get("avaliacao"))

    disc_pct = None
    if orig_val is not None and sale_val is not None and orig_val > 0 and orig_val > sale_val:
        disc_pct = round((1 - (sale_val / orig_val)) * 100)
        if disc_pct < 1:
            disc_pct = None
    elif disc_in is not None and disc_in >= 1:
        disc_pct = int(round(disc_in))

    lines: list[str] = []

    if title:
        lines.append(f"🔥 {title}")

    if sale_val is not None:
        price_txt = f"R$ {_format_brl(sale_val)}"
        if disc_pct is not None:
            lines.append(f"💰 {price_txt} (-{disc_pct}%)")
        else:
            lines.append(f"💰 {price_txt}")

    # Avaliação só se existir no CSV
    if rating_val is not None and rating_val > 0:
        lines.append(f"⭐ Avaliação {rating_val:.1f}")

    # Uma linha em branco antes do CTA
    lines.append("")
    lines.append(CTA_LINE)

    # Uma linha em branco antes do link (mas link+emoji na MESMA linha)
    lines.append("")
    if link:
        lines.append(f"🔗 {link}")

    return "\n".join(lines).strip()


# ==========================
# WhatsApp Web automation
# ==========================
def open_whatsapp_and_group(page, group_name: str) -> None:
    page.goto("https://web.whatsapp.com/", wait_until="domcontentloaded")
    page.wait_for_selector("#pane-side, div[role='textbox']", timeout=120_000)

    for sel in [
        "div[contenteditable='true'][data-tab='3']",
        "div[role='textbox'][contenteditable='true']",
        "#side div[contenteditable='true']",
        "div[contenteditable='true']",
    ]:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0:
                loc.click(timeout=2500)
                break
        except Exception:
            pass

    page.keyboard.type(group_name, delay=25)
    page.wait_for_timeout(900)
    page.keyboard.press("Enter")
    page.wait_for_timeout(1200)

    page.wait_for_selector("footer div[role='textbox']", timeout=30_000)


def focus_footer_box(page) -> None:
    try:
        page.locator("footer div[role='textbox']").last.focus(timeout=2500)
        return
    except Exception:
        pass

    try:
        page.locator("footer").first.click(timeout=2000, position={"x": 260, "y": 20})
    except Exception:
        pass


def type_multiline_shift_enter(page, text: str) -> None:
    lines = text.splitlines()
    if not lines:
        return

    page.keyboard.type(lines[0], delay=10)
    for line in lines[1:]:
        page.keyboard.down("Shift")
        page.keyboard.press("Enter")
        page.keyboard.up("Shift")
        if line:
            page.keyboard.type(line, delay=10)


def send_image_with_caption_via_clipboard(page, image_url: str, caption: str) -> None:
    img = download_image_force_rgb(image_url)
    set_clipboard_image_from_pil(img)

    focus_footer_box(page)
    page.keyboard.press("Control+V")

    page.wait_for_timeout(1200)

    type_multiline_shift_enter(page, caption)

    page.keyboard.press("Enter")
    page.wait_for_timeout(1200)


def send_text_only(page, caption: str) -> None:
    focus_footer_box(page)
    type_multiline_shift_enter(page, caption)
    page.keyboard.press("Enter")
    page.wait_for_timeout(800)


# ==========================
# Main
# ==========================
def main():
    if not GROUP_NAME:
        raise RuntimeError("Defina WA_GROUP_NAME.")
    if not PICKS_FILE.exists():
        raise RuntimeError(f"Não encontrei PICKS_FILE: {PICKS_FILE}")

    df = pd.read_csv(PICKS_FILE)

    if "itemid" not in df.columns:
        raise RuntimeError("CSV não tem coluna itemid.")

    for col in [
        "itemid",
        "title",
        "sale_price",
        "original_price",
        "discount_pct",
        "rating",
        "avaliacao",
        "image_link",
        "imageUrl",
        "image_url",
        "product_link",
        "product_short_link",
    ]:
        if col not in df.columns:
            df[col] = ""

    df["itemid"] = df["itemid"].astype(str).fillna("").str.strip()

    sent_today = _load_ledger_today()

    if TEST_PICK_ITEMID:
        df = df[df["itemid"] == TEST_PICK_ITEMID]
    else:
        df = df[~df["itemid"].isin(sent_today)]

    if len(df) == 0:
        if TEST_PICK_ITEMID:
            print("⚠️ Item de teste não encontrado no arquivo.", flush=True)
        else:
            print("✅ Nada novo para enviar HOJE (itens já enviados hoje).", flush=True)
        return

    to_send = df.head(1 if TEST_MODE else DAILY_SENDS).to_dict(orient="records")

    print("=== STEP6 DAILY SCHEDULER (MEDIA) ===", flush=True)
    print(f"Grupo:     {GROUP_NAME}", flush=True)
    print(f"Picks:     {PICKS_FILE}", flush=True)
    print(f"Ledger:    {LEDGER_FILE} (diário)", flush=True)
    print(f"Janela:    {WINDOW_START} -> {WINDOW_END}", flush=True)
    print(f"Envios:    {len(to_send)} | Test={TEST_MODE}", flush=True)
    print(f"Intervals: {INTERVALS} min | jitter={JITTER_SECONDS}s", flush=True)
    print("====================================", flush=True)

    _sleep_to_window_start()

    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            headless=HEADLESS,
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = browser.new_page()

        print("[LOGIN] Se necessário, escaneie o QR.", flush=True)
        open_whatsapp_and_group(page, GROUP_NAME)
        print("[OK] Grupo aberto.", flush=True)

        for idx, row in enumerate(to_send, start=1):
            now = datetime.now()
            if not _in_window(now):
                print("[STOP] Fora da janela, encerrando.", flush=True)
                break

            itemid = _safe_str(row.get("itemid"))
            image_url = _safe_str(row.get("image_link") or row.get("imageUrl") or row.get("image_url"))
            caption = build_caption(row)

            try:
                if image_url:
                    send_image_with_caption_via_clipboard(page, image_url, caption)
                else:
                    send_text_only(page, caption)

                if not TEST_PICK_ITEMID:
                    _append_ledger(itemid)

                print(f"[SENT] {idx}/{len(to_send)} item {itemid}", flush=True)

                if TEST_MODE:
                    print("✅ Test mode: parando após 1 envio.", flush=True)
                    break

                wait_min = random.choice(INTERVALS)
                wait_sec = wait_min * 60 + random.randint(0, JITTER_SECONDS)
                print(f"[WAIT] Próximo em ~{wait_min} min (± jitter).", flush=True)
                time.sleep(wait_sec)

            except Exception as e:
                print(f"[ERR] item {itemid}: {e}", flush=True)

        browser.close()


if __name__ == "__main__":
    main()
