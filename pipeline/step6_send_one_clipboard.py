from __future__ import annotations

import os
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

WA_GROUP_NAME = os.getenv("WA_GROUP_NAME", "").strip()
WA_PROFILE_DIR = os.getenv("WA_PROFILE_DIR", ".wa_chrome_profile").strip()
WA_HEADLESS = os.getenv("WA_HEADLESS", "0").strip().lower() in ("1", "true")

PICKS_FILE = Path(os.getenv("WA_PICKS_FILE", r"outputs\picks_refinados_com_links.csv"))
TEST_ITEMID = os.getenv("WA_TEST_PICK_ITEMID", "").strip()

# ===== Windows Clipboard (CF_DIB) safe bindings =====
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


def _safe_str(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _winerr(msg: str) -> RuntimeError:
    err = ctypes.get_last_error()
    return RuntimeError(f"{msg} (winerr={err})")


def set_clipboard_image_from_pil(img: Image.Image) -> None:
    output = BytesIO()
    img.convert("RGB").save(output, "BMP")
    bmp = output.getvalue()
    output.close()

    dib = bmp[14:]
    size = len(dib)

    if not user32.OpenClipboard(None):
        raise _winerr("OpenClipboard falhou")

    try:
        if not user32.EmptyClipboard():
            raise _winerr("EmptyClipboard falhou")

        hglob = kernel32.GlobalAlloc(GMEM_MOVEABLE, size)
        if not hglob:
            raise _winerr("GlobalAlloc falhou")

        ptr = kernel32.GlobalLock(hglob)
        if not ptr:
            raise _winerr("GlobalLock falhou")

        try:
            ctypes.memmove(ptr, dib, size)
        finally:
            kernel32.GlobalUnlock(hglob)

        hset = user32.SetClipboardData(CF_DIB, hglob)
        if not hset:
            raise _winerr("SetClipboardData falhou")
    finally:
        user32.CloseClipboard()


def download_image_force_jpg(image_url: str) -> Image.Image:
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(image_url, headers=headers, timeout=30)
    r.raise_for_status()
    return Image.open(BytesIO(r.content)).convert("RGB")


def build_caption(row: dict) -> str:
    title = _safe_str(row.get("title"))
    link = _safe_str(row.get("product_short_link") or row.get("product_link"))

    # preço
    price_txt = ""
    price = row.get("sale_price")
    try:
        if price is not None and _safe_str(price) != "":
            p = float(price)
            price_txt = f"{p:.2f}".replace(".", ",")
    except Exception:
        price_txt = _safe_str(price)

    blocks = []

    if title:
        blocks.append(f"🛍️ {title}")

    if price_txt:
        blocks.append(f"💰 R$ {price_txt}")

    if link:
        blocks.append(f"🔗 {link}")

    blocks.append("⚡ Aproveita antes que acabe!")

    # linha em branco entre blocos
    return "\n\n".join(blocks).strip()



def open_whatsapp_and_group(page, group_name: str) -> None:
    page.goto("https://web.whatsapp.com/", wait_until="domcontentloaded")
    page.wait_for_selector("#pane-side, div[role='textbox']", timeout=120_000)

    # foco busca
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

    try:
        page.keyboard.press("Control+Alt+/")
    except Exception:
        pass

    page.keyboard.type(group_name, delay=25)
    page.wait_for_timeout(900)
    page.keyboard.press("Enter")
    page.wait_for_timeout(1200)

    page.wait_for_selector("footer div[role='textbox']", timeout=30_000)


def focus_footer_box(page) -> None:
    # tenta focar sem click arriscado: usa atalho e um click leve no footer container
    try:
        page.locator("footer").first.click(timeout=2000, position={"x": 200, "y": 20})
    except Exception:
        pass
    try:
        page.locator("footer div[role='textbox']").last.click(timeout=2000, force=True)
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


def main() -> None:
    if not WA_GROUP_NAME:
        raise RuntimeError("Defina WA_GROUP_NAME (nome EXATO do grupo no WhatsApp Web).")
    if not PICKS_FILE.exists():
        raise RuntimeError(f"Não encontrei {PICKS_FILE}")

    df = pd.read_csv(PICKS_FILE)
    for col in ["itemid", "title", "sale_price", "image_link", "product_link", "product_short_link", "category"]:
        if col not in df.columns:
            df[col] = ""

    df["itemid"] = df["itemid"].astype(str).fillna("").str.strip()
    df["image_link"] = df["image_link"].astype(str).fillna("").str.strip()
    df = df[df["image_link"].str.len() > 0].copy()
    if df.empty:
        raise RuntimeError("CSV não tem nenhum item com image_link preenchido.")

    if TEST_ITEMID:
        df2 = df[df["itemid"] == TEST_ITEMID].copy()
        if df2.empty:
            raise RuntimeError(f"Não achei itemid={TEST_ITEMID} no CSV com image_link.")
        row = df2.iloc[0].to_dict()
    else:
        row = df.iloc[0].to_dict()

    itemid = _safe_str(row.get("itemid"))
    image_url = _safe_str(row.get("image_link"))
    caption = build_caption(row)

    print("=== STEP6 CLIPBOARD SEND ONE ===")
    print(f"Grupo:   {WA_GROUP_NAME}")
    print(f"Item:    {itemid}")
    print(f"Imagem:  {image_url[:90]}...")
    print("================================")

    # clipboard
    img = download_image_force_jpg(image_url)
    set_clipboard_image_from_pil(img)
    print("[OK] Imagem copiada para o clipboard.")

    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            user_data_dir=WA_PROFILE_DIR,
            headless=WA_HEADLESS,
        )
        page = browser.new_page()

        print("[LOGIN] Se necessário, escaneie o QR...")
        open_whatsapp_and_group(page, WA_GROUP_NAME)
        print("[OK] Grupo aberto.")

        # foco e cola
        focus_footer_box(page)
        page.keyboard.press("Control+V")

        # espera o WhatsApp "prender" a mídia no composer
        page.wait_for_timeout(1200)

        # IMPORTANTE: NÃO clicar em textbox (interceptado pelo botão de anexo).
        # Só digitar via teclado. Quebras com Shift+Enter.
        type_multiline_shift_enter(page, caption)

        # envia (Enter envia a mídia + legenda em um único envio)
        page.keyboard.press("Enter")

        page.wait_for_timeout(1200)
        print("[SENT] Enviado (imagem + legenda em UMA mensagem).")
        browser.close()


if __name__ == "__main__":
    main()
