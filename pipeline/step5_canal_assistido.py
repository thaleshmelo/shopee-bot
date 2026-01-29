import os
import time
import random
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# ================= HELPERS =================
def now_local() -> datetime:
    return datetime.now()

def parse_hhmm(hhmm: str):
    h, m = hhmm.split(":")
    return int(h), int(m)

def in_window(start_hhmm: str, end_hhmm: str) -> bool:
    t = now_local()
    sh, sm = parse_hhmm(start_hhmm)
    eh, em = parse_hhmm(end_hhmm)
    start = t.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end = t.replace(hour=eh, minute=em, second=0, microsecond=0)
    return start <= t <= end

def seconds_until_next_start(start_hhmm: str) -> int:
    t = now_local()
    sh, sm = parse_hhmm(start_hhmm)
    start = t.replace(hour=sh, minute=sm, second=0, microsecond=0)
    if t <= start:
        return int((start - t).total_seconds())
    return int((start.timestamp() + 86400) - t.timestamp())

def copy_to_clipboard_utf8(text: str):
    tmp = Path("outputs") / "_clipboard_utf8.txt"
    tmp.parent.mkdir(exist_ok=True)
    tmp.write_text(text, encoding="utf-8")
    cmd = (
        'powershell -NoProfile -Command '
        '"Get-Content -Raw -Encoding UTF8 \\"' + str(tmp) + '\\" | Set-Clipboard"'
    )
    os.system(cmd)

def interval_minutes(interval_str: str) -> int:
    vals = [int(x) for x in interval_str.split(",") if x.strip().isdigit()]
    return random.choice(vals or [2, 6, 8])

def fmt_price(v):
    try:
        return f"R$ {float(str(v).replace(',', '.')):.2f}"
    except Exception:
        return "Confira no link"

def fmt_disc(v):
    try:
        return f"(-{int(float(str(v).replace(',', '.')))}%)"
    except Exception:
        return ""

def time_bucket() -> str:
    h = now_local().hour
    if 9 <= h < 13:
        return "morning"
    if 13 <= h < 19:
        return "afternoon"
    return "night"

# ================= COPY VARIATION =================
HOOKS = {
    "morning": ["Bom dia! Achado de hoje 👇", "Começando o dia com desconto 👇", "Oferta boa pra resolver hoje 👇"],
    "afternoon": ["Achado da tarde 👇", "Se liga nessa oferta 👇", "Promoção rolando agora 👇"],
    "night": ["Últimas ofertas do dia 👇", "Antes de dormir, olha isso 👇", "Oferta da noite (pode acabar) 👇"],
}
CTAS = {
    "morning": ["👉 Garanta o seu agora", "📌 Clique e aproveite", "⚡ Corre que é por tempo limitado"],
    "afternoon": ["🔥 Aproveite enquanto está ativo", "👉 Pega antes que mude o preço", "📌 Link direto abaixo"],
    "night": ["⏳ Últimas unidades/tempo", "👉 Se curtiu, pega agora", "📌 Link direto pra Shopee"],
}
TEMPLATES = [
    "{hook}\n🔥 {title}\n💰 {price} {disc}\n{cta}\n🔗 {link}",
    "{hook}\n✅ {title}\n💰 {price} {disc}\n{cta}\n🔗 {link}",
    "{hook}\n🔥 {title}\n🔖 {disc} | {price}\n🔗 {link}",
]

BLOCKLIST_TERMS = [
    "vibrador", "bullet", "sexy", "lingerie", "erótico", "erotico", "sexshop",
    "patê", "pate", "pimenta", "alimento", "comida", "whey", "creatina", "psyllium",
    "suplemento", "vitamina", "capsula", "cápsula",
    "xbox 360", "ps3", "playstation 3", "wii",
]

def looks_blocked(text: str) -> bool:
    s = (text or "").lower()
    return any(term in s for term in BLOCKLIST_TERMS)

def load_picks(path: str) -> pd.DataFrame:
    if not Path(path).exists():
        raise SystemExit(f"Arquivo não encontrado: {path}")

    df = pd.read_csv(path, low_memory=False)

    for c in ["title", "product_short_link"]:
        if c not in df.columns:
            raise SystemExit(f"CSV precisa ter coluna '{c}'.")

    if "global_category1" in df.columns:
        df["category"] = df["global_category1"].astype(str).str.strip()
    else:
        df["category"] = "geral"

    df["link_final"] = df["product_short_link"].astype(str).str.strip()
    df = df[df["link_final"].ne("")].copy()

    def row_blocked(r):
        if looks_blocked(str(r.get("title", ""))):
            return True
        if "description" in df.columns and looks_blocked(str(r.get("description", ""))):
            return True
        return False

    df = df[~df.apply(row_blocked, axis=1)].copy()
    df = df.sample(frac=1.0, random_state=random.randint(1, 99999)).reset_index(drop=True)
    return df

def pick_next(df: pd.DataFrame, recent_items: list, recent_categories: list, item_buf: int, cat_buf: int) -> pd.Series:
    candidates = df.copy()

    if "itemid" in candidates.columns and recent_items:
        candidates = candidates[~candidates["itemid"].isin(recent_items)]

    if recent_categories:
        last_cat = recent_categories[-1]
        diversified = candidates[candidates["category"] != last_cat]
        if not diversified.empty:
            candidates = diversified

    if candidates.empty:
        candidates = df.copy()

    return candidates.sample(n=1).iloc[0]

def build_message(row: pd.Series) -> str:
    bucket = time_bucket()
    hook = random.choice(HOOKS[bucket])
    cta = random.choice(CTAS[bucket])

    title = str(row.get("title", "")).strip()
    price = fmt_price(row.get("sale_price", ""))
    disc = fmt_disc(row.get("discount_percentage", ""))
    link = str(row.get("product_short_link", "")).strip()

    msg = random.choice(TEMPLATES).format(
        hook=hook, cta=cta, title=title, price=price, disc=disc, link=link
    ).strip()
    return "\n".join([line.rstrip() for line in msg.splitlines() if line.strip()])

def main():
    # Força o caminho do .env na raiz do projeto:
    # .../pipeline/step5_canal_assistido.py -> parents[1] = raiz do repo
    project_root = Path(__file__).resolve().parents[1]
    dotenv_path = project_root / ".env"

    load_dotenv(dotenv_path=str(dotenv_path), override=True)

    start = os.getenv("CHANNEL_START", "09:00").strip()
    end = os.getenv("CHANNEL_END", "23:50").strip()
    intervals = os.getenv("CHANNEL_INTERVALS", "2,6,8").strip()
    picks_file = os.getenv("SHOPEE_PICKS_FILE", str(project_root / "outputs" / "picks_refinados_com_links.csv")).strip()
    max_posts = int(os.getenv("CHANNEL_MAX_POSTS_PER_DAY", "0") or "0")
    item_buf = int(os.getenv("CHANNEL_RECENT_ITEM_BUFFER", "40") or "40")
    cat_buf = int(os.getenv("CHANNEL_RECENT_CATEGORY_BUFFER", "1") or "1")

    print("=== CANAL — POSTAGEM ASSISTIDA (DIAGNÓSTICO) ===")
    print(f"project_root: {project_root}")
    print(f".env esperado: {dotenv_path} (existe? {dotenv_path.exists()})")
    print(f"CHANNEL_INTERVALS (lido): {intervals}")
    print("=== CANAL — POSTAGEM ASSISTIDA (DIVERSIFICADA + COPY VARIADA) ===")
    print(f"Janela: {start} → {end}")
    print(f"Intervalos: {intervals} min")
    print(f"Fonte: {picks_file}")
    print("Ctrl + C para parar\n")

    df = load_picks(picks_file)
    if df.empty:
        raise SystemExit("Nenhuma oferta válida após filtros.")

    today = now_local().strftime("%Y-%m-%d")
    posts_today = 0
    recent_items = []
    recent_categories = []

    while True:
        if now_local().strftime("%Y-%m-%d") != today:
            today = now_local().strftime("%Y-%m-%d")
            df = load_picks(picks_file)
            posts_today = 0
            recent_items.clear()
            recent_categories.clear()
            print("\n=== Novo dia — picks recarregados ===\n")

        if not in_window(start, end):
            wait = seconds_until_next_start(start)
            print(f"Fora da janela. Aguardando ~{wait//60} min...")
            time.sleep(max(30, wait))
            continue

        if max_posts and posts_today >= max_posts:
            time.sleep(60)
            continue

        row = pick_next(df, recent_items, recent_categories, item_buf, cat_buf)
        msg = build_message(row)

        copy_to_clipboard_utf8(msg)

        print("\n" + "=" * 60)
        print(f"[{now_local().strftime('%H:%M:%S')}] Mensagem COPIADA (Ctrl+V no Canal)")
        print(msg)
        print("=" * 60 + "\n")

        if "itemid" in df.columns:
            recent_items.append(row.get("itemid"))
            recent_items[:] = recent_items[-item_buf:]

        recent_categories.append(row.get("category"))
        recent_categories[:] = recent_categories[-cat_buf:]

        posts_today += 1
        mins = interval_minutes(intervals)
        print(f"Próxima em ~{mins} min.")
        time.sleep(mins * 60)

if __name__ == "__main__":
    main()
