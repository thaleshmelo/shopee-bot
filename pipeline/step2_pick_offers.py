import os
import pandas as pd

# =======================
# CONFIG
# =======================
FEED_FILE = os.getenv("SHOPEE_FEED_FILE", "").strip()

MAX_ITEMS = 20  # quantos mostrar no console e salvar no picks_refinados.csv

PRICE_MIN = 20
PRICE_MAX = 120

MIN_DISCOUNT = 25
MIN_RATING = 4.3

GOOD_CATEGORIES = {
    "Home & Living",
    "Electronics Accessories",
    "Health & Beauty",
    "Toys, Kids & Babies",
    "Sports & Outdoors",
}

# =======================
# HEURÍSTICAS (TEXTO)
# =======================

BASIC_FASHION = [
    "meia", "meias", "bermuda", "bermudas", "camiseta", "camisetas",
    "cueca", "cuecas", "pijama", "pijamas", "regata", "short", "shorts"
]

FASHION_DECISION = [
    "vestido", "tricot", "tricô", "praia", "biquíni", "bikini",
    "feminino", "masculino", "fashion", "estilo", "look",
    "elegante", "social", "festa", "casual chic"
]

NICHE_WORDS = [
    "aro", "rosca", "mtb", "bike", "bicicleta", "garfo", "movimento",
    "direção", "rolamento", "parafuso", "porca", "suspensão", "eixo", "cubo",
    "automotivo", "moto", "carro", "diesel", "injeção",
    "placa mãe", "motherboard", "gpu", "placa de vídeo", "rtx", "gtx",
]

UTILITY_WORDS = [
    "organizador", "caixa", "escova", "limpador", "cabo", "carregador",
    "fita", "cola", "kit", "tampa", "suporte", "armazenamento",
    "cozinha", "fatiador", "ralador", "cortador", "transparente",
    "bateria", "recarregável", "lanterna",
    "gaveta", "divisória", "divisoria",
    "vassoura", "rodinho", "gancho", "adesivo", "autocolante",
]

# Bloqueados no grupo geral (family-safe + utilidade ampla)
BLOCK_WORDS = [
    # Adulto / sexual
    "vibrador", "bullet", "sexo", "erótico", "erotico", "adulto",

    # Ingeríveis / comida / suplemento
    "comida", "alimento", "suplemento", "chá", "cha", "café", "cafe",
    "pimenta", "patê", "pate", "bebida", "energético", "energetico",
    "psyllium", "spirulina", "vitamina", "whey", "creatina",
    "colágeno", "colageno",

    # Games / mídia antiga (e similares)
    "xbox 360", "ps3", "dvd", "cd", "jogo físico", "jogo fisico",
    "midia fisica", "mídia física",

    # Bebê sensível (opcional)
    "fralda", "leite", "mamadeira",
]

# =======================
# FUNÇÕES
# =======================

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().replace(" ", "_") for c in df.columns]
    return df

def safe_num(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

def pick_link_col(df: pd.DataFrame) -> str | None:
    for c in ["product_short_link", "product_link"]:
        if c in df.columns:
            return c
    for c in df.columns:
        if "link" in c.lower():
            return c
    return None

def text_hits(title: str) -> tuple[int, int, int, int, int]:
    t = (title or "").lower()

    util = sum(1 for w in UTILITY_WORDS if w in t)
    niche = sum(1 for w in NICHE_WORDS if w in t)
    basic_fashion = sum(1 for w in BASIC_FASHION if w in t)
    decision_fashion = sum(1 for w in FASHION_DECISION if w in t)
    blocked = sum(1 for w in BLOCK_WORDS if w in t)

    return util, niche, basic_fashion, decision_fashion, blocked

# =======================
# MAIN
# =======================

def main() -> None:
    if not FEED_FILE:
        raise SystemExit("Defina a variável de ambiente SHOEPEE_FEED_FILE com o caminho do CSV.")

    df = pd.read_csv(FEED_FILE, low_memory=False)
    df = normalize_cols(df)

    # numéricos
    for col in ["sale_price", "discount_percentage", "item_rating"]:
        safe_num(df, col)

    # essenciais
    required = {"title", "sale_price", "discount_percentage", "itemid", "image_link"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Faltando colunas essenciais no CSV: {missing}")

    # filtros básicos
    df = df[
        (df["discount_percentage"] >= MIN_DISCOUNT) &
        (df["sale_price"] >= PRICE_MIN) &
        (df["sale_price"] <= PRICE_MAX)
    ].copy()

    if "item_rating" in df.columns:
        df = df[df["item_rating"] >= MIN_RATING].copy()

    if "global_category1" in df.columns:
        df = df[df["global_category1"].isin(GOOD_CATEGORIES)].copy()

    df = df.drop_duplicates(subset=["itemid"]).copy()

    # heurísticas por título
    util_list = []
    niche_list = []
    basic_f_list = []
    decision_f_list = []
    blocked_list = []

    for t in df["title"].astype(str).tolist():
        u, n, bf, dfash, bl = text_hits(t)
        util_list.append(u)
        niche_list.append(n)
        basic_f_list.append(bf)
        decision_f_list.append(dfash)
        blocked_list.append(bl)

    df["util_hits"] = util_list
    df["niche_hits"] = niche_list
    df["basic_fashion_hits"] = basic_f_list
    df["decision_fashion_hits"] = decision_f_list
    df["blocked_hits"] = blocked_list

    # score
    rating = df["item_rating"] if "item_rating" in df.columns else 4.0

    df["score"] = (
        df["discount_percentage"] * 0.55 +
        (1 - (df["sale_price"] / PRICE_MAX)) * 25 +
        rating * 8 +
        df["util_hits"] * 6 +
        df["basic_fashion_hits"] * 4 -
        df["decision_fashion_hits"] * 16 -
        df["niche_hits"] * 18 -
        df["blocked_hits"] * 100
    )

    # penaliza títulos muito longos (evita spam/descrição gigante)
    df["title_len"] = df["title"].astype(str).str.len()
    df["score"] = df["score"] - (df["title_len"] > 120) * 8

    # ordena pelos melhores
    df = df.sort_values("score", ascending=False)

    # pega top itens e salva para o Step 4
    picks = df.head(MAX_ITEMS).copy()
    out_name = "picks_refinados.csv"
    picks.to_csv(out_name, index=False, encoding="utf-8-sig")
    print(f"\n📁 Picks refinados salvos em: {out_name}")

    link_col = pick_link_col(picks)

    print("\n===== OFERTAS DO DROP (REFINADO + BLOQUEIOS) =====\n")

    for _, r in picks.iterrows():
        title = str(r["title"]).strip()
        price = float(r["sale_price"])
        disc = int(float(r["discount_percentage"]))
        rating_val = r.get("item_rating", None)

        print("🔥", title)
        print(f"💰 R$ {price:.2f}  (-{disc}%)")

        if rating_val is not None and pd.notna(rating_val):
            try:
                print("⭐ Avaliação:", round(float(rating_val), 1))
            except Exception:
                pass

        print(
            "🧠 util:", int(r["util_hits"]),
            "| moda básica:", int(r["basic_fashion_hits"]),
            "| moda decisão:", int(r["decision_fashion_hits"]),
            "| nicho:", int(r["niche_hits"]),
            "| bloqueado:", int(r["blocked_hits"])
        )

        if link_col and pd.notna(r.get(link_col)):
            print("🔗", str(r.get(link_col)).strip())

        print("🖼️", str(r["image_link"]).strip())
        print("-" * 60)

if __name__ == "__main__":
    main()
