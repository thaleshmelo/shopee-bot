# pipeline/step2_pick_offers.py
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, Optional, Tuple, Set, List

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

FEED_FILE = os.getenv("SHOPEE_FEED_FILE", "").strip()
OUTPUT_FILE = DATA_DIR / "picks_refinados.csv"

# ===================== CONFIG =====================
MAX_ITEMS = int(os.getenv("STEP2_MAX_ITEMS", "250"))

PRICE_MIN = float(os.getenv("STEP2_PRICE_MIN", "20"))
PRICE_MAX = float(os.getenv("STEP2_PRICE_MAX", "250"))

IDEAL_PRICE_LOW = float(os.getenv("STEP2_IDEAL_PRICE_LOW", "30"))
IDEAL_PRICE_HIGH = float(os.getenv("STEP2_IDEAL_PRICE_HIGH", "120"))

MIN_RATING = float(os.getenv("STEP2_MIN_RATING", "4.6"))
REQUIRE_IMAGE = os.getenv("STEP2_REQUIRE_IMAGE", "1").strip() not in ("0", "false", "False")

RATING_COVERAGE_MIN = float(os.getenv("STEP2_RATING_COVERAGE_MIN", "10.0"))  # percent

# IMPORTANTE: esse era o gargalo (6). Pode ajustar via env também.
MAX_PER_CATEGORY = int(os.getenv("STEP2_MAX_PER_CATEGORY", "6"))

W_PRICE = float(os.getenv("STEP2_W_PRICE", "40"))
W_TRUST = float(os.getenv("STEP2_W_TRUST", "35"))
W_DECISION = float(os.getenv("STEP2_W_DECISION", "25"))

EASY_WORDS = [
    "kit", "combo", "3 em 1", "2 em 1", "pronto", "recarregável", "universal",
    "original", "oficial", "premium", "rápido", "turbo", "sem fio", "portable", "portátil",
]
HARD_WORDS = [
    "compatível", "modelo", "versão", "instalação", "adaptador específico",
    "refil", "reposicao", "reposição", "sem garantia", "genérico", "réplica",
]
# ===================== /CONFIG =====================


def _make_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(map(str, df.columns))
    seen: Dict[str, int] = {}
    new_cols = []
    dup_found = False
    for c in cols:
        if c in seen:
            seen[c] += 1
            new_cols.append(f"{c}__dup{seen[c]}")
            dup_found = True
        else:
            seen[c] = 0
            new_cols.append(c)
    if dup_found:
        df = df.copy()
        df.columns = new_cols
        print("INFO Step2: colunas duplicadas detectadas e renomeadas automaticamente.")
    return df


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _first_existing(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower()
        if key in cols_lower:
            return cols_lower[key]
    return None


def _best_nonempty_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    cols_lower = {c.lower(): c for c in df.columns}
    found = []
    for cand in candidates:
        c = cols_lower.get(cand.lower())
        if c and c not in found:
            found.append(c)
    best = None
    best_cov = 0.0
    for c in found:
        s = df[c].astype(str).fillna("").str.strip()
        cov = float((s.str.len() > 0).mean())
        if cov > best_cov:
            best_cov = cov
            best = c
    return best


def _parse_brl_money_series(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce")
    txt = s.astype(str).fillna("").str.strip()
    txt = txt.str.replace("R$", "", regex=False).str.replace("r$", "", regex=False)
    txt = txt.str.replace("\u00a0", " ", regex=False).str.replace(" ", "", regex=False)
    txt = txt.str.replace(r"[^0-9,.\-]", "", regex=True)
    has_comma = txt.str.contains(",", regex=False)
    txt = txt.where(~has_comma, txt.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
    return pd.to_numeric(txt, errors="coerce")


def _schema_map(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    df = _normalize_cols(df)
    mapping: Dict[str, str] = {}

    c_itemid = _first_existing(df, ["produto_id", "itemid", "item_id", "id", "product_id", "offerid", "offer_id", "itemId"])
    if c_itemid:
        mapping["itemid"] = c_itemid

    c_title = _best_nonempty_col(df, ["nome_curto", "productName", "offerName", "title", "name"])
    if c_title:
        mapping["title"] = c_title

    c_img = _best_nonempty_col(df, ["imageUrl", "image_link", "imageurl", "image_url", "img"])
    if c_img:
        mapping["image_link"] = c_img

    c_sale = _first_existing(df, ["preco_atual", "sale_price", "price", "salePrice", "priceMin"])
    if c_sale:
        mapping["sale_price"] = c_sale

    c_link = _best_nonempty_col(df, ["link_afiliado", "productLink", "offerLink", "originalLink", "product_link", "url", "link"])
    if c_link:
        mapping["product_link"] = c_link

    c_cat = _best_nonempty_col(df, ["categoria", "category", "categoryName", "category_name"])
    if c_cat:
        mapping["category"] = c_cat

    c_rating = _first_existing(df, ["avaliacao", "rating", "itemRating", "item_rating"])
    if c_rating:
        mapping["rating"] = c_rating

    df = df.rename(columns={v: k for k, v in mapping.items()})
    return df, mapping


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _price_impulse_score(price: float) -> float:
    if pd.isna(price):
        return 0.0
    if price < PRICE_MIN or price > PRICE_MAX:
        return 0.0
    if IDEAL_PRICE_LOW <= price <= IDEAL_PRICE_HIGH:
        return 1.0
    if price < IDEAL_PRICE_LOW:
        return _clamp((price - PRICE_MIN) / max(1.0, (IDEAL_PRICE_LOW - PRICE_MIN)), 0.0, 1.0)
    return _clamp((PRICE_MAX - price) / max(1.0, (PRICE_MAX - IDEAL_PRICE_HIGH)), 0.0, 1.0)


def _trust_score(rating: float) -> float:
    # Se rating vier vazio, não mata o item
    if pd.isna(rating):
        return 0.35
    return _clamp((float(rating) - 4.0) / 1.0, 0.0, 1.0)


def _decision_score(title: str) -> float:
    t = (title or "").lower()
    bonus = sum(1 for w in EASY_WORDS if w in t)
    malus = sum(1 for w in HARD_WORDS if w in t)
    return _clamp(0.55 + 0.10 * bonus - 0.15 * malus, 0.0, 1.0)


def _normalize_title(t: str) -> str:
    t = (t or "").lower()
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"\b(novo|promoção|oferta|frete|grátis|original)\b", "", t)
    return re.sub(r"\s+", " ", t).strip()


def _pick_pass(
    df_sorted: pd.DataFrame,
    used_titles: Set[str],
    per_cat: Dict[str, int],
    respect_category_cap: bool,
) -> List[dict]:
    picked: List[dict] = []

    for _, row in df_sorted.iterrows():
        title = str(row.get("title", "")).strip()
        tnorm = _normalize_title(title)
        if not tnorm or tnorm in used_titles:
            continue

        cat = (str(row.get("category_norm", "")).strip() or "sem_categoria")

        if respect_category_cap:
            if per_cat.get(cat, 0) >= MAX_PER_CATEGORY:
                continue

        picked.append(row.to_dict())
        used_titles.add(tnorm)
        per_cat[cat] = per_cat.get(cat, 0) + 1

        if len(picked) >= MAX_ITEMS:
            break

    return picked


def main() -> None:
    if not FEED_FILE:
        raise RuntimeError("SHOPEE_FEED_FILE não definido. Use: $env:SHOPEE_FEED_FILE='data\\feed_validado.csv'")

    df_raw = pd.read_csv(FEED_FILE)
    if df_raw.empty:
        print("⚠️ FEED vazio.")
        return

    df_raw = _make_unique_columns(df_raw)
    df, mapping = _schema_map(df_raw)
    df = _make_unique_columns(df)

    for col in ["itemid", "title", "sale_price", "product_link", "image_link", "category", "rating"]:
        if col not in df.columns:
            df[col] = ""

    df["title"] = df["title"].astype(str).fillna("").str.strip()
    df["product_link"] = df["product_link"].astype(str).fillna("").str.strip()
    df["image_link"] = df["image_link"].astype(str).fillna("").str.strip()
    df["category"] = df["category"].astype(str).fillna("").str.strip()

    df["sale_price"] = _parse_brl_money_series(df["sale_price"])
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    # gates mínimos
    df = df[(df["title"].str.len() > 0) & (df["product_link"].str.len() > 0)].copy()

    # gate preço (se ficar pequeno, relaxa)
    df_price = df[(df["sale_price"].notna()) & (df["sale_price"] >= PRICE_MIN) & (df["sale_price"] <= PRICE_MAX)].copy()
    if len(df_price) >= MAX_ITEMS:
        df = df_price
    else:
        print(f"INFO Step2: gate preço relaxado (na faixa {PRICE_MIN}-{PRICE_MAX}: {len(df_price)})")

    # imagem obrigatória
    if REQUIRE_IMAGE:
        before = len(df)
        df = df[df["image_link"].str.len() > 0].copy()
        print(f"INFO Step2: REQUIRE_IMAGE=ON -> {before} -> {len(df)}")

    # rating gate (apenas se coverage ok)
    rating_cov = float(df["rating"].notna().mean() * 100.0)
    if rating_cov >= RATING_COVERAGE_MIN:
        before = len(df)
        df = df[df["rating"] >= MIN_RATING].copy()
        print(f"INFO Step2: rating gate aplicado (coverage={rating_cov:.2f}%) -> {before} -> {len(df)}")
    else:
        print(f"INFO Step2: rating gate IGNORADO (coverage={rating_cov:.2f}% < {RATING_COVERAGE_MIN}%)")

    if df.empty:
        print("⚠️ Nenhum item após gates.")
        return

    # scores
    df["price_score"] = df["sale_price"].apply(_price_impulse_score)
    df["trust_score"] = df["rating"].apply(_trust_score)
    df["decision_score"] = df["title"].apply(_decision_score)

    df["_score"] = (
        (W_PRICE / 100.0) * df["price_score"] +
        (W_TRUST / 100.0) * df["trust_score"] +
        (W_DECISION / 100.0) * df["decision_score"]
    )

    df["category_norm"] = df["category"].astype(str).str.lower().str.strip()
    df_sorted = df.sort_values("_score", ascending=False).reset_index(drop=True)

    used_titles: Set[str] = set()
    per_cat: Dict[str, int] = {}

    # PASSO 1: com diversidade por categoria (cap)
    picked = _pick_pass(df_sorted, used_titles, per_cat, respect_category_cap=True)

    # PASSO 2: completa ignorando cap de categoria (corrige seu caso de "6")
    if len(picked) < MAX_ITEMS:
        picked2 = _pick_pass(df_sorted, used_titles, per_cat, respect_category_cap=False)
        # picked2 pode conter até MAX_ITEMS, mas vamos só completar o que falta
        needed = MAX_ITEMS - len(picked)
        picked.extend(picked2[:needed])

    out = pd.DataFrame(picked)
    if out.empty:
        print("⚠️ Nenhum pick final.")
        return

    cols_out = ["itemid", "title", "sale_price", "image_link", "product_link", "category", "rating", "_score"]
    for c in cols_out:
        if c not in out.columns:
            out[c] = ""
    out = out[cols_out]

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"OK Step2: {len(out)} picks salvos em: {OUTPUT_FILE}")
    print(f"INFO Step2: schema mapping usado: {mapping}")


if __name__ == "__main__":
    main()
