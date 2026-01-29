# pipeline/step2_pick_offers.py
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

FEED_FILE = os.getenv("SHOPEE_FEED_FILE", "").strip()
OUTPUT_FILE = DATA_DIR / "picks_refinados.csv"

# ===================== CONFIG =====================
MAX_ITEMS = int(os.getenv("STEP2_MAX_ITEMS", "20"))
MIN_ITEMS_BEFORE_RELAX = int(os.getenv("STEP2_MIN_ITEMS_BEFORE_RELAX", "10"))

PRICE_MIN = float(os.getenv("STEP2_PRICE_MIN", "20"))
PRICE_MAX = float(os.getenv("STEP2_PRICE_MAX", "250"))

IDEAL_PRICE_LOW = float(os.getenv("STEP2_IDEAL_PRICE_LOW", "30"))
IDEAL_PRICE_HIGH = float(os.getenv("STEP2_IDEAL_PRICE_HIGH", "120"))

MIN_DISCOUNT_PCT = float(os.getenv("STEP2_MIN_DISCOUNT_PCT", "15"))
MIN_DISCOUNT_ABS = float(os.getenv("STEP2_MIN_DISCOUNT_ABS", "10"))

MIN_RATING = float(os.getenv("STEP2_MIN_RATING", "4.5"))
RATING_COVERAGE_MIN = float(os.getenv("STEP2_RATING_COVERAGE_MIN", "0.35"))

MAX_PER_CATEGORY = int(os.getenv("STEP2_MAX_PER_CATEGORY", "6"))
MIN_DISTINCT_CATEGORIES = int(os.getenv("STEP2_MIN_DISTINCT_CATEGORIES", "8"))

USE_GOOD_CATEGORIES = os.getenv("STEP2_USE_GOOD_CATEGORIES", "0").strip() not in ("0", "false", "False")

W_OFFER = float(os.getenv("STEP2_W_OFFER", "35"))
W_PRICE = float(os.getenv("STEP2_W_PRICE", "25"))
W_TRUST = float(os.getenv("STEP2_W_TRUST", "25"))
W_DECISION = float(os.getenv("STEP2_W_DECISION", "15"))

EASY_WORDS = [
    "kit", "combo", "3 em 1", "2 em 1", "pronto", "recarregável", "universal",
    "original", "oficial", "premium", "rápido", "turbo", "sem fio", "portable", "portátil",
]
HARD_WORDS = [
    "compatível", "modelo", "versão", "instalação", "adaptador específico",
    "refil", "reposicao", "reposição", "sem garantia", "genérico", "réplica",
]

LOW_APPEAL_REGEX = os.getenv("STEP2_LOW_APPEAL_REGEX", "").strip()

GOOD_CATEGORIES = {
    "beleza", "saúde", "utilidades", "cozinha", "casa", "organização", "limpeza",
    "pet", "papelaria", "acessórios", "eletrônicos", "gadget", "games",
}
# ===================== /CONFIG =====================


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
    """
    Escolhe a coluna com MAIOR cobertura de strings não vazias dentre os candidates.
    Evita mapear para colunas que existem, mas vêm vazias (ex: offerName/offerLink).
    """
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


def _auto_fix_cents(series: pd.Series, *, price_max: float) -> Tuple[pd.Series, bool]:
    s = pd.to_numeric(series, errors="coerce")
    non_na = s.dropna()
    if non_na.empty:
        return s, False

    frac_gt = float((non_na > (price_max * 10)).mean())
    median = float(non_na.median())

    if frac_gt >= 0.80 and median >= (price_max * 10):
        return s / 100.0, True

    return s, False


def _schema_map(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    df = _normalize_cols(df)
    mapping: Dict[str, str] = {}

    c_itemid = _first_existing(df, ["produto_id", "itemid", "item_id", "id", "product_id", "offerid", "offer_id", "itemId"])
    if c_itemid:
        mapping["itemid"] = c_itemid

    # ✅ FIX CRÍTICO: título também por COBERTURA (não “primeiro que existir”)
    c_title = _best_nonempty_col(df, ["nome_curto", "productName", "offerName", "title", "name", "product_name", "offername"])
    if c_title:
        mapping["title"] = c_title

    c_img = _best_nonempty_col(df, ["image_link", "imageUrl", "imageurl", "image_url", "img", "cover"])
    if c_img:
        mapping["image_link"] = c_img

    c_sale = _first_existing(df, ["preco_atual", "sale_price", "final_price", "price", "salePrice", "offer_price", "price_min", "priceMin"])
    if c_sale:
        mapping["sale_price"] = c_sale

    # ✅ link por cobertura (já estava)
    c_link = _best_nonempty_col(df, ["link_afiliado", "productLink", "offerLink", "originalLink", "product_link", "url", "link"])
    if c_link:
        mapping["product_link"] = c_link

    c_price = _first_existing(df, ["original_price", "regular_price", "list_price", "price_original", "price_max", "priceMax", "originalPrice"])
    if c_price:
        mapping["price"] = c_price

    c_disc = _first_existing(df, ["discount_percentage", "discountPercent", "discount_pct", "discountRate", "discount_rate", "discountPercentage"])
    if c_disc:
        mapping["discount_percentage"] = c_disc

    c_cat = _best_nonempty_col(df, ["categoria", "category", "category_name", "categoryName", "global_category1", "categoryId"])
    if c_cat:
        mapping["category"] = c_cat

    c_rating = _first_existing(df, ["avaliacao", "rating", "item_rating", "itemRating", "product_rating"])
    if c_rating:
        mapping["rating"] = c_rating

    c_reviews = _first_existing(df, ["reviews", "review_count", "rating_count", "comment_count"])
    if c_reviews:
        mapping["reviews"] = c_reviews

    c_sold = _first_existing(df, ["sold", "historical_sold", "total_sold", "sales"])
    if c_sold:
        mapping["sold"] = c_sold

    df = df.rename(columns={v: k for k, v in mapping.items()})
    return df, mapping


def _sigmoid(x: float) -> float:
    import math
    return 1.0 / (1.0 + math.exp(-x))


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _price_impulse_score(price: float, price_min: float, price_max: float, ideal_low: float, ideal_high: float) -> float:
    if pd.isna(price):
        return 0.0
    if price < price_min or price > price_max:
        return 0.0
    if ideal_low <= price <= ideal_high:
        return 1.0
    if price < ideal_low:
        return _clamp((price - price_min) / max(1.0, (ideal_low - price_min)), 0.0, 1.0)
    return _clamp((price_max - price) / max(1.0, (price_max - ideal_high)), 0.0, 1.0)


def _offer_score(discount_pct: float, discount_abs: float) -> float:
    dp = 0.0 if pd.isna(discount_pct) else _clamp(discount_pct / 60.0, 0.0, 1.0)
    da = 0.0 if pd.isna(discount_abs) else _clamp(discount_abs / 120.0, 0.0, 1.0)
    return 0.65 * dp + 0.35 * da


def _trust_score(rating: float, reviews: float, sold: float) -> float:
    r = 0.0 if pd.isna(rating) else _clamp((rating - 4.0) / 1.0, 0.0, 1.0)
    rv = 0.0 if pd.isna(reviews) else _clamp(_sigmoid((reviews - 30.0) / 20.0), 0.0, 1.0)
    sd = 0.0 if pd.isna(sold) else _clamp(_sigmoid((sold - 150.0) / 80.0), 0.0, 1.0)
    return 0.65 * r + 0.20 * rv + 0.15 * sd


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


def _apply_gates(df_in: pd.DataFrame, *, price_min: float, price_max: float, min_rating: float, apply_rating_gate: bool) -> Tuple[pd.DataFrame, Dict[str, int]]:
    df = df_in.copy().reset_index(drop=True)
    stats: Dict[str, int] = {"start": len(df)}

    # 1) title + link
    df["title"] = df["title"].astype(str).fillna("").str.strip()
    df["product_link"] = df["product_link"].astype(str).fillna("").str.strip()
    df = df[(df["title"].str.len() > 0) & (df["product_link"].str.len() > 0)].copy().reset_index(drop=True)
    stats["after_title_link"] = len(df)

    # 2) preço
    df["sale_price"] = _parse_brl_money_series(df["sale_price"])
    df["sale_price"], fixed_cents = _auto_fix_cents(df["sale_price"], price_max=price_max)
    df["sale_price"] = pd.to_numeric(df["sale_price"], errors="coerce")

    non_na = df["sale_price"].dropna()
    print(
        "DIAG: sale_price parsed "
        f"(dtype={df['sale_price'].dtype}, non_na={int(df['sale_price'].notna().sum())}/{len(df)}) "
        + (f"(min={non_na.min():.2f}, median={non_na.median():.2f}, max={non_na.max():.2f}) " if not non_na.empty else "")
        + f"| cents_fix={'ON' if fixed_cents else 'OFF'}"
    )

    if "price" in df.columns:
        df["price"] = _parse_brl_money_series(df["price"])
        df["price"], _ = _auto_fix_cents(df["price"], price_max=price_max)
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    for c in ["discount_percentage", "rating", "reviews", "sold"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    mask = df["sale_price"].between(price_min, price_max, inclusive="both")
    print(
        f"DIAG BEFORE BETWEEN: price_min={price_min}, price_max={price_max} | "
        f"sale_price_dtype={df['sale_price'].dtype} | "
        f"non_na={int(df['sale_price'].notna().sum())}/{len(df)} | "
        f"mask_true={int(mask.fillna(False).sum())}"
    )
    df = df[mask.fillna(False)].copy().reset_index(drop=True)
    stats["after_price"] = len(df)

    if "price" in df.columns:
        df["_discount_abs"] = (df["price"] - df["sale_price"]).clip(lower=0)
    else:
        df["_discount_abs"] = pd.NA

    has_pct = "discount_percentage" in df.columns
    has_abs = "price" in df.columns
    if has_pct or has_abs:
        pct_ok = df["discount_percentage"].fillna(0) >= MIN_DISCOUNT_PCT if has_pct else False
        abs_ok = pd.to_numeric(df["_discount_abs"], errors="coerce").fillna(0) >= MIN_DISCOUNT_ABS if has_abs else False
        df = df[pct_ok | abs_ok].copy().reset_index(drop=True)
    stats["after_discount_if_any"] = len(df)

    if apply_rating_gate and "rating" in df.columns:
        df = df[df["rating"].fillna(0) >= min_rating].copy().reset_index(drop=True)
    stats["after_rating_gate"] = len(df)

    banned = df["title"].str.lower().str.contains(r"\br[eé]plica\b|\bsem garantia\b", regex=True)
    df = df[~banned].copy().reset_index(drop=True)
    stats["after_banned_words"] = len(df)

    if LOW_APPEAL_REGEX:
        low = df["title"].str.lower().str.contains(LOW_APPEAL_REGEX, regex=True)
        df = df[~low].copy().reset_index(drop=True)
    stats["after_low_appeal"] = len(df)

    if USE_GOOD_CATEGORIES and "category" in df.columns:
        cat = df["category"].astype(str).fillna("").str.lower()
        df = df[cat.apply(lambda x: any(g in x for g in GOOD_CATEGORIES))].copy().reset_index(drop=True)
    stats["after_good_categories_if_on"] = len(df)

    return df, stats


def _score(df: pd.DataFrame, *, price_min: float, price_max: float, ideal_low: float, ideal_high: float) -> pd.DataFrame:
    disc_pct = df["discount_percentage"] if "discount_percentage" in df.columns else pd.Series([pd.NA] * len(df), index=df.index)
    if "_discount_abs" not in df.columns:
        df["_discount_abs"] = pd.NA

    df["_offer"] = [
        _offer_score(
            float(disc_pct.loc[i]) if pd.notna(disc_pct.loc[i]) else float("nan"),
            float(df.loc[i, "_discount_abs"]) if pd.notna(df.loc[i, "_discount_abs"]) else float("nan"),
        )
        for i in df.index
    ]

    df["_price_imp"] = df["sale_price"].apply(
        lambda x: _price_impulse_score(float(x), price_min, price_max, ideal_low, ideal_high) if pd.notna(x) else 0.0
    )

    rating_s = df["rating"] if "rating" in df.columns else pd.Series([pd.NA] * len(df), index=df.index)
    reviews_s = df["reviews"] if "reviews" in df.columns else pd.Series([pd.NA] * len(df), index=df.index)
    sold_s = df["sold"] if "sold" in df.columns else pd.Series([pd.NA] * len(df), index=df.index)

    df["_trust"] = [
        _trust_score(
            float(rating_s.loc[i]) if pd.notna(rating_s.loc[i]) else float("nan"),
            float(reviews_s.loc[i]) if pd.notna(reviews_s.loc[i]) else float("nan"),
            float(sold_s.loc[i]) if pd.notna(sold_s.loc[i]) else float("nan"),
        )
        for i in df.index
    ]

    df["_decision"] = df["title"].apply(_decision_score)

    offer_available = ("discount_percentage" in df.columns) or ("price" in df.columns)
    w_offer = W_OFFER if offer_available else 0.0
    total_w = w_offer + W_PRICE + W_TRUST + W_DECISION
    if total_w <= 0:
        total_w = 100.0

    df["_score"] = (
        (w_offer * df["_offer"])
        + (W_PRICE * df["_price_imp"])
        + (W_TRUST * df["_trust"])
        + (W_DECISION * df["_decision"])
    ) / total_w * 100.0

    return df


def _diversify(df: pd.DataFrame) -> pd.DataFrame:
    df["_t_norm"] = df["title"].apply(_normalize_title)
    df = df.drop_duplicates(subset=["_t_norm"], keep="first")

    if "category" not in df.columns:
        return df.sort_values("_score", ascending=False).head(MAX_ITEMS)

    df["_cat_norm"] = df["category"].astype(str).fillna("").str.lower().str.strip()
    df = df.sort_values("_score", ascending=False)

    selected = []
    cat_count: Dict[str, int] = {}
    distinct_cats = set()

    for _, row in df.iterrows():
        if len(selected) >= MAX_ITEMS:
            break
        cat = row["_cat_norm"]
        if cat_count.get(cat, 0) >= MAX_PER_CATEGORY:
            continue
        selected.append(row)
        cat_count[cat] = cat_count.get(cat, 0) + 1
        distinct_cats.add(cat)

    out = pd.DataFrame(selected)

    if len(distinct_cats) < MIN_DISTINCT_CATEGORIES and len(out) < MAX_ITEMS:
        remaining = df[~df.index.isin(out.index)]
        for _, row in remaining.iterrows():
            if len(out) >= MAX_ITEMS:
                break
            cat = row["_cat_norm"]
            if cat in distinct_cats:
                continue
            out = pd.concat([out, row.to_frame().T], ignore_index=True)
            distinct_cats.add(cat)

    return out.sort_values("_score", ascending=False).head(MAX_ITEMS)


def main() -> None:
    if not FEED_FILE:
        print("SHOPEE_FEED_FILE não definido.")
        raise SystemExit(1)

    p = Path(FEED_FILE)
    if not p.exists():
        print(f"Arquivo do feed não encontrado: {p}")
        raise SystemExit(1)

    df_raw = pd.read_csv(p, low_memory=False)
    df, mapping = _schema_map(df_raw)

    required = {"itemid", "title", "sale_price", "image_link", "product_link"}
    missing = required - set(df.columns)
    if missing:
        print(f"Faltando colunas essenciais no CSV: {missing}")
        print(f"Colunas encontradas no feed: {list(df.columns)}")
        print(f"Mapeamento aplicado (se houver): {mapping}")
        raise SystemExit(1)

    apply_rating_gate = False
    rating_coverage = 0.0
    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        rating_coverage = float(df["rating"].notna().mean())
        apply_rating_gate = rating_coverage >= RATING_COVERAGE_MIN

    print(f"INFO: rating_coverage={rating_coverage:.2%} | rating_gate={'ON' if apply_rating_gate else 'OFF'} (min {RATING_COVERAGE_MIN:.0%})")

    df1, stats1 = _apply_gates(df, price_min=PRICE_MIN, price_max=PRICE_MAX, min_rating=MIN_RATING, apply_rating_gate=apply_rating_gate)

    relaxed = False
    df_work = df1
    stats_work = stats1

    if len(df_work) < MIN_ITEMS_BEFORE_RELAX:
        relaxed = True
        price_min2 = max(0.0, PRICE_MIN * 0.5)
        price_max2 = PRICE_MAX * 1.5
        min_rating2 = max(0.0, MIN_RATING - 0.3)
        df_work, stats_work = _apply_gates(df, price_min=price_min2, price_max=price_max2, min_rating=min_rating2, apply_rating_gate=apply_rating_gate)

    def _print_stats(label: str, st: Dict[str, int]) -> None:
        print(f"--- DIAG {label} ---")
        for k in ["start", "after_title_link", "after_price", "after_discount_if_any", "after_rating_gate", "after_banned_words", "after_low_appeal", "after_good_categories_if_on"]:
            if k in st:
                print(f"{k}: {st[k]}")
        print("--------------------")

    _print_stats("PASS1", stats1)
    if relaxed:
        _print_stats("RELAX", stats_work)
        print("INFO: gates foram relaxados automaticamente por baixo volume (STEP2_MIN_ITEMS_BEFORE_RELAX).")

    if df_work.empty:
        print("Nenhum produto passou pelos gates do Step2.")
        raise SystemExit(1)

    price_min_used = PRICE_MIN if not relaxed else max(0.0, PRICE_MIN * 0.5)
    price_max_used = PRICE_MAX if not relaxed else PRICE_MAX * 1.5

    df_work = _score(df_work, price_min=price_min_used, price_max=price_max_used, ideal_low=IDEAL_PRICE_LOW, ideal_high=IDEAL_PRICE_HIGH)
    out = _diversify(df_work)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    keep = [c for c in ["itemid", "title", "sale_price", "discount_percentage", "image_link", "product_link", "category", "rating", "price", "reviews", "sold"] if c in out.columns]
    keep += ["_score"]
    out[keep].to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"OK: picks_refinados.csv gerado em: {OUTPUT_FILE}")
    print(f"Itens selecionados: {len(out)}")
    print(f"Mapeamento final usado: {mapping}")


if __name__ == "__main__":
    main()
