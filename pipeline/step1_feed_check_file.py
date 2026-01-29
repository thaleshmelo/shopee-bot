# pipeline/step1_feed_check_file.py
from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

ENV_FEED_FILE = os.getenv("SHOPEE_FEED_FILE", "").strip()

DEFAULT_XLSX = DATA_DIR / "controle_produtos.xlsx"
OUT_FEED_VALIDADO = DATA_DIR / "feed_validado.csv"

MIN_PRECO_COVERAGE = float(os.getenv("STEP1_MIN_PRECO_COVERAGE", "0.20"))


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


def _auto_fix_cents(series: pd.Series) -> Tuple[pd.Series, bool]:
    s = pd.to_numeric(series, errors="coerce")
    non_na = s.dropna()
    if non_na.empty:
        return s, False

    median = float(non_na.median())
    frac_ge_1000 = float((non_na >= 1000).mean())
    if frac_ge_1000 >= 0.80 and median >= 1000:
        return s / 100.0, True

    return s, False


def _pick_first_existing_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def _read_source_dataframe() -> Tuple[pd.DataFrame, str]:
    if DEFAULT_XLSX.exists():
        df = pd.read_excel(DEFAULT_XLSX)
        return df, str(DEFAULT_XLSX)

    if ENV_FEED_FILE:
        p = Path(ENV_FEED_FILE)
        if not p.exists():
            raise FileNotFoundError(f"SHOPEE_FEED_FILE aponta para arquivo inexistente: {p}")

        if p.suffix.lower() in (".xlsx", ".xls"):
            df = pd.read_excel(p)
            return df, str(p)

        df = pd.read_csv(p, low_memory=False)
        return df, str(p)

    raise FileNotFoundError(
        "Nenhuma fonte encontrada. Esperado: data/controle_produtos.xlsx OU definir SHOPEE_FEED_FILE para um CSV/XLSX existente."
    )


def _compute_price_from_col(df: pd.DataFrame, col: str) -> Tuple[pd.Series, float, bool]:
    preco = _parse_brl_money_series(df[col])
    preco, cents_fix = _auto_fix_cents(preco)
    preco = pd.to_numeric(preco, errors="coerce")
    coverage = float(preco.notna().mean())
    return preco, coverage, cents_fix


def _build_preco_atual(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, float, bool]:
    if "preco_atual" in df.columns:
        preco, cov, cents_fix = _compute_price_from_col(df, "preco_atual")
        if cov >= MIN_PRECO_COVERAGE:
            df["preco_atual"] = preco
            return df, "preco_atual", cov, cents_fix

    candidates = [
        "salePrice", "priceMin", "price", "priceMax",
        "sale_price", "price_min", "price_max", "final_price",
        "originalPrice", "original_price",
    ]

    best_col = None
    best_cov = -1.0
    best_series = None
    best_fix = False

    for cand in candidates:
        col = _pick_first_existing_col(df, [cand])
        if not col:
            continue
        s, cov, fix = _compute_price_from_col(df, col)
        if cov > best_cov:
            best_cov = cov
            best_col = col
            best_series = s
            best_fix = fix

    if best_col and best_series is not None and best_cov > 0:
        df["preco_atual"] = best_series
        return df, best_col, best_cov, best_fix

    df["preco_atual"] = pd.NA
    return df, "<nenhuma>", 0.0, False


def _best_link_column(df: pd.DataFrame) -> Tuple[Optional[str], float]:
    """
    Escolhe a melhor coluna de link pela MAIOR cobertura de strings não-vazias.
    Evita o bug de escolher offerLink só porque existe, mesmo vazio.
    """
    candidates = []
    for c in ["link_afiliado", "productLink", "offerLink", "originalLink", "url", "link"]:
        if c in df.columns:
            candidates.append(c)

    best_col = None
    best_cov = 0.0

    for c in candidates:
        s = df[c].astype(str).fillna("").str.strip()
        cov = float((s.str.len() > 0).mean())
        if cov > best_cov:
            best_cov = cov
            best_col = c

    return best_col, best_cov


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    df, source_path = _read_source_dataframe()
    df.columns = [str(c).strip() for c in df.columns]

    df, price_src, coverage, cents_fix = _build_preco_atual(df)

    non_na = df["preco_atual"].dropna()
    if non_na.empty:
        print("❌ Step1: Nenhum preço válido encontrado para gerar feed_validado.")
        print(f"Fonte lida: {source_path}")
        print(f"Colunas disponíveis: {list(df.columns)}")
        print(f"Coluna de preço usada (fallback): {price_src}")
        raise SystemExit(1)

    print(f"INFO Step1: fonte={source_path}")
    print(f"INFO Step1: coluna_preco_usada={price_src} | cents_fix={'ON' if cents_fix else 'OFF'} | coverage={coverage:.2%}")
    print(f"INFO Step1: preco_atual stats -> min={non_na.min():.2f}, median={non_na.median():.2f}, max={non_na.max():.2f}")

    link_col, link_cov = _best_link_column(df)
    print(f"INFO Step1: link_col_escolhida={link_col} | link_coverage={link_cov:.2%}")

    before = len(df)

    # Mantém só linhas com preço
    df = df[df["preco_atual"].notna()].copy()

    # Mantém só linhas com link válido (na melhor coluna)
    if link_col:
        df[link_col] = df[link_col].astype(str).fillna("").str.strip()
        df = df[df[link_col].str.len() > 0].copy()
    else:
        print("❌ Step1: Nenhuma coluna de link encontrada para validar.")
        print(f"Colunas disponíveis: {list(df.columns)}")
        raise SystemExit(1)

    df.to_csv(OUT_FEED_VALIDADO, index=False, encoding="utf-8")
    print(f"OK: Feed validado gerado em: {OUT_FEED_VALIDADO}")
    print(f"Linhas no feed validado: {len(df)} (antes: {before})")

    print(f"\nENV setado automaticamente: SHOPEE_FEED_FILE={OUT_FEED_VALIDADO.resolve()}")


if __name__ == "__main__":
    main()
