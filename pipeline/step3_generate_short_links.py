# pipeline/step3_generate_short_links.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"

PICKS_FILE = DATA_DIR / "picks_refinados.csv"
OUTPUT_FILE = OUTPUTS_DIR / "picks_refinados_com_links.csv"


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _best_nonempty_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols_lower = {c.lower(): c for c in df.columns}
    found: list[str] = []
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


def _clean_link(s: pd.Series) -> pd.Series:
    s = s.astype(str).fillna("").str.strip()
    # trata valores "nan" vindos do pandas
    s = s.replace({"nan": "", "None": "", "NULL": "", "null": ""})
    return s


def main() -> None:
    if not PICKS_FILE.exists():
        raise SystemExit(f"picks_refinados.csv não encontrado em: {PICKS_FILE}")

    df = _normalize_cols(pd.read_csv(PICKS_FILE, low_memory=False))

    # Detecta coluna de link do produto (melhor cobertura)
    link_col = _best_nonempty_col(df, ["product_link", "link_afiliado", "productLink", "offerLink", "originalLink", "url", "link"])
    if not link_col:
        raise SystemExit(f"picks_refinados.csv não tem coluna de link. Colunas atuais: {list(df.columns)}")

    if link_col != "product_link":
        df["product_link"] = df[link_col]
    df["product_link"] = _clean_link(df["product_link"])

    # Garante product_short_link com fallback robusto:
    # se já existir e estiver vazio, preenche com product_link.
    if "product_short_link" not in df.columns:
        df["product_short_link"] = ""

    df["product_short_link"] = _clean_link(df["product_short_link"])

    # ✅ FIX: preencher vazios (e NÃO só criar a coluna)
    df.loc[df["product_short_link"].str.len() == 0, "product_short_link"] = df["product_link"]

    # Sanidade: remove linhas sem link
    df = df[(df["product_link"].str.len() > 0) & (df["product_short_link"].str.len() > 0)].copy()

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    total = len(df)
    filled = int(df["product_short_link"].astype(str).str.strip().str.len().gt(0).sum())
    pct = (filled / total * 100.0) if total else 0.0

    print(f"OK: arquivo gerado em: {OUTPUT_FILE}")
    print(f"Linhas: {len(df)}")
    print(f"INFO: product_short_link preenchido: {pct:.0f}% (fallback aplicado quando vazio).")


if __name__ == "__main__":
    main()
