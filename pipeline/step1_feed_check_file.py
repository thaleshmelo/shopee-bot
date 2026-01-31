# pipeline/step1_feed_check_file.py
from __future__ import annotations

import os
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
SRC_XLSX = DATA_DIR / "controle_produtos.xlsx"
OUT_CSV = DATA_DIR / "feed_validado.csv"


def _make_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(map(str, df.columns))
    seen = {}
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
        print("INFO Step1: colunas duplicadas detectadas e renomeadas automaticamente.")
    return df


def main() -> None:
    if not SRC_XLSX.exists():
        raise FileNotFoundError(f"Não encontrei: {SRC_XLSX}")

    df = pd.read_excel(SRC_XLSX)
    df = _make_unique_columns(df)

    # Colunas mínimas (mantém nomes que seu pipeline já usa)
    keep = []
    for c in ["produto_id", "nome_curto", "preco_atual", "avaliacao", "categoria", "link_afiliado", "imageUrl", "image_link"]:
        if c in df.columns:
            keep.append(c)

    # Se não achar alguma, mantém tudo (não quebra)
    if keep:
        df = df[keep].copy()

    # Padroniza imagem
    if "imageUrl" not in df.columns and "image_link" in df.columns:
        df["imageUrl"] = df["image_link"]
    if "image_link" not in df.columns and "imageUrl" in df.columns:
        df["image_link"] = df["imageUrl"]

    # Limpezas básicas
    for col in ["link_afiliado", "nome_curto", "categoria", "imageUrl", "image_link"]:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna("").str.strip()

    if "preco_atual" in df.columns:
        df["preco_atual"] = pd.to_numeric(df["preco_atual"], errors="coerce")

    if "avaliacao" in df.columns:
        df["avaliacao"] = pd.to_numeric(df["avaliacao"], errors="coerce")

    # Remove linhas sem link
    if "link_afiliado" in df.columns:
        before = len(df)
        df = df[df["link_afiliado"].astype(str).str.len() > 0].copy()
        print(f"INFO Step1: link_coverage={(len(df)/max(1,before))*100:.2f}%")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")

    # Logs úteis
    print(f"INFO Step1: fonte={SRC_XLSX}")
    print(f"OK: Feed validado gerado em: {OUT_CSV}")
    print(f"Linhas no feed validado: {len(df)}")

    # Mostra cobertura de imagem
    if "imageUrl" in df.columns:
        s = df["imageUrl"].fillna("").astype(str).str.strip()
        print(f"INFO Step1: imageUrl coverage={((s!='').mean()*100):.2f}% | com imagem={(s!='').sum()}")

    # Mostra cobertura rating
    if "avaliacao" in df.columns:
        r = pd.to_numeric(df["avaliacao"], errors="coerce")
        print(f"INFO Step1: rating coverage={(r.notna().mean()*100):.2f}%")


if __name__ == "__main__":
    main()
