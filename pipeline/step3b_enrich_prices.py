import os
import pandas as pd
from pathlib import Path


PICKS_FILE = Path(os.getenv("WA_PICKS_FILE", r"outputs\picks_refinados_com_links.csv"))
CONTROLE_XLSX = Path(os.getenv("STEP0_CONTROLE_XLSX", r"data\controle_produtos.xlsx"))


def _col(df: pd.DataFrame, names: list[str]) -> str | None:
    """Retorna o nome real da primeira coluna existente (case-insensitive)."""
    lower_map = {c.lower(): c for c in df.columns}
    for n in names:
        key = n.lower()
        if key in lower_map:
            return lower_map[key]
    return None


def _to_float_series(s: pd.Series) -> pd.Series:
    """Converte série para float aceitando 'R$ 1.234,56' e números."""
    return pd.to_numeric(
        s.astype(str)
        .str.replace("R$", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.strip(),
        errors="coerce",
    )


def main():
    if not PICKS_FILE.exists():
        raise RuntimeError(f"Não encontrei: {PICKS_FILE}")
    if not CONTROLE_XLSX.exists():
        raise RuntimeError(f"Não encontrei: {CONTROLE_XLSX}")

    picks = pd.read_csv(PICKS_FILE)

    if "itemid" not in picks.columns:
        raise RuntimeError("picks_refinados_com_links.csv não tem coluna 'itemid'.")

    picks["itemid"] = picks["itemid"].astype(str).fillna("").str.strip()

    ctrl = pd.read_excel(CONTROLE_XLSX)

    # id no controle
    id_col = _col(ctrl, ["itemid", "itemId", "produto_id", "product_id"])
    if not id_col:
        raise RuntimeError(
            "controle_produtos.xlsx: não encontrei coluna de id (itemid/itemId/produto_id/product_id)."
        )

    ctrl[id_col] = ctrl[id_col].astype(str).fillna("").str.strip()

    # Step0/productOfferV2 costuma ter: price, priceMin, priceMax
    price_col = _col(ctrl, ["price", "preco", "preco_atual", "sale_price", "preco_promocional"])
    price_min_col = _col(ctrl, ["pricemin", "price_min", "preco_min"])
    price_max_col = _col(ctrl, ["pricemax", "price_max", "preco_max", "preco_cheio", "preco_original"])

    aux = pd.DataFrame({"itemid": ctrl[id_col]})

    if price_min_col:
        aux["promo_price_from_ctrl"] = _to_float_series(ctrl[price_min_col])
    elif price_col:
        aux["promo_price_from_ctrl"] = _to_float_series(ctrl[price_col])
    else:
        aux["promo_price_from_ctrl"] = pd.NA

    if price_max_col:
        aux["original_price_from_ctrl"] = _to_float_series(ctrl[price_max_col])
    elif price_col:
        aux["original_price_from_ctrl"] = _to_float_series(ctrl[price_col])
    else:
        aux["original_price_from_ctrl"] = pd.NA

    # merge nos picks
    out = picks.merge(aux, on="itemid", how="left")

    # sale_price do picks (promo atual)
    if "sale_price" in out.columns:
        sale = _to_float_series(out["sale_price"])
    else:
        sale = pd.Series([pd.NA] * len(out))

    # original_price já existente ou vindo do ctrl
    if "original_price" in out.columns:
        orig = _to_float_series(out["original_price"])
    else:
        orig = _to_float_series(out["original_price_from_ctrl"])

    out["original_price"] = orig

    # fallback: se original_price vazio e promo_from_ctrl > sale, usa promo_from_ctrl como "cheio"
    promo_from_ctrl = _to_float_series(out["promo_price_from_ctrl"])

    mask_fill = out["original_price"].isna() & promo_from_ctrl.notna() & sale.notna() & (promo_from_ctrl > sale)
    out.loc[mask_fill, "original_price"] = promo_from_ctrl[mask_fill]

    # discount_pct (opcional)
    out["discount_pct"] = pd.NA
    mask_disc = out["original_price"].notna() & sale.notna() & (out["original_price"] > 0)
    out.loc[mask_disc, "discount_pct"] = (
        ((out["original_price"] - sale) / out["original_price"] * 100).round(0)
    )

    # remove colunas auxiliares
    out.drop(
        columns=[c for c in ["promo_price_from_ctrl", "original_price_from_ctrl"] if c in out.columns],
        inplace=True,
        errors="ignore",
    )

    out.to_csv(PICKS_FILE, index=False, encoding="utf-8")
    print(f"OK Step3b: enriquecido {PICKS_FILE} com original_price e discount_pct.")


if __name__ == "__main__":
    main()
