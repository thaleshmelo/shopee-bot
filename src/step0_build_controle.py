import os
import re
from datetime import datetime
import pandas as pd

# ==========================
# CONFIG
# ==========================
CSV_PICKS = os.getenv("PICKS_REFINADOS_CSV", "picks_refinados.csv")
import os
ARQUIVO_CONTROLE = os.getenv("CONTROLE_PRODUTOS_XLSX", "data/controle_produtos.xlsx")

ABA_BASE = "produtos_base"

MIN_AVALIACAO = 4.5

# Quantos itens importar por execução (evita inflar a base sem controle)
MAX_ITENS = 500

# Quota mínima por geração para preencher o dia (5 manhã / 5 tarde / 5 noite)
MIN_POR_GERACAO = 5


# ==========================
# HELPERS
# ==========================
def to_float(x):
    if pd.isna(x):
        return None
    try:
        s = str(x).replace("R$", "").strip().replace(",", ".")
        return float(s)
    except Exception:
        return None

def clean_title(title: str, max_len: int = 60) -> str:
    if title is None or (isinstance(title, float) and pd.isna(title)):
        return ""
    s = str(title).strip()
    s = re.sub(r"\s+", " ", s)
    return s[:max_len]

def pick_link(row) -> str:
    short = str(row.get("product_short_link", "")).strip()
    full = str(row.get("product_link", "")).strip()
    return short if short and short.lower() != "nan" else full

def build_categoria(row) -> str:
    c1 = str(row.get("global_category1", "")).strip()
    c2 = str(row.get("global_category2", "")).strip()
    if c1 and c2 and c1.lower() != "nan" and c2.lower() != "nan":
        return f"{c1} > {c2}"
    return c1 if c1.lower() != "nan" else (c2 if c2.lower() != "nan" else "")

def decide_status(row) -> str:
    blocked = row.get("blocked_hits", 0)
    try:
        blocked = int(blocked)
    except Exception:
        blocked = 0
    return "pausado" if blocked > 0 else "ativo"

def decide_geracao(row) -> str:
    """
    Heurística inicial (pode ser refinada depois).
    """
    price = to_float(row.get("sale_price"))
    if price is None:
        price = to_float(row.get("price")) or 0.0

    util = int(row.get("util_hits", 0) or 0)
    niche = int(row.get("niche_hits", 0) or 0)
    basic_f = int(row.get("basic_fashion_hits", 0) or 0)
    decision_f = int(row.get("decision_fashion_hits", 0) or 0)

    score = row.get("score", 0)
    try:
        score = float(score)
    except Exception:
        score = 0.0

    fashion = basic_f + decision_f

    if util >= max(niche, fashion) and price <= 60:
        return "A"
    if niche >= fashion and price <= 120:
        return "B"
    return "C"

def rebalance_generations(df_in: pd.DataFrame, min_per_gen: int = 5) -> pd.DataFrame:
    """
    Força um mínimo por geração (A/B/C) para preencher blocos do dia (5/5/5).
    Se faltar B ou C, promove itens de A (melhores por score_num) para B e depois C.
    """
    df = df_in.copy()

    if len(df) < 3 * min_per_gen:
        # Não dá para garantir 5/5/5 se tiver menos de 15 itens.
        return df

    if "score_num" not in df.columns:
        df["score_num"] = 0.0

    df["geracao"] = df["geracao"].astype(str).str.upper().str.strip()
    df["score_num"] = pd.to_numeric(df["score_num"], errors="coerce").fillna(0.0)

    def count_gen(g):
        return int((df["geracao"] == g).sum())

    # 1) Garantir B puxando de A (melhores por score)
    need_b = max(0, min_per_gen - count_gen("B"))
    if need_b > 0:
        idx = df[df["geracao"] == "A"].sort_values("score_num", ascending=False).head(need_b).index
        df.loc[idx, "geracao"] = "B"

    # 2) Garantir C puxando de A, depois de B (sem derrubar B abaixo do mínimo)
    need_c = max(0, min_per_gen - count_gen("C"))
    if need_c > 0:
        candidates_a = df[df["geracao"] == "A"].sort_values("score_num", ascending=False)
        take_a = min(need_c, len(candidates_a))
        idx_a = candidates_a.head(take_a).index
        df.loc[idx_a, "geracao"] = "C"
        need_c -= take_a

        if need_c > 0:
            excess_b = count_gen("B") - min_per_gen
            if excess_b > 0:
                candidates_b = df[df["geracao"] == "B"].sort_values("score_num", ascending=False)
                take_b = min(need_c, excess_b, len(candidates_b))
                idx_b = candidates_b.head(take_b).index
                df.loc[idx_b, "geracao"] = "C"

    return df


# ==========================
# LOAD CSV / EXISTING BASE
# ==========================
def load_picks(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    required = [
        "itemid", "title", "price", "sale_price", "item_rating",
        "product_link", "product_short_link"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSV sem colunas necessárias: {missing}")
    return df

def load_existing_base(xlsx_path: str) -> pd.DataFrame:
    try:
        df = pd.read_excel(xlsx_path, sheet_name=ABA_BASE)
        return df
    except Exception:
        return pd.DataFrame(columns=[
            "produto_id", "nome_curto", "link_afiliado", "preco_atual", "avaliacao",
            "categoria", "geracao", "ultimo_envio", "status"
        ])

def merge_base(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    """
    - Não apaga nada.
    - Atualiza campos para ids já existentes.
    - Mantém ultimo_envio do existente.
    - Mantém status "pausado" se já estava pausado (prioriza bloqueio manual).
    """
    existing = existing.copy()
    incoming = incoming.copy()

    existing["produto_id"] = existing["produto_id"].astype(str).str.strip()
    incoming["produto_id"] = incoming["produto_id"].astype(str).str.strip()

    existing.set_index("produto_id", inplace=True, drop=False)
    incoming.set_index("produto_id", inplace=True, drop=False)

    for pid, row in incoming.iterrows():
        if pid in existing.index:
            last_send = existing.at[pid, "ultimo_envio"]
            old_status = str(existing.at[pid, "status"]).strip().lower()
            new_status = str(row["status"]).strip().lower()
            final_status = "pausado" if old_status == "pausado" else new_status

            existing.loc[pid, ["nome_curto", "link_afiliado", "preco_atual", "avaliacao",
                               "categoria", "geracao", "status"]] = [
                row["nome_curto"], row["link_afiliado"], row["preco_atual"], row["avaliacao"],
                row["categoria"], row["geracao"], final_status
            ]
            existing.at[pid, "ultimo_envio"] = last_send
        else:
            existing.loc[pid] = row

    existing.reset_index(drop=True, inplace=True)
    return existing

def save_base(xlsx_path: str, df_base: pd.DataFrame):
    # regrava o arquivo inteiro mantendo outras abas se existirem
    try:
        xls = pd.ExcelFile(xlsx_path)
        sheets = xls.sheet_names
    except Exception:
        sheets = []

    with pd.ExcelWriter(xlsx_path, engine="openpyxl", mode="w") as writer:
        df_base.to_excel(writer, sheet_name=ABA_BASE, index=False)

        for s in sheets:
            if s == ABA_BASE:
                continue
            try:
                pd.read_excel(xlsx_path, sheet_name=s).to_excel(writer, sheet_name=s, index=False)
            except Exception:
                pass


# ==========================
# MAIN
# ==========================
def main():
    df = load_picks(CSV_PICKS)

    # Ordena por score (se existir) e corta
    if "score" in df.columns:
        df["score_num"] = pd.to_numeric(df["score"], errors="coerce").fillna(0.0)
        df = df.sort_values("score_num", ascending=False)
    df = df.head(MAX_ITENS).copy()

    base_rows = []
    for _, r in df.iterrows():
        pid = str(r.get("itemid", "")).strip()
        if not pid or pid.lower() == "nan":
            continue

        preco = to_float(r.get("sale_price"))
        if preco is None:
            preco = to_float(r.get("price"))

        avaliacao = pd.to_numeric(r.get("item_rating"), errors="coerce")
        avaliacao = float(avaliacao) if pd.notna(avaliacao) else None

        # filtro mínimo (remova se quiser guardar tudo)
        if avaliacao is not None and avaliacao < MIN_AVALIACAO:
            continue

        score_num = pd.to_numeric(r.get("score", 0), errors="coerce")
        score_num = float(score_num) if pd.notna(score_num) else 0.0

        row = {
            "produto_id": pid,
            "nome_curto": clean_title(r.get("title", "")),
            "link_afiliado": pick_link(r),
            "preco_atual": preco,
            "avaliacao": avaliacao,
            "categoria": build_categoria(r),
            "geracao": decide_geracao(r),
            "ultimo_envio": None,
            "status": decide_status(r),
            "score_num": score_num,  # usado só para balancear
        }
        base_rows.append(row)

    df_in = pd.DataFrame(base_rows)

    # ✅ QUOTA 5/5/5
    df_in = rebalance_generations(df_in, min_per_gen=MIN_POR_GERACAO)

    # não salvar score_num no Excel
    if "score_num" in df_in.columns:
        df_in = df_in.drop(columns=["score_num"])

    df_existing = load_existing_base(ARQUIVO_CONTROLE)
    df_merged = merge_base(df_existing, df_in)
    save_base(ARQUIVO_CONTROLE, df_merged)

    print("✅ Base 'produtos_base' criada/atualizada a partir de picks_refinados.csv")
    print(f"Arquivo: {ARQUIVO_CONTROLE}")
    print(f"Itens importados nesta execução: {len(df_in)}")
    print(f"Itens totais na base: {len(df_merged)}")
    print("Geração (contagem):")
    if len(df_merged) > 0:
        print(df_merged["geracao"].value_counts().to_string())

if __name__ == "__main__":
    main()
