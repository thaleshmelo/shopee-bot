import os
import random
from datetime import datetime
import pandas as pd

# ==========================
# CONFIG
# ==========================
ARQUIVO_CONTROLE = os.getenv("CONTROLE_PRODUTOS_XLSX", "controle_produtos.xlsx")

ABA_BASE = "produtos_base"
ABA_AGENDA = "agenda_dia"

# Saída (arquivo do dia)
HOJE_STR = datetime.now().date().isoformat()
ARQUIVO_SAIDA = os.getenv("MENSAGENS_WHATSAPP_XLSX", f"mensagens_whatsapp_{HOJE_STR}.xlsx")

# CTA com variação para evitar fadiga
CTA_REACOES = [
    "👇 O que você achou dessa oferta?\n👍 Vale a pena  ❤️ Compraria  😮 Achei barato  🤔 Não curti",
    "💬 Opinião rápida:\n👍 Bom preço  ❤️ Compraria  😮 Surpreendeu  🤔 Não gostei",
    "👀 Esse preço tá justo?\n👍 Sim  ❤️ Levo agora  😮 Muito barato  🤔 Passo",
]

# Template principal (curto, direto, parecido com canais grandes)
def formatar_preco(x):
    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
        return ""
    try:
        v = float(x)
        # Formato BR simples: 24,99
        return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(x)

def safe_str(x):
    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
        return ""
    return str(x).strip()

def carregar_aba(path: str, sheet: str) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet)

def validar_colunas(df: pd.DataFrame, required_cols: list, nome: str):
    faltando = [c for c in required_cols if c not in df.columns]
    if faltando:
        raise ValueError(f"Colunas faltando em '{nome}': {faltando}")

def montar_mensagem(prod: dict) -> str:
    nome = safe_str(prod.get("nome_curto"))
    link = safe_str(prod.get("link_afiliado"))
    preco = formatar_preco(prod.get("preco_atual"))
    avaliacao = prod.get("avaliacao")
    categoria = safe_str(prod.get("categoria"))

    rating_str = ""
    try:
        if avaliacao is not None and not pd.isna(avaliacao):
            rating_str = f"⭐ {float(avaliacao):.1f}/5"
    except Exception:
        rating_str = ""

    # Mensagem curta e “escaneável”
    linhas = []
    linhas.append("🔥 OFERTA SHOPEE 🔥")
    linhas.append("")
    if nome:
        linhas.append(f"🛒 {nome}")
    if categoria:
        linhas.append(f"🏷️ {categoria}")
    if preco:
        linhas.append(f"💰 Por: R$ {preco}")
    if rating_str:
        linhas.append(rating_str)
    linhas.append("")
    if link:
        linhas.append(f"👉 {link}")
    else:
        linhas.append("👉 (link indisponível)")

    # CTA de reação (varia por mensagem)
    linhas.append("")
    linhas.append(random.choice(CTA_REACOES))

    return "\n".join(linhas).strip()

def main():
    # Carrega dados
    df_base = carregar_aba(ARQUIVO_CONTROLE, ABA_BASE)
    df_agenda = carregar_aba(ARQUIVO_CONTROLE, ABA_AGENDA)

    # Valida colunas essenciais
    validar_colunas(df_base, ["produto_id", "nome_curto", "link_afiliado", "preco_atual", "avaliacao", "categoria"], ABA_BASE)
    validar_colunas(df_agenda, ["horario", "produto_id", "geracao", "valido"], ABA_AGENDA)

    # Filtra apenas os itens do dia que devem ser postados
    agenda_ok = df_agenda[df_agenda["valido"].astype(str).str.upper().str.strip() == "SIM"].copy()
    if agenda_ok.empty:
        print("⚠️ Nenhum item 'SIM' em agenda_dia. Nada para formatar.")
        return

    # Normaliza IDs
    df_base = df_base.copy()
    df_base["produto_id"] = df_base["produto_id"].astype(str).str.strip()
    agenda_ok["produto_id"] = agenda_ok["produto_id"].astype(str).str.strip()

    # Index rápido por produto_id
    base_idx = df_base.set_index("produto_id", drop=False)

    # Monta mensagens
    saida = []
    for _, row in agenda_ok.iterrows():
        pid = safe_str(row.get("produto_id"))
        horario = safe_str(row.get("horario"))
        geracao = safe_str(row.get("geracao"))

        if not pid:
            continue

        if pid not in base_idx.index:
            # Produto não encontrado na base (não deveria acontecer)
            mensagem = (
                "🔥 OFERTA SHOPEE 🔥\n\n"
                f"🆔 Produto: {pid}\n"
                "⚠️ Não encontrado na base 'produtos_base'.\n\n"
                + random.choice(CTA_REACOES)
            )
        else:
            prod = base_idx.loc[pid].to_dict()
            mensagem = montar_mensagem(prod)

        saida.append({
            "horario": horario,
            "produto_id": pid,
            "geracao": geracao,
            "mensagem": mensagem
        })

    df_out = pd.DataFrame(saida)

    # Ordena por horário (se vier como HH:MM já ajuda)
    try:
        df_out["_h"] = pd.to_datetime(df_out["horario"], format="%H:%M", errors="coerce")
        df_out = df_out.sort_values("_h").drop(columns=["_h"])
    except Exception:
        pass

    # Salva no XLSX
    with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl", mode="w") as writer:
        df_out.to_excel(writer, sheet_name="mensagens", index=False)

    print("✅ Mensagens geradas com CTA de reações.")
    print(f"Arquivo: {ARQUIVO_SAIDA}")
    print(f"Total de mensagens: {len(df_out)}")

if __name__ == "__main__":
    main()
