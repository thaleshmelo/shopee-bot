import os
from datetime import datetime, timedelta, date
import pandas as pd

# ==========================
# CONFIGURAÇÕES
# ==========================
ARQUIVO_CONTROLE = os.getenv("CONTROLE_PRODUTOS_XLSX", "controle_produtos.xlsx")

ABA_BASE = "produtos_base"
ABA_AGENDA = "agenda_dia"
ABA_LOG = "log_envios"

MIN_AVALIACAO = 4.5
COOLDOWN_HORAS = 48  # 2 dias

TOTAL_MENSAGENS = 15
TIME_BLOCKS = [
    ("09:00", "14:00", 5, "A"),
    ("14:00", "20:00", 5, "B"),
    ("20:00", "23:30", 5, "C"),
]

# Se True, ele NÃO altera ultimo_envio automaticamente
MODO_SEGURO = False


# ==========================
# FUNÇÕES UTILITÁRIAS
# ==========================
def parse_hora(hhmm: str) -> datetime:
    hoje = datetime.now().date()
    h, m = map(int, hhmm.split(":"))
    return datetime.combine(hoje, datetime.min.time()).replace(hour=h, minute=m)

def distribuir_horarios(time_blocks):
    horarios = []
    for start, end, qty, geracao in time_blocks:
        start_dt = parse_hora(start)
        end_dt = parse_hora(end)
        step = (end_dt - start_dt) / qty
        for i in range(qty):
            horarios.append((start_dt + step * i, geracao))
    return horarios  # lista de (datetime, geracao)

def to_date_or_none(x):
    if pd.isna(x) or x is None or str(x).strip() == "":
        return None
    # aceita datetime, date ou string
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    # tenta parsear string
    try:
        return pd.to_datetime(x).date()
    except Exception:
        return None

def cooldown_ok(ultimo_envio, now_dt: datetime, cooldown_horas: int) -> bool:
    if ultimo_envio is None:
        return True
    # último envio como date -> considera 00:00 daquele dia
    last_dt = datetime.combine(ultimo_envio, datetime.min.time())
    return (now_dt - last_dt) >= timedelta(hours=cooldown_horas)


# ==========================
# CARREGAMENTO DA BASE
# ==========================
def carregar_base(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=ABA_BASE)
    # normaliza colunas esperadas (não cria, mas garante leitura)
    colunas_necessarias = [
        "produto_id", "nome_curto", "link_afiliado", "preco_atual",
        "avaliacao", "categoria", "geracao", "ultimo_envio", "status"
    ]
    faltando = [c for c in colunas_necessarias if c not in df.columns]
    if faltando:
        raise ValueError(f"Colunas faltando na aba '{ABA_BASE}': {faltando}")

    df["status"] = df["status"].astype(str).str.strip().str.lower()
    df["geracao"] = df["geracao"].astype(str).str.strip().str.upper()
    df["avaliacao"] = pd.to_numeric(df["avaliacao"], errors="coerce")
    df["ultimo_envio"] = df["ultimo_envio"].apply(to_date_or_none)
    return df

def filtrar_elegiveis(df: pd.DataFrame, now_dt: datetime) -> pd.DataFrame:
    # status ativo + avaliação mínima + geração válida
    elegiveis = df[
        (df["status"] == "ativo") &
        (df["avaliacao"].fillna(0) >= MIN_AVALIACAO) &
        (df["geracao"].isin(["A", "B", "C"]))
    ].copy()

    # cooldown
    elegiveis["cooldown_ok"] = elegiveis["ultimo_envio"].apply(
        lambda d: cooldown_ok(d, now_dt, COOLDOWN_HORAS)
    )
    elegiveis = elegiveis[elegiveis["cooldown_ok"] == True].copy()

    # ordena por "nunca enviado primeiro", depois mais antigo
    # (None vira uma data antiga artificial)
    def sort_key(d):
        return d if d is not None else date(1900, 1, 1)

    elegiveis["ultimo_envio_sort"] = elegiveis["ultimo_envio"].apply(sort_key)
    elegiveis = elegiveis.sort_values(["geracao", "ultimo_envio_sort"], ascending=[True, True])
    return elegiveis.drop(columns=["cooldown_ok"])

def montar_agenda(df_elegiveis: pd.DataFrame, horarios) -> pd.DataFrame:
    """
    horarios: lista de (datetime, geracao)
    Seleciona produtos por geração sem repetir produto no dia.
    """
    usados = set()
    linhas = []

    # agrupa produtos por geração
    pool = {
        "A": df_elegiveis[df_elegiveis["geracao"] == "A"].copy(),
        "B": df_elegiveis[df_elegiveis["geracao"] == "B"].copy(),
        "C": df_elegiveis[df_elegiveis["geracao"] == "C"].copy(),
    }

    # ponteiros por geração
    idx = {"A": 0, "B": 0, "C": 0}

    for dt_horario, ger in horarios:
        lista = pool[ger].reset_index(drop=True)
        escolhido = None

        while idx[ger] < len(lista):
            row = lista.loc[idx[ger]]
            idx[ger] += 1
            pid = str(row["produto_id"]).strip()
            if pid not in usados:
                escolhido = row
                usados.add(pid)
                break

        if escolhido is None:
            # Sem produto disponível -> deixa vazio com valido=NAO
            linhas.append({
                "horario": dt_horario.strftime("%H:%M"),
                "produto_id": "",
                "geracao": ger,
                "valido": "NAO",
                "motivo": "SEM_PRODUTO_ELEGIVEL"
            })
        else:
            linhas.append({
                "horario": dt_horario.strftime("%H:%M"),
                "produto_id": str(escolhido["produto_id"]).strip(),
                "geracao": ger,
                "valido": "SIM",
                "motivo": ""
            })

    agenda = pd.DataFrame(linhas)
    return agenda

def salvar_excel(path: str, df_base: pd.DataFrame, agenda: pd.DataFrame):
    # Lê todas as abas existentes e regrava mantendo o conteúdo
    xls = pd.ExcelFile(path)
    abas_existentes = xls.sheet_names

    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
        # regrava todas as abas que existiam
        for aba in abas_existentes:
            if aba == ABA_BASE:
                df_base.to_excel(writer, sheet_name=ABA_BASE, index=False)
            elif aba == ABA_AGENDA:
                agenda.to_excel(writer, sheet_name=ABA_AGENDA, index=False)
            else:
                pd.read_excel(path, sheet_name=aba).to_excel(writer, sheet_name=aba, index=False)

        # se não existia, cria
        if ABA_AGENDA not in abas_existentes:
            agenda.to_excel(writer, sheet_name=ABA_AGENDA, index=False)
        if ABA_BASE not in abas_existentes:
            df_base.to_excel(writer, sheet_name=ABA_BASE, index=False)

def registrar_log(path: str, agenda: pd.DataFrame):
    """
    Registra SOMENTE os itens válidos (SIM) no log com data de hoje.
    """
    hoje = datetime.now().date().isoformat()
    df_log_new = agenda[agenda["valido"] == "SIM"].copy()
    df_log_new["data"] = hoje
    df_log_new = df_log_new[["data", "horario", "produto_id", "geracao"]]

    try:
        df_log = pd.read_excel(path, sheet_name=ABA_LOG)
    except Exception:
        df_log = pd.DataFrame(columns=["data", "horario", "produto_id", "geracao"])

    df_log = pd.concat([df_log, df_log_new], ignore_index=True)

    # regrava o arquivo mantendo as abas (com log atualizado)
    xls = pd.ExcelFile(path)
    abas_existentes = xls.sheet_names

    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
        for aba in abas_existentes:
            if aba == ABA_LOG:
                df_log.to_excel(writer, sheet_name=ABA_LOG, index=False)
            else:
                pd.read_excel(path, sheet_name=aba).to_excel(writer, sheet_name=aba, index=False)

        if ABA_LOG not in abas_existentes:
            df_log.to_excel(writer, sheet_name=ABA_LOG, index=False)

def aplicar_ultimo_envio(df_base: pd.DataFrame, agenda: pd.DataFrame) -> pd.DataFrame:
    """
    Atualiza ultimo_envio para os produtos postados hoje (valido == SIM).
    """
    hoje = datetime.now().date()
    enviados = set(agenda.loc[agenda["valido"] == "SIM", "produto_id"].astype(str).str.strip())

    def upd(row):
        pid = str(row["produto_id"]).strip()
        if pid in enviados:
            return hoje
        return row["ultimo_envio"]

    df_base = df_base.copy()
    df_base["ultimo_envio"] = df_base.apply(upd, axis=1)
    return df_base


# ==========================
# MAIN
# ==========================
def main():
    now_dt = datetime.now()
    df_base = carregar_base(ARQUIVO_CONTROLE)

    df_elegiveis = filtrar_elegiveis(df_base, now_dt)

    horarios = distribuir_horarios(TIME_BLOCKS)
    agenda = montar_agenda(df_elegiveis, horarios)

    # Se não for modo seguro, já atualiza ultimo_envio automaticamente
    df_out = df_base
    if not MODO_SEGURO:
        df_out = aplicar_ultimo_envio(df_base, agenda)

    salvar_excel(ARQUIVO_CONTROLE, df_out, agenda)

    print("✅ Agenda do dia gerada com controle de geração e cooldown.")
    print(f"Arquivo: {ARQUIVO_CONTROLE}")
    print("Resumo:")
    print(agenda["valido"].value_counts(dropna=False).to_string())

    if MODO_SEGURO:
        print("\n⚠️ MODO_SEGURO=TRUE: ultimo_envio NÃO foi alterado.")
        print("Quando você quiser registrar/envios, rode com MODO_SEGURO=False ou use registrar_log().")

if __name__ == "__main__":
    main()
