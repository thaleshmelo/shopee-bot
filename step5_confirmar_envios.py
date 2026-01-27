import os
from datetime import datetime, date
import pandas as pd

ARQUIVO_CONTROLE = os.getenv("CONTROLE_PRODUTOS_XLSX", "controle_produtos.xlsx")

ABA_BASE = "produtos_base"
ABA_AGENDA = "agenda_dia"
ABA_LOG = "log_envios"

def normalizar_pid(x) -> str:
    if pd.isna(x) or x is None:
        return ""
    return str(x).strip()

def carregar_aba(path: str, sheet: str) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet)

def salvar_abas(path: str, abas: dict):
    """
    abas: dict[str, DataFrame] com todas as abas a serem gravadas.
    Regrava o arquivo inteiro (mais seguro que tentar append parcial).
    """
    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
        for name, df in abas.items():
            df.to_excel(writer, sheet_name=name, index=False)

def confirmar_envios():
    # Carrega abas necessárias
    df_base = carregar_aba(ARQUIVO_CONTROLE, ABA_BASE)
    df_agenda = carregar_aba(ARQUIVO_CONTROLE, ABA_AGENDA)

    # Valida colunas mínimas
    for col in ["produto_id", "ultimo_envio"]:
        if col not in df_base.columns:
            raise ValueError(f"Coluna '{col}' não encontrada em '{ABA_BASE}'")

    for col in ["produto_id", "geracao", "horario", "valido"]:
        if col not in df_agenda.columns:
            raise ValueError(f"Coluna '{col}' não encontrada em '{ABA_AGENDA}'")

    # Filtra itens válidos da agenda
    agenda_ok = df_agenda[df_agenda["valido"].astype(str).str.upper().str.strip() == "SIM"].copy()
    if agenda_ok.empty:
        print("⚠️ Nenhum item 'SIM' na agenda_dia. Nada para confirmar.")
        return

    agenda_ok["produto_id"] = agenda_ok["produto_id"].apply(normalizar_pid)
    agenda_ok = agenda_ok[agenda_ok["produto_id"] != ""].copy()

    if agenda_ok.empty:
        print("⚠️ Agenda 'SIM' sem produto_id preenchido. Nada para confirmar.")
        return

    # Prepara log
    hoje = datetime.now().date().isoformat()
    df_log_new = agenda_ok[["horario", "produto_id", "geracao"]].copy()
    df_log_new.insert(0, "data", hoje)

    # Lê log existente (se não existir, cria)
    try:
        df_log = carregar_aba(ARQUIVO_CONTROLE, ABA_LOG)
        # garante colunas
        for c in ["data", "horario", "produto_id", "geracao"]:
            if c not in df_log.columns:
                df_log[c] = ""
        df_log = df_log[["data", "horario", "produto_id", "geracao"]]
    except Exception:
        df_log = pd.DataFrame(columns=["data", "horario", "produto_id", "geracao"])

    # Evita duplicar confirmação no mesmo dia (mesmo produto_id + data)
    df_log["produto_id"] = df_log["produto_id"].apply(normalizar_pid)
    existing_keys = set(zip(df_log["data"].astype(str), df_log["produto_id"].astype(str)))
    new_keys = [(hoje, pid) for pid in df_log_new["produto_id"].astype(str)]
    mask_new = [k not in existing_keys for k in new_keys]
    df_log_new = df_log_new[mask_new].copy()

    if df_log_new.empty:
        print("ℹ️ Nada novo para confirmar (provavelmente já confirmado hoje).")
        return

    df_log = pd.concat([df_log, df_log_new], ignore_index=True)

    # Atualiza ultimo_envio na base SOMENTE dos confirmados agora
    confirmados = set(df_log_new["produto_id"].astype(str))
    df_base["produto_id"] = df_base["produto_id"].apply(normalizar_pid)

    hoje_date = datetime.now().date()
    def upd(row):
        pid = row["produto_id"]
        if pid in confirmados:
            return hoje_date
        return row["ultimo_envio"]

    df_base["ultimo_envio"] = df_base.apply(upd, axis=1)

    # Mantém outras abas existentes (sem perder nada)
    xls = pd.ExcelFile(ARQUIVO_CONTROLE)
    abas_out = {}
    for s in xls.sheet_names:
        if s == ABA_BASE:
            abas_out[s] = df_base
        elif s == ABA_LOG:
            abas_out[s] = df_log
        else:
            abas_out[s] = pd.read_excel(ARQUIVO_CONTROLE, sheet_name=s)

    # Se não existia log, cria
    if ABA_LOG not in abas_out:
        abas_out[ABA_LOG] = df_log

    salvar_abas(ARQUIVO_CONTROLE, abas_out)

    print("✅ Confirmação concluída.")
    print(f"- Confirmados agora: {len(confirmados)}")
    print(f"- Log total: {len(df_log)}")
    print("➡️ 'ultimo_envio' atualizado para hoje para os confirmados.")

if __name__ == "__main__":
    confirmar_envios()
