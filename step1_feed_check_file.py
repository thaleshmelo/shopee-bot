import os
import pandas as pd

# Coloque o caminho do arquivo aqui OU use variável de ambiente
FEED_FILE = os.getenv("SHOPEE_FEED_FILE", "").strip()

def read_csv_robust(path: str) -> pd.DataFrame:
    # tenta encodings comuns
    encodings = ["utf-8-sig", "utf-8", "latin1", "cp1252"]
    seps = [",", ";", "\t", "|"]

    last_err = None
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(path, encoding=enc, sep=sep, low_memory=False)
                # heurística: precisa ter algumas colunas e algumas linhas
                if len(df.columns) >= 3 and len(df) >= 1:
                    return df
            except Exception as e:
                last_err = e

    raise RuntimeError(f"Falha ao ler CSV. Último erro: {repr(last_err)}")

def main():
    if not FEED_FILE:
        raise SystemExit(
            "Defina a variável SHOEPEE_FEED_FILE com o caminho do arquivo CSV.\n"
            "Exemplo (PowerShell):\n"
            "$env:SHOEPEE_FEED_FILE='C:\\Users\\thale\\Downloads\\ShopeeBrasil2022.csv'\n"
            "Depois rode:\n"
            "python step1_feed_check_file.py"
        )

    df = read_csv_robust(FEED_FILE)

    print("[OK] Feed lido do arquivo local")
    print(f"[INFO] Linhas: {len(df)} | Colunas: {len(df.columns)}")

    print("\n[COLUNAS]")
    for col in df.columns:
        print("-", col)

    print("\n[AMOSTRA]")
    print(df.head(3).to_string(index=False))

if __name__ == "__main__":
    main()