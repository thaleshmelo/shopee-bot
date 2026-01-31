import subprocess
import sys
from pathlib import Path


def run(cmd: str):
    print(f"\n[CMD] {cmd}", flush=True)
    subprocess.run(cmd, shell=True, check=True)


def main():
    project_root = Path(__file__).resolve().parent
    venv_python = project_root / ".venv" / "Scripts" / "python.exe"

    if not venv_python.exists():
        raise RuntimeError(f"Não encontrei o python da venv em: {venv_python}")

    run(f'"{venv_python}" pipeline\\step0_fetch_offers.py')
    run(f'"{venv_python}" pipeline\\step1_feed_check_file.py')
    run(f'"{venv_python}" pipeline\\step2_pick_offers.py')
    run(f'"{venv_python}" pipeline\\step3_generate_short_links.py')

    # NEW: adiciona preço cheio (original_price) no CSV final
    run(f'"{venv_python}" pipeline\\step3b_enrich_prices.py')

    print("\n✅ run_pipeline_daily finalizado (picks do dia gerados + preço cheio).", flush=True)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print(f"\n[ERR] Comando falhou: {e}", flush=True)
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERR] {e}", flush=True)
        sys.exit(1)
