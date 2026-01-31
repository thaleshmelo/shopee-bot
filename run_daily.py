import os
import subprocess
import sys
from pathlib import Path


def run(cmd, env=None):
    print(f"\n[CMD] {cmd}", flush=True)
    subprocess.run(cmd, shell=True, check=True, env=env)


def main():
    project_root = Path(__file__).resolve().parent
    venv_python = project_root / ".venv" / "Scripts" / "python.exe"

    if not venv_python.exists():
        raise RuntimeError(f"Não encontrei o python da venv em: {venv_python}")

    env = os.environ.copy()

    # Defaults do envio diário
    env.setdefault("WA_GROUP_NAME", "Achadinhos da Yuki")
    env.setdefault("WA_TEST_MODE", "0")
    env.setdefault("WA_DAILY_SENDS", "75")
    env.setdefault("WA_WINDOW_START", "09:00")
    env.setdefault("WA_WINDOW_END", "23:50")
    env.setdefault("WA_INTERVALS", "8,10,12")
    env.setdefault("WA_JITTER_SECONDS", "25")
    env.setdefault("WA_PICKS_FILE", r"outputs\picks_refinados_com_links.csv")
    env.setdefault("WA_SENT_LEDGER", r"outputs\sent_ledger_media.csv")
    env.setdefault("WA_PROFILE_DIR", ".wa_chrome_profile")
    env.setdefault("WA_HEADLESS", "0")

    # (Recomendado) forçar o Step2 a exigir imagem
    env.setdefault("STEP2_REQUIRE_IMAGE", "1")

    print("\n=== RUN_DAILY ===", flush=True)
    print(f"VENV PY: {venv_python}", flush=True)
    print(f"GROUP:   {env.get('WA_GROUP_NAME')}", flush=True)
    print(f"PICKS:   {env.get('WA_PICKS_FILE')}", flush=True)
    print(f"LEDGER:  {env.get('WA_SENT_LEDGER')}", flush=True)
    print(f"WINDOW:  {env.get('WA_WINDOW_START')} -> {env.get('WA_WINDOW_END')}", flush=True)
    print(f"TARGET:  {env.get('WA_DAILY_SENDS')}/dia | TEST={env.get('WA_TEST_MODE')}", flush=True)
    print("===============", flush=True)

    # 1) Gera picks do dia (SEM Step5)
    run(f'"{venv_python}" run_pipeline_daily.py', env=env)

    # 2) Scheduler abre WhatsApp e envia
    run(f'"{venv_python}" pipeline\\step6_scheduler_daily.py', env=env)

    print("\n✅ run_daily finalizado.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print(f"\n[ERR] Comando falhou: {e}", flush=True)
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERR] {e}", flush=True)
        sys.exit(1)
