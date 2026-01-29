# run_pipeline.py
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parent
PIPELINE_DIR = PROJECT_ROOT / "pipeline"
DATA_DIR = PROJECT_ROOT / "data"
VALIDATED_FEED_CSV = DATA_DIR / "feed_validado.csv"

STEPS: List[Tuple[int, str]] = [
    (0, "step0_fetch_offers.py"),
    (1, "step1_feed_check_file.py"),
    (2, "step2_pick_offers.py"),
    (3, "step3_generate_short_links.py"),
    (4, "step4_schedule_messages.py"),
    (5, "step5_canal_assistido.py"),
]


def run_step(step_id: int, filename: str) -> None:
    script_path = PIPELINE_DIR / filename
    if not script_path.exists():
        raise FileNotFoundError(f"Step{step_id} não encontrado: {script_path}")

    print(f"\n=== Rodando Step{step_id}: {filename} ===")

    env = os.environ.copy()

    # Garantir que a raiz do projeto esteja no PYTHONPATH
    # (para imports como "from src.xxx import yyy" funcionarem sempre)
    current_pp = env.get("PYTHONPATH", "")
    root_str = str(PROJECT_ROOT)
    if current_pp:
        if root_str not in current_pp.split(os.pathsep):
            env["PYTHONPATH"] = root_str + os.pathsep + current_pp
    else:
        env["PYTHONPATH"] = root_str

    cmd = [sys.executable, str(script_path)]
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT), env=env)

    if result.returncode != 0:
        raise RuntimeError(f"Step{step_id} falhou com código {result.returncode}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Executa o pipeline Shopee-Bot (Step0->Step5).")
    parser.add_argument("--from-step", type=int, default=0)
    parser.add_argument("--to-step", type=int, default=5)
    args = parser.parse_args()

    if args.from_step < 0 or args.to_step > 5 or args.from_step > args.to_step:
        print("Intervalo inválido. Use --from-step 0-5 e --to-step 0-5, com from <= to.")
        return 2

    if not PIPELINE_DIR.exists():
        print(f"Pasta pipeline não encontrada em: {PIPELINE_DIR}")
        return 2

    try:
        # Step0..Step1
        for sid, fname in STEPS:
            if sid < 2 and args.from_step <= sid <= args.to_step:
                run_step(sid, fname)

        # Após Step1, garantir feed_validado.csv e setar env para o restante
        if args.to_step >= 2 and args.from_step <= 2:
            if not VALIDATED_FEED_CSV.exists():
                raise RuntimeError(
                    "Step1 terminou, mas não encontrei data/feed_validado.csv.\n"
                    "Isso indica que o Step1 não gerou o feed validado."
                )
            os.environ["SHOPEE_FEED_FILE"] = str(VALIDATED_FEED_CSV)
            print(f"\nENV setado automaticamente: SHOPEE_FEED_FILE={VALIDATED_FEED_CSV}")

        # Step2..Step5
        for sid, fname in STEPS:
            if sid >= 2 and args.from_step <= sid <= args.to_step:
                run_step(sid, fname)

        print("\n✅ Pipeline finalizado com sucesso.")
        return 0

    except Exception as e:
        print(f"\n❌ Pipeline interrompido: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
