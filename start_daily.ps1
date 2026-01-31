# start_daily.ps1
$ErrorActionPreference = "Stop"

# WhatsApp
$env:WA_GROUP_NAME="Achadinhos da Yuki"
$env:WA_HEADLESS="0"
$env:WA_PROFILE_DIR=".wa_chrome_profile"

# Meta diária
$env:WA_DAILY_SENDS="75"
$env:WA_WINDOW_START="09:00"
$env:WA_WINDOW_END="23:50"
$env:WA_INTERVALS="8,10,12"
$env:WA_JITTER_SECONDS="25"

# Pipeline (puxa bastante produto)
$env:STEP0_LIMIT="50"
$env:STEP0_MAX_PAGES="30"

# Step2 (somente com imagem, bastante picks)
$env:STEP2_REQUIRE_IMAGE="1"
$env:STEP2_PICKS_N="250"

# IMPORTANTE: produção (não teste)
$env:WA_TEST_MODE="0"
Remove-Item Env:\WA_TEST_PICK_ITEMID -ErrorAction SilentlyContinue

python run_daily.py
