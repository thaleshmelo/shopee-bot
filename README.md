# Shopee Bot / Achadinhos da Yuki

Pipeline em Python para curadoria de ofertas (Shopee) com **controle de repetição (cooldown)**, **agenda diária**, **geração de mensagens** e **registro/confirmação de envios** para operação em canal de ofertas (WhatsApp).

> Objetivo: reduzir trabalho operacional e manter consistência de postagens, mantendo espaço para “posts premium” manuais.

---

## ✨ Principais recursos

- **Base viva de produtos** (`data/controle_produtos.xlsx`)
  - status (ativo/pausado)
  - controle de `ultimo_envio`
  - geração/ciclo de rotação

- **Agenda diária** (`agenda_dia`)
  - distribuição em blocos de horário
  - seleção balanceada por geração (ex.: A/B/C)
  - marcação `SIM/NAO` com motivo

- **Cooldown anti-repetição**
  - evita repostar o mesmo item em curto intervalo
  - reduz fadiga da audiência

- **Mensagens prontas para WhatsApp**
  - exporta arquivo diário com texto pronto
  - inclui **CTA de reações** (engajamento)

- **Confirmação de envios + log**
  - registra envios do dia
  - atualiza `ultimo_envio` apenas quando confirmado

---

## 🗂 Estrutura do repositório

SHOPEE-BOT/
├── src/
│ ├── step0_build_controle.py
│ ├── gerar_agenda.py
│ ├── step3_format_whatsapp.py
│ └── step5_confirmar_envios.py
│
├── pipeline/
│ ├── step1_feed_check_file.py
│ ├── step2_pick_offers.py
│ └── step4_schedule_messages.py
│
├── data/
│ ├── controle_produtos.xlsx
│ └── picks_refinados.csv
│
├── outputs/ # artefatos diários (não versionar)
│ ├── mensagens_whatsapp_YYYY-MM-DD.xlsx
│ └── agenda_envios_YYYY-MM-DD.xlsx
│
├── .gitignore
└── README.md


---

## ⚙️ Pré-requisitos

- Python 3.10+
- Ambiente virtual (opcional, recomendado)
- Bibliotecas principais:
  - `pandas`
  - `openpyxl`

---

## 🚀 Fluxo diário de uso

### 1️⃣ Atualizar base de produtos
Importa/atualiza os produtos elegíveis.

```bash
python src/step0_build_controle.py
