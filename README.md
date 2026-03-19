# HP Spell Counter — RAG Grounding Experiment

A recreation and extension of a viral AI experiment demonstrating a fundamental
limitation of RAG (Retrieval-Augmented Generation) systems built on vector search.

See [`docs/FINDINGS.md`](docs/FINDINGS.md) for the full write-up and results.

---

## Structure

```
hp-spell-counter/
├── src/
│   ├── parse_epub.py        # Parses EPUBs, replaces real spells with fake ones
│   ├── load_to_snowflake.py # Chunks and loads text into Snowflake
│   ├── agent.py             # Runs the Cortex Agent experiment
│   └── test_connection.py   # Validates keypair auth
├── data/
│   └── output/              # Generated text files (gitignored)
├── docs/
│   └── FINDINGS.md          # Full experiment write-up
├── keys/                    # RSA keypair for Snowflake auth (gitignored)
└── requirements.txt
```

---

## Setup

### 1. Dependencies
```bash
pip install -r requirements.txt
```

### 2. Books
Place `hp1.epub` through `hp7.epub` in the project root. These are gitignored.

### 3. Snowflake
- Personal trial account at trial.snowflake.com (AWS us-east-1 recommended)
- Keypair auth — place `rsa_key.p8` and `rsa_key.pub` in `keys/`
- Connection configured in `~/.snowflake/connections.toml` as `hp_demo`

### 4. Environment
```bash
export PRIVATE_KEY_PASSPHRASE="your passphrase"
```

---

## Running the Experiment

```bash
# Step 1 — Parse all 7 books and replace real spells with fake ones
python src/parse_epub.py --output hp_series_modified.txt --inject-spells

# Step 2 — Load into Snowflake and create Cortex Search Service
python src/load_to_snowflake.py

# Step 3 — Run the agent experiment
python src/agent.py

# Optional — ask a specific question
python src/agent.py --question "List every spell cast in the graveyard scene"
```

---

## The Experiment

Three well-known spells (`Expelliarmus`, `Stupefy`, `Accio`) are replaced with
invented ones (`Vinculum Umbrae`, `Glaciofors`, `Pyroclavis`) in 3 scenes each
across the series. The replacements are contextually seamless.

The question: will a Cortex Agent, grounded on a Cortex Search Service built
from the modified text, report the fake spells — or the real ones it learned
during training?

**Result:** The agent reported the real spell names. See `docs/FINDINGS.md`.
