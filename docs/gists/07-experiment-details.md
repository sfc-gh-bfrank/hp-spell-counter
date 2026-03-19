| Parameter | Value |
|---|---|
| Books | Harry Potter 1–7 (J.K. Rowling) |
| Total chapters | 178 |
| Corpus size | ~6.4MB |
| Spells replaced | 3 unique × ~3 occurrences each = 7 total |
| Platform | Snowflake Cortex (trial account, AWS us-east-1, cross-region enabled) |
| Part 1 — Search agent | Cortex Search + Cortex Agent (`HP_SPELL_AGENT`) |
| Part 1 — Agent model | claude-sonnet-4-5 |
| Part 2 — Extraction model | claude-opus-4-5 (via `CORTEX.COMPLETE()`) |
| Part 2 — Enrichment model | claude-sonnet-4-5 (via `CORTEX.COMPLETE()`, one call per row) |
| Part 2 — Analyst agent | Cortex Analyst + Semantic View + Cortex Agent (`HP_SPELL_ANALYST_AGENT`) |
| Part 2 — Agent model | claude-sonnet-4-5 |
| Spell occurrences extracted (raw) | 357 total, 131 unique |
| Spell occurrences after enrichment | 354 valid, 90 unique canonical spells |
| Fake spells surviving enrichment (v1 prompt) | 4 / 7 |
| Fake spells surviving enrichment (v2 prompt) | 7 / 7 |
