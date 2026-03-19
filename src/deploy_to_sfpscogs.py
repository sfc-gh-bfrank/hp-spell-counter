"""
deploy_to_sfpscogs.py

Deploys the full HP Spell Counter pipeline to SFPSCOGS-BFRANK using PAT auth.
Steps:
  1. Create HP_DEMO database, SPELLS schema, use DEMO_WH
  2. Create + load BOOK_CHUNKS table from local parsed text
  3. Create Cortex Search Service
  4. Run extraction (CORTEX.COMPLETE claude-opus-4-5)
  5. Run enrichment (CORTEX.COMPLETE claude-sonnet-4-5) — updated, no fake-spell bias
  6. Create Semantic View + Analyst Agent
  7. Create RAG Search Agent
  8. Verify results

Usage:
    python deploy_to_sfpscogs.py
"""

import re
import sys
import time
from pathlib import Path

import snowflake.connector

PAT_FILE = Path("/Users/bfrank/KEYS/demo_pat.key")
INPUT_FILE = Path(__file__).parent.parent / "data" / "output" / "hp_series_modified.txt"

ACCOUNT = "SFPSCOGS-BFRANK"
USER = "BFRANK"
WAREHOUSE = "DEMO_WH"
DATABASE = "HP_DEMO"
SCHEMA = "SPELLS"


def get_connection():
    pat = PAT_FILE.read_text().strip()
    return snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        token=pat,
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        warehouse=WAREHOUSE,
    )


def run(cur, sql, desc=None):
    if desc:
        print(f"\n{'='*60}\n{desc}\n{'='*60}")
    cur.execute(sql)
    try:
        rows = cur.fetchall()
        if rows:
            for r in rows[:10]:
                print("  ", r)
            if len(rows) > 10:
                print(f"  ... ({len(rows)} total rows)")
        return rows
    except Exception:
        return []


def parse_chunks(text):
    chunks = []
    book_pattern = re.compile(r"#{70}\n# BOOK (\d+): (.+?)\n#{70}", re.MULTILINE)
    chapter_pattern = re.compile(r"={60}\n(.+?)\n={60}", re.MULTILINE)

    book_matches = list(book_pattern.finditer(text))
    for i, book_match in enumerate(book_matches):
        book_num = int(book_match.group(1))
        book_title = book_match.group(2).strip()
        book_start = book_match.end()
        book_end = book_matches[i + 1].start() if i + 1 < len(book_matches) else len(text)
        book_text = text[book_start:book_end]

        chapter_matches = list(chapter_pattern.finditer(book_text))
        for j, chap_match in enumerate(chapter_matches):
            chapter_title = chap_match.group(1).strip()
            chap_start = chap_match.end()
            chap_end = chapter_matches[j + 1].start() if j + 1 < len(chapter_matches) else len(book_text)
            chunk_text = book_text[chap_start:chap_end].strip()
            if chunk_text:
                chunks.append((book_num, book_title, chapter_title, chunk_text))

    return chunks


def step1_create_infrastructure(cur):
    run(cur, f"CREATE DATABASE IF NOT EXISTS {DATABASE}", "Step 1a: Create database")
    run(cur, f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{SCHEMA}", "Step 1b: Create schema")
    run(cur, f"USE DATABASE {DATABASE}")
    run(cur, f"USE SCHEMA {SCHEMA}")
    run(cur, f"USE WAREHOUSE {WAREHOUSE}")


def step2_load_book_chunks(cur):
    run(cur, """
        CREATE OR REPLACE TABLE BOOK_CHUNKS (
            chunk_id      NUMBER AUTOINCREMENT PRIMARY KEY,
            book_num      NUMBER,
            book_title    VARCHAR,
            chapter_title VARCHAR,
            chunk_text    TEXT
        )
    """, "Step 2a: Create BOOK_CHUNKS table")

    print("\nStep 2b: Parsing local text file...")
    text = INPUT_FILE.read_text(encoding="utf-8")
    chunks = parse_chunks(text)
    print(f"  Parsed {len(chunks)} chapters")

    print("Step 2c: Loading chunks...")
    cur.executemany(
        "INSERT INTO BOOK_CHUNKS (book_num, book_title, chapter_title, chunk_text) VALUES (%s, %s, %s, %s)",
        chunks,
    )
    print(f"  Loaded {len(chunks)} rows")
    run(cur, "SELECT book_num, book_title, COUNT(*) FROM BOOK_CHUNKS GROUP BY 1,2 ORDER BY 1", "Verify load:")


def step3_create_search_service(cur):
    run(cur, f"""
        CREATE OR REPLACE CORTEX SEARCH SERVICE {DATABASE}.{SCHEMA}.BOOK_SEARCH_SVC
          ON chunk_text
          ATTRIBUTES book_num, book_title, chapter_title
          WAREHOUSE = {WAREHOUSE}
          TARGET_LAG = '1 minute'
          AS (
            SELECT chunk_id, book_num, book_title, chapter_title, chunk_text
            FROM {DATABASE}.{SCHEMA}.BOOK_CHUNKS
          )
    """, "Step 3: Create Cortex Search Service")
    print("  Search service created. Waiting 30s for indexing...")
    time.sleep(30)


def step4_extract_spells(cur):
    run(cur, """
        CREATE OR REPLACE TABLE SPELL_OCCURRENCES (
            occurrence_id  NUMBER AUTOINCREMENT PRIMARY KEY,
            book_num       NUMBER,
            book_title     VARCHAR,
            chapter_title  VARCHAR,
            spell_name     VARCHAR,
            caster         VARCHAR,
            effect         VARCHAR,
            context        VARCHAR
        )
    """, "Step 4a: Create SPELL_OCCURRENCES table")

    print("\nStep 4b: Running extraction with claude-opus-4-5 (this takes ~5-10 min)...")
    cur.execute("""
        INSERT INTO SPELL_OCCURRENCES (book_num, book_title, chapter_title, spell_name, caster, effect, context)
        WITH raw_extractions AS (
            SELECT
                book_num, book_title, chapter_title,
                SNOWFLAKE.CORTEX.COMPLETE(
                    'claude-opus-4-5',
                    CONCAT(
                        'You are a precise text extraction engine. Your job is to find every spoken wand incantation in the following Harry Potter chapter text.\n\n',
                        'RULES:\n',
                        '- Extract ONLY words or phrases that are explicitly spoken aloud as a wand spell incantation in the text\n',
                        '- A spell incantation is a word or phrase spoken by a character immediately before or while casting a spell with a wand\n',
                        '- Look for patterns like: "Expelliarmus!", ''Stupefy!'', cried/shouted/said followed by a capitalized word\n',
                        '- Include the spell even if you do not recognize it — do not filter based on your prior knowledge\n',
                        '- Do NOT include: character names, exclamations of surprise, non-spell dialogue\n',
                        '- For each spell found, extract: spell_name, caster (character who cast it, or "unknown"), effect (what the spell did, from the text), context (the exact sentence containing the spell, max 200 chars)\n',
                        '- Return a JSON array of objects. If no spells are found, return an empty array []\n',
                        '- Return ONLY the JSON array, no explanation, no markdown\n\n',
                        'TEXT:\n', chunk_text
                    )
                ) AS extraction_json
            FROM BOOK_CHUNKS
        ),
        parsed AS (
            SELECT book_num, book_title, chapter_title,
                TRY_PARSE_JSON(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(extraction_json), '^```json\\s*', '', 1, 1, 'ms'), '\\s*```\\s*$', '', 1, 1, 'ms'))) AS spells_array
            FROM raw_extractions WHERE extraction_json IS NOT NULL
        ),
        flattened AS (
            SELECT book_num, book_title, chapter_title,
                f.value:spell_name::VARCHAR AS spell_name,
                f.value:caster::VARCHAR AS caster,
                f.value:effect::VARCHAR AS effect,
                f.value:context::VARCHAR AS context
            FROM parsed, LATERAL FLATTEN(input => spells_array) f
            WHERE spells_array IS NOT NULL AND ARRAY_SIZE(spells_array) > 0
        )
        SELECT * FROM flattened WHERE spell_name IS NOT NULL AND TRIM(spell_name) != ''
    """)
    count = cur.execute("SELECT COUNT(*) FROM SPELL_OCCURRENCES").fetchone()[0]
    print(f"  Extracted {count} spell occurrences")

    run(cur, """
        SELECT spell_name, caster, book_title FROM SPELL_OCCURRENCES
        WHERE UPPER(spell_name) IN ('VINCULUM UMBRAE', 'GLACIOFORS', 'PYROCLAVIS') ORDER BY book_num
    """, "Step 4c: Verify fake spells found in extraction:")


def step5_enrich_spells(cur):
    run(cur, """
        CREATE OR REPLACE TABLE SPELL_OCCURRENCES_ENRICHED (
            occurrence_id       NUMBER PRIMARY KEY,
            book_num            NUMBER,
            book_title          VARCHAR,
            chapter_title       VARCHAR,
            raw_spell_name      VARCHAR,
            canonical_spell     VARCHAR,
            is_valid_spell      BOOLEAN,
            caster              VARCHAR,
            effect              VARCHAR,
            context             VARCHAR,
            canonicalization_notes VARCHAR
        )
    """, "Step 5a: Create SPELL_OCCURRENCES_ENRICHED table")

    print("\nStep 5b: Running enrichment with claude-sonnet-4-5 (updated prompt, no fake-spell bias)...")
    cur.execute("""
        INSERT INTO SPELL_OCCURRENCES_ENRICHED (
            occurrence_id, book_num, book_title, chapter_title,
            raw_spell_name, canonical_spell, is_valid_spell,
            caster, effect, context, canonicalization_notes
        )
        WITH enriched AS (
            SELECT
                occurrence_id, book_num, book_title, chapter_title,
                spell_name AS raw_spell_name, caster, effect, context,
                SNOWFLAKE.CORTEX.COMPLETE(
                    'claude-sonnet-4-5',
                    CONCAT(
                        'You are canonicalizing spell names extracted from the Harry Potter book series. ',
                        'Your job is to use the spell name, effect, and surrounding context to determine ',
                        'the canonical (standard) form of the spell, exactly as J.K. Rowling intended.\n\n',
                        'Rules:\n',
                        '- If the spell name is a typo, partial word, or interrupted mid-cast (e.g. "STUP—", "STUBEFY"), ',
                        'identify the intended spell using the context and effect as evidence\n',
                        '- If the spell name is all-caps, title-case, or lowercase, normalize to standard title case ',
                        '(e.g. "STUPEFY" → "Stupefy", "expecto patronum" → "Expecto Patronum")\n',
                        '- If the spell includes an object being summoned (e.g. "Accio Salmon", "Accio Wand"), ',
                        'the canonical form is just "Accio" — the object is not part of the incantation\n',
                        '- If the entry is clearly not a wand incantation (e.g. a phrase, a charm description, ',
                        'a non-magical exclamation), set is_valid_spell to false\n',
                        '- If you do not recognize a spell name but it appears in valid casting context, keep it as-is — ',
                        'do not correct it to a known spell\n',
                        '- Return ONLY a JSON object with three fields: ',
                        '"canonical_spell" (string), "is_valid_spell" (boolean), "notes" (one sentence max)\n\n',
                        'spell_name: ', spell_name, '\n',
                        'effect: ', COALESCE(effect, 'unknown'), '\n',
                        'context: ', COALESCE(context, 'unknown')
                    )
                ) AS enrichment_json
            FROM SPELL_OCCURRENCES
        ),
        parsed AS (
            SELECT occurrence_id, book_num, book_title, chapter_title,
                raw_spell_name, caster, effect, context,
                TRY_PARSE_JSON(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(enrichment_json), '^```json\\s*', '', 1, 1, 'ms'), '\\s*```\\s*$', '', 1, 1, 'ms'))) AS enrichment
            FROM enriched
        )
        SELECT
            occurrence_id, book_num, book_title, chapter_title, raw_spell_name,
            enrichment:canonical_spell::VARCHAR AS canonical_spell,
            enrichment:is_valid_spell::BOOLEAN AS is_valid_spell,
            caster, effect, context,
            enrichment:notes::VARCHAR AS canonicalization_notes
        FROM parsed WHERE enrichment IS NOT NULL
    """)

    rows = run(cur, """
        SELECT COUNT(*) AS total, COUNT(DISTINCT canonical_spell) AS unique_spells
        FROM SPELL_OCCURRENCES_ENRICHED WHERE is_valid_spell = TRUE
    """, "Step 5c: Enrichment results:")
    print(f"\n  Valid occurrences: {rows[0][0]}, Unique spells: {rows[0][1]}")


def step6_create_semantic_view_and_agents(cur):
    run(cur, f"""
        CREATE OR REPLACE SEMANTIC VIEW {DATABASE}.{SCHEMA}.SPELL_OCCURRENCES_SV
          TABLES (
            spells AS {DATABASE}.{SCHEMA}.SPELL_OCCURRENCES_ENRICHED PRIMARY KEY (occurrence_id)
          )
          DIMENSIONS (
            spells.canonical_spell        AS canonical_spell,
            spells.raw_spell_name         AS raw_spell_name,
            spells.caster                 AS caster,
            spells.book_title             AS book_title,
            spells.book_num               AS book_num,
            spells.chapter_title          AS chapter_title,
            spells.effect                 AS effect,
            spells.context                AS context,
            spells.canonicalization_notes AS canonicalization_notes
          )
          METRICS (
            spells.total_occurrences  AS COUNT(spells.occurrence_id),
            spells.unique_spell_count AS COUNT(DISTINCT spells.canonical_spell)
          )
          COMMENT = 'Canonicalized spell occurrences extracted and enriched from HP series text by Cortex AI'
          AI_SQL_GENERATION 'This semantic view contains every valid spoken wand incantation extracted from all 7 Harry Potter books, with spell names canonicalized by a second AI pass (e.g. typos corrected, Accio variants collapsed, interrupted spells resolved using context). The spell_name dimension is the canonical form. raw_spell_name is what appeared literally in the text. Includes unfamiliar spell names not in standard HP canon. When asked to list spells, always use SELECT DISTINCT canonical_spell.'
    """, "Step 6a: Create Semantic View")

    run(cur, f"""
        CREATE OR REPLACE AGENT {DATABASE}.{SCHEMA}.HP_SPELL_AGENT
          COMMENT = 'Part 1 agent — RAG search over book text (demonstrates prior-knowledge bias)'
          FROM SPECIFICATION
          $$
          models:
            orchestration: claude-sonnet-4-5

          instructions:
            system: >
              You are a Harry Potter spell expert. Search the book text thoroughly
              to find and list every spell, incantation, and charm mentioned in the
              Harry Potter series. Be exhaustive — do not rely on your own knowledge,
              search the documents for evidence.
            response: >
              Be precise. Include spell names, who cast them, and which book.
              Give a total count of unique spells found.

          tools:
            - tool_spec:
                type: cortex_search
                name: book_search
                description: >
                  Search the full text of all 7 Harry Potter books (chunked by chapter).

          tool_resources:
            book_search:
              cortex_search_service: {DATABASE}.{SCHEMA}.BOOK_SEARCH_SVC
          $$
    """, "Step 6b: Create RAG Search Agent (HP_SPELL_AGENT)")

    run(cur, f"""
        CREATE OR REPLACE AGENT {DATABASE}.{SCHEMA}.HP_SPELL_ANALYST_AGENT
          COMMENT = 'Part 2 agent — answers spell questions via SQL over structured extracted data, not search'
          FROM SPECIFICATION
          $$
          models:
            orchestration: claude-sonnet-4-5

          instructions:
            system: >
              You are a precise data analyst for the Harry Potter spell database.
              All spell data was extracted directly from the text of all 7 books by an
              AI that read every chapter, then enriched by a second AI pass that
              canonicalized spell names using surrounding context (correcting typos,
              collapsing Accio variants, resolving interrupted casts). The spell_name
              dimension contains canonical forms; raw_spell_name is the literal text.
              Always query the data to answer questions. Never answer from memory.
              When listing spells, return every distinct canonical_spell from the database.
              is_valid_spell is always TRUE in this view — invalid entries were excluded.
            response: >
              Be precise. Show exact spell names as they appear in the data.
              When giving counts, show your SQL logic. Cite book and chapter for notable findings.
            sample_questions:
              - question: How many unique spells are in the series?
                answer: I will query the database for a count of distinct spell names.
              - question: List every spell cast by Harry Potter
                answer: I will query for all spells where caster contains Harry.
              - question: Which spells appear in the text that you don't recognize?
                answer: I will return all distinct spell names from the database without filtering.

          tools:
            - tool_spec:
                type: cortex_analyst_text_to_sql
                name: spell_analyst
                description: >
                  Query the structured spell occurrences table extracted from the HP book text.
                  Use this to answer any question about spells, casters, books, or chapters.

          tool_resources:
            spell_analyst:
              semantic_view: {DATABASE}.{SCHEMA}.SPELL_OCCURRENCES_SV
              execution_environment:
                type: warehouse
                warehouse: {WAREHOUSE}
          $$
    """, "Step 6c: Create Analyst Agent (HP_SPELL_ANALYST_AGENT)")


def step7_verify(cur):
    run(cur, """
        SELECT canonical_spell, raw_spell_name, caster, book_title, chapter_title
        FROM SPELL_OCCURRENCES_ENRICHED
        WHERE UPPER(canonical_spell) IN ('VINCULUM UMBRAE', 'GLACIOFORS', 'PYROCLAVIS')
        ORDER BY canonical_spell, book_num
    """, "Step 7: Verify fake spells survived enrichment WITHOUT bias prompt")

    run(cur, "SHOW AGENTS IN SCHEMA HP_DEMO.SPELLS", "Agents in HP_DEMO.SPELLS:")


def main():
    conn = get_connection()
    cur = conn.cursor()

    try:
        step1_create_infrastructure(cur)
        conn.commit()

        step2_load_book_chunks(cur)
        conn.commit()

        step3_create_search_service(cur)

        step4_extract_spells(cur)
        conn.commit()

        step5_enrich_spells(cur)
        conn.commit()

        step6_create_semantic_view_and_agents(cur)

        step7_verify(cur)

        print("\n" + "=" * 60)
        print("DEPLOYMENT COMPLETE")
        print("=" * 60)
        print(f"Account:  {ACCOUNT}")
        print(f"Database: {DATABASE}.{SCHEMA}")
        print(f"Objects:  BOOK_CHUNKS, SPELL_OCCURRENCES, SPELL_OCCURRENCES_ENRICHED")
        print(f"Services: BOOK_SEARCH_SVC, SPELL_OCCURRENCES_SV")
        print(f"Agents:   HP_SPELL_AGENT (RAG), HP_SPELL_ANALYST_AGENT (Analyst)")

    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
