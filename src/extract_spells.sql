-- =============================================================================
-- extract_spells.sql
-- Part 2: Structured extraction pipeline
--
-- Uses CORTEX.COMPLETE() with claude-opus-4-5 to extract every spoken wand
-- incantation from each chapter chunk, then stores results in a structured
-- table for SQL-based querying via Cortex Analyst.
-- =============================================================================

USE DATABASE HP_DEMO;
USE SCHEMA SPELLS;
USE WAREHOUSE HP_DEMO_WH;

-- -----------------------------------------------------------------------------
-- Step 1: Create the structured output table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SPELL_OCCURRENCES (
    occurrence_id  NUMBER AUTOINCREMENT PRIMARY KEY,
    book_num       NUMBER,
    book_title     VARCHAR,
    chapter_title  VARCHAR,
    spell_name     VARCHAR,
    caster         VARCHAR,
    effect         VARCHAR,
    context        VARCHAR  -- short surrounding quote from the text
);

-- -----------------------------------------------------------------------------
-- Step 2: Extract spell occurrences from every chapter using Opus
--
-- The prompt is strict:
--   - Only spoken wand incantations (words shouted/said to cast a spell)
--   - Must appear explicitly in the provided text
--   - Return JSON array — empty array if none found
--   - No prior knowledge: if a spell name is unfamiliar, include it anyway
-- -----------------------------------------------------------------------------
INSERT INTO SPELL_OCCURRENCES (
    book_num, book_title, chapter_title, spell_name, caster, effect, context
)
WITH raw_extractions AS (
    SELECT
        book_num,
        book_title,
        chapter_title,
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
                'TEXT:\n',
                chunk_text
            )
        ) AS extraction_json
    FROM BOOK_CHUNKS
),
parsed AS (
    SELECT
        book_num,
        book_title,
        chapter_title,
        -- Strip any accidental markdown code fences
        TRY_PARSE_JSON(
            TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        TRIM(extraction_json),
                        '^```json\\s*', '', 1, 1, 'ms'
                    ),
                    '\\s*```\\s*$', '', 1, 1, 'ms'
                )
            )
        ) AS spells_array
    FROM raw_extractions
    WHERE extraction_json IS NOT NULL
),
flattened AS (
    SELECT
        book_num,
        book_title,
        chapter_title,
        f.value:spell_name::VARCHAR   AS spell_name,
        f.value:caster::VARCHAR       AS caster,
        f.value:effect::VARCHAR       AS effect,
        f.value:context::VARCHAR      AS context
    FROM parsed,
    LATERAL FLATTEN(input => spells_array) f
    WHERE spells_array IS NOT NULL
      AND ARRAY_SIZE(spells_array) > 0
)
SELECT * FROM flattened
WHERE spell_name IS NOT NULL
  AND TRIM(spell_name) != '';

-- -----------------------------------------------------------------------------
-- Step 3: Verify the results
-- -----------------------------------------------------------------------------
SELECT
    book_num,
    book_title,
    COUNT(*) AS total_occurrences,
    COUNT(DISTINCT spell_name) AS unique_spells
FROM SPELL_OCCURRENCES
GROUP BY book_num, book_title
ORDER BY book_num;

-- Check that our fake spells were found
SELECT spell_name, caster, book_title, chapter_title, context
FROM SPELL_OCCURRENCES
WHERE UPPER(spell_name) IN (
    'VINCULUM UMBRAE', 'GLACIOFORS', 'PYROCLAVIS'
)
ORDER BY book_num;

-- Full unique spell list
SELECT DISTINCT spell_name, COUNT(*) AS occurrences
FROM SPELL_OCCURRENCES
GROUP BY spell_name
ORDER BY occurrences DESC;
