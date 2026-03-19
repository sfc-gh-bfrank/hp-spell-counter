-- =============================================================================
-- enrich_spells.sql
-- Part 2b: Semantic enrichment pass
--
-- Takes the raw SPELL_OCCURRENCES table (faithful to text) and runs a second
-- CORTEX.COMPLETE() pass to canonicalize spell names using the extracted
-- context, effect, and spell_name together.
--
-- This demonstrates a different AI capability from the extraction pass:
--   - Extraction: faithfulness (include what's in the text, no filtering)
--   - Enrichment: semantic understanding (recognize STUBEFY = Stupefy the
--     way a human reader would, using surrounding context as evidence)
--
-- The raw table is preserved. Enriched results go to SPELL_OCCURRENCES_ENRICHED.
-- =============================================================================

USE DATABASE HP_DEMO;
USE SCHEMA SPELLS;
USE WAREHOUSE HP_DEMO_WH;

-- -----------------------------------------------------------------------------
-- Step 1: Create the enriched output table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SPELL_OCCURRENCES_ENRICHED (
    occurrence_id       NUMBER PRIMARY KEY,  -- FK to SPELL_OCCURRENCES
    book_num            NUMBER,
    book_title          VARCHAR,
    chapter_title       VARCHAR,
    raw_spell_name      VARCHAR,             -- original extracted value
    canonical_spell     VARCHAR,             -- normalized by AI
    is_valid_spell      BOOLEAN,             -- FALSE = not a wand incantation
    caster              VARCHAR,
    effect              VARCHAR,
    context             VARCHAR,
    canonicalization_notes VARCHAR           -- AI's reasoning
);

-- -----------------------------------------------------------------------------
-- Step 2: Enrichment pass — one COMPLETE() call per row
--
-- The model sees:
--   - raw_spell_name: what was extracted (may be partial, typo, all-caps, etc.)
--   - effect:         what the spell did in the text
--   - context:        the exact sentence it appeared in
--
-- It returns a JSON object with:
--   - canonical_spell: the standard spell name (e.g. "Stupefy")
--   - is_valid_spell:  true/false
--   - notes:           brief reasoning (especially for edge cases)
-- -----------------------------------------------------------------------------
INSERT INTO SPELL_OCCURRENCES_ENRICHED (
    occurrence_id, book_num, book_title, chapter_title,
    raw_spell_name, canonical_spell, is_valid_spell,
    caster, effect, context, canonicalization_notes
)
WITH enriched AS (
    SELECT
        occurrence_id,
        book_num,
        book_title,
        chapter_title,
        spell_name AS raw_spell_name,
        caster,
        effect,
        context,
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
    SELECT
        occurrence_id,
        book_num,
        book_title,
        chapter_title,
        raw_spell_name,
        caster,
        effect,
        context,
        TRY_PARSE_JSON(
            TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        TRIM(enrichment_json),
                        '^```json\\s*', '', 1, 1, 'ms'
                    ),
                    '\\s*```\\s*$', '', 1, 1, 'ms'
                )
            )
        ) AS enrichment
    FROM enriched
)
SELECT
    occurrence_id,
    book_num,
    book_title,
    chapter_title,
    raw_spell_name,
    enrichment:canonical_spell::VARCHAR   AS canonical_spell,
    enrichment:is_valid_spell::BOOLEAN    AS is_valid_spell,
    caster,
    effect,
    context,
    enrichment:notes::VARCHAR             AS canonicalization_notes
FROM parsed
WHERE enrichment IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Step 3: Review results
-- -----------------------------------------------------------------------------

-- How many were flagged as not-valid?
SELECT is_valid_spell, COUNT(*) AS cnt
FROM SPELL_OCCURRENCES_ENRICHED
GROUP BY is_valid_spell;

-- What got canonicalized (raw != canonical)?
SELECT raw_spell_name, canonical_spell, canonicalization_notes
FROM SPELL_OCCURRENCES_ENRICHED
WHERE raw_spell_name != canonical_spell
  AND is_valid_spell = TRUE
ORDER BY raw_spell_name;

-- What was flagged as not a spell?
SELECT raw_spell_name, context, canonicalization_notes
FROM SPELL_OCCURRENCES_ENRICHED
WHERE is_valid_spell = FALSE
ORDER BY raw_spell_name;

-- Final clean unique spell list
SELECT canonical_spell, COUNT(*) AS occurrences
FROM SPELL_OCCURRENCES_ENRICHED
WHERE is_valid_spell = TRUE
GROUP BY canonical_spell
ORDER BY occurrences DESC;

-- Confirm fake spells survived enrichment
SELECT canonical_spell, raw_spell_name, caster, book_title, chapter_title
FROM SPELL_OCCURRENCES_ENRICHED
WHERE UPPER(canonical_spell) IN ('VINCULUM UMBRAE', 'GLACIOFORS', 'PYROCLAVIS')
ORDER BY book_num;
