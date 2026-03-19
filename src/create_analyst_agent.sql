-- =============================================================================
-- create_analyst_agent.sql
-- Part 2: Semantic View + Cortex Analyst Agent
--
-- Creates a native Snowflake Semantic View over SPELL_OCCURRENCES_ENRICHED
-- and a Cortex Agent backed by Cortex Analyst (text-to-SQL) instead of
-- Cortex Search. This agent can enumerate every spell in the table including
-- the fake ones — prior knowledge cannot bias a SELECT DISTINCT.
--
-- The enriched table is the output of enrich_spells.sql, which ran a second
-- CORTEX.COMPLETE() pass to canonicalize spell names using surrounding context
-- (e.g. STUBEFY → Stupefy, Accio Salmon → Accio, STUP— → Stupefy).
-- =============================================================================

USE DATABASE HP_DEMO;
USE SCHEMA SPELLS;
USE WAREHOUSE HP_DEMO_WH;

-- -----------------------------------------------------------------------------
-- Step 1: Semantic View over SPELL_OCCURRENCES_ENRICHED
-- -----------------------------------------------------------------------------
CREATE OR REPLACE SEMANTIC VIEW HP_DEMO.SPELLS.SPELL_OCCURRENCES_SV
  TABLES (
    spells AS HP_DEMO.SPELLS.SPELL_OCCURRENCES_ENRICHED PRIMARY KEY (occurrence_id)
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
  AI_SQL_GENERATION 'This semantic view contains every valid spoken wand incantation extracted from all 7 Harry Potter books, with spell names canonicalized by a second AI pass (e.g. typos corrected, Accio variants collapsed, interrupted spells resolved using context). The spell_name dimension is the canonical form. raw_spell_name is what appeared literally in the text. Includes unfamiliar spell names not in standard HP canon. When asked to list spells, always use SELECT DISTINCT canonical_spell.';

-- Verify
DESCRIBE SEMANTIC VIEW HP_DEMO.SPELLS.SPELL_OCCURRENCES_SV;

-- -----------------------------------------------------------------------------
-- Step 2: Create the Cortex Analyst Agent (Part 2)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE AGENT HP_DEMO.SPELLS.HP_SPELL_ANALYST_AGENT
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
      semantic_view: HP_DEMO.SPELLS.SPELL_OCCURRENCES_SV
      execution_environment:
        type: warehouse
        warehouse: HP_DEMO_WH
  $$;

-- -----------------------------------------------------------------------------
-- Step 3: Verify both agents exist
-- -----------------------------------------------------------------------------
SHOW AGENTS IN SCHEMA HP_DEMO.SPELLS;
