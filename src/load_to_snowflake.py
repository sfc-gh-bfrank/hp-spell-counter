"""
load_to_snowflake.py

Loads the modified HP series text into Snowflake, chunked by chapter,
then creates a Cortex Search Service over it.

Usage:
    export PRIVATE_KEY_PASSPHRASE="your passphrase"
    python load_to_snowflake.py
"""

import re
from pathlib import Path
import snowflake.connector
from snowflake.connector import DictCursor

INPUT_FILE = Path(__file__).parent.parent / "data" / "output" / "hp_series_modified.txt"
PRIVATE_KEY_PATH = Path(__file__).parent.parent / "keys" / "rsa_key.p8"

SNOWFLAKE_ACCOUNT = "WJTXXEX-HOC83051"
SNOWFLAKE_USER = "HP_DEMO_SVC"
SNOWFLAKE_ROLE = "SYSADMIN"
SNOWFLAKE_WAREHOUSE = "HP_DEMO_WH"
SNOWFLAKE_DATABASE = "HP_DEMO"
SNOWFLAKE_SCHEMA = "SPELLS"


def get_connection():
    import os
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    from cryptography.hazmat.backends import default_backend

    passphrase = os.environ.get("PRIVATE_KEY_PASSPHRASE")
    if not passphrase:
        raise EnvironmentError("Set PRIVATE_KEY_PASSPHRASE environment variable first.")

    private_key = load_pem_private_key(
        PRIVATE_KEY_PATH.read_bytes(),
        password=passphrase.encode(),
        backend=default_backend(),
    )

    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=private_key,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def parse_chunks(text: str) -> list[dict]:
    """
    Split the combined text file into chunks by chapter.
    Returns list of dicts with book_num, book_title, chapter_title, chunk_text.
    """
    chunks = []
    current_book_num = None
    current_book_title = None

    # Split on chapter separators (=== lines)
    book_pattern = re.compile(r"#{70}\n# BOOK (\d+): (.+?)\n#{70}", re.MULTILINE)
    chapter_pattern = re.compile(r"={60}\n(.+?)\n={60}", re.MULTILINE)

    # Find all book headers and their positions
    book_matches = list(book_pattern.finditer(text))

    for i, book_match in enumerate(book_matches):
        book_num = int(book_match.group(1))
        book_title = book_match.group(2).strip()

        # Text for this book is between this book header and the next
        book_start = book_match.end()
        book_end = book_matches[i + 1].start() if i + 1 < len(book_matches) else len(text)
        book_text = text[book_start:book_end]

        # Find all chapters within this book
        chapter_matches = list(chapter_pattern.finditer(book_text))
        for j, chap_match in enumerate(chapter_matches):
            chapter_title = chap_match.group(1).strip()
            chap_start = chap_match.end()
            chap_end = chapter_matches[j + 1].start() if j + 1 < len(chapter_matches) else len(book_text)
            chunk_text = book_text[chap_start:chap_end].strip()

            if chunk_text:
                chunks.append({
                    "book_num": book_num,
                    "book_title": book_title,
                    "chapter_title": chapter_title,
                    "chunk_text": chunk_text,
                })

    return chunks


def setup_schema(cur):
    print("Setting up table...")
    cur.execute("""
        CREATE OR REPLACE TABLE HP_DEMO.SPELLS.BOOK_CHUNKS (
            chunk_id      NUMBER AUTOINCREMENT PRIMARY KEY,
            book_num      NUMBER,
            book_title    VARCHAR,
            chapter_title VARCHAR,
            chunk_text    TEXT
        )
    """)


def load_chunks(cur, chunks: list[dict]):
    print(f"Loading {len(chunks)} chunks...")
    cur.executemany(
        """
        INSERT INTO HP_DEMO.SPELLS.BOOK_CHUNKS
            (book_num, book_title, chapter_title, chunk_text)
        VALUES (%s, %s, %s, %s)
        """,
        [(c["book_num"], c["book_title"], c["chapter_title"], c["chunk_text"]) for c in chunks],
    )
    print(f"  Loaded {len(chunks)} rows.")


def create_cortex_search(cur):
    print("Creating Cortex Search Service (this may take a minute)...")
    cur.execute("""
        CREATE OR REPLACE CORTEX SEARCH SERVICE HP_DEMO.SPELLS.BOOK_SEARCH_SVC
          ON chunk_text
          ATTRIBUTES book_num, book_title, chapter_title
          WAREHOUSE = HP_DEMO_WH
          TARGET_LAG = '1 minute'
          AS (
            SELECT
                chunk_id,
                book_num,
                book_title,
                chapter_title,
                chunk_text
            FROM HP_DEMO.SPELLS.BOOK_CHUNKS
          )
    """)
    print("  Cortex Search Service created.")


def main():
    print(f"Reading {INPUT_FILE} ...")
    text = INPUT_FILE.read_text(encoding="utf-8")
    chunks = parse_chunks(text)
    print(f"Parsed {len(chunks)} chapters across the series.")

    # Show book breakdown
    from collections import Counter
    book_counts = Counter(c["book_num"] for c in chunks)
    for book_num in sorted(book_counts):
        title = next(c["book_title"] for c in chunks if c["book_num"] == book_num)
        print(f"  Book {book_num}: {book_counts[book_num]} chapters — {title}")

    conn = get_connection()
    cur = conn.cursor()

    try:
        setup_schema(cur)
        load_chunks(cur, chunks)
        conn.commit()
        create_cortex_search(cur)
        print("\nAll done. Ready to query with the Cortex Agent.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
