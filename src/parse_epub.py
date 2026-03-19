"""
parse_epub.py

Extracts plain text from all 7 Harry Potter EPUBs, replaces a subset of
real spell occurrences with fake ones, and outputs a combined text file.

Usage:
    # Process all 7 books with spell replacements
    python parse_epub.py --output hp_series_modified.txt --inject-spells

    # Process a single book
    python parse_epub.py --epub hp1.epub --output hp1_clean.txt

    # Process all without modification (clean baseline)
    python parse_epub.py --output hp_series_clean.txt
"""

import argparse
import re
from pathlib import Path

import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Book definitions
# ---------------------------------------------------------------------------
BOOKS = [
    (1, "hp1.epub", "Harry Potter and the Philosopher's Stone"),
    (2, "hp2.epub", "Harry Potter and the Chamber of Secrets"),
    (3, "hp3.epub", "Harry Potter and the Prisoner of Azkaban"),
    (4, "hp4.epub", "Harry Potter and the Goblet of Fire"),
    (5, "hp5.epub", "Harry Potter and the Order of the Phoenix"),
    (6, "hp6.epub", "Harry Potter and the Half-Blood Prince"),
    (7, "hp7.epub", "Harry Potter and the Deathly Hallows"),
]

# ---------------------------------------------------------------------------
# Spell replacements — swap a real spell with a fake one
# Each entry: (real_spell, fake_spell, max_replacements)
# We replace only a subset of occurrences so the real spell still appears
# elsewhere (making the experiment harder — the model has seen the real one too)
# ---------------------------------------------------------------------------
SPELL_REPLACEMENTS = [
    ("Expelliarmus", "Vinculum Umbrae", 3),
    ("Stupefy",      "Glaciofors",      3),
    ("Accio",        "Pyroclavis",      3),
]


def extract_chapters(epub_path: str) -> list[tuple[str, str]]:
    """Return list of (title, plain_text) tuples, one per chapter."""
    book = epub.read_epub(epub_path)
    chapters = []

    for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
        soup = BeautifulSoup(item.get_content(), "html.parser")

        heading = soup.find(["h1", "h2", "h3"])
        title = heading.get_text(strip=True) if heading else item.get_name()

        paragraphs = []
        for tag in soup.find_all(["p", "div"]):
            text = tag.get_text(separator=" ", strip=True)
            if text:
                paragraphs.append(text)

        body = "\n\n".join(paragraphs)
        if body.strip():
            chapters.append((title, body))

    return chapters


def replace_spells(
    all_chapters: dict[int, list[tuple[str, str]]]
) -> dict[int, list[tuple[str, str]]]:
    """
    Replace a fixed number of real spell occurrences with fake ones across
    the full series. Replacements are spread evenly across books so the fake
    spell appears in multiple narrative contexts.
    """
    # Flatten to a single list of (book_num, chapter_idx, title, text)
    flat = [
        (book_num, idx, title, text)
        for book_num, chapters in sorted(all_chapters.items())
        for idx, (title, text) in enumerate(chapters)
    ]

    # Work on a mutable copy
    modified = {book_num: list(chapters) for book_num, chapters in all_chapters.items()}

    for real_spell, fake_spell, max_replacements in SPELL_REPLACEMENTS:
        # Case-sensitive regex — match the spell as a whole word
        pattern = re.compile(rf'\b{re.escape(real_spell)}\b')

        total_replaced = 0
        replacements_log = []

        # Walk through chapters and replace up to max_replacements occurrences,
        # picking every Nth match so they're spread across the series
        # First pass: count total occurrences
        all_matches = []
        for book_num, idx, title, text in flat:
            for m in pattern.finditer(text):
                all_matches.append((book_num, idx, title))

        if not all_matches:
            print(f"  WARNING: '{real_spell}' not found in text")
            continue

        total = len(all_matches)
        # Pick evenly spaced indices across all occurrences
        step = max(1, total // max_replacements)
        target_indices = set(range(0, min(total, max_replacements * step), step))
        target_indices = set(list(target_indices)[:max_replacements])

        # Second pass: apply replacements at target positions
        occurrence = 0
        for book_num, idx, title, text in flat:
            new_text = text
            offset = 0
            for m in pattern.finditer(text):
                if occurrence in target_indices:
                    # Replace this occurrence in the chapter's current text
                    current_text = modified[book_num][idx][1]
                    # Re-find the match position accounting for prior replacements
                    new_text = pattern.sub(
                        lambda m, f=fake_spell: f,
                        current_text,
                        count=1
                    )
                    modified[book_num][idx] = (modified[book_num][idx][0], new_text)
                    replacements_log.append(f"    Book {book_num}, '{title[:50]}': {real_spell} → {fake_spell}")
                    total_replaced += 1
                occurrence += 1

        print(f"\n  '{real_spell}' → '{fake_spell}' ({total_replaced}/{max_replacements} replacements):")
        for log in replacements_log:
            print(log)

    return modified


def write_output(
    all_chapters: dict[int, list[tuple[str, str]]],
    book_titles: dict[int, str],
    output_path: str,
) -> None:
    out = Path(output_path)
    total_chapters = 0
    with out.open("w", encoding="utf-8") as f:
        for book_num in sorted(all_chapters.keys()):
            title = book_titles[book_num]
            chapters = all_chapters[book_num]
            f.write(f"\n\n{'#'*70}\n# BOOK {book_num}: {title}\n{'#'*70}\n")
            for chapter_title, text in chapters:
                f.write(f"\n\n{'='*60}\n{chapter_title}\n{'='*60}\n\n")
                f.write(text)
            total_chapters += len(chapters)

    size_kb = out.stat().st_size // 1024
    print(f"\nWrote {total_chapters} chapters across {len(all_chapters)} books → {out} ({size_kb} KB)")


def main():
    parser = argparse.ArgumentParser(description="Parse HP EPUBs and optionally replace spells.")
    parser.add_argument("--epub", help="Single EPUB file (overrides multi-book mode)")
    parser.add_argument("--output", required=True, help="Output .txt file path")
    parser.add_argument("--inject-spells", action="store_true", help="Replace real spells with fake ones")
    args = parser.parse_args()

    base_dir = Path(__file__).parent.parent
    output_dir = base_dir / "data" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    books_dir = base_dir / "data" / "books"

    if args.epub:
        epub_path = str(books_dir / args.epub)
        print(f"Reading {epub_path} ...")
        chapters = extract_chapters(epub_path)
        print(f"Found {len(chapters)} content sections.")
        all_chapters = {0: chapters}
        book_titles = {0: args.epub}
    else:
        all_chapters = {}
        book_titles = {}
        for book_num, filename, title in BOOKS:
            epub_path = books_dir / filename
            if not epub_path.exists():
                print(f"  Skipping Book {book_num} ({filename}) — not found")
                continue
            print(f"Reading Book {book_num}: {title} ...")
            chapters = extract_chapters(str(epub_path))
            print(f"  → {len(chapters)} sections")
            all_chapters[book_num] = chapters
            book_titles[book_num] = title

    if args.inject_spells:
        print("\nReplacing spells:")
        all_chapters = replace_spells(all_chapters)
        print("\nReplacement summary:")
        for real, fake, n in SPELL_REPLACEMENTS:
            print(f"  {real} → {fake} (up to {n} occurrences)")

    write_output(all_chapters, book_titles, str(output_dir / args.output))


if __name__ == "__main__":
    main()
