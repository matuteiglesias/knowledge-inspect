from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

PRODUCER_SCHEMA = "speech_chunk_set.s3.v1"
PRODUCER_REPO = "matuteiglesias/politics-wiki"
TOKENIZER_VERSION = "unicode_word_fold.s3.v1"
INDEX_SCHEMA = "knowledge_inspect.speech_lexical_index.s3.v1"


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def jsonl(rows: list[dict]) -> str:
    return "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows)


def fold_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).casefold()


def tokenize(value: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", fold_text(value))


def load_speech_projection(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "schema_name",
        "producer",
        "projection_id",
        "source_release_id",
        "chunk_count",
        "chunk_inventory_sha256",
        "chunking",
        "chunks",
    }
    missing = sorted(required - payload.keys())
    if missing:
        raise ValueError(f"speech projection missing fields: {', '.join(missing)}")
    if payload["schema_name"] != PRODUCER_SCHEMA or payload["producer"] != PRODUCER_REPO:
        raise ValueError("unsupported speech projection producer/schema")
    chunks = payload["chunks"]
    if not isinstance(chunks, list) or len(chunks) != payload["chunk_count"]:
        raise ValueError("speech projection chunk_count mismatch")
    ids: set[str] = set()
    prior: tuple[str, int] | None = None
    required_chunk = {
        "chunk_uid",
        "speech_uid",
        "capture_id",
        "source_text_sha256",
        "chunk_index",
        "text",
        "text_sha256",
        "source_url",
        "source_id",
        "actor_id",
        "title",
        "published_date",
    }
    for idx, chunk in enumerate(chunks):
        absent = sorted(required_chunk - chunk.keys())
        if absent:
            raise ValueError(f"chunk {idx} missing fields: {', '.join(absent)}")
        if chunk["chunk_uid"] in ids:
            raise ValueError(f"duplicate producer chunk_uid {chunk['chunk_uid']}")
        ids.add(chunk["chunk_uid"])
        if sha256_text(chunk["text"]) != chunk["text_sha256"]:
            raise ValueError(f"chunk text hash mismatch for {chunk['chunk_uid']}")
        order_key = (chunk["speech_uid"], chunk["chunk_index"])
        if prior is not None and order_key < prior:
            raise ValueError("producer chunk order is not stable")
        prior = order_key
    if sha256_text(jsonl(chunks)) != payload["chunk_inventory_sha256"]:
        raise ValueError("producer chunk inventory hash mismatch")
    return payload


def _write_once(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != text:
        raise ValueError(f"immutable index artifact collision at {path.name}")
    path.write_text(text, encoding="utf-8")


def build_index(projection_path: Path, index_root: Path) -> dict:
    projection = load_speech_projection(projection_path)
    chunks = projection["chunks"]
    postings: dict[str, list[list[int]]] = defaultdict(list)
    for position, chunk in enumerate(chunks):
        counts = Counter(tokenize(chunk["text"]))
        for term, frequency in sorted(counts.items()):
            postings[term].append([position, frequency])
    chunks_text = jsonl(chunks)
    postings_text = json.dumps(postings, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    index_identity = {
        "projection_id": projection["projection_id"],
        "source_release_id": projection["source_release_id"],
        "chunk_inventory_sha256": projection["chunk_inventory_sha256"],
        "tokenizer_version": TOKENIZER_VERSION,
    }
    index_id = "speech-lexical-index:" + sha256_text(canonical_json(index_identity))[:24]
    index_dir = index_root / sha256_text(index_id)
    manifest = {
        "schema_name": INDEX_SCHEMA,
        "index_id": index_id,
        "producer": PRODUCER_REPO,
        "projection_id": projection["projection_id"],
        "source_release_id": projection["source_release_id"],
        "producer_chunk_inventory_sha256": projection["chunk_inventory_sha256"],
        "chunk_count": len(chunks),
        "tokenizer": {"version": TOKENIZER_VERSION},
        "chunks_sha256": sha256_text(chunks_text),
        "postings_sha256": sha256_text(postings_text),
    }
    _write_once(index_dir / "chunks.jsonl", chunks_text)
    _write_once(index_dir / "postings.json", postings_text)
    _write_once(
        index_dir / "index.json",
        json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
    )
    return {**manifest, "index_dir": index_dir}


def load_index(index_dir: Path) -> tuple[dict, list[dict], dict[str, list[list[int]]]]:
    manifest = json.loads((index_dir / "index.json").read_text(encoding="utf-8"))
    if manifest.get("schema_name") != INDEX_SCHEMA:
        raise ValueError("unsupported speech index schema")
    chunks_text = (index_dir / "chunks.jsonl").read_text(encoding="utf-8")
    postings_text = (index_dir / "postings.json").read_text(encoding="utf-8")
    if sha256_text(chunks_text) != manifest["chunks_sha256"]:
        raise ValueError("indexed chunk inventory hash mismatch")
    if sha256_text(postings_text) != manifest["postings_sha256"]:
        raise ValueError("lexical postings hash mismatch")
    chunks = [json.loads(line) for line in chunks_text.splitlines() if line.strip()]
    postings = json.loads(postings_text)
    return manifest, chunks, postings


def query_index(index_dir: Path, query: str, top_k: int = 5) -> dict:
    if top_k < 1 or top_k > 100:
        raise ValueError("top_k must be between 1 and 100")
    terms = list(dict.fromkeys(tokenize(query)))
    if not terms:
        raise ValueError("query must contain at least one searchable token")
    manifest, chunks, postings = load_index(index_dir)
    scores: dict[int, int] = defaultdict(int)
    matched: dict[int, set[str]] = defaultdict(set)
    for term in terms:
        for position, frequency in postings.get(term, []):
            scores[position] += int(frequency)
            matched[position].add(term)
    ranked = sorted(scores, key=lambda pos: (-scores[pos], chunks[pos]["chunk_uid"]))[:top_k]
    results = []
    for position in ranked:
        chunk = chunks[position]
        results.append(
            {
                "score": scores[position],
                "matched_terms": sorted(matched[position]),
                "excerpt": chunk["text"][:240].strip(),
                "evidence": {
                    "source_release_id": manifest["source_release_id"],
                    "projection_id": manifest["projection_id"],
                    "speech_uid": chunk["speech_uid"],
                    "capture_id": chunk["capture_id"],
                    "chunk_uid": chunk["chunk_uid"],
                    "chunk_index": chunk["chunk_index"],
                    "source_text_sha256": chunk["source_text_sha256"],
                    "source_url": chunk["source_url"],
                    "source_id": chunk["source_id"],
                    "actor_id": chunk["actor_id"],
                    "title": chunk["title"],
                    "published_date": chunk["published_date"],
                },
            }
        )
    return {
        "schema_name": "knowledge_inspect.speech_query_results.s3.v1",
        "index_id": manifest["index_id"],
        "query": query,
        "query_terms": terms,
        "top_k": top_k,
        "result_count": len(results),
        "results": results,
    }
