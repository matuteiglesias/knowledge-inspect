# S3 — speech chunk consumer proof

Status: **PROVEN against one exact `politics-wiki` producer artifact**

This proof establishes the smallest interface needed for the first Speech Atlas retrieval consumer. It does not make Knowledge Inspect a speech-source authority and does not promote the producer-local S3 projection into a shared ecosystem contract.

## Exact producer evidence

Producer repository: `matuteiglesias/politics-wiki`

Producer PR: `#10`

Producer head used for the proof:

```text
9df6af0f91145b70b0aa5257ed4bbe005193ceae
```

GitHub Actions run:

```text
33340133664
```

Producer evidence artifact:

```text
artifact ID   9740295058
name          speech-s3-producer-evidence
ZIP SHA-256   a19ae99e12fcd90545ddb787a7d3fcf42ca00a9d9650d740b8d292b5637775c0
```

Exact live corpus and projection:

```text
source release          speech-release:ed23fb80b11f342fcc72b65f
speeches                20
captures                 20
projection              speech-chunks:47e79e2e782cef5ed53ea4a4
chunks                  415
chunk inventory SHA-256 0ccc3d797eb884667c4661d95de0711b51160aa9fe0292e37115931a2ed74f47
projection file SHA-256 8fab7a940f3ca71c238525a9c1ddcd2aa22deeaf1b0583e879575ad870f63a05
```

The producer's release job acquired 10 Casa Rosada / CFK speeches and 10 Provincia de Buenos Aires / Kicillof speeches, materialized the corpus, replayed it with zero new captures, then projected that exact immutable release with:

```text
algorithm      word_windows
version        speech_word_windows.s3.v1
max_words      180
overlap_words   30
```

## Exact consumer

Consumer repository: `matuteiglesias/knowledge-inspect`

Consumer PR: `#28`

Consumer head used for the real cross-repo proof:

```text
3c287e0de5f2634deab5e7fee4159af731414642
```

The exact `kb/speech_inspect.py` blob at that head was used. Its offline CI is also green in workflow `33340255504`; existing W3 and W4 regression workflows remained green on the same head.

The exact real producer artifact built the deterministic derived index:

```text
speech-lexical-index:aa464c1f0015105b46e73c11
```

The source projection SHA-256 was identical before and after indexing/querying, so the inspection proof did not mutate producer evidence.

## Acceptance queries

No expected political interpretation was encoded. Query semantics are exact lexical token matching after Unicode accent/case folding, ranked only by summed term frequency with deterministic `chunk_uid` tie breaking.

Each query below returned a bounded top-5 result set. The table records the top evidence reference only; the proof checked all 25 returned hits against the producer release.

| Query | Matching chunks in index | Top speech | Top capture | Top chunk |
| --- | ---: | --- | --- | --- |
| `industria` | 39 | `speech:casa-rosada-cfk:dc8fd07dcf268d6438192f33` | `capture:8e1c1a8d3e255f1c7ae1aead84824916` | `speech-chunk:33b9d601ae6a670d11be303ee7ce3551` |
| `inflación` | 6 | `speech:casa-rosada-cfk:4beaf4f8a6a99ff72a877133` | `capture:e9ec21706ea71206706957deba3fd14a` | `speech-chunk:7424a9c860017e85e31c5e8d38bdf58b` |
| `producción` | 42 | `speech:gba-kicillof:a49b97f630a050d7fc773905` | `capture:25ee5a9b1b352f7bf01381f8943d5d30` | `speech-chunk:6c28502c14ca2c4b73c51e7d0b69c9c4` |
| `deuda` | 11 | `speech:gba-kicillof:69312675f45f94fbc3f5d9e3` | `capture:a1c589c95811799bd4e5a659ebd258d1` | `speech-chunk:29f4dc440cededaca4672ed4d4114842` |
| `educación` | 41 | `speech:gba-kicillof:8a0b68078b08939df991889c` | `capture:bf3c40650081c54b9c2300cebcf0aff1` | `speech-chunk:4b2d9716efb678d1fd9df7ffc25649ae` |

Independent resolution over all 25 returned hits proved:

```text
chunk_uid
  -> exact producer chunk
  -> speech_uid
  -> exact released speech record
  -> capture_id == released record latest_capture_id
  -> exact immutable capture
  -> identical source_text_sha256
  -> publisher source_url
```

The 25 hits covered 9 distinct speeches and 23 distinct chunks. No hit required fuzzy source resolution.

## Smallest sufficient producer interface

Knowledge Inspect needed only these projection-level fields:

- producer/schema identity;
- `projection_id`;
- `source_release_id`;
- `chunk_count`;
- `chunk_inventory_sha256`;
- explicit chunking metadata;
- ordered chunk inventory.

For each chunk the consumer actually needed:

- `chunk_uid`;
- `speech_uid`;
- `capture_id`;
- `source_text_sha256`;
- `chunk_index`;
- `text` + `text_sha256`;
- `source_url`;
- source/actor/title/date display metadata.

Knowledge Inspect does **not** import or copy `speech_record.v1`, `speech_capture.v1`, or publisher acquisition semantics. Those remain producer authority.

## Smallest sufficient consumer semantics

The Speech Atlas use case pulled exactly two query inputs:

```text
query: string
top_k: integer 1..100
```

The proven consumer behavior is:

1. tokenize with `unicode_word_fold.s3.v1`;
2. exact token lookup in a deterministic inverted index;
3. sum term frequencies;
4. rank descending score, then producer `chunk_uid` for deterministic ties;
5. return a bounded excerpt plus producer evidence references.

This was sufficient for all five acceptance concepts. S3 therefore provides no evidence-based reason to add embeddings, semantic similarity, filters, cutoffs, rerankers, or a universal RAG API.

## Commands

After obtaining one exact producer projection:

```bash
make speech-index \
  SPEECH_CHUNK_SET=/path/to/speech_chunk_set.json \
  SPEECH_INDEX_ROOT=artifacts/speech_indexes
```

Then query the exact generated index directory:

```bash
make speech-query \
  SPEECH_INDEX_DIR=/path/to/index \
  SPEECH_QUERY='educación' \
  SPEECH_TOP_K=5
```

Offline adapter/index/query invariants:

```bash
make verify-speech-consumer
```

## Boundary after S3

Proven:

```text
politics-wiki exact speech release
  -> producer-owned speech chunk projection
  -> bounded Knowledge Inspect adapter
  -> deterministic derived lexical index
  -> query + top_k
  -> evidence-bearing result
  -> exact chunk / capture / speech / publisher URL
```

Not proven and therefore not claimed:

- universal retrieval semantics;
- semantic/vector retrieval necessity;
- reranking/filter/cutoff policy;
- ideology or political interpretation;
- shared cross-repository authority over speech schemas;
- promotion of `speech_chunk_set.s3.v1` to a shared ecosystem contract.
