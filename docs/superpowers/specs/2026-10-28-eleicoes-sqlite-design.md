# Eleições 2026 BU → SQLite: Design

Date: 2026-10-28

## Goal

Load the TSE Boletim de Urna (BU) 2026 data (55 zips in `bu/`, rounds 1 and 2, all UFs plus ZZ; RR has no round 2) into one SQLite database, `eleicoes2026.db`, for analytical queries and fraud/anomaly auditing.

## Source facts (observed)

- `bu/bweb_{1t|2t}_{UF}_{timestamp}.zip`, 4.9 GB compressed, about 32 GB of CSV uncompressed.
- Each zip holds one CSV and `_leiame-boletimurnaweb.pdf` (data dictionary).
- CSV format: `;` separator, all text fields quoted, latin-1 encoding, CRLF, 45 columns.
- Long format: one row per ballot box (seção) x office (cargo) x votable (candidate, party or blank/null vote). Ballot-box metadata repeats on every row.
- Sentinels: `#NULO#`, empty string (DT_ABERTURA/DT_ENCERRAMENTO in ZZ), and `-1` in NR_PARTIDO, NR_JUNTA_APURADORA and NR_TURMA_APURADORA.
- Checked on AC round 1 (258,052 rows):
  - Urn, timestamp and location columns are constant per seção (turno, UF, município, zona, seção).
  - QT_APTOS, QT_COMPARECIMENTO, QT_ABSTENCOES and DS_CARGO_PERGUNTA_SECAO are constant per seção x cargo. They are not constant per seção, because the federal and state elections report turnout separately.
  - QT_APTOS = QT_COMPARECIMENTO + QT_ABSTENCOES in every case.
  - The sum of QT_VOTOS per seção x cargo equals QT_COMPARECIMENTO for every cargo except Senador (cd 5), where it equals 2 x QT_COMPARECIMENTO (two seats in 2026).
  - Candidate attributes are constant per (CD_ELEICAO, cargo, UF, NR_VOTAVEL).
  - Round 2 uses different CD_ELEICAO values (6258 federal, 6260 state) and CD_PLEITO 3221.

## Decisions (confirmed with user)

- Use: analytical queries and fraud/anomaly auditing. This means a normalized schema, with no columns dropped (the urn technical columns stay).
- Scope: one database with all 55 zips.
- CSVs are streamed from the zips and never extracted to disk. An optional `--extract` flag may be added later.
- Approach A: Python stdlib only (`zipfile`, `csv`, `sqlite3`), single pass, dimension keys held in memory dicts.

## Components (`D:\git\eleicoes2026\eleicoes_db\`)

| File | Responsibility |
|---|---|
| `source.py` | List and parse zip names; stream CSV rows from a zip; clean values (sentinels to NULL, ints, ISO dates, derived `SG_UF_ABRANGENCIA`). Shared by every other module. |
| `analyze.py` | Stream each CSV and compute per-column inferred type, null/sentinel counts, distinct counts, and functional dependencies between columns. Writes `schema_report.md` and `schema_report.json`. |
| `schema.py` | DDL for tables, indexes and views. Derived from the analysis report and confirmed against it. |
| `load.py` | Streaming load: decode, sentinel to NULL, ISO dates, resolve dimension ids, batch insert. |
| `validate.py` | Post-load checks (below). |
| `build_db.py` | CLI with the subcommands `analyze`, `load`, `validate`, `all`. Flags: `--zips DIR`, `--db PATH`. |

## Schema

DB column names are the source column names in lowercase, so every value traces back to the CSV.

| Table | Key | Attributes |
|---|---|---|
| `eleicao` | cd_eleicao (natural) | ano, tipo, pleito, dt_pleito, nr_turno, ds_eleicao |
| `cargo` | cd_cargo_pergunta (natural) | ds_cargo_pergunta, votos_por_eleitor (not from CSV: 2 for Senador, else 1) |
| `municipio` | cd_municipio (natural) | sg_uf, nm_municipio |
| `partido` | nr_partido (natural) | sg_partido, nm_partido |
| `votavel` | id; unique (cd_eleicao, cd_cargo_pergunta, sg_uf_abrangencia, nr_votavel) | tipo votável, nm_votavel, nr_partido |
| `secao` | id; unique (nr_turno, sg_uf, cd_municipio, nr_zona, nr_secao) | local, urn technical columns, open/close/received/issued timestamps, aggregated sections, junta/turma, arquivo_id |
| `apuracao` | id; unique (secao_id, cd_cargo_pergunta) | cd_eleicao, qt_aptos, qt_comparecimento, qt_abstencoes, ds_cargo_pergunta_secao |
| `votos` | (apuracao_id, votavel_id), not enforced unique | qt_votos |
| `arquivo_carga` | id; unique nome | turno, UF, sha256, size, dt/hh_geracao, linhas, soma_votos |
| `conflito` | id | arquivo_id, tabela, chave, coluna, valor_existente, valor_novo |

`sg_uf_abrangencia` is `BR` for Presidente (the same candidate in every UF file) and the row's UF otherwise.

Rules:
- All 45 source columns are stored. A coverage test enforces this.
- Dates are stored as ISO text: `dd/mm/yyyy` becomes `yyyy-mm-dd`, and the existing `yyyy-mm-dd hh:mm:ss` values are kept as is.
- `#NULO#`, `#NE#` and the empty string become NULL in every column. `-1` becomes NULL only in NR_PARTIDO, NR_JUNTA_APURADORA and NR_TURMA_APURADORA.
- A key whose attributes differ between rows keeps its first value. Each differing value is written to `conflito`; it is never silently dropped.
- Views:
  - `v_votos`: a denormalized join for convenience.
  - `v_anomalia_apuracao`: covers comparecimento > aptos, aptos ≠ comparecimento + abstenções, and Σvotos ≠ comparecimento × votos_por_eleitor.

## Loading

- One transaction per zip. A zip already in `arquivo_carga` is skipped, so reruns are idempotent.
- Bulk-load PRAGMAs: `synchronous=OFF`, about 1 GB of cache, `temp_store=MEMORY`. The default rollback journal stays on, so a crashed or failed file rolls back cleanly.
- A file that fails is rolled back and logged, and the remaining files still load. The CLI exits non-zero.
- Indexes and `ANALYZE` run after the load.

## Validation

- Integrity checks fail the validation:
  - For each zip, the row count and Σ QT_VOTOS in the DB match both `arquivo_carga` and a re-stream of the source CSV.
  - Every zip in the folder has been loaded.
- Audit findings are counted and reported but do not fail the validation:
  - the `v_anomalia_apuracao` categories
  - duplicate (apuracao, votavel) rows
  - seções with no opening time
  - urns used in more than one seção in the same round
  - rows in `conflito`
- Nothing is silently fixed.

## Testing

- Fixtures: small synthetic zips written by a test helper in the real format (latin-1, `;`, quoted). They hold 2 seções x (Governador, Senador) with consistent totals, and variants with injected defects.
- Unit tests (pytest): parsing and encoding, sentinel handling, schema coverage, dependency detection, idempotent reload, rollback of a failed file, conflict recording, every validation check.
- Real-data test (`pytest -m real`, not run by default): loads and validates the AC round 1 zip and expects 258,052 vote rows and zero vote-sum anomalies.
- Then one full run on all 55 files.

## Out of scope

- Downloading (done by `baixabu.py`).
- Dashboards and pre-aggregated summary tables.
- Candidate registry (DivulgaCand) joins.

## Risks

- DB size, estimated 8-15 GB. About 20 GB of free disk is needed.
- Run time for 32 GB of CSV, probably tens of minutes to a few hours. Progress is logged per zip.
- The normalization assumptions are verified by `analyze` before the DDL is fixed.
