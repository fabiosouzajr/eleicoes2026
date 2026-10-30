# Eleições 2026 BU → SQLite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stream the 55 TSE Boletim de Urna zips in `bu/` into one normalized SQLite database (`eleicoes2026.db`), with a structure-analysis report and a validation pass for load integrity and audit anomalies.

**Architecture:**
- `eleicoes_db/source.py` reads CSVs straight out of the zips and cleans values.
- `eleicoes_db/schema.py` declares each table as a `Dim` (key + attributes) and generates the DDL. The same declarations drive the analyzer (which checks that the attributes really are constant per key) and the loader (which resolves ids with in-memory caches).
- `load.py` loads one transaction per zip and skips zips already loaded.
- `validate.py` re-reads the CSVs to compare totals, and counts audit anomalies.
- `build_db.py` is the CLI.

**Tech Stack:** Python 3.11 standard library only (`zipfile`, `csv`, `sqlite3`, `argparse`, `hashlib`); SQLite 3.45; pytest 9 for tests.

**Spec:** `docs/superpowers/specs/2026-10-28-eleicoes-sqlite-design.md`

## Global Constraints

- Python 3.11, standard library only at runtime. pytest is used for tests only.
- Never extract the zips to disk. Read with `zipfile.ZipFile.open` and wrap the stream in `io.TextIOWrapper(encoding="latin-1", newline="")`.
- CSV: delimiter `;`, 45 columns, header row first.
- Sentinels → NULL: `""`, `#NULO#`, `#NE#` in every column; `-1` only in `NR_PARTIDO`, `NR_JUNTA_APURADORA`, `NR_TURMA_APURADORA`.
- DB column names = source column names in lowercase. Surrogate key column = `id`.
- All 45 source columns are stored somewhere (enforced by a test).
- `sg_uf_abrangencia` = `"BR"` for `CD_CARGO_PERGUNTA = 1` (Presidente), otherwise the row's `SG_UF`.
- `votos_por_eleitor` = 2 for `cd_cargo_pergunta = 5` (Senador), 1 otherwise.
- One transaction per zip. A failed zip is rolled back and the other zips still load.
- When an attribute conflicts, the first value is kept and every differing value is recorded in `conflito`. Data is never silently fixed.
- User-facing messages, docstrings and comments are in Portuguese, matching `baixabu.py`. Identifiers are in English.
- Defaults: zips in `bu/`, DB `eleicoes2026.db`, reports in the project root.

## File Structure

| Path | Responsibility |
|---|---|
| `.gitignore` | Ignore `bu/`, DB files, reports, caches |
| `pytest.ini` | Test paths, `pythonpath = .`, `real` marker excluded by default |
| `eleicoes_db/__init__.py` | Package marker |
| `eleicoes_db/source.py` | Zip discovery, raw CSV streaming, value cleaning |
| `eleicoes_db/schema.py` | `Dim` declarations, DDL, views, indexes, `finalize` |
| `eleicoes_db/analyze.py` | Column stats + dependency checks → `schema_report.md/.json` |
| `eleicoes_db/load.py` | `connect`, `Loader`, `load_all` |
| `eleicoes_db/validate.py` | `validate`, `ValidationReport`, `format_report` |
| `build_db.py` | CLI: `analyze`, `load`, `validate`, `all` |
| `tests/helpers.py` | `BASE` row, `sample_rows()`, `write_zip()` fixture builders |
| `tests/test_source.py`, `test_schema.py`, `test_analyze.py`, `test_load.py`, `test_validate.py`, `test_build_db.py` | Tests per module |

---

### Task 1: Project setup + `source.py`

**Files:**
- Create: `.gitignore`, `pytest.ini`, `eleicoes_db/__init__.py`, `eleicoes_db/source.py`, `tests/helpers.py`, `tests/test_source.py`

**Interfaces:**
- Produces:
  - `COLUMNS: tuple[str, ...]` (45 names, in file order)
  - `INT_COLUMNS: frozenset[str]`, `NULL_SENTINELS: frozenset[str]`, `NEG1_NULL_COLUMNS: frozenset[str]`, `NATIONAL_CARGOS: frozenset[int]`
  - `ZipSource(path: Path, turno: int, uf: str, timestamp: str)` with a `.name` property
  - `parse_zip_name(path: Path) -> ZipSource` (raises `ValueError`)
  - `list_zips(directory: Path) -> list[ZipSource]` (sorted by file name)
  - `iter_raw(zip_path: Path) -> Iterator[list[str]]` (the first item is the header)
  - `clean_value(column: str, value: str) -> int | str | None`
  - `clean_row(header: Sequence[str], values: Sequence[str]) -> dict[str, object]` (adds `SG_UF_ABRANGENCIA`)
  - `iter_rows(zip_path: Path) -> Iterator[dict[str, object]]` (raises `ValueError` on missing columns or a bad field count)
  - `tests/helpers.py`: `BASE: dict[str, str]`, `sample_rows() -> list[dict[str, str]]`, `write_zip(path: Path, rows: list[dict], header: Sequence[str] = COLUMNS) -> Path`, `am(rows) -> list[dict]` (moves rows to UF AM)

- [ ] **Step 1: Initialize the repo and config files**

The project is not a git repo yet. Run in `D:\git\eleicoes2026`:

```bash
git init
```

Create `.gitignore`:

```gitignore
bu/
*.db
*.db-journal
schema_report.md
schema_report.json
__pycache__/
.pytest_cache/
```

Create `pytest.ini`:

```ini
[pytest]
testpaths = tests
pythonpath = .
addopts = -m "not real"
markers =
    real: usa os zips reais em bu/ (lento)
```

Create `eleicoes_db/__init__.py`:

```python
"""Carga dos Boletins de Urna (BU) 2026 do TSE em SQLite."""
```

- [ ] **Step 2: Write test helpers**

Create `tests/helpers.py`:

```python
"""Monta zips sintéticos no mesmo formato dos arquivos de BU do TSE."""

import csv
import io
import zipfile
from pathlib import Path

from eleicoes_db.source import COLUMNS

BASE = {
    "DT_GERACAO": "05/10/2026", "HH_GERACAO": "14:03:43", "ANO_ELEICAO": "2026",
    "CD_TIPO_ELEICAO": "0", "NM_TIPO_ELEICAO": "Eleição Ordinária", "CD_PLEITO": "3220",
    "DT_PLEITO": "2026-10-04 00:00:00", "NR_TURNO": "1", "CD_ELEICAO": "6259",
    "DS_ELEICAO": "Eleições Gerais Estaduais 2026", "SG_UF": "AC", "CD_MUNICIPIO": "1074",
    "NM_MUNICIPIO": "CRUZEIRO DO SUL", "NR_ZONA": "4", "NR_SECAO": "175",
    "NR_LOCAL_VOTACAO": "2143", "CD_CARGO_PERGUNTA": "3", "DS_CARGO_PERGUNTA": "Governador",
    "NR_PARTIDO": "-1", "SG_PARTIDO": "#NULO#", "NM_PARTIDO": "#NULO#",
    "DT_BU_RECEBIDO": "2026-10-04 18:25:49", "QT_APTOS": "100", "QT_COMPARECIMENTO": "80",
    "QT_ABSTENCOES": "20", "CD_TIPO_URNA": "1", "DS_TIPO_URNA": "Apurada",
    "CD_TIPO_VOTAVEL": "2", "DS_TIPO_VOTAVEL": "Branco", "NR_VOTAVEL": "95",
    "NM_VOTAVEL": "Branco", "QT_VOTOS": "0", "NR_URNA_EFETIVADA": "2280689",
    "CD_CARGA_1_URNA_EFETIVADA": "426.486.557.318.259.284.",
    "CD_CARGA_2_URNA_EFETIVADA": "790.494", "CD_FLASHCARD_URNA_EFETIVADA": "375A44ED",
    "DT_CARGA_URNA_EFETIVADA": "2026-09-24 17:33:00", "DS_CARGO_PERGUNTA_SECAO": "3 - 175",
    "DS_SECOES_AGREGADAS": "#NULO#", "DT_ABERTURA": "2026-10-04 06:00:01",
    "DT_ENCERRAMENTO": "2026-10-04 15:01:05", "QT_ELEI_BIOM_SEM_HABILITACAO": "10",
    "DT_EMISSAO_BU": "2026-10-04 15:03:56", "NR_JUNTA_APURADORA": "-1",
    "NR_TURMA_APURADORA": "-1",
}

PSD = ("55", "PSD", "PARTIDO SOCIAL DEMOCRÁTICO")
PT = ("13", "PT", "PARTIDO DOS TRABALHADORES")
_TIPOS = {1: "Nominal", 2: "Branco", 3: "Nulo", 4: "Legenda"}


def _votavel(tipo, nr, nome, partido=None):
    d = {"CD_TIPO_VOTAVEL": str(tipo), "DS_TIPO_VOTAVEL": _TIPOS[tipo],
         "NR_VOTAVEL": str(nr), "NM_VOTAVEL": nome}
    if partido:
        d.update(NR_PARTIDO=partido[0], SG_PARTIDO=partido[1], NM_PARTIDO=partido[2])
    return d


def sample_rows():
    """2 seções x (Governador, Senador): 14 linhas, 480 votos, somas consistentes.

    Governador soma 80 = comparecimento; Senador soma 160 = 2 x comparecimento.
    Ordem por seção: 4 linhas de governador e depois 3 de senador.
    """
    rows = []
    for secao, urna in (("175", "2280689"), ("23", "2266408")):
        s = {"NR_SECAO": secao, "NR_URNA_EFETIVADA": urna}
        gov = {"CD_CARGO_PERGUNTA": "3", "DS_CARGO_PERGUNTA": "Governador",
               "DS_CARGO_PERGUNTA_SECAO": f"3 - {secao}"}
        sen = {"CD_CARGO_PERGUNTA": "5", "DS_CARGO_PERGUNTA": "Senador",
               "DS_CARGO_PERGUNTA_SECAO": f"5 - {secao}"}
        for v, q in ((_votavel(1, 55, "ELTER NÓBREGA", PSD), 50),
                     (_votavel(4, 13, "PT", PT), 20),
                     (_votavel(2, 95, "Branco"), 6),
                     (_votavel(3, 96, "Nulo"), 4)):
            rows.append({**s, **gov, **v, "QT_VOTOS": str(q)})
        for v, q in ((_votavel(1, 555, "FULANO DE TAL", PSD), 100),
                     (_votavel(1, 131, "BELTRANA", PT), 50),
                     (_votavel(2, 95, "Branco"), 10)):
            rows.append({**s, **sen, **v, "QT_VOTOS": str(q)})
    return rows


def am(rows):
    """Mesmas linhas, movidas para a UF AM (outro município)."""
    return [{**r, "SG_UF": "AM", "CD_MUNICIPIO": "2000", "NM_MUNICIPIO": "MANAUS"} for r in rows]


def write_zip(path: Path, rows, header=COLUMNS) -> Path:
    """Grava um zip com um CSV latin-1 ';' (campos entre aspas) e um PDF de leiame."""
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    w.writerow(header)
    for r in rows:
        full = {**BASE, **r}
        w.writerow([full.get(c, "") for c in header])
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(path.stem + ".csv", buf.getvalue().encode("latin-1"))
        zf.writestr("_leiame-boletimurnaweb.pdf", b"%PDF-1.4")
    return path
```

- [ ] **Step 3: Write the failing tests**

Create `tests/test_source.py`:

```python
from pathlib import Path

import pytest

from eleicoes_db.source import (
    COLUMNS, clean_row, clean_value, iter_rows, list_zips, parse_zip_name,
)
from helpers import BASE, sample_rows, write_zip


def test_has_45_unique_columns():
    assert len(COLUMNS) == 45
    assert len(set(COLUMNS)) == 45


def test_sentinels_become_none():
    assert clean_value("SG_PARTIDO", "#NULO#") is None
    assert clean_value("NM_VOTAVEL", "#NE#") is None
    assert clean_value("DT_ABERTURA", "") is None


def test_minus_one_is_null_only_in_listed_columns():
    assert clean_value("NR_PARTIDO", "-1") is None
    assert clean_value("NR_JUNTA_APURADORA", "-1") is None
    assert clean_value("NR_TURMA_APURADORA", "-1") is None
    assert clean_value("QT_VOTOS", "-1") == -1


def test_int_and_text_columns():
    assert clean_value("QT_VOTOS", "6") == 6
    assert clean_value("QT_ELEI_BIOM_SEM_HABILITACAO", "10") == 10
    assert clean_value("CD_CARGA_2_URNA_EFETIVADA", "790.494") == "790.494"
    assert clean_value("CD_FLASHCARD_URNA_EFETIVADA", "375A44ED") == "375A44ED"
    # inteiro inválido é mantido como veio; o analyze aponta a coluna
    assert clean_value("NR_SECAO", "abc") == "abc"


def test_dates_to_iso():
    assert clean_value("DT_GERACAO", "05/10/2026") == "2026-10-05"
    assert clean_value("DT_ABERTURA", "2026-10-04 06:00:01") == "2026-10-04 06:00:01"
    assert clean_value("HH_GERACAO", "14:03:43") == "14:03:43"


def test_abrangencia_is_br_for_president_only():
    header = list(COLUMNS)
    pres = {**BASE, "CD_CARGO_PERGUNTA": "1"}
    gov = {**BASE, "CD_CARGO_PERGUNTA": "3"}
    assert clean_row(header, [pres[c] for c in header])["SG_UF_ABRANGENCIA"] == "BR"
    assert clean_row(header, [gov[c] for c in header])["SG_UF_ABRANGENCIA"] == "AC"


def test_wrong_field_count_raises():
    with pytest.raises(ValueError, match="campos"):
        clean_row(list(COLUMNS), ["x"])


def test_iter_rows_reads_latin1_zip(tmp_path):
    z = write_zip(tmp_path / "bweb_1t_AC_051020261403.zip", sample_rows())
    rows = list(iter_rows(z))
    assert len(rows) == 14
    assert rows[0]["NM_VOTAVEL"] == "ELTER NÓBREGA"
    assert rows[0]["DS_ELEICAO"] == "Eleições Gerais Estaduais 2026"
    assert sum(r["QT_VOTOS"] for r in rows) == 480


def test_iter_rows_rejects_missing_columns(tmp_path):
    z = write_zip(tmp_path / "bweb_1t_AC_051020261403.zip", sample_rows(), header=COLUMNS[:-1])
    with pytest.raises(ValueError, match="NR_TURMA_APURADORA"):
        list(iter_rows(z))


def test_parse_and_list_zips(tmp_path):
    write_zip(tmp_path / "bweb_2t_RS_261020260742.zip", sample_rows())
    write_zip(tmp_path / "bweb_1t_AC_051020261403.zip", sample_rows())
    srcs = list_zips(tmp_path)
    assert [(s.turno, s.uf, s.timestamp) for s in srcs] == [
        (1, "AC", "051020261403"), (2, "RS", "261020260742")]
    assert srcs[0].name == "bweb_1t_AC_051020261403.zip"
    with pytest.raises(ValueError):
        parse_zip_name(Path("outro.zip"))
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `python -m pytest tests/test_source.py -v`
Expected: collection ERROR, `ModuleNotFoundError: No module named 'eleicoes_db.source'`

- [ ] **Step 5: Implement `source.py`**

Create `eleicoes_db/source.py`:

```python
"""Leitura dos zips de Boletim de Urna (BU) do TSE, sem extrair para o disco."""

from __future__ import annotations

import csv
import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

ENCODING = "latin-1"
DELIMITER = ";"

COLUMNS = (
    "DT_GERACAO", "HH_GERACAO", "ANO_ELEICAO", "CD_TIPO_ELEICAO", "NM_TIPO_ELEICAO",
    "CD_PLEITO", "DT_PLEITO", "NR_TURNO", "CD_ELEICAO", "DS_ELEICAO", "SG_UF",
    "CD_MUNICIPIO", "NM_MUNICIPIO", "NR_ZONA", "NR_SECAO", "NR_LOCAL_VOTACAO",
    "CD_CARGO_PERGUNTA", "DS_CARGO_PERGUNTA", "NR_PARTIDO", "SG_PARTIDO", "NM_PARTIDO",
    "DT_BU_RECEBIDO", "QT_APTOS", "QT_COMPARECIMENTO", "QT_ABSTENCOES", "CD_TIPO_URNA",
    "DS_TIPO_URNA", "CD_TIPO_VOTAVEL", "DS_TIPO_VOTAVEL", "NR_VOTAVEL", "NM_VOTAVEL",
    "QT_VOTOS", "NR_URNA_EFETIVADA", "CD_CARGA_1_URNA_EFETIVADA",
    "CD_CARGA_2_URNA_EFETIVADA", "CD_FLASHCARD_URNA_EFETIVADA",
    "DT_CARGA_URNA_EFETIVADA", "DS_CARGO_PERGUNTA_SECAO", "DS_SECOES_AGREGADAS",
    "DT_ABERTURA", "DT_ENCERRAMENTO", "QT_ELEI_BIOM_SEM_HABILITACAO", "DT_EMISSAO_BU",
    "NR_JUNTA_APURADORA", "NR_TURMA_APURADORA",
)

INT_COLUMNS = frozenset({
    "ANO_ELEICAO", "CD_TIPO_ELEICAO", "CD_PLEITO", "NR_TURNO", "CD_ELEICAO",
    "CD_MUNICIPIO", "NR_ZONA", "NR_SECAO", "NR_LOCAL_VOTACAO", "CD_CARGO_PERGUNTA",
    "NR_PARTIDO", "QT_APTOS", "QT_COMPARECIMENTO", "QT_ABSTENCOES", "CD_TIPO_URNA",
    "CD_TIPO_VOTAVEL", "NR_VOTAVEL", "QT_VOTOS", "NR_URNA_EFETIVADA",
    "QT_ELEI_BIOM_SEM_HABILITACAO", "NR_JUNTA_APURADORA", "NR_TURMA_APURADORA",
})

# Valores que o TSE usa para "sem informação" em qualquer coluna.
NULL_SENTINELS = frozenset({"", "#NULO#", "#NE#"})
# "-1" só significa "sem informação" nestas colunas; nas demais pode ser dado real.
NEG1_NULL_COLUMNS = frozenset({"NR_PARTIDO", "NR_JUNTA_APURADORA", "NR_TURMA_APURADORA"})
# Cargos votados no país inteiro: o mesmo candidato aparece em todos os arquivos de UF.
NATIONAL_CARGOS = frozenset({1})  # Presidente

_DDMMYYYY = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")
_ZIP_NAME = re.compile(r"^bweb_(\d)t_([A-Z]{2})_(\d{12})\.zip$")


@dataclass(frozen=True)
class ZipSource:
    path: Path
    turno: int
    uf: str
    timestamp: str

    @property
    def name(self) -> str:
        return self.path.name


def parse_zip_name(path: Path) -> ZipSource:
    m = _ZIP_NAME.match(path.name)
    if not m:
        raise ValueError(f"nome de arquivo inesperado: {path.name}")
    return ZipSource(path, int(m.group(1)), m.group(2), m.group(3))


def list_zips(directory: Path) -> list[ZipSource]:
    return [parse_zip_name(p) for p in sorted(Path(directory).glob("bweb_*.zip"))]


def iter_raw(zip_path: Path) -> Iterator[list[str]]:
    """Gera as linhas do CSV como listas de strings; a primeira é o cabeçalho."""
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(csv_names) != 1:
            raise ValueError(f"{Path(zip_path).name}: esperado 1 CSV, encontrado {csv_names}")
        with zf.open(csv_names[0]) as raw:
            text = io.TextIOWrapper(raw, encoding=ENCODING, newline="")
            yield from csv.reader(text, delimiter=DELIMITER)


def clean_value(column: str, value: str):
    if value in NULL_SENTINELS or (value == "-1" and column in NEG1_NULL_COLUMNS):
        return None
    if column in INT_COLUMNS:
        try:
            return int(value)
        except ValueError:
            return value  # mantido como veio; o analyze aponta a coluna
    m = _DDMMYYYY.match(value)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return value


def clean_row(header: Sequence[str], values: Sequence[str]) -> dict:
    if len(values) != len(header):
        raise ValueError(f"linha com {len(values)} campos, esperado {len(header)}")
    row = {c: clean_value(c, v) for c, v in zip(header, values)}
    row["SG_UF_ABRANGENCIA"] = (
        "BR" if row["CD_CARGO_PERGUNTA"] in NATIONAL_CARGOS else row["SG_UF"])
    return row


def iter_rows(zip_path: Path) -> Iterator[dict]:
    """Gera as linhas já limpas (sentinelas -> None, inteiros, datas ISO)."""
    raw = iter_raw(zip_path)
    header = next(raw)
    missing = set(COLUMNS) - set(header)
    if missing:
        raise ValueError(f"{Path(zip_path).name}: colunas ausentes {sorted(missing)}")
    for line_no, values in enumerate(raw, start=2):
        try:
            yield clean_row(header, values)
        except ValueError as e:
            raise ValueError(f"{Path(zip_path).name}: linha {line_no}: {e}") from e
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `python -m pytest tests/test_source.py -v`
Expected: 10 passed

- [ ] **Step 7: Commit**

```bash
git add .gitignore pytest.ini eleicoes_db/__init__.py eleicoes_db/source.py tests/helpers.py tests/test_source.py docs/
git commit -m "feat: leitura em streaming dos zips de BU com limpeza de valores

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `schema.py`, the declarative model and DDL

**Files:**
- Create: `eleicoes_db/schema.py`, `tests/test_schema.py`

**Interfaces:**
- Consumes: `INT_COLUMNS` from `source.py`
- Produces:
  - `Dim(table: str, key: tuple[str, ...], attrs: tuple[str, ...], id_col: str | None = None, extra_ddl: tuple[str, ...] = ())` with a `.surrogate -> bool` property
  - Constants `ELEICAO, CARGO, MUNICIPIO, PARTIDO, VOTAVEL, SECAO, APURACAO, ARQUIVO`
  - `DIMS` (resolution order), `GLOBAL_DIMS`, `FILE_DIMS`, `FACT_COLUMNS`, `FILE_COLUMNS`, `DERIVED_COLUMNS`, `VOTOS_POR_ELEITOR: dict[int, int]`
  - `create_schema(conn) -> None` (idempotent: tables + views)
  - `finalize(conn) -> None` (indexes, `votos_por_eleitor`, `ANALYZE`)
  - Row dict keys produced for later tasks: `SECAO_ID`, `APURACAO_ID`, `VOTAVEL_ID` (surrogate ids), `ARQUIVO_ID`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_schema.py`:

```python
import sqlite3

from eleicoes_db.schema import (
    ARQUIVO, DERIVED_COLUMNS, DIMS, FACT_COLUMNS, create_schema, finalize,
)
from eleicoes_db.source import COLUMNS


def _mapped():
    cols = set(FACT_COLUMNS)
    for d in DIMS + (ARQUIVO,):
        cols |= set(d.key) | set(d.attrs)
    return cols


def _names(conn, kind):
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = ?", (kind,))}


def test_every_source_column_is_stored():
    assert set(COLUMNS) - _mapped() == set()


def test_no_unknown_columns_referenced():
    assert _mapped() - set(COLUMNS) - DERIVED_COLUMNS == set()


def test_create_schema_tables_and_types():
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    assert {"eleicao", "cargo", "municipio", "partido", "votavel", "secao", "apuracao",
            "votos", "arquivo_carga", "conflito"} <= _names(conn, "table")
    secao = {r[1]: r[2] for r in conn.execute("PRAGMA table_info(secao)")}
    assert secao["id"] == "INTEGER"
    assert secao["nr_urna_efetivada"] == "INTEGER"
    assert secao["cd_flashcard_urna_efetivada"] == "TEXT"
    assert secao["arquivo_id"] == "INTEGER"
    apuracao = {r[1] for r in conn.execute("PRAGMA table_info(apuracao)")}
    assert {"secao_id", "cd_cargo_pergunta", "qt_aptos", "qt_comparecimento"} <= apuracao
    cargo = {r[1] for r in conn.execute("PRAGMA table_info(cargo)")}
    assert "votos_por_eleitor" in cargo


def test_create_schema_is_idempotent_and_has_views():
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    create_schema(conn)
    assert {"v_votos", "v_anomalia_apuracao"} <= _names(conn, "view")


def test_finalize_sets_votos_por_eleitor_and_indexes():
    conn = sqlite3.connect(":memory:", isolation_level=None)
    create_schema(conn)
    conn.execute("INSERT INTO cargo (cd_cargo_pergunta, ds_cargo_pergunta) "
                 "VALUES (5, 'Senador'), (3, 'Governador')")
    finalize(conn)
    assert dict(conn.execute("SELECT cd_cargo_pergunta, votos_por_eleitor FROM cargo")) == {5: 2, 3: 1}
    assert {"ix_votos_apuracao", "ix_votos_votavel", "ix_secao_urna"} <= _names(conn, "index")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_schema.py -v`
Expected: collection ERROR, `ModuleNotFoundError: No module named 'eleicoes_db.schema'`

- [ ] **Step 3: Implement `schema.py`**

Create `eleicoes_db/schema.py`:

```python
"""Modelo normalizado do BU. Toda coluna do CSV é guardada em pelo menos uma tabela.

Cada tabela de dimensão é um `Dim`: `key` identifica a linha e `attrs` devem ser
constantes para a mesma chave. O analyze confere essa premissa nos dados e o load
usa as mesmas declarações para resolver os ids.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .source import INT_COLUMNS


@dataclass(frozen=True)
class Dim:
    table: str
    key: tuple[str, ...]
    attrs: tuple[str, ...]
    id_col: str | None = None  # campo do row que recebe o id substituto; None = chave natural
    extra_ddl: tuple[str, ...] = ()

    @property
    def surrogate(self) -> bool:
        return self.id_col is not None


ELEICAO = Dim("eleicao", ("CD_ELEICAO",), (
    "ANO_ELEICAO", "CD_TIPO_ELEICAO", "NM_TIPO_ELEICAO", "CD_PLEITO", "DT_PLEITO",
    "NR_TURNO", "DS_ELEICAO"))
CARGO = Dim("cargo", ("CD_CARGO_PERGUNTA",), ("DS_CARGO_PERGUNTA",),
            extra_ddl=("votos_por_eleitor INTEGER NOT NULL DEFAULT 1",))
MUNICIPIO = Dim("municipio", ("CD_MUNICIPIO",), ("SG_UF", "NM_MUNICIPIO"))
PARTIDO = Dim("partido", ("NR_PARTIDO",), ("SG_PARTIDO", "NM_PARTIDO"))
VOTAVEL = Dim(
    "votavel", ("CD_ELEICAO", "CD_CARGO_PERGUNTA", "SG_UF_ABRANGENCIA", "NR_VOTAVEL"),
    ("CD_TIPO_VOTAVEL", "DS_TIPO_VOTAVEL", "NM_VOTAVEL", "NR_PARTIDO"), "VOTAVEL_ID")
SECAO = Dim(
    "secao", ("NR_TURNO", "SG_UF", "CD_MUNICIPIO", "NR_ZONA", "NR_SECAO"),
    ("NR_LOCAL_VOTACAO", "DT_BU_RECEBIDO", "CD_TIPO_URNA", "DS_TIPO_URNA",
     "NR_URNA_EFETIVADA", "CD_CARGA_1_URNA_EFETIVADA", "CD_CARGA_2_URNA_EFETIVADA",
     "CD_FLASHCARD_URNA_EFETIVADA", "DT_CARGA_URNA_EFETIVADA", "DS_SECOES_AGREGADAS",
     "DT_ABERTURA", "DT_ENCERRAMENTO", "QT_ELEI_BIOM_SEM_HABILITACAO", "DT_EMISSAO_BU",
     "NR_JUNTA_APURADORA", "NR_TURMA_APURADORA", "ARQUIVO_ID"),
    "SECAO_ID")
APURACAO = Dim(
    "apuracao", ("SECAO_ID", "CD_CARGO_PERGUNTA"),
    ("CD_ELEICAO", "QT_APTOS", "QT_COMPARECIMENTO", "QT_ABSTENCOES",
     "DS_CARGO_PERGUNTA_SECAO"),
    "APURACAO_ID")
# Constantes por arquivo; gravadas em arquivo_carga (não gera DDL própria).
ARQUIVO = Dim("arquivo_carga", ("ARQUIVO_ID",), ("DT_GERACAO", "HH_GERACAO"))

DIMS = (ELEICAO, CARGO, MUNICIPIO, PARTIDO, VOTAVEL, SECAO, APURACAO)  # ordem de resolução
GLOBAL_DIMS = (ELEICAO, CARGO, MUNICIPIO, PARTIDO, VOTAVEL)
# Chaves que nunca se repetem entre arquivos (cada arquivo é um turno x UF).
FILE_DIMS = (SECAO, APURACAO)
FACT_COLUMNS = ("APURACAO_ID", "VOTAVEL_ID", "QT_VOTOS")
FILE_COLUMNS = ARQUIVO.attrs
DERIVED_COLUMNS = frozenset({"SG_UF_ABRANGENCIA", "SECAO_ID", "APURACAO_ID", "VOTAVEL_ID",
                             "ARQUIVO_ID"})

# Votos por eleitor em cada cargo (2026: duas vagas de senador por UF).
VOTOS_POR_ELEITOR = {5: 2}


def _sql_type(col: str) -> str:
    return "INTEGER" if col in INT_COLUMNS or col.endswith("_ID") else "TEXT"


def dim_ddl(dim: Dim) -> str:
    cols = ["id INTEGER PRIMARY KEY"] if dim.surrogate else []
    cols += [f"{c.lower()} {_sql_type(c)}" for c in dim.key + dim.attrs]
    cols += list(dim.extra_ddl)
    key = ", ".join(c.lower() for c in dim.key)
    cols.append(f"UNIQUE ({key})" if dim.surrogate else f"PRIMARY KEY ({key})")
    return f"CREATE TABLE IF NOT EXISTS {dim.table} (\n    " + ",\n    ".join(cols) + "\n);\n"


STATIC_DDL = """
CREATE TABLE IF NOT EXISTS votos (
    apuracao_id INTEGER NOT NULL,
    votavel_id INTEGER NOT NULL,
    qt_votos INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS arquivo_carga (
    id INTEGER PRIMARY KEY,
    nome TEXT NOT NULL UNIQUE,
    nr_turno INTEGER NOT NULL,
    sg_uf TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    tamanho_bytes INTEGER NOT NULL,
    dt_geracao TEXT,
    hh_geracao TEXT,
    linhas INTEGER,
    soma_votos INTEGER,
    carregado_em TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS conflito (
    id INTEGER PRIMARY KEY,
    arquivo_id INTEGER NOT NULL,
    tabela TEXT NOT NULL,
    chave TEXT NOT NULL,
    coluna TEXT NOT NULL,
    valor_existente TEXT,
    valor_novo TEXT
);
"""

VIEWS_DDL = """
CREATE VIEW IF NOT EXISTS v_votos AS
SELECT s.nr_turno, s.sg_uf, s.cd_municipio, m.nm_municipio, s.nr_zona, s.nr_secao,
       a.cd_eleicao, a.cd_cargo_pergunta, c.ds_cargo_pergunta,
       v.cd_tipo_votavel, v.ds_tipo_votavel, v.nr_votavel, v.nm_votavel,
       v.nr_partido, p.sg_partido, vo.qt_votos
FROM votos vo
JOIN apuracao a ON a.id = vo.apuracao_id
JOIN secao s ON s.id = a.secao_id
JOIN municipio m ON m.cd_municipio = s.cd_municipio
JOIN cargo c ON c.cd_cargo_pergunta = a.cd_cargo_pergunta
JOIN votavel v ON v.id = vo.votavel_id
LEFT JOIN partido p ON p.nr_partido = v.nr_partido;

CREATE VIEW IF NOT EXISTS v_anomalia_apuracao AS
SELECT a.id AS apuracao_id, s.nr_turno, s.sg_uf, s.cd_municipio, s.nr_zona, s.nr_secao,
       a.cd_cargo_pergunta, a.qt_aptos, a.qt_comparecimento, a.qt_abstencoes,
       t.soma_votos, c.votos_por_eleitor,
       a.qt_comparecimento > a.qt_aptos AS comparecimento_maior_que_aptos,
       a.qt_aptos <> a.qt_comparecimento + a.qt_abstencoes AS aptos_inconsistente,
       t.soma_votos <> a.qt_comparecimento * c.votos_por_eleitor AS votos_inconsistente
FROM apuracao a
JOIN secao s ON s.id = a.secao_id
JOIN cargo c ON c.cd_cargo_pergunta = a.cd_cargo_pergunta
JOIN (SELECT apuracao_id, SUM(qt_votos) AS soma_votos FROM votos GROUP BY apuracao_id) t
  ON t.apuracao_id = a.id
WHERE a.qt_comparecimento > a.qt_aptos
   OR a.qt_aptos <> a.qt_comparecimento + a.qt_abstencoes
   OR t.soma_votos <> a.qt_comparecimento * c.votos_por_eleitor;
"""

INDEXES_DDL = """
CREATE INDEX IF NOT EXISTS ix_votos_apuracao ON votos(apuracao_id);
CREATE INDEX IF NOT EXISTS ix_votos_votavel ON votos(votavel_id);
CREATE INDEX IF NOT EXISTS ix_secao_municipio ON secao(cd_municipio);
CREATE INDEX IF NOT EXISTS ix_secao_urna ON secao(nr_urna_efetivada);
CREATE INDEX IF NOT EXISTS ix_votavel_partido ON votavel(nr_partido);
"""


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("".join(dim_ddl(d) for d in DIMS) + STATIC_DDL + VIEWS_DDL)


def finalize(conn: sqlite3.Connection) -> None:
    """Pós-carga: índices, votos por eleitor e estatísticas do otimizador."""
    conn.executescript(INDEXES_DDL)
    conn.executemany("UPDATE cargo SET votos_por_eleitor = ? WHERE cd_cargo_pergunta = ?",
                     [(n, c) for c, n in VOTOS_POR_ELEITOR.items()])
    conn.execute("ANALYZE")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_schema.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add eleicoes_db/schema.py tests/test_schema.py
git commit -m "feat: modelo normalizado declarativo e DDL do banco

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `analyze.py`, the CSV structure report

**Files:**
- Create: `eleicoes_db/analyze.py`, `tests/test_analyze.py`

**Interfaces:**
- Consumes: `COLUMNS`, `NULL_SENTINELS`, `NEG1_NULL_COLUMNS`, `ZipSource`, `iter_raw`, `clean_row` (source); `DIMS`, `FILE_DIMS`, `ARQUIVO`, `Dim` (schema)
- Produces:
  - `analyze_files(sources: list[ZipSource]) -> dict`. Report shape:
    - `{"arquivos": [{"nome", "linhas" (int | None if skipped), "linhas_invalidas", "colunas_ausentes", "colunas_extras"}], ...}`
    - `"colunas": {COL: {"tipos": {kind: n}, "sentinelas": {raw: n}, "distintos": int | ">1000", "exemplos": [str], "tamanho_max": int}}`
    - `"dependencias": {table: {"chave": [...], "violacoes": {COL: n}, "exemplos": [...]}}`
  - `write_report(report: dict, out_dir: Path) -> tuple[Path, Path]` (md path, json path)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_analyze.py`:

```python
import json
import zipfile

from eleicoes_db.analyze import analyze_files, write_report
from eleicoes_db.source import COLUMNS, list_zips
from helpers import sample_rows, write_zip

NAME = "bweb_1t_AC_051020261403.zip"


def _report(tmp_path, rows):
    write_zip(tmp_path / NAME, rows)
    return analyze_files(list_zips(tmp_path))


def test_column_stats(tmp_path):
    r = _report(tmp_path, sample_rows())
    assert r["arquivos"][0]["linhas"] == 14
    cols = r["colunas"]
    assert cols["QT_VOTOS"]["tipos"] == {"int": 14}
    assert cols["DT_ABERTURA"]["tipos"] == {"datetime": 14}
    assert cols["DT_GERACAO"]["tipos"] == {"date": 14}
    assert cols["HH_GERACAO"]["tipos"] == {"time": 14}
    assert cols["SG_PARTIDO"]["sentinelas"] == {"#NULO#": 6}
    assert cols["NR_PARTIDO"]["sentinelas"] == {"-1": 6}
    assert cols["NR_PARTIDO"]["tipos"] == {"int": 8}
    assert cols["NM_VOTAVEL"]["distintos"] == 6


def test_no_violations_on_consistent_data(tmp_path):
    r = _report(tmp_path, sample_rows())
    assert all(not d["violacoes"] for d in r["dependencias"].values())


def test_detects_secao_attribute_varying(tmp_path):
    rows = sample_rows()
    rows[1]["NR_URNA_EFETIVADA"] = "9999999"
    r = _report(tmp_path, rows)
    assert r["dependencias"]["secao"]["violacoes"] == {"NR_URNA_EFETIVADA": 1}
    assert r["dependencias"]["secao"]["exemplos"][0]["coluna"] == "NR_URNA_EFETIVADA"


def test_detects_turnout_varying_within_apuracao(tmp_path):
    rows = sample_rows()
    rows[2]["QT_APTOS"] = "999"
    r = _report(tmp_path, rows)
    assert r["dependencias"]["apuracao"]["violacoes"] == {"QT_APTOS": 1}
    assert r["dependencias"]["secao"]["violacoes"] == {}


def test_header_mismatch_skips_file(tmp_path):
    write_zip(tmp_path / NAME, sample_rows(), header=COLUMNS[:-1] + ("COLUNA_NOVA",))
    f = analyze_files(list_zips(tmp_path))["arquivos"][0]
    assert f["colunas_ausentes"] == ["NR_TURMA_APURADORA"]
    assert f["colunas_extras"] == ["COLUNA_NOVA"]
    assert f["linhas"] is None


def test_invalid_lines_are_counted(tmp_path):
    write_zip(tmp_path / NAME, sample_rows())
    with zipfile.ZipFile(tmp_path / NAME) as zf:
        csv_bytes = zf.read(NAME.replace(".zip", ".csv"))
    with zipfile.ZipFile(tmp_path / NAME, "w") as zf:
        zf.writestr(NAME.replace(".zip", ".csv"), csv_bytes + b'"so";"dois"\r\n')
    f = analyze_files(list_zips(tmp_path))["arquivos"][0]
    assert f["linhas"] == 15
    assert f["linhas_invalidas"] == 1


def test_write_report(tmp_path):
    r = _report(tmp_path, sample_rows())
    md, js = write_report(r, tmp_path / "out")
    assert json.loads(js.read_text(encoding="utf-8"))["arquivos"][0]["linhas"] == 14
    text = md.read_text(encoding="utf-8")
    assert "QT_VOTOS" in text
    assert "Nenhuma violação" in text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_analyze.py -v`
Expected: collection ERROR, `ModuleNotFoundError: No module named 'eleicoes_db.analyze'`

- [ ] **Step 3: Implement `analyze.py`**

Create `eleicoes_db/analyze.py`:

```python
"""Analisa a estrutura dos CSVs e confere as premissas do modelo normalizado.

Para cada coluna: tipos encontrados, sentinelas, distintos e exemplos.
Para cada tabela do modelo: quantas vezes um atributo variou dentro da mesma chave.
"""

from __future__ import annotations

import json
import logging
import re
from collections import Counter
from functools import lru_cache
from pathlib import Path

from .schema import ARQUIVO, DIMS, FILE_DIMS, Dim
from .source import COLUMNS, NEG1_NULL_COLUMNS, NULL_SENTINELS, ZipSource, clean_row, iter_raw

log = logging.getLogger(__name__)

DISTINCT_CAP = 1000
MAX_EXAMPLES = 5

_KINDS = (
    ("int", re.compile(r"^-?\d+$")),
    ("date", re.compile(r"^\d{2}/\d{2}/\d{4}$")),
    ("datetime", re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")),
    ("time", re.compile(r"^\d{2}:\d{2}:\d{2}$")),
)


@lru_cache(maxsize=1_000_000)
def classify(value: str) -> str:
    for kind, pattern in _KINDS:
        if pattern.match(value):
            return kind
    return "text"


class _ColumnStats:
    def __init__(self):
        self.kinds = Counter()
        self.sentinels = Counter()
        self.distinct = set()
        self.overflow = False
        self.examples = []
        self.max_len = 0

    def add(self, column: str, value: str) -> None:
        if value in NULL_SENTINELS or value == "-1":
            self.sentinels[value] += 1
            if value != "-1" or column in NEG1_NULL_COLUMNS:
                return
        self.kinds[classify(value)] += 1
        self.max_len = max(self.max_len, len(value))
        if self.overflow or value in self.distinct:
            return
        self.distinct.add(value)
        if len(self.examples) < MAX_EXAMPLES:
            self.examples.append(value)
        if len(self.distinct) > DISTINCT_CAP:
            self.overflow = True
            self.distinct = set()

    def to_dict(self) -> dict:
        return {
            "tipos": dict(self.kinds),
            "sentinelas": dict(self.sentinels),
            "distintos": f">{DISTINCT_CAP}" if self.overflow else len(self.distinct),
            "exemplos": self.examples,
            "tamanho_max": self.max_len,
        }


class _DependencyCheck:
    """Conta as linhas em que um atributo difere do primeiro valor visto para a chave."""

    def __init__(self, dim: Dim):
        self.dim = dim
        self.seen = {}
        self.violations = Counter()
        self.examples = []

    def add(self, row: dict) -> None:
        key = tuple(row[c] for c in self.dim.key)
        if all(k is None for k in key):
            return
        attrs = tuple(row[c] for c in self.dim.attrs)
        first = self.seen.setdefault(key, attrs)
        if first is attrs or first == attrs:
            return
        for col, a, b in zip(self.dim.attrs, first, attrs):
            if a != b:
                self.violations[col] += 1
                if len(self.examples) < MAX_EXAMPLES:
                    self.examples.append(
                        {"chave": [str(k) for k in key], "coluna": col, "primeiro": a, "outro": b})

    def to_dict(self) -> dict:
        return {"chave": list(self.dim.key), "violacoes": dict(self.violations),
                "exemplos": self.examples}


def analyze_files(sources: list[ZipSource]) -> dict:
    columns = {c: _ColumnStats() for c in COLUMNS}
    checks = {d.table: _DependencyCheck(d) for d in DIMS + (ARQUIVO,)}
    files = []
    for idx, src in enumerate(sources, start=1):
        raw = iter_raw(src.path)
        header = next(raw)
        info = {
            "nome": src.name,
            "linhas": None,
            "linhas_invalidas": 0,
            "colunas_ausentes": sorted(set(COLUMNS) - set(header)),
            "colunas_extras": [c for c in header if c not in COLUMNS],
        }
        files.append(info)
        if info["colunas_ausentes"]:
            log.warning("%s: colunas ausentes %s; arquivo pulado", src.name,
                        info["colunas_ausentes"])
            continue
        # Seções e apurações são únicas por arquivo; não vale guardar entre arquivos.
        for d in FILE_DIMS + (ARQUIVO,):
            checks[d.table].seen.clear()
        stats = [columns.setdefault(c, _ColumnStats()) for c in header]
        n = 0
        for values in raw:
            n += 1
            try:
                row = clean_row(header, values)
            except ValueError:
                info["linhas_invalidas"] += 1
                continue
            for c, st, v in zip(header, stats, values):
                st.add(c, v)
            row["ARQUIVO_ID"] = idx
            for d in DIMS:
                if d.surrogate:
                    row[d.id_col] = tuple(row[c] for c in d.key)
            for chk in checks.values():
                chk.add(row)
        info["linhas"] = n
        log.info("[%d/%d] %s: %d linhas analisadas", idx, len(sources), src.name, n)
    return {
        "arquivos": files,
        "colunas": {c: s.to_dict() for c, s in columns.items()},
        "dependencias": {t: chk.to_dict() for t, chk in checks.items()},
    }


def _fmt(counter: dict) -> str:
    return ", ".join(f"{k or '(vazio)'}: {v:,}" for k, v in counter.items())


def _cell(value) -> str:
    return str(value).replace("|", "\\|")


def write_report(report: dict, out_dir: Path) -> tuple[Path, Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    js = out_dir / "schema_report.json"
    js.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    total = sum(f["linhas"] or 0 for f in report["arquivos"])
    lines = ["# Relatório de estrutura dos CSVs de BU", "",
             f"Arquivos: {len(report['arquivos'])}; linhas: {total:,}", ""]
    problems = [f for f in report["arquivos"]
                if f["colunas_ausentes"] or f["colunas_extras"] or f["linhas_invalidas"]]
    if problems:
        lines += ["## Arquivos com problemas", "",
                  "| arquivo | colunas ausentes | colunas extras | linhas inválidas |",
                  "|---|---|---|---|"]
        lines += [f"| {f['nome']} | {', '.join(f['colunas_ausentes'])} | "
                  f"{', '.join(f['colunas_extras'])} | {f['linhas_invalidas']} |"
                  for f in problems]
        lines.append("")

    lines += ["## Colunas", "",
              "| coluna | tipos | sentinelas | distintos | tam. máx. | exemplos |",
              "|---|---|---|---|---|---|"]
    for c, s in report["colunas"].items():
        lines.append(f"| {c} | {_fmt(s['tipos'])} | {_fmt(s['sentinelas'])} | "
                     f"{s['distintos']} | {s['tamanho_max']} | "
                     f"{_cell(', '.join(s['exemplos']))} |")

    lines += ["", "## Premissas do modelo (dependências funcionais)", ""]
    violated = [(t, d) for t, d in report["dependencias"].items() if d["violacoes"]]
    if not violated:
        lines.append("Nenhuma violação: cada atributo é constante dentro da chave da sua tabela.")
    else:
        lines += ["| tabela | chave | coluna | linhas divergentes |", "|---|---|---|---|"]
        for t, d in violated:
            for col, n in d["violacoes"].items():
                lines.append(f"| {t} | {', '.join(d['chave'])} | {col} | {n:,} |")
        lines += ["", "### Exemplos", ""]
        for t, d in violated:
            for ex in d["exemplos"]:
                lines.append(f"- {t} {ex['chave']}: {ex['coluna']} = "
                             f"{ex['primeiro']!r} / {ex['outro']!r}")
    md = out_dir / "schema_report.md"
    md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return md, js
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_analyze.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add eleicoes_db/analyze.py tests/test_analyze.py
git commit -m "feat: análise da estrutura dos CSVs e das premissas do modelo

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: `load.py`, the streaming loader

**Files:**
- Create: `eleicoes_db/load.py`, `tests/test_load.py`

**Interfaces:**
- Consumes: `ZipSource`, `iter_rows` (source); `DIMS`, `GLOBAL_DIMS`, `FILE_DIMS`, `ARQUIVO`, `Dim`, `create_schema`, `finalize` (schema)
- Produces:
  - `connect(db_path: Path) -> sqlite3.Connection` (autocommit, `isolation_level=None`)
  - `BATCH_SIZE: int` (module-level, read at runtime)
  - `Loader(conn)` with `.load_file(src: ZipSource) -> bool` (False = skipped because already loaded)
  - `load_all(conn, sources: list[ZipSource]) -> list[tuple[str, str]]` (failures as `(zip name, error message)`; runs `finalize`)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_load.py`:

```python
import pytest

from eleicoes_db.load import connect, load_all
from eleicoes_db.source import list_zips
from helpers import am, sample_rows, write_zip

AC = "bweb_1t_AC_051020261403.zip"
AM = "bweb_1t_AM_051020261403.zip"


@pytest.fixture
def db(tmp_path):
    conn = connect(tmp_path / "t.db")
    yield conn
    conn.close()


def _count(conn, table):
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_loads_normalized_tables(tmp_path, db):
    write_zip(tmp_path / AC, sample_rows())
    assert load_all(db, list_zips(tmp_path)) == []
    assert _count(db, "votos") == 14
    assert db.execute("SELECT SUM(qt_votos) FROM votos").fetchone()[0] == 480
    assert _count(db, "secao") == 2
    assert _count(db, "apuracao") == 4
    assert _count(db, "votavel") == 7
    assert _count(db, "partido") == 2
    assert _count(db, "eleicao") == 1
    assert _count(db, "cargo") == 2
    assert _count(db, "municipio") == 1
    assert _count(db, "conflito") == 0
    arq = db.execute("SELECT linhas, soma_votos, dt_geracao, hh_geracao, nr_turno, sg_uf "
                     "FROM arquivo_carga").fetchone()
    assert arq == (14, 480, "2026-10-05", "14:03:43", 1, "AC")
    idx = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert "ix_votos_votavel" in idx


def test_values_are_cleaned(tmp_path, db):
    write_zip(tmp_path / AC, sample_rows())
    load_all(db, list_zips(tmp_path))
    assert db.execute("SELECT nr_partido FROM votavel "
                      "WHERE nr_votavel = 95 AND cd_cargo_pergunta = 3").fetchone() == (None,)
    assert db.execute("SELECT DISTINCT ds_secoes_agregadas, nr_junta_apuradora "
                      "FROM secao").fetchall() == [(None, None)]
    assert db.execute("SELECT nm_votavel FROM votavel "
                      "WHERE nr_votavel = 55").fetchone() == ("ELTER NÓBREGA",)
    assert db.execute("SELECT votos_por_eleitor FROM cargo "
                      "WHERE cd_cargo_pergunta = 5").fetchone() == (2,)


def test_rerun_skips_loaded_files(tmp_path, db):
    write_zip(tmp_path / AC, sample_rows())
    srcs = list_zips(tmp_path)
    load_all(db, srcs)
    load_all(db, srcs)
    assert _count(db, "votos") == 14
    assert _count(db, "arquivo_carga") == 1


def test_second_file_reuses_global_dimensions(tmp_path, db):
    write_zip(tmp_path / AC, sample_rows())
    write_zip(tmp_path / AM, am(sample_rows()))
    assert load_all(db, list_zips(tmp_path)) == []
    assert _count(db, "secao") == 4
    assert _count(db, "votos") == 28
    assert _count(db, "partido") == 2
    assert _count(db, "eleicao") == 1
    assert _count(db, "votavel") == 14  # candidatos estaduais são por UF


def test_president_candidates_are_national(tmp_path, db):
    pres = [{"CD_ELEICAO": "6257", "DS_ELEICAO": "Eleição Geral Federal 2026",
             "CD_CARGO_PERGUNTA": "1", "DS_CARGO_PERGUNTA": "Presidente",
             "DS_CARGO_PERGUNTA_SECAO": "1 - 175", "CD_TIPO_VOTAVEL": "1",
             "DS_TIPO_VOTAVEL": "Nominal", "NR_VOTAVEL": "13", "NM_VOTAVEL": "CANDIDATO X",
             "NR_PARTIDO": "13", "SG_PARTIDO": "PT", "NM_PARTIDO": "PARTIDO DOS TRABALHADORES",
             "QT_VOTOS": "80"}]
    write_zip(tmp_path / AC, pres)
    write_zip(tmp_path / AM, am(pres))
    assert load_all(db, list_zips(tmp_path)) == []
    assert db.execute("SELECT sg_uf_abrangencia, COUNT(*) FROM votavel "
                      "GROUP BY 1").fetchall() == [("BR", 1)]


def test_conflicting_attribute_is_recorded_and_first_value_kept(tmp_path, db):
    rows = sample_rows()
    rows[1]["NR_URNA_EFETIVADA"] = "9999999"
    write_zip(tmp_path / AC, rows)
    assert load_all(db, list_zips(tmp_path)) == []
    assert db.execute("SELECT tabela, coluna, valor_existente, valor_novo "
                      "FROM conflito").fetchall() == [
        ("secao", "nr_urna_efetivada", "2280689", "9999999")]
    assert db.execute("SELECT nr_urna_efetivada FROM secao "
                      "WHERE nr_secao = 175").fetchone() == (2280689,)


def test_failed_file_is_rolled_back_and_can_be_reloaded(tmp_path, db):
    write_zip(tmp_path / AC, sample_rows())
    bad = sample_rows()
    bad[5]["QT_VOTOS"] = "#NULO#"
    write_zip(tmp_path / AM, am(bad))
    failures = load_all(db, list_zips(tmp_path))
    assert [name for name, _ in failures] == [AM]
    assert "QT_VOTOS" in failures[0][1]
    assert _count(db, "arquivo_carga") == 1
    assert _count(db, "votos") == 14
    assert _count(db, "secao") == 2
    assert _count(db, "municipio") == 1

    write_zip(tmp_path / AM, am(sample_rows()))
    assert load_all(db, list_zips(tmp_path)) == []
    assert _count(db, "votos") == 28
    assert _count(db, "secao") == 4


def test_small_batches(tmp_path, db, monkeypatch):
    monkeypatch.setattr("eleicoes_db.load.BATCH_SIZE", 3)
    write_zip(tmp_path / AC, sample_rows())
    assert load_all(db, list_zips(tmp_path)) == []
    assert _count(db, "votos") == 14
    assert db.execute("SELECT SUM(qt_votos) FROM votos").fetchone()[0] == 480
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_load.py -v`
Expected: collection ERROR, `ModuleNotFoundError: No module named 'eleicoes_db.load'`

- [ ] **Step 3: Implement `load.py`**

Create `eleicoes_db/load.py`:

```python
"""Carga dos zips de BU no SQLite: streaming, uma transação por arquivo.

Arquivos já presentes em arquivo_carga são pulados, então a carga pode ser
interrompida e retomada. Um arquivo com erro é desfeito por inteiro e os
demais continuam.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from pathlib import Path

from .schema import ARQUIVO, DIMS, FILE_DIMS, GLOBAL_DIMS, Dim, create_schema, finalize
from .source import ZipSource, iter_rows

log = logging.getLogger(__name__)

BATCH_SIZE = 50_000

# Com o journal padrão, um crash do processo desfaz só o arquivo em andamento.
LOAD_PRAGMAS = (
    "PRAGMA synchronous = OFF",
    "PRAGMA cache_size = -1000000",  # ~1 GB
    "PRAGMA temp_store = MEMORY",
)


def connect(db_path: Path) -> sqlite3.Connection:
    """Abre o banco em modo autocommit; as transações são explícitas."""
    return sqlite3.connect(db_path, isolation_level=None)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _text(value):
    return None if value is None else str(value)


class _Conflicts:
    """Atributos que deveriam ser constantes por chave mas vieram diferentes."""

    def __init__(self):
        self.rows = []
        self._seen = set()

    def record(self, arquivo_id, dim: Dim, key, known, new) -> None:
        for col, old, cur in zip(dim.attrs, known, new):
            if old == cur:
                continue
            sig = (dim.table, key, col, cur)
            if sig in self._seen:
                continue
            self._seen.add(sig)
            self.rows.append((arquivo_id, dim.table, json.dumps(key, ensure_ascii=False),
                              col.lower(), _text(old), _text(cur)))


class _DimCache:
    """Chave -> (id, atributos) de uma tabela, com as inserções pendentes."""

    def __init__(self, dim: Dim, next_id: int):
        self.dim = dim
        self.entries = {}
        self.pending = []
        self.next_id = next_id
        cols = [c.lower() for c in dim.key + dim.attrs]
        if dim.surrogate:
            cols.insert(0, "id")
        self.columns = cols
        self.insert_sql = (f"INSERT INTO {dim.table} ({', '.join(cols)}) "
                           f"VALUES ({', '.join('?' * len(cols))})")

    def preload(self, conn: sqlite3.Connection) -> None:
        n = len(self.dim.key)
        for r in conn.execute(f"SELECT {', '.join(self.columns)} FROM {self.dim.table}"):
            if self.dim.surrogate:
                self.entries[tuple(r[1:1 + n])] = (r[0], tuple(r[1 + n:]))
            else:
                self.entries[tuple(r[:n])] = (r[0], tuple(r[n:]))

    def resolve(self, row: dict, arquivo_id: int, conflicts: _Conflicts):
        dim = self.dim
        key = tuple(row[c] for c in dim.key)
        if all(k is None for k in key):
            return None
        attrs = tuple(row[c] for c in dim.attrs)
        hit = self.entries.get(key)
        if hit is not None:
            if hit[1] != attrs:
                conflicts.record(arquivo_id, dim, key, hit[1], attrs)
            return hit[0]
        if dim.surrogate:
            ident = self.next_id
            self.next_id += 1
            self.pending.append((ident, *key, *attrs))
        else:
            ident = key[0]
            self.pending.append((*key, *attrs))
        self.entries[key] = (ident, attrs)
        return ident

    def flush(self, conn: sqlite3.Connection) -> None:
        if self.pending:
            conn.executemany(self.insert_sql, self.pending)
            self.pending.clear()


class Loader:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        for pragma in LOAD_PRAGMAS:
            conn.execute(pragma)
        create_schema(conn)
        self._reset_caches()

    def _reset_caches(self) -> None:
        self.caches = {}
        for dim in DIMS:
            next_id = 1
            if dim.surrogate:
                next_id = self.conn.execute(
                    f"SELECT COALESCE(MAX(id), 0) + 1 FROM {dim.table}").fetchone()[0]
            cache = _DimCache(dim, next_id)
            if dim in GLOBAL_DIMS:
                cache.preload(self.conn)
            self.caches[dim.table] = cache

    def is_loaded(self, src: ZipSource) -> bool:
        return self.conn.execute("SELECT 1 FROM arquivo_carga WHERE nome = ?",
                                 (src.name,)).fetchone() is not None

    def _flush(self, votos: list) -> None:
        for cache in self.caches.values():
            cache.flush(self.conn)
        if votos:
            self.conn.executemany(
                "INSERT INTO votos (apuracao_id, votavel_id, qt_votos) VALUES (?, ?, ?)", votos)
            votos.clear()

    def load_file(self, src: ZipSource) -> bool:
        if self.is_loaded(src):
            log.info("%s: já carregado, pulando", src.name)
            return False
        started = time.monotonic()
        digest = sha256_file(src.path)
        conflicts = _Conflicts()
        arquivo = _DimCache(ARQUIVO, 1)  # só confere DT/HH_GERACAO; nunca é gravado
        for dim in FILE_DIMS:
            self.caches[dim.table].entries.clear()
        self.conn.execute("BEGIN")
        try:
            arquivo_id = self.conn.execute(
                "INSERT INTO arquivo_carga (nome, nr_turno, sg_uf, sha256, tamanho_bytes) "
                "VALUES (?, ?, ?, ?, ?)",
                (src.name, src.turno, src.uf, digest, src.path.stat().st_size)).lastrowid
            votos = []
            linhas = soma = 0
            for row in iter_rows(src.path):
                linhas += 1
                qt = row["QT_VOTOS"]
                if not isinstance(qt, int):
                    raise ValueError(f"linha {linhas + 1}: QT_VOTOS inválido: {qt!r}")
                row["ARQUIVO_ID"] = arquivo_id
                arquivo.resolve(row, arquivo_id, conflicts)
                for dim in DIMS:
                    ident = self.caches[dim.table].resolve(row, arquivo_id, conflicts)
                    if dim.surrogate:
                        row[dim.id_col] = ident
                votos.append((row["APURACAO_ID"], row["VOTAVEL_ID"], qt))
                soma += qt
                if len(votos) >= BATCH_SIZE:
                    self._flush(votos)
            self._flush(votos)
            dt_geracao, hh_geracao = next(iter(arquivo.entries.values()), (None, (None, None)))[1]
            self.conn.execute(
                "UPDATE arquivo_carga SET linhas = ?, soma_votos = ?, dt_geracao = ?, "
                "hh_geracao = ? WHERE id = ?",
                (linhas, soma, dt_geracao, hh_geracao, arquivo_id))
            if conflicts.rows:
                self.conn.executemany(
                    "INSERT INTO conflito (arquivo_id, tabela, chave, coluna, valor_existente, "
                    "valor_novo) VALUES (?, ?, ?, ?, ?, ?)", conflicts.rows)
                log.warning("%s: %d conflitos de atributos registrados na tabela conflito",
                            src.name, len(conflicts.rows))
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            self._reset_caches()
            raise
        elapsed = time.monotonic() - started
        log.info("%s: %d linhas, %d votos em %.0fs (%.0f linhas/s)",
                 src.name, linhas, soma, elapsed, linhas / elapsed if elapsed else 0)
        return True


def load_all(conn: sqlite3.Connection, sources: list[ZipSource]) -> list[tuple[str, str]]:
    """Carrega todos os zips; devolve [(nome, erro)] dos que falharam."""
    loader = Loader(conn)
    failures = []
    for i, src in enumerate(sources, start=1):
        log.info("[%d/%d] %s", i, len(sources), src.name)
        try:
            loader.load_file(src)
        except Exception as e:
            log.error("%s: FALHOU, carga do arquivo desfeita: %s", src.name, e)
            failures.append((src.name, str(e)))
    log.info("criando índices e estatísticas...")
    finalize(conn)
    return failures
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_load.py -v`
Expected: 8 passed

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -v`
Expected: 30 passed

- [ ] **Step 6: Commit**

```bash
git add eleicoes_db/load.py tests/test_load.py
git commit -m "feat: carga em streaming, uma transação por arquivo, com registro de conflitos

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: `validate.py`, the integrity and audit checks

**Files:**
- Create: `eleicoes_db/validate.py`, `tests/test_validate.py`

**Interfaces:**
- Consumes: `ZipSource`, `iter_raw` (source); `create_schema` (schema); views `v_anomalia_apuracao`; tables `arquivo_carga`, `votos`, `apuracao`, `secao`, `conflito`
- Produces:
  - `ValidationReport` with fields `divergencias: list[str]`, `nao_carregados: list[str]`, `anomalias: dict[str, int]`, `conflitos: int`, and a property `ok: bool`
  - Anomaly keys: `apuracoes_inconsistentes`, `comparecimento_maior_que_aptos`, `aptos_inconsistente`, `votos_inconsistente`, `votos_duplicados`, `secoes_sem_abertura`, `urna_em_mais_de_uma_secao`
  - `validate(conn, sources: list[ZipSource], restream: bool = True) -> ValidationReport`
  - `format_report(rep: ValidationReport) -> str` (first line `Validação: OK` or `Validação: FALHOU`)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_validate.py`:

```python
from eleicoes_db.load import connect, load_all
from eleicoes_db.source import list_zips
from eleicoes_db.validate import format_report, validate
from helpers import sample_rows, write_zip

AC = "bweb_1t_AC_051020261403.zip"


def _loaded(tmp_path, rows):
    write_zip(tmp_path / AC, rows)
    conn = connect(tmp_path / "t.db")
    srcs = list_zips(tmp_path)
    assert load_all(conn, srcs) == []
    return conn, srcs


def test_clean_load_passes(tmp_path):
    conn, srcs = _loaded(tmp_path, sample_rows())
    rep = validate(conn, srcs)
    assert rep.ok
    assert rep.divergencias == []
    assert rep.nao_carregados == []
    assert rep.conflitos == 0
    assert all(v == 0 for v in rep.anomalias.values()), rep.anomalias
    assert format_report(rep).startswith("Validação: OK")


def test_detects_rows_missing_from_db(tmp_path):
    conn, srcs = _loaded(tmp_path, sample_rows())
    conn.execute("DELETE FROM votos WHERE rowid = (SELECT MIN(rowid) FROM votos)")
    rep = validate(conn, srcs)
    assert not rep.ok
    assert len(rep.divergencias) == 2  # contra arquivo_carga e contra o CSV
    assert format_report(rep).startswith("Validação: FALHOU")


def test_reports_unloaded_zip(tmp_path):
    conn, _ = _loaded(tmp_path, sample_rows())
    write_zip(tmp_path / "bweb_2t_AC_261020260742.zip", sample_rows())
    rep = validate(conn, list_zips(tmp_path), restream=False)
    assert rep.nao_carregados == ["bweb_2t_AC_261020260742.zip"]
    assert not rep.ok


def test_flags_turnout_anomalies_without_failing(tmp_path):
    rows = sample_rows()
    for r in rows[:4]:  # apuração de governador da seção 175
        r["QT_APTOS"] = "70"
        r["QT_ABSTENCOES"] = "0"
    conn, srcs = _loaded(tmp_path, rows)
    rep = validate(conn, srcs)
    assert rep.ok  # achados de auditoria não falham a validação
    assert rep.anomalias["apuracoes_inconsistentes"] == 1
    assert rep.anomalias["comparecimento_maior_que_aptos"] == 1
    assert rep.anomalias["aptos_inconsistente"] == 1
    assert rep.anomalias["votos_inconsistente"] == 0


def test_flags_vote_sum_mismatch(tmp_path):
    rows = sample_rows()
    rows[0]["QT_VOTOS"] = "51"
    conn, srcs = _loaded(tmp_path, rows)
    rep = validate(conn, srcs)
    assert rep.anomalias["votos_inconsistente"] == 1


def test_senator_two_votes_per_voter_is_not_an_anomaly(tmp_path):
    conn, srcs = _loaded(tmp_path, sample_rows())
    assert validate(conn, srcs, restream=False).anomalias["votos_inconsistente"] == 0


def test_flags_duplicate_vote_rows(tmp_path):
    rows = sample_rows()
    rows.append(dict(rows[0]))
    conn, srcs = _loaded(tmp_path, rows)
    rep = validate(conn, srcs)
    assert rep.ok
    assert rep.anomalias["votos_duplicados"] == 1


def test_flags_missing_opening_time(tmp_path):
    rows = sample_rows()
    for r in rows:
        r["DT_ABERTURA"] = ""
    conn, srcs = _loaded(tmp_path, rows)
    assert validate(conn, srcs, restream=False).anomalias["secoes_sem_abertura"] == 2


def test_flags_urna_in_two_secoes(tmp_path):
    rows = sample_rows()
    for r in rows:
        r["NR_URNA_EFETIVADA"] = "2280689"
    conn, srcs = _loaded(tmp_path, rows)
    assert validate(conn, srcs, restream=False).anomalias["urna_em_mais_de_uma_secao"] == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_validate.py -v`
Expected: collection ERROR, `ModuleNotFoundError: No module named 'eleicoes_db.validate'`

- [ ] **Step 3: Implement `validate.py`**

Create `eleicoes_db/validate.py`:

```python
"""Confere a carga contra os CSVs de origem e conta achados de auditoria.

Divergências de carga (linhas/votos diferentes, arquivo não carregado) fazem a
validação falhar. Anomalias nos dados do TSE são só contadas e reportadas.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field

from .schema import create_schema
from .source import ZipSource, iter_raw

log = logging.getLogger(__name__)

AUDIT_QUERIES = {
    "votos_duplicados":
        "SELECT COUNT(*) FROM (SELECT 1 FROM votos "
        "GROUP BY apuracao_id, votavel_id HAVING COUNT(*) > 1)",
    "secoes_sem_abertura":
        "SELECT COUNT(*) FROM secao WHERE dt_abertura IS NULL",
    "urna_em_mais_de_uma_secao":
        "SELECT COUNT(*) FROM (SELECT 1 FROM secao WHERE nr_urna_efetivada IS NOT NULL "
        "GROUP BY nr_turno, nr_urna_efetivada HAVING COUNT(*) > 1)",
}


@dataclass
class ValidationReport:
    divergencias: list[str] = field(default_factory=list)
    nao_carregados: list[str] = field(default_factory=list)
    anomalias: dict[str, int] = field(default_factory=dict)
    conflitos: int = 0

    @property
    def ok(self) -> bool:
        return not self.divergencias and not self.nao_carregados


def source_totals(src: ZipSource) -> tuple[int, int]:
    """(linhas, soma de QT_VOTOS) lidos direto do CSV, sem a limpeza do loader."""
    raw = iter_raw(src.path)
    i = next(raw).index("QT_VOTOS")
    n = total = 0
    for values in raw:
        n += 1
        total += int(values[i])
    return n, total


def validate(conn: sqlite3.Connection, sources: list[ZipSource],
             restream: bool = True) -> ValidationReport:
    create_schema(conn)
    rep = ValidationReport()
    loaded = {nome: (aid, linhas, soma) for aid, nome, linhas, soma in conn.execute(
        "SELECT id, nome, linhas, soma_votos FROM arquivo_carga")}
    in_db = {aid: (n, s) for aid, n, s in conn.execute(
        "SELECT s.arquivo_id, COUNT(*), SUM(vo.qt_votos) FROM votos vo "
        "JOIN apuracao a ON a.id = vo.apuracao_id "
        "JOIN secao s ON s.id = a.secao_id "
        "GROUP BY s.arquivo_id")}
    for src in sources:
        if src.name not in loaded:
            rep.nao_carregados.append(src.name)
            continue
        aid, linhas, soma = loaded[src.name]
        got = in_db.get(aid, (0, 0))
        if got != (linhas, soma):
            rep.divergencias.append(f"{src.name}: banco tem {got[0]} linhas/{got[1]} votos, "
                                    f"carga registrou {linhas}/{soma}")
        if restream:
            log.info("%s: relendo o CSV para conferir", src.name)
            expected = source_totals(src)
            if expected != got:
                rep.divergencias.append(f"{src.name}: CSV tem {expected[0]} linhas/"
                                        f"{expected[1]} votos, banco tem {got[0]}/{got[1]}")
    total, maior, aptos, votos = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(comparecimento_maior_que_aptos), 0), "
        "COALESCE(SUM(aptos_inconsistente), 0), COALESCE(SUM(votos_inconsistente), 0) "
        "FROM v_anomalia_apuracao").fetchone()
    rep.anomalias = {
        "apuracoes_inconsistentes": total,
        "comparecimento_maior_que_aptos": maior,
        "aptos_inconsistente": aptos,
        "votos_inconsistente": votos,
    }
    for name, sql in AUDIT_QUERIES.items():
        rep.anomalias[name] = conn.execute(sql).fetchone()[0]
    rep.conflitos = conn.execute("SELECT COUNT(*) FROM conflito").fetchone()[0]
    return rep


def format_report(rep: ValidationReport) -> str:
    lines = ["Validação: " + ("OK" if rep.ok else "FALHOU")]
    lines += [f"  não carregado: {n}" for n in rep.nao_carregados]
    lines += [f"  divergência: {d}" for d in rep.divergencias]
    lines.append("Achados de auditoria (dados do TSE, não são erros de carga):")
    lines += [f"  {k}: {v:,}" for k, v in rep.anomalias.items()]
    lines.append(f"  conflitos de atributos (tabela conflito): {rep.conflitos:,}")
    return "\n".join(lines)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_validate.py -v`
Expected: 9 passed

- [ ] **Step 5: Commit**

```bash
git add eleicoes_db/validate.py tests/test_validate.py
git commit -m "feat: validação da carga contra os CSVs e contagem de anomalias

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: `build_db.py` CLI + real-data test

**Files:**
- Create: `build_db.py`, `tests/test_build_db.py`

**Interfaces:**
- Consumes: `list_zips` (source), `analyze_files`, `write_report` (analyze), `connect`, `load_all` (load), `validate`, `format_report` (validate)
- Produces: `main(argv: list[str] | None = None) -> int`
  - Exit codes: 0 = ok; 1 = a load failure or a failed validation; 2 = no zips found

- [ ] **Step 1: Write the failing tests**

Create `tests/test_build_db.py`:

```python
import sqlite3
from pathlib import Path

import pytest

import build_db
from eleicoes_db.load import connect, load_all
from eleicoes_db.source import list_zips
from eleicoes_db.validate import validate
from helpers import am, sample_rows, write_zip


@pytest.fixture
def zips(tmp_path):
    d = tmp_path / "bu"
    d.mkdir()
    write_zip(d / "bweb_1t_AC_051020261403.zip", sample_rows())
    write_zip(d / "bweb_1t_AM_051020261403.zip", am(sample_rows()))
    return d


def test_all_command(tmp_path, zips, capsys):
    rc = build_db.main(["all", "--zips", str(zips), "--db", str(tmp_path / "e.db"),
                        "--relatorio", str(tmp_path)])
    assert rc == 0
    assert (tmp_path / "schema_report.md").exists()
    assert "Validação: OK" in capsys.readouterr().out


def test_uf_filter(tmp_path, zips):
    db = tmp_path / "e.db"
    assert build_db.main(["load", "--zips", str(zips), "--db", str(db), "--uf", "am"]) == 0
    conn = sqlite3.connect(db)
    assert [r[0] for r in conn.execute("SELECT sg_uf FROM arquivo_carga")] == ["AM"]
    conn.close()


def test_validate_fails_when_not_loaded(tmp_path, zips):
    rc = build_db.main(["validate", "--zips", str(zips), "--db", str(tmp_path / "e.db"),
                        "--sem-reler"])
    assert rc == 1


def test_no_zips_returns_2(tmp_path):
    assert build_db.main(["load", "--zips", str(tmp_path), "--db",
                          str(tmp_path / "e.db")]) == 2


@pytest.mark.real
def test_real_ac_first_round(tmp_path):
    real = Path(__file__).resolve().parent.parent / "bu"
    srcs = [s for s in list_zips(real) if s.uf == "AC" and s.turno == 1] if real.exists() else []
    if not srcs:
        pytest.skip("zip real do AC (1º turno) não disponível em bu/")
    conn = connect(tmp_path / "ac.db")
    assert load_all(conn, srcs) == []
    rep = validate(conn, srcs)
    assert rep.ok, rep.divergencias
    assert conn.execute("SELECT COUNT(*) FROM votos").fetchone()[0] == 258052
    assert rep.anomalias["votos_inconsistente"] == 0
    assert rep.anomalias["aptos_inconsistente"] == 0
    conn.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_build_db.py -v`
Expected: collection ERROR, `ModuleNotFoundError: No module named 'build_db'`

- [ ] **Step 3: Implement `build_db.py`**

Create `build_db.py` in the project root:

```python
#!/usr/bin/env python3
"""
Cria e popula o banco SQLite com os Boletins de Urna (BU) 2026 do TSE,
lendo os CSVs direto dos zips baixados por baixabu.py (sem extrair).

Uso (Windows, PowerShell ou CMD):
    python build_db.py analyze      # gera schema_report.md / schema_report.json
    python build_db.py load         # carrega no banco (retoma de onde parou)
    python build_db.py validate     # confere contra os CSVs e lista anomalias
    python build_db.py all          # analyze + load + validate
    python build_db.py load --uf AC,RR,ZZ          # só algumas UFs
    python build_db.py validate --sem-reler        # não relê os CSVs (rápido)

Requer apenas Python 3 (sem dependências externas).
"""

import argparse
import logging
import sys
from pathlib import Path

from eleicoes_db.analyze import analyze_files, write_report
from eleicoes_db.load import connect, load_all
from eleicoes_db.source import list_zips
from eleicoes_db.validate import format_report, validate


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="BU 2026 (TSE) -> SQLite")
    p.add_argument("comando", choices=["analyze", "load", "validate", "all"])
    p.add_argument("--zips", type=Path, default=Path("bu"), help="pasta dos zips (padrão: bu)")
    p.add_argument("--db", type=Path, default=Path("eleicoes2026.db"),
                   help="arquivo do banco (padrão: eleicoes2026.db)")
    p.add_argument("--relatorio", type=Path, default=Path("."),
                   help="pasta do schema_report (padrão: pasta atual)")
    p.add_argument("--uf", help="UFs separadas por vírgula, ex.: AC,RR,ZZ")
    p.add_argument("--sem-reler", action="store_true",
                   help="validate sem reler os CSVs (só confere o banco)")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
                        datefmt="%H:%M:%S")

    sources = list_zips(args.zips)
    if args.uf:
        ufs = {u.strip().upper() for u in args.uf.split(",")}
        sources = [s for s in sources if s.uf in ufs]
    if not sources:
        print(f"nenhum zip bweb_*.zip encontrado em {args.zips}", file=sys.stderr)
        return 2

    rc = 0
    if args.comando in ("analyze", "all"):
        report = analyze_files(sources)
        md, _ = write_report(report, args.relatorio)
        print(f"relatório de estrutura: {md}")
        if any(d["violacoes"] for d in report["dependencias"].values()):
            print("ATENÇÃO: premissas do modelo violadas; os valores divergentes "
                  "irão para a tabela conflito. Veja o relatório.")

    if args.comando in ("load", "all"):
        conn = connect(args.db)
        try:
            failures = load_all(conn, sources)
        finally:
            conn.close()
        for name, err in failures:
            print(f"FALHOU: {name}: {err}", file=sys.stderr)
        if failures:
            rc = 1

    if args.comando in ("validate", "all"):
        conn = connect(args.db)
        try:
            rep = validate(conn, sources, restream=not args.sem_reler)
        finally:
            conn.close()
        print(format_report(rep))
        if not rep.ok:
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest -v`
Expected: 43 passed, 1 deselected (the `real` test)

- [ ] **Step 5: Run the real-data test**

Run: `python -m pytest -m real -v`
Expected: 1 passed, in about 1–3 minutes. If `votos_inconsistente` or `aptos_inconsistente` is not 0, stop. Look at `v_anomalia_apuracao` in the test DB and report the finding to the user before changing anything. Those checks passed in the pre-plan probe of this file.

- [ ] **Step 6: Commit**

```bash
git add build_db.py tests/test_build_db.py
git commit -m "feat: CLI build_db.py (analyze, load, validate, all)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Full run on all 55 zips

**Files:**
- Generated (not committed): `schema_report.md`, `schema_report.json`, `eleicoes2026.db`
- Modify (only if the analysis contradicts the spec): `docs/superpowers/specs/2026-10-28-eleicoes-sqlite-design.md`

- [ ] **Step 1: Check free disk space**

Run: `df -h /d`
Expected: at least 20 GB free. 783 GB were free on 2026-10-28.

- [ ] **Step 2: Run the structure analysis on everything**

Run: `python build_db.py analyze` (in the background, because it streams 32 GB)
Expected:
- It prints `relatório de estrutura: schema_report.md`.
- The report has 55 files and no files under "Arquivos com problemas".
- The dependencies section reads "Nenhuma violação".

If there are violations, report them to the user with the examples from the report before you load. A violation does not block the load (the loader records conflicts), but the user should decide whether the model needs to change.

- [ ] **Step 3: Load everything**

Run: `python build_db.py load` (in the background; expect tens of minutes to a few hours)
Expected:
- Each zip logs `N linhas, M votos em Xs`.
- The run ends with `criando índices e estatísticas...`.
- Exit code 0.

If it is interrupted, re-running the same command resumes it.

- [ ] **Step 4: Validate everything**

Run: `python build_db.py validate` (in the background; it re-streams all CSVs)
Expected:
- `Validação: OK`.
- Audit counts are printed. Record them for the user: they are findings, not errors.

- [ ] **Step 5: Report results**

Report to the user:
- the DB size (`ls -la eleicoes2026.db`)
- the total rows in `votos`
- the load time
- the validation output
- any dependency violations or `conflito` rows, with examples (`SELECT * FROM conflito LIMIT 20`)

Update the spec only if a finding changed the model, and commit that change:

```bash
git add docs/superpowers/specs/2026-10-28-eleicoes-sqlite-design.md
git commit -m "docs: ajusta o spec com os achados da carga completa

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
