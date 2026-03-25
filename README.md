# risk_assessment_list

Python library for determining whether a chemical is on Japan's published
risk-assessment obligation lists and for surfacing GHS-based hazard notices.

## What it does

- downloads the requested JOHAS, MHLW, and NITE reference files into `reference/`
- converts the published MHLW obligation lists and NITE GHS classifications into
  a bundled SQLite database
- exposes exact lookup, synonym-aware fuzzy candidate search, and mixture
  evaluation APIs

## Data sources

- `https://cheminfo.johas.go.jp/step/list.html`
- `https://www.mhlw.go.jp/content/11300000/001168179.xlsx`
- `https://www.mhlw.go.jp/content/11300000/001474394.xlsx`
- `https://www.chem-info.nite.go.jp/chem/ghs/files/list_nite_all.xlsx`

## Setup

```bash
uv sync --extra build --extra test
uv run python scripts/fetch_reference.py
uv run python scripts/build_db.py
```

`fetch_reference.py` is offline-first by default. If the files already exist in
`reference/` and their `sha256` values match `reference/manifest.json`, the
script uses them as cache hits and skips the network.

```bash
uv run python scripts/fetch_reference.py
uv run python scripts/fetch_reference.py --refresh
uv run python scripts/fetch_reference.py --refresh-key nite_list_nite_all
```

## Build Workflow

`scripts/build_db.py` now builds the packaged database in stages:

1. stage raw MHLW and NITE workbook rows with file, sheet, and row provenance
2. normalize them into canonical substance, legal obligation, GHS classification,
   pictogram, and alias tables
3. validate row counts and URL cleanup rules
4. publish the SQLite snapshot atomically into `src/risk_assessment_list/data/`

The packaged SQLite snapshot is query-oriented rather than blob-oriented. GHS
classifications and fuzzy-search aliases are stored in normalized tables, and
an `FTS5` index is built over aliases for runtime discovery.

The build also exports `reference/generated_synonyms.sqlite3`, a sidecar
database containing synonym groups, identifiers, and aliases for the hazardous
substances covered by the generated data set.

## Usage

```python
from risk_assessment_list import MixtureComponent
from risk_assessment_list import evaluate_mixture
from risk_assessment_list import evaluate_substance
from risk_assessment_list import search_substances

candidates = search_substances("ホルムアルデヒド")
fuzzy_candidates = search_substances("maleik acid", mode="fuzzy")

result = evaluate_substance("50-00-0")
print(result.legal_ra_required)
print(result.ghs_notice_required)
print(result.ghs_pictograms)

mixture = evaluate_mixture(
    [
        MixtureComponent(identifier="50-00-0", weight_percent=0.2),
        MixtureComponent(identifier="7732-18-5", weight_percent=99.8),
    ]
)
print(mixture.legal_ra_required)
```

## Notes

- `legal_ra_required` follows the published union of the downloaded MHLW lists.
- `ghs_notice_required` is broader and fires whenever matched NITE data contains
  an assigned GHS classification, including environmental classes.
- `search_substances(..., mode="balanced")` is the default. It prefers exact
  aliases and strong prefix matches, and returns `[]` for obvious garbage input.
- `search_substances(..., mode="fuzzy")` is the broad finder mode. It keeps
  typo tolerance and can return wider near matches and derivatives.
- search returns scored candidates only; it does not perform the legal decision
  on its own.
- fuzzy search expands explicit aliases and conservative synonyms such as
  parenthetical aliases, Greek-letter variants, English/Japanese name pairs,
  and a small curated abbreviation set like `PCB`, `DDT`, `TCE`, and `PCE`.
