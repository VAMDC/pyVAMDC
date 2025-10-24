# pyVAMDC CLI Parameter Guide

## Spectral Line Query Parameters

### Required Parameters

**`--inchikey TEXT`**
- 27-character identifier for a chemical species
- Standardized hash of the InChI (International Chemical Identifier)
- Find via: `vamdc get species --filter-by "name:SPECIES_NAME"`
- Example: `FYYHWMGAXLPEAU-UHFFFAOYSA-N` (neutral magnesium)

**`--node TEXT`**
- Database node identifier, accepts multiple formats:
  - Short name: `vald`, `hitran`, `basecol`, `chianti`
  - IVO ID: `ivo://vamdc/vald/uu/django`
  - TAP URL: `http://vald.astro.uu.se/atoms-12.07/tap/`
- List all nodes: `vamdc get nodes`

### Wavelength Parameters

**`--lambda-min FLOAT`** (default: 0.0)
- Minimum wavelength in Ångströms (Å)
- Typical ranges:
  - UV: 100–4000 Å
  - Optical: 3000–10000 Å (covers visible + near-UV/IR)
  - Infrared: 10000–100000 Å
  - Microwave: 100000+ Å

**`--lambda-max FLOAT`** (default: 1e9)
- Maximum wavelength in Ångströms

### Output Parameters

**`--format [xsams|csv|json|table]`**
- `xsams`: XSAMS XML (default for spectral lines, saved to file automatically)
- `csv`: Comma-separated values (most useful for further processing)
- `json`: JSON array (structured data)
- `table`: Human-readable terminal table

**`--output PATH`**
- Save output to specified file
- Extensions determine file format when not specified
- Examples: `output.csv`, `output.json`, `output.xlsx`

## Species Query Parameters

### Filter Parameters

**`--filter-by TEXT`**
- Filter species by column and value
- Format: `"column:value"`

**String matching:**
- `--filter-by "name:CO"` — case-insensitive substring match
- Returns all species whose name contains "CO"

**Numeric range:**
- `--filter-by "massNumber:100-200"`
- Returns species with mass between 100 and 200 amu

### Species Output Formats

**`--format [json|csv|excel|table]`**
- `csv`: Standard tabular (grep-searchable)
- `excel`: XLSX format (good for spreadsheet analysis)
- `json`: JSON array (API-style output)
- `table`: Terminal display

## Global Options

**`--verbose, -v`**
- Enable detailed logging output
- Useful for debugging connection issues or large queries
- Usage: `vamdc --verbose get lines ...`

**`--refresh`**
- Force refresh of cached data
- Ignores 24-hour cache expiration
- Useful when database updates may have occurred

## Common Parameter Combinations

**Query neutral oxygen from VALD (optical region):**
```bash
vamdc get lines \
  --inchikey=QVGXLLKOCUKJST-UHFFFAOYSA-N \
  --node=vald \
  --lambda-min=3000 \
  --lambda-max=10000 \
  --format csv \
  --output oxygen_optical.csv
```

**Query water from HITRAN (infrared):**
```bash
vamdc get lines \
  --inchikey=XLYOFNOQVNFUOH-UHFFFAOYSA-N \
  --node=hitran \
  --lambda-min=1000 \
  --lambda-max=100000 \
  --format csv \
  --output water_ir.csv
```

**Preview result count before downloading:**
```bash
vamdc count lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=basecol
```

**Export all species with mass 28 amu (e.g., N2, CO):**
```bash
vamdc get species \
  --filter-by "massNumber:27-29" \
  --format csv \
  --output species_mass28.csv
```
