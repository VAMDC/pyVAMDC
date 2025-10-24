---
name: vamdc
description: Query atomic and molecular spectroscopic data using the pyVAMDC command-line tool. Provides access to VAMDC (Virtual Atomic and Molecular Data Centre) infrastructure for retrieving spectral lines, species metadata, and node information. Use this skill to fetch spectroscopic data from various databases, discover available species, or count results before downloading.
---

# VAMDC - Spectroscopic Data Queries

## Overview

The pyVAMDC CLI provides command-line access to atomic and molecular spectroscopic data from VAMDC, a federation of databases containing transition frequencies, oscillator strengths, collision data, and radiative properties.

**Background:** VAMDC aggregates data from ~30 specialized databases covering atoms, molecules, and their interactions across different wavelength regimes (UV, optical, infrared, microwave). Each database is a "node" serving data via a standardized interface.

**Access model:** Always access data through the CLI—never bypass it to access TAP endpoints directly.

## Setup

Clone and install pyVAMDC:

```bash
git clone https://github.com/VAMDC/pyVAMDC.git
cd pyVAMDC
```

If you get a network error, try it this way:

```bash
cd /home/claude && curl -L https://github.com/VAMDC/pyVAMDC/archive/refs/heads/main.zip -o pyVAMDC.zip && unzip -q pyVAMDC.zip && mv pyVAMDC-main pyVAMDC && ls -la pyVAMDC | head -20
```

Get dependencies with
```bash
uv sync
```

Run commands using:

```bash
uvx --from . vamdc [COMMAND]
```


## Core Workflows

### 1. Explore Available Data

List all VAMDC nodes (databases):

```bash
vamdc get nodes  # prints to stdout
vamdc get nodes --format csv --output nodes.csv
```

List all species in the database:

```bash
vamdc get species # prints to stdout
vamdc get species --format csv --output species.csv
vamdc get species --format excel --output species.xlsx
```

Filter species by name or other criteria:

```bash
vamdc get species --filter-by "name:CO"
vamdc get species --filter-by "name:H2O"
vamdc get species --filter-by "massNumber:100-200"
```

Species and nodes are cached, see `vamdc cache status`.

### 2. Query Spectral Lines

**Workflow:** (1) Find species InChIKey, (2) optionally check data availability, (3) download lines.

**Step 1: Find InChIKey**

```bash
vamdc get species --filter-by "name:Magnesium"
# Extract the InChIKey from output
```

**Step 2 (optional): Preview data before downloading**

Check how many lines will be retrieved without downloading full data:

```bash
vamdc count lines \
  --inchikey=FYYHWMGAXLPEAU-UHFFFAOYSA-N \
  --node=vald \
  --lambda-min=2500 \
  --lambda-max=5000
```

**Step 3: Download lines**

```bash
vamdc get lines \
  --inchikey=FYYHWMGAXLPEAU-UHFFFAOYSA-N \
  --node=vald \
  --lambda-min=2500 \
  --lambda-max=5000 \
  --format csv \
  --output mg_lines.csv
```

### 3. Output Formats

**Nodes:**
- `table` (default): Human-readable terminal output
- `csv`: Comma-separated values
- `json`: JSON array

**Species:**
- `table` (default): Human-readable terminal output
- `csv`: Comma-separated values
- `json`: JSON array
- `excel`: Excel spreadsheet

**Spectral lines:**
- `xsams` (default): XSAMS XML (saved automatically)
- `csv`: Tabular format (wavelength, oscillator strength, state labels, etc.)
- `json`: JSON array
- `table`: Human-readable table

## Identifying Nodes and Species

### Node Matching

The `--node` parameter accepts multiple formats:

- **Short name:** `vald`, `basecol`, `hitran`
- **IVO identifier:** `ivo://vamdc/vald/uu/django`
- **TAP endpoint URL:** `http://vald.astro.uu.se/atoms-12.07/tap/`

Retrieve available nodes:

```bash
vamdc get nodes --format csv | grep -i "keyword"
```

### Finding InChIKeys

InChI (International Chemical Identifier) is a standardized representation of molecular structure. InChIKey is a 27-character hash of the InChI.

To find an InChIKey for a species:

```bash
vamdc get species --filter-by "name:CO"
# Look for the "InChIKey" column in output
```

Or download and search locally:

```bash
vamdc get species --format csv --output species.csv
grep -i "your_species_name" species.csv
```

## Cache Management

The CLI automatically caches nodes, species lists, and metadata to avoid redundant network requests.

**Check cache status:**

```bash
vamdc cache status
```

**Clear cache:**

```bash
vamdc cache clear
```

**Force refresh without clearing:**

```bash
vamdc get species --refresh
vamdc get nodes --refresh
```

**Override cache location:**

```bash
export VAMDC_CACHE_DIR=/path/to/custom/cache
```

## Debugging

**Enable verbose output:**

```bash
vamdc --verbose get lines --inchikey=... --node=...
```

**Common issues:**

- **"Node not found"**: Run `vamdc get nodes` to verify node name/ID
- **"No species with InChIKey"**: Verify InChIKey is correct via `vamdc get species`
- **Unexpected cache behavior**: Clear cache with `vamdc cache clear`

**View full command help:**

```bash
vamdc --help
vamdc get --help
vamdc get lines --help
vamdc count --help
```

## Key Parameters Reference

See `references/parameter_guide.md` for detailed parameter descriptions, wavelength units, and advanced filtering options.

## Common Task Patterns

**Get all carbon monoxide lines from a specific node:**

1. Find InChIKey: `vamdc get species --filter-by "name:CO"`
2. Query: `vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --node=hitran --format csv --output co_lines.csv`

**Build a spectral catalog across multiple nodes:**

1. Get species list: `vamdc get species --format csv --output species.csv`
2. For each species of interest, query multiple nodes and aggregate results
3. Use postprocessing (Python/awk/etc.) to combine and filter

**Sample specific wavelength regions:**

- Use `--lambda-min` and `--lambda-max` (in Ångströms) to restrict queries
- Example: UV region (100–4000 Å), optical (3000–10000 Å), IR (10000–100000 Å)
