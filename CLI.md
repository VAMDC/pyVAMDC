# VAMDC Command-Line Interface

The `vamdc` command-line tool provides access to atomic and molecular spectroscopic data from the VAMDC (Virtual Atomic and Molecular Data Centre) infrastructure.

## Installation

### Recommended: Using uv

Install [uv](https://docs.astral.sh/uv/) and add a shell alias:

```bash
# Install uv (if not already installed)
# See https://docs.astral.sh/uv/ for installation instructions

# Add to ~/.bashrc or ~/.zshrc
alias vamdc='uv run vamdc'
```

After adding the alias, restart your shell or run `source ~/.bashrc` (or `~/.zshrc`).

### Alternative: Using pip

```bash
pip install pyVAMDC
```

## Command Structure

The CLI is organized into command groups:

```
vamdc
├── get          # Retrieve data from VAMDC
│   ├── nodes    # List available data nodes
│   ├── species  # List chemical species
│   └── lines    # Query spectral lines
├── count        # Inspect metadata without downloading
│   └── lines    # Get line counts and metadata
└── cache        # Manage local cache
    ├── status   # Show cache information
    └── clear    # Remove cached data
```

## Global Options

- `--verbose, -v`: Enable verbose output with detailed logging

## Commands

### `vamdc get nodes`

Get list of VAMDC data nodes and cache them locally.

**Options:**
- `-f, --format [json|csv|table]`: Output format (default: table)
- `-o, --output PATH`: Save output to file
- `--refresh`: Force refresh cache

**Examples:**
```bash
vamdc get nodes
vamdc get nodes --format csv --output nodes.csv
vamdc get nodes --refresh
```

### `vamdc get species`

Get list of chemical species and cache them locally.

**Options:**
- `-f, --format [json|csv|excel|table]`: Output format (default: table)
- `-o, --output PATH`: Save output to file
- `--refresh`: Force refresh cache
- `--filter-by TEXT`: Filter by criteria (format: "column:value")

**Examples:**
```bash
vamdc get species
vamdc get species --format csv --output species.csv
vamdc get species --format excel --output species.xlsx
vamdc get species --filter-by "name:CO"
```

**Filter format:**
- String matching: `"name:CO"` (case-insensitive substring match)
- Numeric range: `"massNumber:100-200"`

### `vamdc get lines`

Get spectral lines for a species in a wavelength range.

**Required Options:**
- `--inchikey TEXT`: InChIKey of the species
- `--node TEXT`: Node identifier (shortname, IVO ID, or TAP endpoint)

**Optional Options:**
- `--lambda-min FLOAT`: Minimum wavelength in Angstrom (default: 0.0)
- `--lambda-max FLOAT`: Maximum wavelength in Angstrom (default: 1.0e9)
- `-f, --format [xsams|csv|json|table]`: Output format (default: xsams)
- `-o, --output PATH`: Output file path

**Examples:**
```bash
# Query carbon monoxide (CO) spectral lines from basecol node
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=basecol \
  --lambda-min=3000 \
  --lambda-max=5000

# Output as CSV
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=basecol \
  --lambda-min=3000 \
  --lambda-max=5000 \
  --format csv \
  --output lines.csv
```

**Node matching:**
The `--node` parameter matches against:
- TAP endpoint URL
- IVO identifier
- Short name (if available in metadata)

**Output formats:**
- `xsams`: XSAMS XML files (default, saved automatically)
- `csv`: Tabular format with spectral line data
- `json`: JSON array of line records
- `table`: Human-readable table

### `vamdc count lines`

Inspect HEAD metadata for spectroscopic line queries without downloading full data.

**Required Options:**
- `--inchikey TEXT`: InChIKey of the species
- `--node TEXT`: Node identifier

**Optional Options:**
- `--lambda-min FLOAT`: Minimum wavelength in Angstrom (default: 0.0)
- `--lambda-max FLOAT`: Maximum wavelength in Angstrom (default: 1.0e9)

**Examples:**
```bash
vamdc count lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=basecol \
  --lambda-min=3000 \
  --lambda-max=5000
```

This command performs HEAD requests to retrieve count headers without downloading full datasets, showing:
- Available count headers per sub-query
- Aggregated totals across all sub-queries

### `vamdc cache status`

Show cache status and metadata.

**Example:**
```bash
vamdc cache status
```

Output shows:
- Cache directory location
- Expiration time (24 hours)
- Status of each cached dataset (VALID, EXPIRED, or NOT CACHED)
- Cache timestamps

### `vamdc cache clear`

Remove all cached data.

**Example:**
```bash
vamdc cache clear
```

## Caching System

The CLI automatically caches downloaded data to avoid redundant network requests.

**Cache location:**
- Default: `~/.cache/vamdc/`
- Override with `VAMDC_CACHE_DIR` environment variable

**Cached data:**
- Nodes list (CSV format)
- Species database (CSV format)
- Species-nodes mapping (CSV format)

**Cache expiration:**
- 24 hours from last fetch
- Use `--refresh` flag to force update
- Check status with `vamdc cache status`

**Cache files:**
- `nodes.csv` - VAMDC data nodes
- `species.csv` - Chemical species database
- `species_nodes.csv` - Species-to-node mappings
- `*_timestamp.json` - Metadata files tracking cache timestamps

## Environment Variables

- `VAMDC_CACHE_DIR`: Override default cache directory location

## Finding Species InChIKeys

To find the InChIKey for a species:

```bash
# Download species list
vamdc get species --format csv --output species.csv

# Search for your species (e.g., CO)
grep -i "CO" species.csv

# Or use the filter option
vamdc get species --filter-by "name:CO"
```

## Common Workflows

### Explore available data

```bash
# List all nodes
vamdc get nodes

# Get full species database
vamdc get species --format csv --output species.csv

# Find a specific molecule
vamdc get species --filter-by "name:H2O"
```

### Query spectral lines

```bash
# First, find the InChIKey
vamdc get species --filter-by "name:CO"

# Check available data before downloading
vamdc count lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=basecol \
  --lambda-min=3000 \
  --lambda-max=5000

# Download the data
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=basecol \
  --lambda-min=3000 \
  --lambda-max=5000 \
  --format csv \
  --output co_lines.csv
```

## Troubleshooting

### "Node not found" error

Ensure you're using a valid node identifier. Check available nodes:

```bash
vamdc get nodes
```

### "No species with InChIKey ... found"

Verify the InChIKey is correct:

```bash
vamdc get species --format csv --output species.csv
grep "YOUR_INCHIKEY" species.csv
```

### Cache issues

Clear the cache if you experience unexpected behavior:

```bash
vamdc cache clear
```

### Enable verbose output

For debugging, use the `--verbose` flag:

```bash
vamdc --verbose get lines --inchikey=... --node=... --lambda-min=... --lambda-max=...
```

## Getting Help

View command help:

```bash
vamdc --help
vamdc get --help
vamdc get nodes --help
vamdc get species --help
vamdc get lines --help
vamdc count --help
vamdc count lines --help
vamdc cache --help
```
