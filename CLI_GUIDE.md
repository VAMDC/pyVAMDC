# VAMDC Command-Line Interface (CLI) Guide

The `vamdc` command-line tool provides an easy way to query atomic and molecular spectroscopic data from the VAMDC (Virtual Atomic and Molecular Data Centre) infrastructure.

## Installation

The CLI is included with pyVAMDC. Simply install the package:

```bash
pip install pyVAMDC
```

Or with development dependencies:

```bash
pip install pyVAMDC[dev]
```

## Quick Start

### Get Available Data Nodes

Retrieve and cache the list of all VAMDC data nodes:

```bash
vamdc get nodes
vamdc get nodes --format csv --output nodes.csv
vamdc get nodes --format json --output nodes.json
```

### Get Chemical Species

Download and cache the complete chemical species database:

```bash
vamdc get species
vamdc get species --format csv --output species.csv
vamdc get species --format excel --output species.xlsx
```

Filter by species name:

```bash
vamdc get species --filter-by "name:CO"
vamdc get species --filter-by "name:H2O"
```

### Get Spectral Lines

Query spectral lines for a specific species in a wavelength range:

```bash
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --lambda-min=3000 \
  --lambda-max=5000
```

Query a specific VAMDC node:

```bash
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=astrophysics \
  --lambda-min=3000 \
  --lambda-max=5000
```

Output as CSV:

```bash
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --lambda-min=3000 \
  --lambda-max=5000 \
  --format csv \
  --output lines.csv
```

## Command Reference

### Global Options

- `--verbose, -v`: Enable verbose output with detailed logging
- `--cache-dir TEXT`: Specify custom cache directory (default: `~/.vamdc_cache`)

### `vamdc get nodes`

Get list of VAMDC data nodes and cache them locally.

**Options:**
- `-f, --format [json|csv|table]`: Output format (default: table)
- `-o, --output PATH`: Save output to file
- `--refresh`: Force refresh cache (ignore cached data)

**Examples:**
```bash
vamdc get nodes
vamdc get nodes --format csv --output nodes.csv
vamdc get nodes --refresh  # Force update from server
```

### `vamdc get species`

Get list of chemical species and cache them locally.

**Options:**
- `-f, --format [json|csv|parquet|excel|table]`: Output format (default: table)
- `-o, --output PATH`: Save output to file
- `--refresh`: Force refresh cache
- `--filter-by TEXT`: Filter by criteria (format: "column:value")

**Examples:**
```bash
vamdc get species
vamdc get species --format csv --output species.csv
vamdc get species --format excel --output species.xlsx
vamdc get species --filter-by "name:CO"
vamdc get species --filter-by "speciesType:molecule"
```

**Available Columns for Filtering:**
- `name`: Species name
- `stoichiometricFormula`: Chemical formula
- `InChIKey`: Chemical identifier
- `speciesType`: Type (molecule, atom, particle)
- `massNumber`: Mass number (numeric range: e.g., "100-200")
- `charge`: Electric charge (numeric range)

### `vamdc get lines`

Get spectral lines for a species in a wavelength range.

**Required Options:**
- `--inchikey TEXT`: InChIKey of the species (required)
- `--lambda-min FLOAT`: Minimum wavelength in Angstrom (required)
- `--lambda-max FLOAT`: Maximum wavelength in Angstrom (required)

**Optional Options:**
- `--node TEXT`: Specific VAMDC node to query
- `-f, --format [xsams|csv|json|table]`: Output format (default: xsams)
- `-o, --output PATH`: Save output to file

**Examples:**
```bash
# Query carbon monoxide (CO) spectral lines
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --lambda-min=3000 \
  --lambda-max=5000

# Query specific node with CSV output
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=astrophysics \
  --lambda-min=3000 \
  --lambda-max=5000 \
  --format csv \
  --output lines.csv

# Query water (H2O) in infrared
vamdc get lines \
  --inchikey=XLYOFNOQVQJGKK-UHFFFAOYSA-N \
  --lambda-min=2000 \
  --lambda-max=100000 \
  --format json \
  --output h2o_ir.json
```

**Note:** Default XSAMS XML files are saved to `./XSAMS/` directory

### `vamdc cache`

Manage local cache.

**Subcommands:**

#### `vamdc cache clear`
Remove all cached data:
```bash
vamdc cache clear
```

#### `vamdc cache status`
View cache status and metadata:
```bash
vamdc cache status
```

Output example:
```
Cache directory: /home/user/.vamdc_cache
Expiration time: 24 hours

Nodes: VALID (cached at 2024-10-16 10:30:45.123456)
Species: VALID (cached at 2024-10-16 10:31:20.456789)
```

## Caching System

The CLI automatically caches downloaded data to avoid redundant network requests:

- **Cache Location:** `~/.vamdc_cache/`
- **Expiration Time:** 24 hours
- **Cached Data:**
  - Node list (JSON format)
  - Species database (Parquet format for efficiency)

### Cache Management

Check cache status:
```bash
vamdc cache status
```

Clear cache:
```bash
vamdc cache clear
```

Force refresh (ignore cache):
```bash
vamdc get nodes --refresh
vamdc get species --refresh
```

## Output Formats

### `vamdc get nodes` and `vamdc get species`

- **table**: Pretty-printed table (default)
- **csv**: Comma-separated values
- **json**: JSON format
- **parquet**: Parquet binary format (species only)
- **excel**: Excel spreadsheet (species only)

### `vamdc get lines`

- **xsams**: XSAMS XML format (default) - saved to `./XSAMS/` directory
- **csv**: Comma-separated values
- **json**: JSON format
- **table**: Pretty-printed table

## Verbose Output

Enable detailed logging to troubleshoot issues:

```bash
vamdc -v get nodes
vamdc --verbose get species
vamdc -v get lines --inchikey=... --lambda-min=... --lambda-max=...
```

## Common Use Cases

### Download All Available Data

```bash
# Get nodes
vamdc get nodes --format csv --output all_nodes.csv

# Get species
vamdc get species --format csv --output all_species.csv

# Get all species with computed properties
vamdc get species --format excel --output species_database.xlsx
```

### Find a Specific Species

```bash
# Get all species info as CSV for analysis
vamdc get species --format csv --output all_species.csv

# Then filter locally (e.g., with grep or other tools)
grep "CO" all_species.csv
```

### Query Multiple Species

```bash
# First download the species database to find InChIKeys
vamdc get species --format csv --output species.csv

# Query each species
vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --lambda-min=3000 --lambda-max=5000 --format csv --output co_lines.csv
vamdc get lines --inchikey=XLYOFNOQVQJGKK-UHFFFAOYSA-N --lambda-min=2000 --lambda-max=100000 --format csv --output h2o_lines.csv
```

### Export for External Analysis

```bash
# Export all species as JSON
vamdc get species --format json --output species.json

# Export spectral lines as JSON
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --lambda-min=3000 \
  --lambda-max=5000 \
  --format json \
  --output lines.json
```

## Environment Variables

Currently, the CLI uses the following defaults:

- Cache directory: `~/.vamdc_cache/` (can be overridden with `--cache-dir`)
- Cache expiration: 24 hours (built-in)

## Troubleshooting

### "Node not found" error

Ensure you're using the correct node name. Get available nodes with:
```bash
vamdc get nodes --format csv | cut -d, -f1 | head -20
```

### "Species with InChIKey ... not found"

Make sure the InChIKey is correct. Get available species InChIKeys with:
```bash
vamdc get species --format csv --output species.csv
grep "CO" species.csv | cut -d, -f4  # InChIKey is usually column 4
```

### Cache issues

If you experience unexpected behavior, clear the cache:
```bash
vamdc cache clear
```

Then retry your command.

### Network timeouts

Large queries may take time. If you experience timeouts, try:
1. Reducing the wavelength range for spectral line queries
2. Targeting a specific node instead of all nodes
3. Checking your internet connection

## Advanced Usage

### Using the CLI Programmatically

While primarily a command-line tool, you can also import and use the CLI functions directly in Python:

```python
from spectral.cli import format_output, apply_filter
from spectral import species

# Get all species
df_species, df_nodes = species.getAllSpecies()

# Apply custom filter
filtered = apply_filter(df_species, "name:CO")

# Format as CSV
csv_output = format_output(filtered, 'csv')
print(csv_output)
```

### Custom Scripts

Create shell scripts for common queries:

```bash
#!/bin/bash
# query_co.sh - Query CO spectral lines in infrared

vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --lambda-min=2500 \
  --lambda-max=15000 \
  --format csv \
  --output co_infrared_$(date +%Y%m%d).csv
```

## Getting Help

View command help:
```bash
vamdc --help
vamdc get --help
vamdc get nodes --help
vamdc get species --help
vamdc get lines --help
vamdc cache --help
```

## Citation

If you use the VAMDC CLI in your research, please cite the underlying pyVAMDC library:

```bibtex
@software{vamdc2024,
  title={pyVAMDC: Python Virtual Atomic and Molecular Data Centre},
  author={Zwölf, Carlo Maria and Moreau, Nicolas},
  year={2024},
  url={https://github.com/VAMDC/pyVAMDC}
}
```

## License

This CLI is part of pyVAMDC, which is licensed under the MIT License. See the LICENSE file for details.

## Contributing

Contributions are welcome! Please open issues or pull requests on the [GitHub repository](https://github.com/VAMDC/pyVAMDC).

## Support

For issues or questions:
- GitHub Issues: https://github.com/VAMDC/pyVAMDC/issues
- VAMDC Website: https://www.vamdc.org/

---

**Version:** 0.1 (Alpha)
**Last Updated:** 2024-10-16
