# VAMDC CLI Quick Start

Get the `vamdc` command-line tool working in seconds!

## Installation

```bash
pip install pyVAMDC
```

## Essential Commands

### Get Data Nodes
```bash
vamdc get nodes
vamdc get nodes --format csv --output nodes.csv
```

### Get Chemical Species
```bash
vamdc get species
vamdc get species --format csv --output species.csv
vamdc get species --filter-by "name:CO"
```

### Get Spectral Lines
```bash
# Find InChIKey for a species first
vamdc get species --format csv | grep "CO" | cut -d, -f4

# Then query lines (example with CO)
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --lambda-min=3000 \
  --lambda-max=5000

# Export as CSV or JSON
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --lambda-min=3000 --lambda-max=5000 \
  --format csv --output lines.csv
```

### Manage Cache
```bash
vamdc cache status    # View cache info
vamdc cache clear     # Clear all cached data
vamdc get nodes --refresh  # Force refresh from server
```

## Common Workflows

### 1. Download Everything
```bash
vamdc get nodes --format csv --output nodes.csv
vamdc get species --format excel --output species.xlsx
```

### 2. Query Multiple Species
```bash
# Save species list
vamdc get species --format csv --output species.csv

# Get InChIKeys and query each one
for inchikey in $(grep "^Alkali Metal" species.csv | cut -d, -f4); do
  vamdc get lines --inchikey=$inchikey --lambda-min=1000 --lambda-max=10000 --format csv --output "${inchikey}.csv"
done
```

### 3. Filter Species
```bash
vamdc get species --filter-by "speciesType:molecule" --format csv --output molecules.csv
vamdc get species --filter-by "name:H2O" --format csv --output h2o.csv
```

## All Command Options

```bash
vamdc --help                    # Show main help
vamdc get --help                # Show get commands
vamdc get nodes --help          # Show nodes command options
vamdc get species --help        # Show species command options
vamdc get lines --help          # Show lines command options
vamdc cache --help              # Show cache commands
```

## Output Formats

| Command | Formats |
|---------|---------|
| `vamdc get nodes` | table, json, csv |
| `vamdc get species` | table, json, csv, parquet, excel |
| `vamdc get lines` | xsams, json, csv, table |

## Tips

- **First time is slower**: First query caches data (takes 5-30s), subsequent queries are instant
- **Verbose mode**: Use `-v` flag for detailed logging: `vamdc -v get nodes`
- **Save to file**: Use `-o` flag to save: `vamdc get species -o data.csv`
- **Force refresh**: Use `--refresh` to ignore cache: `vamdc get nodes --refresh`
- **Custom cache location**: Use `--cache-dir`: `vamdc --cache-dir /tmp/cache get species`

## Troubleshooting

```bash
# Check if everything is installed correctly
vamdc --help

# View cache status
vamdc cache status

# Clear cache if issues occur
vamdc cache clear

# See detailed errors
vamdc -v get species
```

## Next Steps

- Read **CLI_GUIDE.md** for comprehensive documentation
- Read **CLI_SUMMARY.md** for technical implementation details
- Check **README.md** for library information

---

**Need help?** Run any command with `--help` or check the full documentation.
