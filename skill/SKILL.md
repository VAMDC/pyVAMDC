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

**For AI agents - CRITICAL BEST PRACTICES:**

1. **Always use `--quiet` flag** to minimize output and avoid context saturation
2. **Always use count-then-get workflow** when searching for lines - never query lines directly without counting first
3. **Always redirect stderr** (`2>errors.log`) to avoid RDKit warning clutter in logs

```bash
# ✓ CORRECT: Count first, then get only if lines exist
uvx --from . vamdc --quiet count lines --inchikey=... --node=... 2>errors.log
# If count > 0, then:
uvx --from . vamdc --quiet get lines --inchikey=... --node=... 2>>errors.log

# ✗ WRONG: Never query lines directly without counting first
uvx --from . vamdc --quiet get lines --inchikey=... --node=...  # Wasteful!
```

**Suppressing stderr warnings:** The `--quiet` flag suppresses most output but not warnings from underlying libraries (like RDKit InChI conversion warnings). For cleaner logs when running programmatic queries, redirect stderr to a log file:

```bash
# Bash/shell commands
vamdc --quiet get species --filter-by "name:CO" --format csv --output co_species.csv 2>vamdc_errors.log
```

The file pyVAMDC/CLI.md contains the full documentation of this command. 

## Core Workflows

### 1. Explore Available Data

List all VAMDC nodes (databases):

```bash
vamdc --quiet get nodes --format csv --output nodes.csv 2>vamdc_errors.log
```

List all species in the database:

```bash
vamdc --quiet get species --format csv --output species.csv 2>vamdc_errors.log
vamdc --quiet get species --format excel --output species.xlsx 2>vamdc_errors.log
```

Filter species by name or other criteria:

```bash
vamdc --quiet get species --filter-by "name:CO" --format csv --output co_species.csv 2>vamdc_errors.log
vamdc --quiet get species --filter-by "name:H2O" --format csv --output h2o_species.csv 2>vamdc_errors.log
vamdc --quiet get species --filter-by "massNumber:100-200" --format csv --output species_filtered.csv 2>vamdc_errors.log
```

Species and nodes are cached, see `vamdc cache status`.

### 2. Query Spectral Lines

**Workflow:** (1) Find species InChIKey, (2) **ALWAYS count lines first**, (3) only then download lines if count > 0.

**Step 1: Find InChIKey**

```bash
vamdc --quiet get species --filter-by "name:Magnesium" --format csv --output mg_species.csv 2>errors.log
# Extract the InChIKey from output file
```

**Step 2: ALWAYS count lines before downloading (MANDATORY for AI agents)**

From the result of the _vamdc get species_ you can check if a given species is present in a given database. 
If the database does not contain the given species we are looking for, it is useless to query spectroscopic data on it. 

**CRITICAL:** Always use `count lines` before `get lines`. The count command is lightweight (HEAD request only) and prevents wasteful downloads of empty datasets. This is especially important when iterating through many species.

To check how many lines will be retrieved without downloading full data:

```bash
vamdc --quiet count lines \
  --inchikey=FYYHWMGAXLPEAU-UHFFFAOYSA-N \
  --node=vald \
  --lambda-min=2500 \
  --lambda-max=5000 \
  2>vamdc_errors.log
```

**Step 3: Download lines (ONLY if count > 0)**

**IMPORTANT:** Only proceed with `get lines` if the count from Step 2 showed radiative transitions exist.

Units for lambda to be submitted are in Angstrom.
```bash
# Only run this if vamdc-count-radiative > 0 from Step 2
vamdc --quiet get lines \
  --inchikey=FYYHWMGAXLPEAU-UHFFFAOYSA-N \
  --node=vald \
  --lambda-min=2500 \
  --lambda-max=5000 \
  --format csv \
  --output mg_lines.csv \
  2>>errors.log
```

**Using --accept-truncation flag:** When querying large datasets or iterating through many species, use the `--accept-truncation` flag to accept partial results without recursive splitting:

```bash
vamdc --quiet get lines \
  --inchikey=FYYHWMGAXLPEAU-UHFFFAOYSA-N \
  --node=cdms \
  --lambda-min=2500 \
  --lambda-max=5000 \
  --accept-truncation \
  --format csv \
  --output lines.csv\
  2>vamdc_errors.log
```

If you need to convert units to submit the previous query in Angstrom, you can use 
```bash
# Convert 500 nanometers to angstrom
vamdc convert energy 500 --from-unit=nanometer --to-unit=angstrom

# Convert 1.5 eV to angstrom
vamdc convert energy 1.5 --from-unit=eV --to-unit=angstrom


# Convert frequency to wavelength in angstrom
vamdc convert energy 100 --from-unit=gigahertz --to-unit=angstrom
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
vamdc --quiet get nodes --format csv --output nodes.csv
grep -i "keyword" nodes.csv
```

### Finding InChIKeys

InChI (International Chemical Identifier) is a standardized representation of molecular structure. InChIKey is a 27-character hash of the InChI.

To find an InChIKey for a species:

```bash
vamdc --quiet get species --filter-by "name:CO" --format csv --output co_species.csv 2>vamdc_errors.log
# Look for the "InChIKey" column in output file
```

Or download all species and search locally:

```bash
vamdc --quiet get species --format csv --output species.csv 2>vamdc_errors.log
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
vamdc --quiet get species --refresh --format csv --output species.csv 2>vamdc_errors.log
vamdc --quiet get nodes --refresh --format csv --output nodes.csv 2>vamdc_errors.log
```

**Override cache location:**

```bash
export VAMDC_CACHE_DIR=/path/to/custom/cache
```

## Debugging

**For human debugging only, enable verbose output:**

```bash
vamdc --verbose get lines --inchikey=... --node=...
vamdc --debug get lines --inchikey=... --node=...  # Full tracebacks
```

**Note:** AI agents should always use `--quiet` mode and never use `--verbose` or `--debug`.

**Managing error output:** Even with `--quiet`, underlying libraries may produce warnings. To keep logs clean during batch processing:

```bash
# Redirect stderr to a log file
vamdc --quiet get species ... 2>errors.log

# Or discard stderr entirely (use with caution)
vamdc --quiet get species ... 2>/dev/null

# In Python scripts
import subprocess
result = subprocess.run(
    cmd,
    stdout=subprocess.PIPE,
    stderr=open('vamdc_errors.log', 'w'),  # Log errors
    # OR: stderr=subprocess.DEVNULL,        # Discard errors
    text=True
)
```

**Common issues:**

- **"Node not found"**: Run `vamdc --quiet get nodes --format csv --output nodes.csv` to verify node name/ID
- **"No species with InChIKey"**: Verify InChIKey via `vamdc --quiet get species --format csv --output species.csv`
- **Unexpected cache behavior**: Clear cache with `vamdc cache clear`
- **RDKit InChI warnings**: These are normal for some species with non-standard InChI representations; redirect stderr if they clutter output

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

**Systematic search through multiple species (RECOMMENDED PATTERN):**

```bash
# Efficient count-first approach
while IFS=',' read -r inchikey name formula; do
  echo "Checking $formula..."
  
  # Step 1: Count (fast, lightweight)
  count_output=$(vamdc --quiet count lines \
    --inchikey="$inchikey" \
    --node=cdms \
    --lambda-min=26006000 \
    --lambda-max=26010000 \
    2>>batch_errors.log | grep "vamdc-count-radiative")
  
  # Step 2: Only get lines if count > 0
  if echo "$count_output" | grep -q "vamdc-count-radiative: [1-9]"; then
    echo "  ✓ Found lines! Downloading..."
    vamdc --quiet get lines \
      --inchikey="$inchikey" \
      --node=cdms \
      --lambda-min=26006000 \
      --lambda-max=26010000 \
      --accept-truncation \
      --format csv \
      --output "lines_${formula}.csv" \
      2>>batch_errors.log
  else
    echo "  No lines found"
  fi
done < species_list.csv
```

**Get all carbon monoxide lines from a specific node:**

1. Find InChIKey: `vamdc --quiet get species --filter-by "name:CO" --format csv --output co_species.csv 2>errors.log`
2. Count lines: `vamdc --quiet count lines --inchikey=UGFAIRIUMAVXCW-UHFFFAOYSA-N --node=hitran --lambda-min=26006000 --lambda-max=26010000 2>>errors.log`
3. If count > 0, get lines: `vamdc --quiet get lines --inchikey=UGFAIRIUMAVXCW-UHFFFAOYSA-N --node=hitran --lambda-min=26006000 --lambda-max=26010000 --format csv --output co_lines.csv 2>>errors.log`

**Build a spectral catalog across multiple nodes:**

1. Get species list: `vamdc --quiet get species --format csv --output species.csv 2>errors.log`
2. For each species and node of interest, use count-first workflow (see "Systematic search" pattern above)
3. Use postprocessing (Python/awk/etc.) to combine and filter results

**Sample specific wavelength regions:**

- Use `--lambda-min` and `--lambda-max` (in Ångströms) to restrict queries
- Example: UV region (100–4000 Å), optical (3000–10000 Å), IR (10000–100000 Å)

**Python example of count-first workflow:**

```python
import subprocess
import re

def has_lines(inchikey, node, lambda_min, lambda_max):
    """Check if species has lines in range using count command."""
    cmd = [
        'uvx', '--from', '.', 'vamdc', '--quiet', 'count', 'lines',
        '--inchikey', inchikey,
        '--node', node,
        '--lambda-min', str(lambda_min),
        '--lambda-max', str(lambda_max)
    ]
    
    result = subprocess.run(
        cmd,
        cwd='/home/claude/pyVAMDC',
        capture_output=True,
        text=True,
        stderr=open('errors.log', 'a')
    )
    
    # Parse count from output
    match = re.search(r'vamdc-count-radiative:\s*(\d+)', result.stdout)
    if match:
        return int(match.group(1)) > 0
    return False

def get_lines(inchikey, node, lambda_min, lambda_max, output_file):
    """Download lines only after confirming they exist."""
    cmd = [
        'uvx', '--from', '.', 'vamdc', '--quiet', 'get', 'lines',
        '--inchikey', inchikey,
        '--node', node,
        '--lambda-min', str(lambda_min),
        '--lambda-max', str(lambda_max),
        '--accept-truncation',
        '--format', 'csv',
        '--output', output_file
    ]
    
    subprocess.run(
        cmd,
        cwd='/home/claude/pyVAMDC',
        stderr=open('errors.log', 'a')
    )

# Usage
inchikey = 'UGFAIRIUMAVXCW-UHFFFAOYSA-N'
if has_lines(inchikey, 'cdms', 26006000, 26010000):
    print("Lines found! Downloading...")
    get_lines(inchikey, 'cdms', 26006000, 26010000, 'co_lines.csv')
else:
    print("No lines in this range")
```
