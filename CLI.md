# VAMDC Command-Line Interface

The `vamdc` command-line tool provides access to atomic and molecular spectroscopic data from the VAMDC (Virtual Atomic and Molecular Data Centre) infrastructure.

The CLI supports querying **multiple species and multiple nodes simultaneously**, leveraging high-level wrapper functions from the `lines` module for better performance and flexibility.

## Installation

### Recommended: Using uv

Install [uv](https://docs.astral.sh/uv/) and add a shell alias:

```bash
# Install uv (if not already installed)
# See https://docs.astral.sh/uv/ for installation instructions

# Add to ~/.bashrc or ~/.zshrc
alias vamdc='uv run -m pyVAMDC.spectral.cli'
```

After adding the alias, restart your shell or run `source ~/.bashrc` (or `~/.zshrc`).

### Alternative: Direct execution

```bash
python -m pyVAMDC.spectral.cli
```

## Command Structure

The CLI is organized into command groups:

```
vamdc
├── get          # Retrieve data from VAMDC
│   ├── nodes    # List available data nodes
│   ├── species  # List chemical species
│   └── lines    # Query spectral lines (supports multiple species/nodes)
├── count        # Inspect metadata without downloading
│   └── lines    # Get line counts and metadata (supports multiple species/nodes)
├── convert      # Perform unit conversions
│   └── energy   # Convert between energy, frequency, and wavelength units
└── cache        # Manage local cache
    ├── status   # Show cache information (includes XSAMS files)
    └── clear    # Remove cached data
```

## Features

✨ **Multiple species support**: Query multiple species in one command  
✨ **Multiple nodes support**: Query multiple data nodes simultaneously  
✨ **Intelligent node resolution**: Use short names, IVO IDs, or full endpoints  
✨ **XSAMS cache integration**: XSAMS files stored in cache by default  
✨ **Parallel processing**: Leverages multiprocessing for faster queries  
✨ **Enhanced metadata**: Added node and species_type columns to output  
✨ **Flexible truncation handling**: Control query splitting behavior  
✨ **Unit conversion**: Convert between energy, frequency, and wavelength units

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

**Sample output:**
```
Fetching nodes from VAMDC Species Database...
Fetched 32 nodes and cached at ~/.cache/vamdc/nodes.csv
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

**Sample output:**
```
Fetching species from VAMDC Species Database...
Fetched 4958 species and cached at ~/.cache/vamdc/species.csv
```

### `vamdc get lines` ⭐

Get spectral lines for one or more species from one or more nodes.

**Options:**
- `--inchikey TEXT`: InChIKey of the species (**can be specified multiple times**)
- `--node TEXT`: Node identifier - TAP endpoint, IVO ID, or shortname (**can be specified multiple times**)
- `--lambda-min FLOAT`: Minimum wavelength in Angstrom (default: 0.0)
- `--lambda-max FLOAT`: Maximum wavelength in Angstrom (default: 1.0e9)
- `-f, --format [xsams|csv|json|table]`: Output format (default: table)
- `-o, --output PATH`: Output file/directory path
- `--accept-truncation`: Accept truncated results without recursive splitting

**Output format behavior:**
- **xsams**: Raw XSAMS XML files
  - Default location: `~/.cache/vamdc/xsams/`
  - Custom location: Specify with `--output /path/to/dir`
- **csv/json/table**: Converted tabular data with columns:
  - All spectroscopic line data fields
  - `node`: TAP endpoint of the data source
  - `species_type`: atom or molecule

**Examples:**

#### Single species, single node (using short name)
```bash
# Query calcium (Ca) from topbase using short name
vamdc get lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --node=topbase \
  --lambda-min=1000 \
  --lambda-max=2000 \
  --accept-truncation
```

#### Multiple species, single node (using short name)
```bash
# Query CO and H2O from CDMS using short name
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --inchikey=XLYOFNOQVPJJNP-UHFFFAOYSA-N \
  --node=cdms \
  --lambda-min=100000 \
  --lambda-max=200000
```

#### Single species, multiple nodes (using short names)
```bash
# Query CO from multiple databases using short names
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=cdms \
  --node=jpl \
  --node=basecol2015 \
  --lambda-min=100000 \
  --lambda-max=200000
```

#### Mixed identifier types (short names, IVO IDs, endpoints)
```bash
# Mix different identifier types in the same command
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=cdms \
  --node="ivo://vamdc/jpl/vamdc-tap_12.07" \
  --node="http://basecoltap2015.vamdc.org/12_07/TAP/" \
  --lambda-min=100000 \
  --lambda-max=200000
```

#### All available species/nodes in wavelength range
```bash
# Query all available data in wavelength range
# (no --inchikey or --node specified)
vamdc get lines \
  --lambda-min=1000 \
  --lambda-max=2000 \
  --accept-truncation
```

#### XSAMS format output
```bash
# Download XSAMS to default cache directory
vamdc get lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --node="http://topbase.obspm.fr/12.07/vamdc/tap//" \
  --lambda-min=1000 \
  --lambda-max=2000 \
  --format xsams \
  --accept-truncation

# Download XSAMS to custom directory
vamdc get lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --format xsams \
  --output /path/to/my/xsams/files \
  --lambda-min=1000 \
  --lambda-max=2000 \
  --accept-truncation
```

#### CSV output with multiple sources
```bash
# Get tabular data from multiple nodes
vamdc get lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --lambda-min=1000 \
  --lambda-max=2000 \
  --format csv \
  --output lines.csv \
  --accept-truncation
```

**Sample output:**
```
Querying spectral lines...
Wavelength range: 1000.0 - 2000.0 Angstrom
Filtering for 1 species...
Found 6 species entries matching InChIKeys
Filtering for 1 nodes...
Found 1 nodes matching identifiers
Fetching lines...
Retrieved atomic data from 2 node(s)
Total spectral lines retrieved: 10079
Lines saved to lines.csv
```

**Understanding query splitting:**

Without `--accept-truncation`, queries that would return truncated results are automatically split into smaller sub-queries:

```bash
# This may be split into multiple sub-queries
vamdc get lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --node="http://topbase.obspm.fr/12.07/vamdc/tap//" \
  --lambda-min=0 \
  --lambda-max=90009076900
```

With `--accept-truncation`, the query executes as-is even if truncated:

```bash
# Executes in one query, may be truncated
vamdc get lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --node="http://topbase.obspm.fr/12.07/vamdc/tap//" \
  --lambda-min=0 \
  --lambda-max=90009076900 \
  --accept-truncation
```

### `vamdc count lines` ⭐

Inspect HEAD metadata for spectroscopic line queries without downloading full data. Supports multiple species and multiple nodes. **Species and node filters are optional** – if not specified, all species across all nodes are queried.

**Options:**
- `--inchikey TEXT`: InChIKey of the species (**can be specified multiple times**, optional)
- `--node TEXT`: Node identifier (**can be specified multiple times**, optional)
- `--lambda-min FLOAT`: Minimum wavelength in Angstrom (default: 0.0)
- `--lambda-max FLOAT`: Maximum wavelength in Angstrom (default: 1.0e9)

**Use cases:**
- Query all available species across all nodes in a wavelength range
- Query specific species only (filter by `--inchikey`)
- Query specific nodes only (filter by `--node`)
- Query specific species from specific nodes (both filters)

**Examples:**

#### Query all species across all nodes
```bash
# Get metadata for all data in a wavelength range
vamdc count lines \
  --lambda-min=0 \
  --lambda-max=90009076900
```

#### Query all nodes for a specific species
```bash
# Get metadata for a species from all nodes that have it
vamdc count lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --lambda-min=0 \
  --lambda-max=90009076900
```

#### Query all species from a specific node (using short name)
```bash
# Get metadata for all species from a specific node
vamdc count lines \
  --node=topbase \
  --lambda-min=0 \
  --lambda-max=90009076900
```

#### Single species, single node (using short name)
```bash
vamdc count lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --node=topbase \
  --lambda-min=0 \
  --lambda-max=90009076900
```

#### Multiple species, multiple nodes (using short names)
```bash
vamdc count lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --inchikey=XLYOFNOQVPJJNP-UHFFFAOYSA-N \
  --node=cdms \
  --node=jpl \
  --lambda-min=100000 \
  --lambda-max=200000
```

#### Query all species from a specific node
```bash
# Get metadata for all species from a specific node
vamdc count lines \
  --node="http://topbase.obspm.fr/12.07/vamdc/tap//" \
  --lambda-min=0 \
  --lambda-max=90009076900
```

#### Single species, single node
```bash
vamdc count lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --node="http://topbase.obspm.fr/12.07/vamdc/tap//" \
  --lambda-min=0 \
  --lambda-max=90009076900
```

**Sample output (all species, all nodes):**
```
Inspecting metadata for spectral lines...
Wavelength range: 0.0 - 90009076900.0 Angstrom
No species or node filters provided; querying all species across all nodes.
Fetching metadata (HEAD requests only)...

Sub-query 1: http://topbase.obspm.fr/12.07/vamdc/tap//sync?LANG=VSS2&REQUEST=doQuery...
  vamdc-approx-size: 66.90
  vamdc-count-radiative: 47778
  vamdc-count-species: 1
  vamdc-count-states: 1007
  vamdc-request-token: topbase:ebfda65c-83d3-4d10-a08b-1213b0a6bf7f:head
  vamdc-truncated: 20.9

Aggregated numeric headers across 1 sub-queries:
  vamdc-approx-size: 66.9
  vamdc-count-radiative: 47778
  vamdc-count-species: 1
  vamdc-count-states: 1007
  vamdc-truncated: 20.9
```

#### Multiple species, multiple nodes
```bash
vamdc count lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --inchikey=XLYOFNOQVPJJNP-UHFFFAOYSA-N \
  --node="https://cdms.astro.uni-koeln.de/cdms/tap/" \
  --node="http://basecoltap2015.vamdc.org/12_07/TAP/" \
  --lambda-min=100000 \
  --lambda-max=200000
```

This command performs HEAD requests to retrieve VAMDC count headers without downloading full datasets, showing:
- Individual metadata per sub-query
- Aggregated totals across all sub-queries
- Truncation status
- Estimated data sizes

### `vamdc cache status`

Show cache status and metadata, including XSAMS files.

**Example:**
```bash
vamdc cache status
```

**Sample output:**
```
Cache directory: /Users/username/.cache/vamdc
Expiration time: 24 hours

Nodes: VALID (cached at 2025-10-21 14:59:35.657232)
Species: VALID (cached at 2025-10-21 14:59:43.941104)
Species Nodes: VALID (cached at 2025-10-21 14:59:43.941198)

XSAMS files: 1 file(s), 8.77 MB
```

Output shows:
- Cache directory location
- Expiration time (24 hours)
- Status of each cached dataset (VALID, EXPIRED, or NOT CACHED)
- Cache timestamps
- **XSAMS files count and total size**

### `vamdc cache clear`

Remove all cached data including XSAMS files.

**Example:**
```bash
vamdc cache clear
```

This removes:
- Nodes cache
- Species cache
- Species-nodes mapping
- **All cached XSAMS files**

### `vamdc convert energy` 🔄

Convert between electromagnetic units (energy, frequency, wavelength). Supports conversions across different physical quantities using fundamental physical constants.

**Arguments:**
- `VALUE`: The numerical value to convert (required, positional)

**Options:**
- `-f, --from-unit TEXT`: Source unit (required)
- `-t, --to-unit TEXT`: Target unit (required)

**Supported Units:**

| Category | Units |
|----------|-------|
| **Energy** | joule, millijoule, microjoule, nanojoule, picojoule, eV, erg, kelvin, rydberg, cm-1 |
| **Frequency** | hertz, kilohertz, megahertz, gigahertz, terahertz |
| **Wavelength** | meter, centimeter, millimeter, micrometer, nanometer, angstrom |

**Features:**
- ✅ Case-insensitive unit names
- ✅ Cross-category conversions (e.g., wavelength → energy)
- ✅ Smart output formatting (scientific notation for very large/small values)
- ✅ Verbose mode with category conversion details

**Examples:**

#### Basic conversions
```bash
# Convert 500 nanometers to electron volts
vamdc convert energy 500 --from-unit=nanometer --to-unit=eV
# Output: 2.479683969 eV

# Convert 1.5 eV to wavenumber (cm-1)
vamdc convert energy 1.5 --from-unit=eV --to-unit=cm-1
# Output: 12098.31591 cm-1

# Convert 3000 angstroms to nanometers
vamdc convert energy 3000 -f angstrom -t nanometer
# Output: 300 nanometer

# Convert frequency to wavelength
vamdc convert energy 100 --from-unit=gigahertz --to-unit=meter
# Output: 0.00299792458 meter
```

#### Case-insensitive input
```bash
# Units are case-insensitive - all of these work:
vamdc convert energy 500 --from-unit=NANOMETER --to-unit=EV
vamdc convert energy 500 --from-unit=NanoMeter --to-unit=eV
vamdc convert energy 500 --from-unit=nanometer --to-unit=ev
# All produce: 2.479683969 eV
```

#### With verbose mode
```bash
# Show conversion details and category information
vamdc --verbose convert energy 100 -f gigahertz -t meter
# Output:
# 0.00299792458 meter
# Conversion details:
#   Input: 100.0 gigahertz
#   Output: 0.00299792458 meter
#   Category conversion: frequency → wavelength
```

#### Scientific notation for extreme values
```bash
# Very small numbers
vamdc convert energy 0.0001 --from-unit=joule --to-unit=eV
# Output: 6.241509e+14 eV

# Very large numbers
vamdc convert energy 1e-10 --from-unit=meter --to-unit=angstrom
# Output: 1e+00 angstrom
```

#### Cross-category conversions

The converter intelligently handles conversions between different physical quantities:

```bash
# Energy → Frequency
vamdc convert energy 1.5 --from-unit=eV --to-unit=terahertz

# Energy → Wavelength
vamdc convert energy 2.479683969 --from-unit=eV --to-unit=nanometer

# Frequency → Wavelength
vamdc convert energy 100 --from-unit=gigahertz --to-unit=meter

# Wavelength → Energy
vamdc convert energy 500 --from-unit=nanometer --to-unit=eV

# Temperature (in Kelvin) → Energy (in eV)
vamdc convert energy 11604.5 --from-unit=kelvin --to-unit=eV
```

**Common conversions:**

```bash
# Visible light range conversions
# Red: 700 nm
vamdc convert energy 700 -f nanometer -t eV
# Output: 1.771390 eV

# Green: 550 nm
vamdc convert energy 550 -f nanometer -t eV
# Output: 2.254581 eV

# Violet: 400 nm
vamdc convert energy 400 -f nanometer -t eV
# Output: 3.099019 eV

# Spectroscopic wavenumber
vamdc convert energy 5000 -f cm-1 -t eV
# Output: 0.619947 eV

# Radio frequency
vamdc convert energy 1.4 -f gigahertz -t meter
# Output: 0.214285714 meter (21.4 cm wavelength - common in radio astronomy)
```

**Error handling:**

Invalid unit specifications show all supported units:

```bash
vamdc convert energy 500 --from-unit=invalid --to-unit=eV
# Error: Invalid from-unit 'invalid'. Supported units:
#   energy: joule, millijoule, microjoule, nanojoule, picojoule, eV, erg, kelvin, rydberg, cm-1
#   frequency: hertz, kilohertz, megahertz, gigahertz, terahertz
#   wavelength: meter, centimeter, millimeter, micrometer, nanometer, angstrom
```

**Use cases:**

1. **Convert spectral line wavelengths to energies**:
   ```bash
   # Convert observed wavelength (Angstroms) to eV for comparison with theory
   vamdc convert energy 4861 -f angstrom -t eV  # Hydrogen Balmer alpha
   # Output: 2.550169 eV
   ```

2. **Convert between observational and theoretical units**:
   ```bash
   # Convert radio frequency observation to wavelength
   vamdc convert energy 345 -f gigahertz -t millimeter
   # Output: 0.869565 millimeter (for CO line in radio astronomy)
   ```

3. **Temperature to energy for thermal populations**:
   ```bash
   # Convert room temperature to energy
   vamdc convert energy 300 -f kelvin -t meV
   vamdc convert energy 300 -f kelvin -t cm-1
   ```

4. **Pipeline integration**:
   ```bash
   # Use in shell scripts
   wavelength=500
   energy=$(vamdc convert energy $wavelength -f nanometer -t eV | awk '{print $1}')
   echo "Wavelength: ${wavelength} nm = ${energy} eV"
   ```

## Caching System

The CLI automatically caches downloaded data to avoid redundant network requests.

**Cache location:**
- Default: `~/.cache/vamdc/`
- Override with `VAMDC_CACHE_DIR` environment variable

**Cached data:**
- `nodes.csv` - VAMDC data nodes
- `species.csv` - Chemical species database (4958+ species)
- `species_nodes.csv` - Species-to-node mappings
- `xsams/` - **XSAMS files directory**
- `*_timestamp.json` - Metadata files tracking cache timestamps

**Cache expiration:**
- Metadata (nodes, species): 24 hours from last fetch
- XSAMS files: No automatic expiration (managed by user)
- Use `--refresh` flag to force metadata update
- Check status with `vamdc cache status`

**XSAMS files management:**
- Default location: `~/.cache/vamdc/xsams/`
- Files named by query token: `<node>:<token>:get.xsams`
- View count and size: `vamdc cache status`
- Clear all XSAMS: `vamdc cache clear`

## Environment Variables

- `VAMDC_CACHE_DIR`: Override default cache directory location

**Example:**
```bash
export VAMDC_CACHE_DIR=~/my_vamdc_cache
vamdc get species  # Uses ~/my_vamdc_cache/
```

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

**Pro tip:** The species database includes:
- InChIKey (unique identifier)
- Chemical formula
- Species name
- Species type (atom/molecule)
- Available nodes (TAP endpoints)

## Common Workflows

### Explore available data

```bash
# List all nodes (32 data centers)
vamdc get nodes

# Get full species database (4958+ species)
vamdc get species --format csv --output species.csv

# Find a specific molecule
vamdc get species --filter-by "name:H2O"

# Check which nodes have your species
vamdc get species --filter-by "name:CO" | grep -i "tapEndpoint"
```

### Query spectral lines efficiently

```bash
# Step 1: Find the InChIKey
vamdc get species --filter-by "name:Ca"
# Result: DONWDOGXJBIXRQ-UHFFFAOYSA-N

# Step 2: Check available data (HEAD request only)
vamdc count lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --node=topbase \
  --lambda-min=1000 \
  --lambda-max=2000

# Step 3: Download the data using short node name
vamdc get lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --node=topbase \
  --lambda-min=1000 \
  --lambda-max=2000 \
  --format csv \
  --output ca_lines.csv \
  --accept-truncation
```

### Explore data availability across all sources

```bash
# Check how much data is available in a wavelength range (no filters)
vamdc count lines \
  --lambda-min=1000 \
  --lambda-max=2000

# This queries all species from all nodes without filtering
# Useful for understanding data coverage across the entire VAMDC infrastructure
```

### Query multiple species simultaneously

```bash
# Get data for multiple molecules in one command
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --inchikey=XLYOFNOQVPJJNP-UHFFFAOYSA-N \
  --inchikey=UGFAIRIUMAVXCW-UHFFFAOYSA-N \
  --lambda-min=100000 \
  --lambda-max=200000 \
  --format csv \
  --output multiple_species.csv
```

### Compare data from multiple nodes

```bash
# Get the same species from different databases using short names
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=cdms \
  --node=jpl \
  --lambda-min=100000 \
  --lambda-max=200000 \
  --format csv \
  --output co_comparison.csv

# The output CSV includes a 'node' column to identify the source
# You can also mix different identifier types:
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=cdms \
  --node="ivo://vamdc/jpl/vamdc-tap_12.07" \
  --node="http://basecoltap2015.vamdc.org/12_07/TAP/" \
  --lambda-min=100000 \
  --lambda-max=200000 \
  --format csv \
  --output co_all_sources.csv
```

### Work with XSAMS files

```bash
# Download XSAMS to cache using short node name
vamdc get lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --node=topbase \
  --lambda-min=1000 \
  --lambda-max=2000 \
  --format xsams \
  --accept-truncation

# Check XSAMS cache status
vamdc cache status

# Download to custom directory for archiving
vamdc get lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --node=topbase \
  --format xsams \
  --output /archive/2025/calcium/ \
  --lambda-min=1000 \
  --lambda-max=2000 \
  --accept-truncation

# Download from multiple nodes
vamdc get lines \
  --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N \
  --node=topbase \
  --node=chianti \
  --format xsams \
  --output /archive/2025/calcium/ \
  --accept-truncation
```

## Node Identifiers

The `--node` parameter accepts three types of identifiers with intelligent resolution:

### Supported Node Identifier Types

1. **Short name** (most convenient):
   ```bash
   --node="cdms"
   --node="jpl"
   --node="topbase"
   ```
   - Short, memorable identifiers for common nodes
   - Case-insensitive matching
   - Example: `vamdc get lines --inchikey=... --node=cdms`

2. **IVO identifier** (programmatic use):
   ```bash
   --node="ivo://vamdc/TOPbase/tap-xsams"
   --node="ivo://vamdc/cdms/vamdc-tap_12.07"
   ```
   - Full Virtual Observatory identifier
   - Unambiguous and machine-readable
   - Example: `vamdc get lines --inchikey=... --node="ivo://vamdc/cdms/vamdc-tap_12.07"`

3. **TAP endpoint URL** (full endpoint):
   ```bash
   --node="http://topbase.obspm.fr/12.07/vamdc/tap//"
   --node="https://cdms.astro.uni-koeln.de/cdms/tap/"
   ```
   - Complete TAP endpoint URL
   - Most explicit identifier
   - Example: `vamdc get lines --inchikey=... --node="https://cdms.astro.uni-koeln.de/cdms/tap/"`

### Resolution Strategy

The CLI uses intelligent 4-step resolution to convert any identifier to a full TAP endpoint:

```
Step 1: Try matching as TAP endpoint (full URL)
  └─ If not found → continue to Step 2

Step 2: Try matching as IVO identifier
  └─ If not found → continue to Step 3

Step 3: Try matching as short name
  └─ If found → Return endpoint ✓

Step 4: Try matching against nodes table (fallback)
  └─ If not found → Raise error with helpful message
```

**Example resolution flow:**
```
User input: "cdms"
├─ Step 1: Is it "https://..." URL? No
├─ Step 2: Is it "ivo://..." ID? No
├─ Step 3: Is it a short name "cdms"? Yes ✓
└─ Result: "https://cdms.astro.uni-koeln.de/cdms/tap/"
```

### Finding Node Identifiers

Get all available node identifiers:

```bash
# View all nodes with their identifiers
vamdc get nodes --format csv

# View specific columns
vamdc get nodes --format csv | cut -d',' -f1,2,3

# Search for a specific node (e.g., CDMS)
vamdc get nodes --format csv | grep -i "cdms"
```

Output includes:
- `shortName`: Short identifier (e.g., "CDMS")
- `ivoIdentifier`: Full IVO ID (e.g., "ivo://vamdc/cdms/vamdc-tap_12.07")
- `tapEndpoint`: Full TAP URL (e.g., "https://cdms.astro.uni-koeln.de/cdms/tap/")

### Examples by Identifier Type

#### Using short name (RECOMMENDED)
```bash
# Simple and readable
vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --node=cdms
vamdc get lines --inchikey=DONWDOGXJBIXRQ-UHFFFAOYSA-N --node=topbase
vamdc get lines --inchikey=XLYOFNOQVPJJNP-UHFFFAOYSA-N --node=basecol2015

# Multiple nodes using short names
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=cdms --node=jpl --node=basecol2015
```

#### Using IVO identifier
```bash
# Explicit and unambiguous
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node="ivo://vamdc/cdms/vamdc-tap_12.07"

# Multiple nodes using IVO identifiers
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node="ivo://vamdc/cdms/vamdc-tap_12.07" \
  --node="ivo://vamdc/basecol2015/vamdc-tap"
```

#### Using TAP endpoint URL
```bash
# Full endpoint
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node="https://cdms.astro.uni-koeln.de/cdms/tap/"

# Mixed identifiers (all types work together)
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=cdms \
  --node="ivo://vamdc/jpl/vamdc-tap_12.07" \
  --node="http://basecoltap2015.vamdc.org/12_07/TAP/"
```

### Error Handling

When an invalid node identifier is provided:

```bash
# Invalid short name
vamdc get lines --inchikey=... --node=invalid_xyz
# Error: No node matching 'invalid_xyz' was found.
#        Try using a full TAP endpoint URL, short name
#        (e.g., 'cdms'), or IVO identifier.
```

To troubleshoot:
1. List all available nodes: `vamdc get nodes`
2. Check the short name, IVO ID, or endpoint format
3. Verify the node has data for your species

## Species Identifiers

The `--inchikey` parameter identifies chemical species for queries. The CLI now supports intelligent species identification with flexible matching.

### Understanding InChIKey

An **InChIKey** is a unique, standardized identifier for chemical substances. It's a fixed-length character string derived from the IUPAC International Chemical Identifier (InChI).

**Format:**
```
OKTJSMMVPCPJKN-UHFFFAOYSA-N
│                           │
│                           └─ Protonation layer indicator
├─ Main layer (14 chars)
└─ First InChI layer (10 chars)
```

**Example InChIKeys:**
- Carbon: `OKTJSMMVPCPJKN-UHFFFAOYSA-N`
- Carbon Monoxide (CO): `LFQSCWFLJHTTHZ-UHFFFAOYSA-N`
- Water (H₂O): `XLYOFNOQVPJJNP-UHFFFAOYSA-N`

### Finding Species InChIKeys

#### Method 1: Search the species database
```bash
# Get all species with "CO" in the name
vamdc get species --filter-by "name:CO"

# Output shows InChIKey and other properties
InChIKey    name           formula  speciesType
LFQSCWFLJHTTHZ-UHFFFAOYSA-N  carbon monoxide  CO  molecule
```

#### Method 2: Export and search
```bash
# Export full species database
vamdc get species --format csv --output species.csv

# Search for specific species
grep -i "carbon" species.csv | head -5
```

#### Method 3: Query single species
```bash
# Query a specific molecule (CO) from a node
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=cdms \
  --lambda-min=100000 \
  --lambda-max=200000
```

### Common Species InChIKeys

Here are some frequently-used species:

| Species | InChIKey | Type |
|---------|----------|------|
| Hydrogen (H) | `UFHXOROCNITJBY-UHFFFAOYSA-N` | atom |
| Helium (He) | `SWQJXJOGLNCZEY-UHFFFAOYSA-N` | atom |
| Carbon (C) | `OKTJSMMVPCPJKN-UHFFFAOYSA-N` | atom |
| Nitrogen (N) | `IJDNQMJBXVCW-UHFFFAOYSA-N` | atom |
| Oxygen (O) | `QVGXLLKGJNJLOE-UHFFFAOYSA-N` | atom |
| Carbon Monoxide (CO) | `LFQSCWFLJHTTHZ-UHFFFAOYSA-N` | molecule |
| Water (H₂O) | `XLYOFNOQVPJJNP-UHFFFAOYSA-N` | molecule |
| Ammonia (NH₃) | `QGZKDVFQNNGYKY-UHFFFAOYSA-N` | molecule |
| Methane (CH₄) | `VNWKTOKETHGBQM-UHFFFAOYSA-N` | molecule |

### Species Resolution for Queries

Unlike node identifiers, species are **always identified by InChIKey**. However, the CLI provides intelligent features:

1. **Multiple species in one query**:
   ```bash
   vamdc get lines \
     --inchikey=OKTJSMMVPCPJKN-UHFFFAOYSA-N \
     --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
     --inchikey=XLYOFNOQVPJJNP-UHFFFAOYSA-N \
     --node=cdms \
     --lambda-min=1000 --lambda-max=10000
   ```

2. **Automatic species validation**: The CLI checks if the InChIKey exists in the database
   ```bash
   # Invalid InChIKey
   vamdc get lines --inchikey=INVALID-INCHIKEY-XXX --node=cdms
   # Error: No species with InChIKey 'INVALID-INCHIKEY-XXX' were found.
   ```

3. **Specifies available nodes for each species**: Automatically identifies which nodes have data for each species
   ```bash
   # The CLI internally checks which of your specified nodes have this species
   vamdc get lines \
     --inchikey=OKTJSMMVPCPJKN-UHFFFAOYSA-N \
     --node=cdms --node=topbase --node=vald
   # Queries all specified nodes that have this species
   ```

### Workflow: Find and Query Species

**Step 1: Find the InChIKey**
```bash
# Search for a species by name or formula
vamdc get species --filter-by "name:CO"

# Find column headers
vamdc get species --format csv | head -1
```

**Step 2: Identify available nodes**
```bash
# Export species info and check available nodes
vamdc get species --format csv --output species.csv

# View data for specific species
grep "LFQSCWFLJHTTHZ-UHFFFAOYSA-N" species.csv

# See which nodes have this species
vamdc get nodes --format csv | grep -i "cdms"
```

**Step 3: Check available data**
```bash
# Use count_lines to see data availability
vamdc count lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=cdms \
  --lambda-min=1000 \
  --lambda-max=10000
```

**Step 4: Download the data**
```bash
# Download spectral lines
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --node=cdms \
  --lambda-min=1000 \
  --lambda-max=10000 \
  --format csv \
  --output co_lines.csv
```

### Combining Multiple Species and Nodes

**Query multiple species from multiple nodes:**
```bash
# Compare CO and H2O across different databases
vamdc get lines \
  --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
  --inchikey=XLYOFNOQVPJJNP-UHFFFAOYSA-N \
  --node=cdms \
  --node=jpl \
  --node=basecol2015 \
  --lambda-min=100000 \
  --lambda-max=200000 \
  --format csv \
  --output molecules.csv
```

Output CSV will include:
- All spectral line data
- `node` column: which database the line came from
- `species_type` column: atom or molecule

### Error Handling for Species

**Invalid InChIKey format:**
```bash
vamdc get lines --inchikey=INVALID-KEY --node=cdms
# Error: No species with InChIKey 'INVALID-KEY' were found.
```

**Solutions:**
1. Check spelling with `vamdc get species --filter-by "name:..."`
2. List all available species: `vamdc get species --format csv`
3. Use the species database export to find exact InChIKey

### Pro Tips

1. **Save common InChIKeys**: Create a reference file
   ```bash
   cat > species_inchikeys.txt << EOF
   # Molecules
   CO=LFQSCWFLJHTTHZ-UHFFFAOYSA-N
   H2O=XLYOFNOQVPJJNP-UHFFFAOYSA-N
   NH3=QGZKDVFQNNGYKY-UHFFFAOYSA-N
   EOF
   ```

2. **Query in a loop**:
   ```bash
   while read inchikey; do
     vamdc get lines \
       --inchikey="$inchikey" \
       --node=cdms \
       --lambda-min=1000 --lambda-max=10000 \
       --format csv \
       --output "lines_${inchikey}.csv"
   done < species_inchikeys.txt
   ```

3. **Combine with node iteration**:
   ```bash
   # Query a species from all available nodes
   for node in cdms jpl topbase basecol2015 vald chianti; do
     vamdc get lines \
       --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
       --node="$node" \
       --lambda-min=100000 --lambda-max=200000 \
       --format csv \
       --output "co_${node}.csv" 2>/dev/null || echo "No data from $node"
   done
   ```

## Performance Tips

1. **Use `count lines` before downloading**: Check data size first
   ```bash
   vamdc count lines --inchikey=... --node=... --lambda-min=... --lambda-max=...
   ```

2. **Use `--accept-truncation` for large queries**: Avoid automatic splitting
   ```bash
   vamdc get lines ... --accept-truncation
   ```

3. **Query multiple species/nodes in one command**: Leverages parallel processing
   ```bash
   vamdc get lines --inchikey=SPECIES1 --inchikey=SPECIES2 --inchikey=SPECIES3 ...
   ```

4. **Use cache**: Metadata is cached for 24 hours
   ```bash
   # First call: downloads metadata
   vamdc get species
   
   # Subsequent calls: uses cache (fast)
   vamdc get species --filter-by "name:..."
   ```

5. **Narrow wavelength ranges**: Reduces data volume and query time
   ```bash
   # Instead of querying the full spectrum
   --lambda-min=0 --lambda-max=1000000000
   
   # Use targeted ranges
   --lambda-min=1000 --lambda-max=2000
   ```

## Troubleshooting

### "Node not found" error

Ensure you're using a valid node identifier. Check available nodes:

```bash
vamdc get nodes --format csv
```

Verify the node has a TAP endpoint (some nodes may not support queries):
```bash
vamdc get nodes --format csv | grep -v ",,"
```

### "No species with InChIKey ... found"

Verify the InChIKey is correct:

```bash
vamdc get species --format csv --output species.csv
grep "YOUR_INCHIKEY" species.csv
```

### "No matching data were found"

This can occur if:
1. The species is not available in the specified node
2. The wavelength range has no data
3. The node/species combination is invalid

Check what's available:
```bash
# Find which nodes have your species
vamdc get species --filter-by "InChIKey:YOUR_INCHIKEY"

# Try a broader wavelength range
--lambda-min=0 --lambda-max=1000000000
```

### "Number of processes must be at least 1"

This occurs when no matching species/node combinations are found. Verify:
1. The InChIKey exists in the species database
2. The node identifier is correct
3. The node has data for that species

### Cache issues

Clear the cache if you experience unexpected behavior:

```bash
vamdc cache clear
vamdc get species --refresh  # Rebuild cache
```

### Enable verbose output

For debugging, use the `--verbose` flag at the beginning:

```bash
vamdc --verbose get lines --inchikey=... --node=... --lambda-min=... --lambda-max=...
```

### Query takes too long

1. Use `count lines` to check data volume first
2. Add `--accept-truncation` to prevent automatic query splitting
3. Narrow your wavelength range
4. Query fewer species/nodes simultaneously

### XSAMS files filling up disk

Check XSAMS cache size:
```bash
vamdc cache status
```

Clear XSAMS files:
```bash
vamdc cache clear
```

Or manually remove specific files:
```bash
rm ~/.cache/vamdc/xsams/*.xsams
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

## Advanced Examples

### Query all data for a specific wavelength range

```bash
# Get all available species in UV range (no filters)
vamdc get lines \
  --lambda-min=1000 \
  --lambda-max=4000 \
  --format csv \
  --output uv_lines.csv \
  --accept-truncation
```

### Pipeline with filtering

```bash
# Get species list, filter, then query
vamdc get species --format csv --output species.csv
awk -F',' '$5=="molecule" {print $6}' species.csv > molecule_inchikeys.txt

# Query first 3 molecules
head -3 molecule_inchikeys.txt | while read inchikey; do
  vamdc get lines \
    --inchikey="$inchikey" \
    --lambda-min=100000 \
    --lambda-max=200000 \
    --format csv \
    --output "lines_${inchikey}.csv" \
    --accept-truncation
done
```

### Check metadata for multiple sources

```bash
# Compare data availability across nodes
for node in "https://cdms.astro.uni-koeln.de/cdms/tap/" \
            "https://cdms.astro.uni-koeln.de/jpl/tap/" \
            "http://basecoltap2015.vamdc.org/12_07/TAP/"; do
  echo "=== $node ==="
  vamdc count lines \
    --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \
    --node="$node" \
    --lambda-min=100000 \
    --lambda-max=200000 \
    2>/dev/null || echo "No data"
done
```

## API Wrapper

The CLI uses high-level wrapper functions:
- `lines_module.getLines()` - Downloads and converts data
- `lines_module.get_metadata_for_lines()` - HEAD requests only
- `lines_module._build_and_run_wrappings()` - Internal parallel processing

These provide better performance and flexibility compared to direct `VamdcQuery` instantiation.

## Acknowledgments

This CLI interfaces with the [VAMDC (Virtual Atomic and Molecular Data Centre)](https://vamdc.org/) infrastructure, which aggregates spectroscopic data from multiple international databases.
