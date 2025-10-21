"""VAMDC command-line interface for querying atomic and molecular data."""

import json
import logging
import os
import shutil
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple

import click
import pandas as pd

try:
    # Try relative imports first (when run as module)
    from spectral import species as species_module
    from spectral import lines as lines_module
    from spectral import filters as filters_module
    from spectral.energyConverter import electromagnetic_conversion
except ImportError:
    # Fall back to absolute imports (when run as console script)
    from pyVAMDC.spectral import species as species_module
    from pyVAMDC.spectral import lines as lines_module
    from pyVAMDC.spectral import filters as filters_module
    from pyVAMDC.spectral.energyConverter import electromagnetic_conversion

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Default wavelength bounds
DEFAULT_LAMBDA_MIN = 0.0
DEFAULT_LAMBDA_MAX = 1.0e9

# Cache expiration time (24 hours)
CACHE_EXPIRATION_HOURS = 24


def get_cache_dir() -> Path:
    """Get cache directory from environment or default XDG location."""
    env_value = os.environ.get("VAMDC_CACHE_DIR")
    if env_value:
        return Path(env_value).expanduser()
    return Path.home() / ".cache" / "vamdc"


# Determine cache directory
CACHE_DIR = get_cache_dir()
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Cache file paths
NODES_CACHE_FILE = CACHE_DIR / 'nodes.csv'
SPECIES_CACHE_FILE = CACHE_DIR / 'species.csv'
SPECIES_NODES_CACHE_FILE = CACHE_DIR / 'species_nodes.csv'


def is_cache_valid(cache_file: Path) -> bool:
    """Check if cache file exists and is not expired."""
    if not cache_file.exists():
        return False

    # Check metadata file for timestamp
    metadata_file = cache_file.parent / f'{cache_file.stem}_timestamp.json'
    if not metadata_file.exists():
        return False

    try:
        with open(metadata_file) as f:
            metadata = json.load(f)
        timestamp = datetime.fromisoformat(metadata.get('timestamp', ''))
        expiration = timestamp + timedelta(hours=CACHE_EXPIRATION_HOURS)
        return datetime.now() < expiration
    except (json.JSONDecodeError, KeyError, ValueError):
        return False


def save_cache_metadata(cache_file: Path):
    """Save timestamp metadata for cache file."""
    metadata_file = cache_file.parent / f'{cache_file.stem}_timestamp.json'
    with open(metadata_file, 'w') as f:
        json.dump({'timestamp': datetime.now().isoformat()}, f)


def clear_cache():
    """Clear all cached data."""
    if CACHE_DIR.exists():
        shutil.rmtree(CACHE_DIR)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        click.echo("Cache cleared.")


def load_species_data(force_refresh: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load species and nodes data from cache or fetch fresh."""
    species_valid = is_cache_valid(SPECIES_CACHE_FILE)
    nodes_valid = is_cache_valid(SPECIES_NODES_CACHE_FILE)

    if species_valid and nodes_valid and not force_refresh:
        species_df = pd.read_csv(SPECIES_CACHE_FILE)
        nodes_df = pd.read_csv(SPECIES_NODES_CACHE_FILE)
        return species_df, nodes_df

    click.echo("Fetching species from VAMDC Species Database...", err=True)
    species_df, nodes_df = species_module.getAllSpecies()

    # Cache the data
    species_df.to_csv(SPECIES_CACHE_FILE, index=False)
    nodes_df.to_csv(SPECIES_NODES_CACHE_FILE, index=False)
    save_cache_metadata(SPECIES_CACHE_FILE)
    save_cache_metadata(SPECIES_NODES_CACHE_FILE)

    return species_df, nodes_df


def filter_species_by_inchikeys(species_df: pd.DataFrame, inchikeys: list) -> pd.DataFrame:
    """Filter species dataframe by one or more InChIKeys."""
    if not inchikeys:
        return species_df
    
    normalized_keys = [key.strip().upper() for key in inchikeys]
    inchikey_series = species_df["InChIKey"].astype(str).str.upper()
    return species_df[inchikey_series.isin(normalized_keys)]


def filter_nodes_by_identifiers(nodes_df: pd.DataFrame, node_identifiers: list) -> pd.DataFrame:
    """Filter nodes dataframe by one or more node identifiers (shortname, IVO ID, or TAP endpoint)."""
    if not node_identifiers:
        return nodes_df
    
    normalized_ids = [nid.strip().lower() for nid in node_identifiers]
    mask = pd.Series([False] * len(nodes_df))
    
    for nid in normalized_ids:
        tap_match = nodes_df["tapEndpoint"].astype(str).str.lower() == nid
        ivo_match = nodes_df["ivoIdentifier"].astype(str).str.lower() == nid
        mask = mask | tap_match | ivo_match
        
        if "shortName" in nodes_df.columns:
            short_match = nodes_df["shortName"].astype(str).str.lower() == nid
            mask = mask | short_match
    
    return nodes_df[mask]


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.pass_context
def cli(ctx: click.Context, verbose: bool):
    """VAMDC CLI - Query atomic and molecular spectroscopic data."""
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose

    if verbose:
        logger.setLevel(logging.DEBUG)


@cli.group()
def get():
    """Get data from VAMDC infrastructure."""
    pass


@get.command()
@click.option('--format', '-f', type=click.Choice(['json', 'csv', 'table']),
              default='table', help='Output format')
@click.option('--output', '-o', type=click.Path(), help='Output file path')
@click.option('--refresh', is_flag=True, help='Force refresh cache')
@click.pass_context
def nodes(ctx: click.Context, format: str, output: Optional[str], refresh: bool):
    """Get list of VAMDC data nodes and cache them locally.

    Example:
        vamdc get nodes
        vamdc get nodes --format csv --output nodes.csv
    """
    try:
        # Check cache
        if is_cache_valid(NODES_CACHE_FILE) and not refresh:
            click.echo("Loading nodes from cache...", err=True)
            df_nodes = pd.read_csv(NODES_CACHE_FILE)
        else:
            click.echo("Fetching nodes from VAMDC Species Database...", err=True)
            df_nodes = species_module.getNodeHavingSpecies()

            # Cache the data
            df_nodes.to_csv(NODES_CACHE_FILE, index=False)
            save_cache_metadata(NODES_CACHE_FILE)
            click.echo(f"Fetched {len(df_nodes)} nodes and cached at {NODES_CACHE_FILE}", err=True)

        # Format output
        output_content = format_output(df_nodes, format)

        # Write to file or stdout
        if output:
            with open(output, 'w') as f:
                f.write(output_content)
            click.echo(f"Nodes saved to {output}")
        else:
            click.echo(output_content)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@get.command(name='species')
@click.option('--format', '-f', type=click.Choice(['json', 'csv', 'excel', 'table']),
              default='table', help='Output format')
@click.option('--output', '-o', type=click.Path(), help='Output file path')
@click.option('--refresh', is_flag=True, help='Force refresh cache')
@click.option('--filter-by', type=click.STRING, help='Filter by criteria (e.g., "name:CO")')
@click.pass_context
def species_cmd(ctx: click.Context, format: str, output: Optional[str], refresh: bool,
                filter_by: Optional[str]):
    """Get list of chemical species and cache them locally.

    Example:
        vamdc get species
        vamdc get species --format csv --output species.csv
        vamdc get species --filter-by "name:CO"
    """
    try:
        # Load species data (uses cache if valid)
        df_species, _ = load_species_data(force_refresh=refresh)

        if not refresh and is_cache_valid(SPECIES_CACHE_FILE):
            click.echo(f"Loaded {len(df_species)} species from cache", err=True)
        else:
            click.echo(f"Fetched {len(df_species)} species and cached at {SPECIES_CACHE_FILE}", err=True)

        # Apply filters if specified
        if filter_by:
            df_species = apply_filter(df_species, filter_by)

        # Format output
        output_content = format_output(df_species, format)

        # Write to file or stdout
        if output:
            if format == 'excel':
                df_species.to_excel(output, index=False)
            elif format == 'csv':
                df_species.to_csv(output, index=False)
            elif format == 'json':
                df_species.to_json(output, orient='records', indent=2)
            else:
                with open(output, 'w') as f:
                    f.write(output_content)
            click.echo(f"Species saved to {output}")
        else:
            click.echo(output_content)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@get.command()
@click.option('--inchikey', multiple=True, help='InChIKey of the species (can be specified multiple times)')
@click.option('--node', multiple=True, help='Node identifier (shortname, IVO ID, or TAP endpoint, can be specified multiple times)')
@click.option('--lambda-min', type=float, default=DEFAULT_LAMBDA_MIN,
              help=f'Minimum wavelength in Angstrom (default: {DEFAULT_LAMBDA_MIN:g})')
@click.option('--lambda-max', type=float, default=DEFAULT_LAMBDA_MAX,
              help=f'Maximum wavelength in Angstrom (default: {DEFAULT_LAMBDA_MAX:g})')
@click.option('--format', '-f', type=click.Choice(['xsams', 'csv', 'json', 'table']),
              default='table', help='Output format (xsams: raw XSAMS files, csv/json/table: converted tabular data)')
@click.option('--output', '-o', type=click.Path(), help='Output file path (tabular) or directory (XSAMS). Default for XSAMS: cache directory')
@click.option('--accept-truncation', is_flag=True, help='Accept truncated query results without recursive splitting')
@click.pass_context
def lines(ctx: click.Context, inchikey: tuple, node: tuple, lambda_min: float,
          lambda_max: float, format: str, output: Optional[str], accept_truncation: bool):
    """Get spectral lines for species in a wavelength range.

    This command uses the high-level getLines wrapper, which supports multiple species
    and multiple nodes in a single query. Results can be returned as raw XSAMS files or 
    converted to tabular formats (CSV, JSON, table).

    Example:
        vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --lambda-min=3000 --lambda-max=5000
        vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --inchikey=UGFAIRIUMAVXCW-UHFFFAOYSA-N \\
                        --node=basecol --node=cdms --lambda-min=3000 --lambda-max=5000 --format csv
        vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --format xsams --output ./my_xsams_files
    """
    try:
        if lambda_max <= lambda_min:
            raise ValueError("--lambda-max must be greater than --lambda-min")

        click.echo(f"Querying spectral lines...", err=True)
        click.echo(f"Wavelength range: {lambda_min} - {lambda_max} Angstrom", err=True)

        # Load species and nodes data
        species_df, nodes_df = load_species_data()

        # Filter by InChIKeys if provided
        filtered_species_df = None
        if inchikey:
            click.echo(f"Filtering for {len(inchikey)} species...", err=True)
            filtered_species_df = filter_species_by_inchikeys(species_df, list(inchikey))
            if filtered_species_df.empty:
                click.echo("No matching species found for the provided InChIKeys.", err=True)
                sys.exit(1)
            click.echo(f"Found {len(filtered_species_df)} species entries matching InChIKeys", err=True)

        # Filter by node identifiers if provided
        filtered_nodes_df = None
        if node:
            click.echo(f"Filtering for {len(node)} nodes...", err=True)
            filtered_nodes_df = filter_nodes_by_identifiers(nodes_df, list(node))
            if filtered_nodes_df.empty:
                click.echo("No matching nodes found for the provided identifiers.", err=True)
                sys.exit(1)
            click.echo(f"Found {len(filtered_nodes_df)} nodes matching identifiers", err=True)

        # Call the high-level getLines function
        click.echo("Fetching lines...", err=True)
        atomic_dict, molecular_dict = lines_module.getLines(
            lambdaMin=lambda_min,
            lambdaMax=lambda_max,
            species_dataframe=filtered_species_df,
            nodes_dataframe=filtered_nodes_df,
            verbose=ctx.obj.get('verbose', False),
            acceptTruncation=accept_truncation
        )

        # Handle XSAMS format output
        if format == 'xsams':
            # For XSAMS format, we need to access the query objects from getLines
            # Since getLines() doesn't return the query objects, we need to use a different approach
            # We'll call _build_and_run_wrappings directly and then download XSAMS files
            click.echo("Retrieving XSAMS files...", err=True)
            
            queries = lines_module._build_and_run_wrappings(
                lambda_min, lambda_max, filtered_species_df, filtered_nodes_df,
                ctx.obj.get('verbose', False), accept_truncation
            )
            
            click.echo(f"Downloading {len(queries)} XSAMS file(s)...", err=True)
            
            xsams_files = []
            for query in queries:
                query.getXSAMSData()
                if query.XSAMSFileName:
                    xsams_files.append(query.XSAMSFileName)
            
            # Determine output directory: user-specified or cache directory
            if output:
                output_dir = Path(output).expanduser()
            else:
                # Use cache directory for XSAMS files by default
                output_dir = CACHE_DIR / 'xsams'
            
            output_dir.mkdir(parents=True, exist_ok=True)
            click.echo(f"Moving XSAMS files to {output_dir}...", err=True)
            
            moved_files = []
            for xsams_file in xsams_files:
                src_path = Path(xsams_file)
                if src_path.exists():
                    dst_path = output_dir / src_path.name
                    src_path.replace(dst_path)
                    moved_files.append(str(dst_path))
            
            click.echo(f"\nDownloaded {len(moved_files)} XSAMS file(s) to {output_dir}:")
            for file_path in moved_files:
                click.echo(f"  {file_path}")
            
            return

        # Combine results for tabular formats
        all_frames = []
        
        if atomic_dict:
            click.echo(f"Retrieved atomic data from {len(atomic_dict)} node(s)", err=True)
            for node_id, df in atomic_dict.items():
                df_copy = df.copy()
                df_copy['node'] = node_id
                df_copy['species_type'] = 'atom'
                all_frames.append(df_copy)
        
        if molecular_dict:
            click.echo(f"Retrieved molecular data from {len(molecular_dict)} node(s)", err=True)
            for node_id, df in molecular_dict.items():
                df_copy = df.copy()
                df_copy['node'] = node_id
                df_copy['species_type'] = 'molecule'
                all_frames.append(df_copy)

        if not all_frames:
            click.echo("No spectral lines found for the specified criteria.", err=True)
            sys.exit(0)

        # Combine all dataframes
        combined_df = pd.concat(all_frames, ignore_index=True)
        click.echo(f"Total spectral lines retrieved: {len(combined_df)}", err=True)

        # Format and output
        output_content = format_output(combined_df, format)

        if output:
            if format == 'csv':
                combined_df.to_csv(output, index=False)
            elif format == 'json':
                combined_df.to_json(output, orient='records', indent=2)
            else:
                with open(output, 'w') as f:
                    f.write(output_content)
            click.echo(f"Lines saved to {output}")
        else:
            click.echo(output_content)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        import traceback
        if ctx.obj.get('verbose', False):
            traceback.print_exc()
        sys.exit(1)


@cli.group()
def count():
    """Inspect metadata without downloading full data."""
    pass


@count.command(name='lines')
@click.option('--inchikey', multiple=True, help='InChIKey of the species (can be specified multiple times)')
@click.option('--node', multiple=True, help='Node identifier (shortname, IVO ID, or TAP endpoint, can be specified multiple times)')
@click.option('--lambda-min', type=float, default=DEFAULT_LAMBDA_MIN,
              help=f'Minimum wavelength in Angstrom (default: {DEFAULT_LAMBDA_MIN:g})')
@click.option('--lambda-max', type=float, default=DEFAULT_LAMBDA_MAX,
              help=f'Maximum wavelength in Angstrom (default: {DEFAULT_LAMBDA_MAX:g})')
@click.pass_context
def count_lines(ctx: click.Context, inchikey: tuple, node: tuple, lambda_min: float, lambda_max: float):
    """Inspect HEAD metadata for spectroscopic line queries without downloading data.

    This command uses the high-level get_metadata_for_lines wrapper, which supports
    multiple species and multiple nodes in a single query.

    Example:
        vamdc count lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --lambda-min=3000 --lambda-max=5000
        vamdc count lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --inchikey=UGFAIRIUMAVXCW-UHFFFAOYSA-N \\
                          --node=basecol --node=cdms --lambda-min=3000 --lambda-max=5000
    """
    try:
        if lambda_max <= lambda_min:
            raise ValueError("--lambda-max must be greater than --lambda-min")

        click.echo(f"Inspecting metadata for spectral lines...", err=True)
        click.echo(f"Wavelength range: {lambda_min} - {lambda_max} Angstrom", err=True)

        # Load species and nodes data
        species_df, nodes_df = load_species_data()

        # Filter by InChIKeys if provided
        filtered_species_df = None
        if inchikey:
            click.echo(f"Filtering for {len(inchikey)} species...", err=True)
            filtered_species_df = filter_species_by_inchikeys(species_df, list(inchikey))
            if filtered_species_df.empty:
                click.echo("No matching species found for the provided InChIKeys.", err=True)
                sys.exit(1)
            click.echo(f"Found {len(filtered_species_df)} species entries matching InChIKeys", err=True)

        # Filter by node identifiers if provided
        filtered_nodes_df = None
        if node:
            click.echo(f"Filtering for {len(node)} nodes...", err=True)
            filtered_nodes_df = filter_nodes_by_identifiers(nodes_df, list(node))
            if filtered_nodes_df.empty:
                click.echo("No matching nodes found for the provided identifiers.", err=True)
                sys.exit(1)
            click.echo(f"Found {len(filtered_nodes_df)} nodes matching identifiers", err=True)

        # Call the high-level get_metadata_for_lines function
        click.echo("Fetching metadata (HEAD requests only)...", err=True)
        metadata_list = lines_module.get_metadata_for_lines(
            lambdaMin=lambda_min,
            lambdaMax=lambda_max,
            species_dataframe=filtered_species_df,
            nodes_dataframe=filtered_nodes_df,
            verbose=ctx.obj.get('verbose', False)
        )

        if not metadata_list:
            click.echo("No matching data were found for the specified criteria.")
            sys.exit(0)

        aggregated_counts: Dict[str, float] = {}

        # Display metadata for each sub-query
        for idx, metadata_entry in enumerate(metadata_list, start=1):
            query_url = metadata_entry.get('query', 'N/A')
            counts = metadata_entry.get('metadata', {})
            
            click.echo(f"\nSub-query {idx}: {query_url}")
            
            if counts:
                for key, value in sorted(counts.items()):
                    click.echo(f"  {key}: {value}")
                    try:
                        numeric = coerce_numeric(value)
                        if numeric is not None:
                            aggregated_counts[key] = aggregated_counts.get(key, 0.0) + numeric
                    except (TypeError, ValueError) as e:
                        if ctx.obj.get('verbose', False):
                            click.echo(f"  Warning: Could not aggregate {key}={value}: {e}", err=True)
            else:
                click.echo("  No VAMDC count headers returned.")

        # Display aggregated counts
        if aggregated_counts:
            click.echo(f"\nAggregated numeric headers across {len(metadata_list)} sub-queries:")
            for key, value in sorted(aggregated_counts.items()):
                click.echo(f"  {key}: {format_numeric(value)}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        import traceback
        if ctx.obj.get('verbose', False):
            traceback.print_exc()
        sys.exit(1)


@cli.group()
def cache():
    """Manage local cache."""
    pass


@cache.command(name='clear')
@click.pass_context
def cache_clear(ctx: click.Context):
    """Clear all cached data."""
    clear_cache()


@cache.command(name='status')
@click.pass_context
def cache_status(ctx: click.Context):
    """Show cache status and metadata."""
    cache_files = {
        'Nodes': NODES_CACHE_FILE,
        'Species': SPECIES_CACHE_FILE,
        'Species Nodes': SPECIES_NODES_CACHE_FILE,
    }

    click.echo(f"Cache directory: {CACHE_DIR}")
    click.echo(f"Expiration time: {CACHE_EXPIRATION_HOURS} hours\n")

    for name, cache_file in cache_files.items():
        metadata_file = cache_file.parent / f'{cache_file.stem}_timestamp.json'
        if cache_file.exists() and metadata_file.exists():
            try:
                with open(metadata_file) as f:
                    metadata = json.load(f)
                timestamp = datetime.fromisoformat(metadata['timestamp'])
                valid = is_cache_valid(cache_file)
                status_str = "VALID" if valid else "EXPIRED"
                click.echo(f"{name}: {status_str} (cached at {timestamp})")
            except (json.JSONDecodeError, KeyError):
                click.echo(f"{name}: INVALID")
        else:
            click.echo(f"{name}: NOT CACHED")
    
    # Check for XSAMS files
    xsams_dir = CACHE_DIR / 'xsams'
    if xsams_dir.exists():
        xsams_files = list(xsams_dir.glob('*.xsams'))
        if xsams_files:
            total_size = sum(f.stat().st_size for f in xsams_files)
            size_mb = total_size / (1024 * 1024)
            click.echo(f"\nXSAMS files: {len(xsams_files)} file(s), {size_mb:.2f} MB")
        else:
            click.echo(f"\nXSAMS files: NONE")
    else:
        click.echo(f"\nXSAMS files: NONE")


# Utility functions

def format_output(df: pd.DataFrame, format: str) -> str:
    """Format DataFrame for output."""
    if format == 'json':
        return df.to_json(orient='records', indent=2)
    elif format == 'csv':
        return df.to_csv(index=False)
    elif format == 'table':
        return df.to_string(index=False)
    else:
        return df.to_string(index=False)


def apply_filter(df: pd.DataFrame, filter_str: str) -> pd.DataFrame:
    """Apply filter to dataframe.

    Format: "column:value" or "column:min-max" for numeric ranges.
    """
    if ':' not in filter_str:
        click.echo(f"Invalid filter format: {filter_str}", err=True)
        return df

    column, value = filter_str.split(':', 1)
    column = column.strip()
    value = value.strip()

    if column not in df.columns:
        click.echo(f"Column {column} not found", err=True)
        return df

    # Check if it's a range filter (numeric)
    if '-' in value and value.replace('-', '').replace('.', '').isdigit():
        try:
            min_val, max_val = map(float, value.split('-'))
            return df[(df[column] >= min_val) & (df[column] <= max_val)]
        except (ValueError, IndexError):
            pass

    # String contains filter
    return df[df[column].astype(str).str.contains(value, case=False, na=False)]


def coerce_numeric(value) -> Optional[float]:
    """Try to convert a value to numeric."""
    if value is None:
        return None
    try:
        numeric = float(value)
        return numeric
    except (TypeError, ValueError):
        return None


def format_numeric(value: float) -> str:
    """Format numeric value as string."""
    if float(value).is_integer():
        return str(int(value))
    return str(value)


if __name__ == '__main__':
    cli()
