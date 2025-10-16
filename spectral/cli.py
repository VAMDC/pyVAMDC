"""VAMDC command-line interface for querying atomic and molecular data."""

import json
import logging
import os
import shutil
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import click
import pandas as pd

try:
    # Try relative imports first (when run as module)
    from spectral import species, lines
    from spectral.energyConverter import electromagnetic_conversion
except ImportError:
    # Fall back to absolute imports (when run as console script)
    from pyVAMDC.spectral import species, lines
    from pyVAMDC.spectral.energyConverter import electromagnetic_conversion

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Determine cache directory
CACHE_DIR = Path.home() / '.vamdc_cache'
CACHE_DIR.mkdir(exist_ok=True)

# Cache file paths
NODES_CACHE_FILE = CACHE_DIR / 'nodes.json'
SPECIES_CACHE_FILE = CACHE_DIR / 'species.parquet'
SPECIES_METADATA_FILE = CACHE_DIR / 'species_metadata.json'

# Cache expiration time (24 hours)
CACHE_EXPIRATION_HOURS = 24


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
        CACHE_DIR.mkdir(exist_ok=True)
        click.echo("Cache cleared.")


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.option('--cache-dir', default=str(CACHE_DIR), help='Cache directory path')
@click.pass_context
def cli(ctx: click.Context, verbose: bool, cache_dir: str):
    """VAMDC CLI - Query atomic and molecular spectroscopic data."""
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose
    ctx.obj['cache_dir'] = Path(cache_dir)

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
            with open(NODES_CACHE_FILE) as f:
                nodes_data = json.load(f)
            df_nodes = pd.DataFrame(nodes_data['data'])
        else:
            click.echo("Fetching nodes from VAMDC Species Database...", err=True)
            _, df_nodes = species.getAllSpecies()

            # Cache the data
            nodes_data = {
                'timestamp': datetime.now().isoformat(),
                'data': df_nodes.to_dict('records')
            }
            with open(NODES_CACHE_FILE, 'w') as f:
                json.dump(nodes_data, f)
            save_cache_metadata(NODES_CACHE_FILE)

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


@get.command()
@click.option('--format', '-f', type=click.Choice(['json', 'csv', 'parquet', 'excel', 'table']),
              default='table', help='Output format')
@click.option('--output', '-o', type=click.Path(), help='Output file path')
@click.option('--refresh', is_flag=True, help='Force refresh cache')
@click.option('--filter-by', type=click.STRING, help='Filter by criteria (e.g., "name:CO")')
@click.pass_context
def species(ctx: click.Context, format: str, output: Optional[str], refresh: bool,
            filter_by: Optional[str]):
    """Get list of chemical species and cache them locally.

    Example:
        vamdc get species
        vamdc get species --format csv --output species.csv
        vamdc get species --filter-by "name:CO"
    """
    try:
        # Check cache
        if is_cache_valid(SPECIES_CACHE_FILE) and not refresh:
            click.echo("Loading species from cache...", err=True)
            df_species = pd.read_parquet(SPECIES_CACHE_FILE)
        else:
            click.echo("Fetching species from VAMDC Species Database...", err=True)
            df_species, _ = species.getAllSpecies()

            # Cache the data in parquet format (more efficient)
            df_species.to_parquet(SPECIES_CACHE_FILE)
            save_cache_metadata(SPECIES_CACHE_FILE)

        # Apply filters if specified
        if filter_by:
            df_species = apply_filter(df_species, filter_by)

        # Format output
        output_content = format_output(df_species, format)

        # Write to file or stdout
        if output:
            if format == 'excel':
                df_species.to_excel(output)
            elif format == 'csv':
                df_species.to_csv(output, index=False)
            elif format == 'parquet':
                df_species.to_parquet(output)
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
@click.option('--inchikey', required=True, help='InChIKey of the species')
@click.option('--node', help='Specific VAMDC node to query')
@click.option('--lambda-min', type=float, required=True, help='Minimum wavelength (Angstrom)')
@click.option('--lambda-max', type=float, required=True, help='Maximum wavelength (Angstrom)')
@click.option('--format', '-f', type=click.Choice(['xsams', 'csv', 'json', 'table']),
              default='xsams', help='Output format')
@click.option('--output', '-o', type=click.Path(), help='Output file path')
@click.pass_context
def lines(ctx: click.Context, inchikey: str, node: Optional[str], lambda_min: float,
          lambda_max: float, format: str, output: Optional[str]):
    """Get spectral lines for a species in a wavelength range.

    Downloads data as XSAMS XML files by default.

    Example:
        vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \\
                        --lambda-min=3000 --lambda-max=5000
        vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \\
                        --node=astrophysics --format csv --output lines.csv
    """
    try:
        click.echo(f"Querying spectral lines for {inchikey}...", err=True)
        click.echo(f"Wavelength range: {lambda_min} - {lambda_max} Angstrom", err=True)

        # Get all species data to filter
        if is_cache_valid(SPECIES_CACHE_FILE):
            df_all_species = pd.read_parquet(SPECIES_CACHE_FILE)
        else:
            click.echo("Fetching species data...", err=True)
            df_all_species, _ = species.getAllSpecies()
            df_all_species.to_parquet(SPECIES_CACHE_FILE)
            save_cache_metadata(SPECIES_CACHE_FILE)

        # Filter to specific species
        df_species = df_all_species[df_all_species['InChIKey'] == inchikey]
        if df_species.empty:
            click.echo(f"Species with InChIKey {inchikey} not found", err=True)
            sys.exit(1)

        # Filter to specific node if requested
        if node:
            df_nodes = df_all_species[df_all_species['shortName'] == node].drop_duplicates(
                subset=['ivoIdentifier'])
            if df_nodes.empty:
                click.echo(f"Node {node} not found", err=True)
                sys.exit(1)
        else:
            df_nodes = None

        # Query lines
        atomic_results, molecular_results = lines.getLines(
            lambda_min, lambda_max,
            species_dataframe=df_species,
            nodes_dataframe=df_nodes,
            verbose=ctx.obj.get('verbose', False)
        )

        # Combine results
        all_results = {**atomic_results, **molecular_results}

        if not all_results:
            click.echo("No spectral lines found matching criteria", err=True)
            return

        # Format output
        if format == 'xsams':
            click.echo("XSAMS files saved to ./XSAMS/ directory", err=True)
            if output:
                click.echo(f"Note: XML files cannot be combined into {output}", err=True)
        else:
            # Combine all dataframes
            df_combined = pd.concat(all_results.values(), ignore_index=True)
            output_content = format_output(df_combined, format)

            if output:
                if format == 'csv':
                    df_combined.to_csv(output, index=False)
                elif format == 'json':
                    df_combined.to_json(output, orient='records', indent=2)
                else:
                    with open(output, 'w') as f:
                        f.write(output_content)
                click.echo(f"Lines saved to {output}")
            else:
                click.echo(output_content)

        click.echo(f"Found {len(all_results)} data sources with spectral lines", err=True)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.group()
def cache():
    """Manage local cache."""
    pass


@cache.command()
@click.pass_context
def clear(ctx: click.Context):
    """Clear all cached data."""
    clear_cache()


@cache.command()
@click.pass_context
def status(ctx: click.Context):
    """Show cache status and metadata."""
    cache_files = {
        'Nodes': NODES_CACHE_FILE,
        'Species': SPECIES_CACHE_FILE,
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


if __name__ == '__main__':
    cli()
