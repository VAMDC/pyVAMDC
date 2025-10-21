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
    from spectral import vamdcQuery
    from spectral.energyConverter import electromagnetic_conversion
except ImportError:
    # Fall back to absolute imports (when run as console script)
    from pyVAMDC.spectral import species as species_module
    from pyVAMDC.spectral import vamdcQuery
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


def resolve_node_and_species(
    inchikey: str, node_hint: str, species_df: pd.DataFrame, nodes_df: pd.DataFrame
) -> Tuple[str, str]:
    """
    Resolve node endpoint and species type from InChIKey and node hint.
    Uses advanced matching logic from CLI 1.
    """
    normalized_inchikey = inchikey.strip().upper()
    normalized_hint = node_hint.strip().lower()

    inchikey_series = species_df["InChIKey"].astype(str).str.upper()
    species_matches = species_df[inchikey_series == normalized_inchikey]
    if species_matches.empty:
        raise ValueError(f"No species with InChIKey '{inchikey}' were found.")

    # Try exact TAP endpoint match
    node_candidates = species_matches[
        species_matches["tapEndpoint"].str.lower() == normalized_hint
    ]

    # Try IVO identifier match
    if node_candidates.empty:
        node_candidates = species_matches[
            species_matches["ivoIdentifier"].str.lower() == normalized_hint
        ]

    # Try shortname match if available
    if node_candidates.empty and "shortname" in species_matches.columns:
        node_candidates = species_matches[
            species_matches["shortname"].astype(str).str.lower() == normalized_hint
        ]

    # Try matching against node table
    if node_candidates.empty:
        node_candidates = match_against_node_table(species_matches, nodes_df, normalized_hint)

    if node_candidates.empty:
        raise ValueError(
            f"No node matching '{node_hint}' for species '{inchikey}' was found in cached metadata."
        )

    row = node_candidates.iloc[0]
    endpoint = row["tapEndpoint"]
    species_type = row["speciesType"]

    # Check if endpoint is valid (not NaN or empty)
    if pd.isna(endpoint) or endpoint == "":
        raise ValueError(
            f"Node '{node_hint}' for species '{inchikey}' has no TAP endpoint configured. "
            f"This node may not support data queries."
        )

    if species_type not in {"atom", "molecule"}:
        raise ValueError(f"Unsupported species type '{species_type}' for lines retrieval.")
    return endpoint, species_type


def match_against_node_table(
    species_matches: pd.DataFrame, nodes_df: pd.DataFrame, normalized_hint: str
) -> pd.DataFrame:
    """Match node hint against nodes table and return matching species."""
    candidate_nodes = pd.DataFrame()
    if not nodes_df.empty:
        tap_match = nodes_df["tapEndpoint"].astype(str).str.lower() == normalized_hint
        ivo_match = nodes_df["ivoIdentifier"].astype(str).str.lower() == normalized_hint
        node_mask = tap_match | ivo_match
        if "shortName" in nodes_df.columns:
            short_match = nodes_df["shortName"].astype(str).str.lower() == normalized_hint
            node_mask = node_mask | short_match
        matching_nodes = nodes_df[node_mask]
        if not matching_nodes.empty:
            endpoints = matching_nodes["tapEndpoint"].str.lower().tolist()
            candidate_nodes = species_matches[
                species_matches["tapEndpoint"].str.lower().isin(endpoints)
            ]
    return candidate_nodes


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
@click.option('--inchikey', required=True, help='InChIKey of the species')
@click.option('--node', required=True, help='Node identifier (shortname, IVO ID, or TAP endpoint)')
@click.option('--lambda-min', type=float, default=DEFAULT_LAMBDA_MIN,
              help=f'Minimum wavelength in Angstrom (default: {DEFAULT_LAMBDA_MIN:g})')
@click.option('--lambda-max', type=float, default=DEFAULT_LAMBDA_MAX,
              help=f'Maximum wavelength in Angstrom (default: {DEFAULT_LAMBDA_MAX:g})')
@click.option('--format', '-f', type=click.Choice(['xsams', 'csv', 'json', 'table']),
              default='xsams', help='Output format')
@click.option('--output', '-o', type=click.Path(), help='Output file path')
@click.pass_context
def lines(ctx: click.Context, inchikey: str, node: str, lambda_min: float,
          lambda_max: float, format: str, output: Optional[str]):
    """Get spectral lines for a species in a wavelength range.

    Downloads data as XSAMS XML files by default.

    Example:
        vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \\
                        --node=basecol --lambda-min=3000 --lambda-max=5000
        vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \\
                        --node=basecol --format csv --output lines.csv
    """
    try:
        if lambda_max <= lambda_min:
            raise ValueError("--lambda-max must be greater than --lambda-min")

        click.echo(f"Querying spectral lines for {inchikey}...", err=True)
        click.echo(f"Wavelength range: {lambda_min} - {lambda_max} Angstrom", err=True)

        # Load species data
        species_df, nodes_df = load_species_data()

        # Resolve node and species type
        node_endpoint, species_type = resolve_node_and_species(
            inchikey, node, species_df, nodes_df
        )

        # Build queries using VamdcQuery
        queries = []
        vamdcQuery.VamdcQuery(
            nodeEndpoint=node_endpoint,
            lambdaMin=lambda_min,
            lambdaMax=lambda_max,
            InchiKey=inchikey,
            speciesType=species_type,
            totalListOfQueries=queries,
            verbose=ctx.obj.get('verbose', False),
        )

        if not queries:
            click.echo("No matching data were found for the specified criteria.", err=True)
            sys.exit(0)

        if output and format == 'xsams' and len(queries) != 1:
            raise ValueError(
                "Explicit --output with XSAMS format requires the query to resolve to a single file. "
                "Consider narrowing the wavelength range."
            )

        xsams_paths = []
        line_frames = []

        for query in queries:
            query.getXSAMSData()
            if output and format == 'xsams':
                xsams_path = Path(output).expanduser()
                xsams_path.parent.mkdir(parents=True, exist_ok=True)
                Path(query.XSAMSFileName).replace(xsams_path)
                query.XSAMSFileName = str(xsams_path)
            xsams_paths.append(query.XSAMSFileName)

            if format != 'xsams':
                query.convertToDataFrame()
                if getattr(query, "lines_df", None) is not None:
                    line_frames.append(query.lines_df)

        if format == 'xsams':
            for path in xsams_paths:
                click.echo(f"XSAMS written to {path}")
        else:
            if line_frames:
                combined = pd.concat(line_frames, ignore_index=True)
                click.echo(f"Fetched {len(combined)} spectral lines from node {node_endpoint}", err=True)

                output_content = format_output(combined, format)

                if output:
                    if format == 'csv':
                        combined.to_csv(output, index=False)
                    elif format == 'json':
                        combined.to_json(output, orient='records', indent=2)
                    else:
                        with open(output, 'w') as f:
                            f.write(output_content)
                    click.echo(f"Lines saved to {output}")
                else:
                    click.echo(output_content)
            else:
                click.echo("Fetched XSAMS data but no tabular lines were produced.", err=True)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.group()
def count():
    """Inspect metadata without downloading full data."""
    pass


@count.command(name='lines')
@click.option('--inchikey', required=True, help='InChIKey of the species')
@click.option('--node', required=True, help='Node identifier (shortname, IVO ID, or TAP endpoint)')
@click.option('--lambda-min', type=float, default=DEFAULT_LAMBDA_MIN,
              help=f'Minimum wavelength in Angstrom (default: {DEFAULT_LAMBDA_MIN:g})')
@click.option('--lambda-max', type=float, default=DEFAULT_LAMBDA_MAX,
              help=f'Maximum wavelength in Angstrom (default: {DEFAULT_LAMBDA_MAX:g})')
@click.pass_context
def count_lines(ctx: click.Context, inchikey: str, node: str, lambda_min: float, lambda_max: float):
    """Inspect HEAD metadata for spectroscopic line queries without downloading data.

    Example:
        vamdc count lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N \\
                          --node=basecol --lambda-min=3000 --lambda-max=5000
    """
    try:
        if lambda_max <= lambda_min:
            raise ValueError("--lambda-max must be greater than --lambda-min")

        click.echo(f"Inspecting metadata for {inchikey}...", err=True)
        click.echo(f"Wavelength range: {lambda_min} - {lambda_max} Angstrom", err=True)

        # Load species data
        species_df, nodes_df = load_species_data()

        # Resolve node and species type
        try:
            node_endpoint, species_type = resolve_node_and_species(
                inchikey, node, species_df, nodes_df
            )
        except Exception as e:
            click.echo(f"Error resolving node: {e}", err=True)
            raise

        click.echo(f"Resolved node: {node_endpoint}, species type: {species_type}", err=True)

        # Build queries (HEAD requests only, stored in query.counts)
        queries = []
        try:
            vamdcQuery.VamdcQuery(
                nodeEndpoint=node_endpoint,
                lambdaMin=lambda_min,
                lambdaMax=lambda_max,
                InchiKey=inchikey,
                speciesType=species_type,
                totalListOfQueries=queries,
                verbose=ctx.obj.get('verbose', False),
            )
        except Exception as e:
            click.echo(f"Error creating query: {e}", err=True)
            raise

        if not queries:
            click.echo("No matching data were found for the specified criteria.")
            sys.exit(0)

        aggregated_counts: Dict[str, float] = {}

        for idx, query in enumerate(queries, start=1):
            click.echo(
                f"\nSub-query {idx}: node={query.nodeEndpoint} "
                f"lambda_min={query.lambdaMin} lambda_max={query.lambdaMax}"
            )
            if query.counts:
                for key, value in sorted(query.counts.items()):
                    click.echo(f"  {key}: {value}")
                    try:
                        numeric = coerce_numeric(value)
                        if numeric is not None:
                            aggregated_counts[key] = aggregated_counts.get(key, 0.0) + numeric
                    except (TypeError, ValueError) as e:
                        click.echo(f"  Warning: Could not aggregate {key}={value}: {e}", err=True)
            else:
                click.echo("  No VAMDC count headers returned.")

        if aggregated_counts:
            click.echo("\nAggregated numeric headers:")
            for key, value in sorted(aggregated_counts.items()):
                click.echo(f"  {key}: {format_numeric(value)}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
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
