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
    from spectral.energyConverter import electromagnetic_conversion, get_conversion_factors
    from spectral.slap import create_slap2_votables_from_species, create_slap2_votables_from_lines
    from spectral.logging_config import set_log_level, get_log_level, LogLevel, configure_python_logging
except ImportError:
    # Fall back to absolute imports (when run as console script)
    from pyVAMDC.spectral import species as species_module
    from pyVAMDC.spectral import lines as lines_module
    from pyVAMDC.spectral import filters as filters_module
    from pyVAMDC.spectral.energyConverter import electromagnetic_conversion, get_conversion_factors
    from pyVAMDC.spectral.slap import create_slap2_votables_from_species, create_slap2_votables_from_lines
    from pyVAMDC.spectral.logging_config import set_log_level, get_log_level, LogLevel, configure_python_logging

# Default logging configuration (will be reconfigured based on CLI flags)
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





def resolve_node_identifier(
    node_hint: str, species_df: pd.DataFrame, nodes_df: pd.DataFrame
) -> str:
    """
    Resolve a node identifier (short name, IVO ID, or TAP endpoint) to a full TAP endpoint.
    Uses multi-step matching: TAP endpoint → IVO identifier → exact short name → fuzzy short name → nodes table.
    Supports fuzzy matching on short names (e.g., "vald" matches "VALD (atoms)").

    Args:
        node_hint: User-provided node identifier (e.g., "vald", "ivo://vamdc/...", or full URL)
        species_df: DataFrame containing species with node metadata
        nodes_df: DataFrame containing node information

    Returns:
        Full TAP endpoint URL

    Raises:
        ValueError: If node cannot be resolved
    """
    normalized_hint = node_hint.strip().lower()

    # Step 1: Try exact TAP endpoint match in species dataframe
    node_candidates = species_df[
        species_df["tapEndpoint"].astype(str).str.lower() == normalized_hint
    ]

    # Step 2: Try IVO identifier match in species dataframe
    if node_candidates.empty:
        node_candidates = species_df[
            species_df["ivoIdentifier"].astype(str).str.lower() == normalized_hint
        ]

    # Step 3: Try exact short name match in species dataframe (if available)
    if node_candidates.empty and "shortName" in species_df.columns:
        node_candidates = species_df[
            species_df["shortName"].astype(str).str.lower() == normalized_hint
        ]

    # Step 4: Try fuzzy short name match in species dataframe (substring match)
    if node_candidates.empty and "shortName" in species_df.columns:
        node_candidates = species_df[
            species_df["shortName"].astype(str).str.lower().str.contains(normalized_hint, regex=False)
        ]

    # Step 5: Try matching against nodes table (includes its own fuzzy matching)
    if node_candidates.empty:
        node_candidates = match_against_node_table(species_df, nodes_df, normalized_hint)

    if node_candidates.empty:
        raise ValueError(
            f"No node matching '{node_hint}' was found. "
            f"Try using a full TAP endpoint URL, short name (e.g., 'vald'), or IVO identifier."
        )

    # Return the first matching endpoint
    endpoint = node_candidates.iloc[0]["tapEndpoint"]

    # Validate endpoint
    if pd.isna(endpoint) or endpoint == "":
        raise ValueError(
            f"Node '{node_hint}' has no TAP endpoint configured. "
            f"This node may not support data queries."
        )

    return str(endpoint)


def match_against_node_table(
    species_df: pd.DataFrame, nodes_df: pd.DataFrame, normalized_hint: str
) -> pd.DataFrame:
    """
    Match node hint against nodes table and return matching species.

    This is a fallback matching strategy when direct species dataframe matching fails.
    It checks the full nodes table for the identifier and returns species using those nodes.
    Supports fuzzy matching on short names (e.g., "vald" matches "VALD (atoms)").

    Args:
        species_df: DataFrame containing species information
        nodes_df: DataFrame containing node metadata
        normalized_hint: Lowercase node identifier to match

    Returns:
        DataFrame with matching species rows
    """
    candidate_nodes = pd.DataFrame()

    if not nodes_df.empty:
        tap_match = nodes_df["tapEndpoint"].astype(str).str.lower() == normalized_hint
        ivo_match = nodes_df["ivoIdentifier"].astype(str).str.lower() == normalized_hint
        node_mask = tap_match | ivo_match

        if "shortName" in nodes_df.columns:
            short_match = nodes_df["shortName"].astype(str).str.lower() == normalized_hint
            node_mask = node_mask | short_match

            if not node_mask.any():
                fuzzy_match = nodes_df["shortName"].astype(str).str.lower().str.contains(normalized_hint, regex=False)
                node_mask = node_mask | fuzzy_match

        matching_nodes = nodes_df[node_mask]

        if not matching_nodes.empty:
            endpoints = matching_nodes["tapEndpoint"].str.lower().tolist()
            candidate_nodes = species_df[
                species_df["tapEndpoint"].str.lower().isin(endpoints)
            ]

    return candidate_nodes


def filter_species_by_inchikeys_resolved(
    inchikeys: list, species_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Filter species dataframe by one or more InChIKeys with resolution.
    
    Args:
        inchikeys: List of InChIKey strings
        species_df: Species dataframe to filter
    
    Returns:
        Filtered species dataframe
    """
    if not inchikeys:
        return species_df
    
    normalized_keys = [key.strip().upper() for key in inchikeys]
    inchikey_series = species_df["InChIKey"].astype(str).str.upper()
    return species_df[inchikey_series.isin(normalized_keys)]


def filter_nodes_by_identifiers_resolved(
    node_identifiers: list, species_df: pd.DataFrame, nodes_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Filter nodes dataframe by one or more node identifiers with intelligent resolution.
    
    This function resolves each node identifier to a full TAP endpoint, then returns
    all matching species entries. This enables support for short names, IVO identifiers,
    and full endpoints, just like CLI.py.
    
    Args:
        node_identifiers: List of node identifiers (shortname, IVO ID, or TAP endpoint)
        species_df: Species dataframe containing node information
        nodes_df: Nodes dataframe for fallback matching
    
    Returns:
        Filtered species dataframe containing all species from matching nodes
    
    Raises:
        ValueError: If any node identifier cannot be resolved
    """
    if not node_identifiers:
        return species_df
    
    resolved_endpoints = []
    
    for node_id in node_identifiers:
        try:
            endpoint = resolve_node_identifier(node_id, species_df, nodes_df)
            resolved_endpoints.append(endpoint.lower())
        except ValueError as e:
            raise ValueError(f"Failed to resolve node '{node_id}': {str(e)}")
    
    # Return species that use any of the resolved endpoints
    endpoint_series = species_df["tapEndpoint"].astype(str).str.lower()
    return species_df[endpoint_series.isin(resolved_endpoints)]


def get_all_supported_units() -> Dict[str, list]:
    """
    Get all supported units organized by category.
    
    Returns:
        A dictionary with keys 'energy', 'frequency', 'wavelength', each containing a list of supported unit names.
    """
    conversion_factors = get_conversion_factors()
    return {
        'energy': list(conversion_factors['energy'].keys()),
        'frequency': list(conversion_factors['frequency'].keys()),
        'wavelength': list(conversion_factors['wavelength'].keys())
    }


def is_valid_unit(unit: str) -> bool:
    """
    Check if a unit is supported by the electromagnetic conversion function.
    
    Args:
        unit: The unit name to validate
    
    Returns:
        True if the unit is supported, False otherwise
    """
    supported_units = get_all_supported_units()
    all_units = set()
    for units_list in supported_units.values():
        all_units.update(units_list)
    return unit in all_units


def get_unit_category(unit: str) -> Optional[str]:
    """
    Get the category of a unit (energy, frequency, or wavelength).
    
    Args:
        unit: The unit name
    
    Returns:
        The category string or None if unit is not found
    """
    supported_units = get_all_supported_units()
    # Get normalized unit first
    normalized_unit = normalize_unit(unit)
    if normalized_unit:
        for category, units_list in supported_units.items():
            if normalized_unit in units_list:
                return category
    return None


def normalize_unit(unit: str) -> Optional[str]:
    """
    Normalize unit name to match exactly what's in the conversion factors.
    Performs case-insensitive matching against known units.
    
    Args:
        unit: The unit name to normalize
    
    Returns:
        The exact unit name as stored in conversion factors, or None if not found
    """
    supported_units = get_all_supported_units()
    # Create a case-insensitive lookup dictionary
    unit_lookup = {}
    for category, units_list in supported_units.items():
        for unit_name in units_list:
            unit_lookup[unit_name.lower()] = unit_name
    
    return unit_lookup.get(unit.lower())


@click.group()
@click.option('--quiet', '-q', is_flag=True, help='Minimal output (errors as one-liners, ideal for AI agents)')
@click.option('--verbose', '-v', is_flag=True, help='Detailed output with context')
@click.option('--debug', is_flag=True, help='Full debug output with tracebacks')
@click.pass_context
def cli(ctx: click.Context, quiet: bool, verbose: bool, debug: bool):
    """VAMDC CLI - Query atomic and molecular spectroscopic data."""
    ctx.ensure_object(dict)
    
    # Determine log level (last flag wins if multiple specified)
    if debug:
        log_level = LogLevel.DEBUG
    elif verbose:
        log_level = LogLevel.VERBOSE
    elif quiet:
        log_level = LogLevel.MINIMAL
    else:
        log_level = LogLevel.NORMAL
    
    # Set global log level
    set_log_level(log_level)
    ctx.obj['log_level'] = log_level
    ctx.obj['verbose'] = (log_level in (LogLevel.VERBOSE, LogLevel.DEBUG))
    
    # Configure Python's logging module
    configure_python_logging()


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
@click.option('--slap2', is_flag=True, help='Generate SLAP2-compliant VOTable files from species data')
@click.pass_context
def species_cmd(ctx: click.Context, format: str, output: Optional[str], refresh: bool,
                filter_by: Optional[str], slap2: bool):
    """Get list of chemical species and cache them locally.

    Use --slap2 flag to generate SLAP2-compliant VOTable files (independent of format).
    VOTable files will be stored in the specified output directory or cache directory.

    Example:
        vamdc get species
        vamdc get species --format csv --output species.csv
        vamdc get species --filter-by "name:CO"
        vamdc get species --slap2
        vamdc get species --slap2 --output /path/to/votables/
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

        # Format output (only for non-SLAP2 operations)
        if not slap2:
            # User wants species data export
            output_content = format_output(df_species, format)

            # Write to file or stdout
            if output:
                if format == 'excel':
                    df_species.to_excel(output, index=False)
                elif format == 'csv':
                    df_species.to_csv(output, index=False)
                elif format == 'json':
                    df_species.to_json(output, orient='records', indent=2)
                else:  # format == 'table'
                    with open(output, 'w') as f:
                        f.write(output_content)
                click.echo(f"Species saved to {output}")
            else:
                click.echo(output_content)

        # Generate SLAP2 VOTables if requested
        if slap2:
            click.echo("Generating SLAP2-compliant VOTable files...", err=True)
            try:
                # Determine output directory for VOTables
                if output:
                    # Use specified output as directory
                    votable_dir = Path(output).expanduser()
                else:
                    # Use cache directory
                    votable_dir = CACHE_DIR / 'votables'
                
                votable_dir.mkdir(parents=True, exist_ok=True)
                
                # Generate VOTables grouped by node
                slap2_results = create_slap2_votables_from_species(
                    output_directory=str(votable_dir)
                )
                
                click.echo(f"\nGenerated {len(slap2_results)} SLAP2 VOTable file(s) to {votable_dir}:")
                for result in slap2_results:
                    votable_file = result['votable_filepath']
                    click.echo(f"  {result['node_shortname']}: {Path(votable_file).name}")
                    click.echo(f"    Species: {result['species_count']}")
            
            except Exception as e:
                click.echo(f"Warning: Failed to generate SLAP2 VOTables: {e}", err=True)

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
@click.option('--format', '-f', type=click.Choice(['xsams', 'slap2', 'csv', 'json', 'table']),
              default='table', help='Output format (xsams/slap2: raw XML files, csv/json/table: converted tabular data)')
@click.option('--output', '-o', type=click.Path(), help='Output file path (tabular) or directory (XSAMS/SLAP2). Default for XSAMS/SLAP2: cache directory')
@click.option('--accept-truncation', is_flag=True, help='Accept truncated query results without recursive splitting')
@click.pass_context
def lines(ctx: click.Context, inchikey: tuple, node: tuple, lambda_min: float,
          lambda_max: float, format: str, output: Optional[str], accept_truncation: bool):
    """Get spectral lines for species in a wavelength range.

    This command uses the high-level getLines wrapper, which supports multiple species
    and multiple nodes in a single query. Results can be returned as raw XSAMS or SLAP2 XML files,
    or converted to tabular formats (CSV, JSON, table).

    Example:
        vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --lambda-min=3000 --lambda-max=5000
        vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --inchikey=UGFAIRIUMAVXCW-UHFFFAOYSA-N \\
                        --node=basecol --node=cdms --lambda-min=3000 --lambda-max=5000 --format csv
        vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --format xsams --output ./my_xsams_files
        vamdc get lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --format slap2 --output ./my_votables/
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
            filtered_species_df = filter_species_by_inchikeys_resolved(list(inchikey), species_df)
            if filtered_species_df.empty:
                click.echo("No matching species found for the provided InChIKeys.", err=True)
                sys.exit(1)
            click.echo(f"Found {len(filtered_species_df)} species entries matching InChIKeys", err=True)

        # Filter by node identifiers if provided (with intelligent resolution)
        filtered_nodes_df = None
        if node:
            click.echo(f"Resolving {len(node)} node identifier(s)...", err=True)
            try:
                filtered_species_df_by_node = filter_nodes_by_identifiers_resolved(
                    list(node), species_df, nodes_df
                )
                if filtered_species_df_by_node.empty:
                    click.echo("No matching nodes found for the provided identifiers.", err=True)
                    sys.exit(1)
                click.echo(f"Resolved nodes, found species from {filtered_species_df_by_node['tapEndpoint'].nunique()} node(s)", err=True)
                
                # If we already filtered by inchikey, intersect the two filters
                if filtered_species_df is not None:
                    filtered_species_df = filtered_species_df.merge(
                        filtered_species_df_by_node[['tapEndpoint', 'InChIKey']].drop_duplicates(),
                        on=['tapEndpoint', 'InChIKey'],
                        how='inner'
                    )
                else:
                    filtered_species_df = filtered_species_df_by_node
            except ValueError as e:
                click.echo(f"Error: {e}", err=True)
                sys.exit(1)

        # Call the high-level getLines function
        click.echo("Fetching lines...", err=True)
        atomic_dict, molecular_dict, queries_metadata = lines_module.getLines(
            lambdaMin=lambda_min,
            lambdaMax=lambda_max,
            species_dataframe=filtered_species_df,
            nodes_dataframe=filtered_nodes_df,
            acceptTruncation=accept_truncation
        )

        # Handle SLAP2 VOTable format output
        if format == 'slap2':
            click.echo("Generating SLAP2-compliant VOTable files...", err=True)
            try:
                # Determine output directory for VOTables
                if output:
                    # Use specified output as directory
                    votable_dir = Path(output).expanduser()
                else:
                    # Use cache directory
                    votable_dir = CACHE_DIR / 'votables'
                
                votable_dir.mkdir(parents=True, exist_ok=True)
                
                # Generate VOTables grouped by node
                slap2_results = create_slap2_votables_from_lines(
                    atomic_dict,
                    molecular_dict,
                    queries_metadata,
                    lambdaMin=lambda_min,
                    lambdaMax=lambda_max,
                    output_directory=str(votable_dir)
                )
                
                click.echo(f"\nGenerated {len(slap2_results)} SLAP2 VOTable file(s) to {votable_dir}:")
                for result in slap2_results:
                    votable_file = result['votable_filepath']
                    species_type = result['species_type']
                    lines_count = result['lines_count']
                    click.echo(f"  {Path(votable_file).name}")
                    click.echo(f"    Species type: {species_type}")
                    click.echo(f"    Lines: {lines_count}")
            
            except Exception as e:
                click.echo(f"Warning: Failed to generate SLAP2 VOTables: {e}", err=True)
            
            return

        # Handle XSAMS format output
        if format == 'xsams':
            # For XSAMS format, use the XSAMS file paths already downloaded by getLines()
            click.echo("Processing XSAMS files...", err=True)
            
            xsams_files = []
            for metadata_entry in queries_metadata:
                xsams_file_path = metadata_entry.get('XSAMS_file_path')
                if xsams_file_path:
                    xsams_files.append(xsams_file_path)
            
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
        if ctx.obj.get('log_level', LogLevel.NORMAL) == LogLevel.DEBUG:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.group()
def count():
    """Inspect metadata without downloading full data."""
    pass


@count.command(name='lines')
@click.option('--inchikey', multiple=True, help='InChIKey of the species (can be specified multiple times). If not specified, all species are included.')
@click.option('--node', multiple=True, help='Node identifier (shortname, IVO ID, or TAP endpoint, can be specified multiple times). If not specified, all nodes are included.')
@click.option('--lambda-min', type=float, default=DEFAULT_LAMBDA_MIN,
              help=f'Minimum wavelength in Angstrom (default: {DEFAULT_LAMBDA_MIN:g})')
@click.option('--lambda-max', type=float, default=DEFAULT_LAMBDA_MAX,
              help=f'Maximum wavelength in Angstrom (default: {DEFAULT_LAMBDA_MAX:g})')
@click.pass_context
def count_lines(ctx: click.Context, inchikey: tuple, node: tuple, lambda_min: float, lambda_max: float):
    """Inspect HEAD metadata for spectroscopic line queries without downloading data.

    This command queries HEAD metadata from VAMDC nodes for spectral lines in a wavelength range.
    Species and node filters are optional; if not specified, all species and nodes are queried.

    Example:
        # Query all species across all nodes in a wavelength range
        vamdc count lines --lambda-min=3000 --lambda-max=5000
        
        # Query specific species only
        vamdc count lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --lambda-min=3000 --lambda-max=5000
        
        # Query multiple species
        vamdc count lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --inchikey=UGFAIRIUMAVXCW-UHFFFAOYSA-N \\
                          --lambda-min=3000 --lambda-max=5000
        
        # Query specific nodes only
        vamdc count lines --node=basecol --node=cdms --lambda-min=3000 --lambda-max=5000
        
        # Query specific species from specific nodes
        vamdc count lines --inchikey=LFQSCWFLJHTTHZ-UHFFFAOYSA-N --node=basecol \\
                          --lambda-min=3000 --lambda-max=5000
    """
    try:
        if lambda_max <= lambda_min:
            raise ValueError("--lambda-max must be greater than --lambda-min")

        click.echo(f"Inspecting metadata for spectral lines...", err=True)
        click.echo(f"Wavelength range: {lambda_min} - {lambda_max} Angstrom", err=True)

        # Load species and nodes data (only if filtering is needed)
        filtered_species_df = None
        filtered_nodes_df = None
        
        if inchikey or node:
            species_df, nodes_df = load_species_data()

            # Filter by InChIKeys if provided
            if inchikey:
                click.echo(f"Filtering for {len(inchikey)} species...", err=True)
                filtered_species_df = filter_species_by_inchikeys_resolved(list(inchikey), species_df)
                if filtered_species_df.empty:
                    click.echo("No matching species found for the provided InChIKeys.", err=True)
                    sys.exit(1)
                click.echo(f"Found {len(filtered_species_df)} species entries matching InChIKeys", err=True)

            # Filter by node identifiers if provided (with intelligent resolution)
            if node:
                click.echo(f"Resolving {len(node)} node identifier(s)...", err=True)
                try:
                    filtered_species_df_by_node = filter_nodes_by_identifiers_resolved(
                        list(node), species_df, nodes_df
                    )
                    if filtered_species_df_by_node.empty:
                        click.echo("No matching nodes found for the provided identifiers.", err=True)
                        sys.exit(1)
                    click.echo(f"Resolved nodes, found species from {filtered_species_df_by_node['tapEndpoint'].nunique()} node(s)", err=True)
                    
                    # If we already filtered by inchikey, intersect the two filters
                    if filtered_species_df is not None:
                        filtered_species_df = filtered_species_df.merge(
                            filtered_species_df_by_node[['tapEndpoint', 'InChIKey']].drop_duplicates(),
                            on=['tapEndpoint', 'InChIKey'],
                            how='inner'
                        )
                    else:
                        filtered_species_df = filtered_species_df_by_node
                except ValueError as e:
                    click.echo(f"Error: {e}", err=True)
                    sys.exit(1)
        else:
            click.echo("No species or node filters provided; querying all species across all nodes.", err=True)

        # Call the high-level get_metadata_for_lines function
        click.echo("Fetching metadata (HEAD requests only)...", err=True)
        metadata_list = lines_module.get_metadata_for_lines(
            lambdaMin=lambda_min,
            lambdaMax=lambda_max,
            species_dataframe=filtered_species_df,
            nodes_dataframe=filtered_nodes_df
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
        if ctx.obj.get('log_level', LogLevel.NORMAL) == LogLevel.DEBUG:
            import traceback
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


@cli.group()
def convert():
    """Convert between electromagnetic units (energy, frequency, wavelength)."""
    pass


@convert.command(name='energy')
@click.argument('value', type=float)
@click.option('--from-unit', '-f', type=str, required=True, 
              help='Source unit (e.g., joule, eV, hertz, meter, angstrom)')
@click.option('--to-unit', '-t', type=str, required=True,
              help='Target unit (e.g., eV, kelvin, cm-1, gigahertz, nanometer)')
@click.pass_context
def convert_energy(ctx: click.Context, value: float, from_unit: str, to_unit: str):
    """Convert energy, frequency, or wavelength values between different units.

    Supports conversions between:
    - Energy units: joule, millijoule, microjoule, nanojoule, picojoule, eV, erg, kelvin, rydberg, cm-1
    - Frequency units: hertz, kilohertz, megahertz, gigahertz, terahertz
    - Wavelength units: meter, centimeter, millimeter, micrometer, nanometer, angstrom

    Example:
        vamdc convert energy 500 --from-unit=nanometer --to-unit=eV
        vamdc convert energy 1.5 --from-unit=eV --to-unit=cm-1
        vamdc convert energy 100 --from-unit=gigahertz --to-unit=meter
        vamdc convert energy 3000 -f angstrom -t nanometer
    """
    try:
        # Normalize unit names to exact case as stored in conversion factors
        from_unit_normalized = normalize_unit(from_unit)
        to_unit_normalized = normalize_unit(to_unit)
        
        # Validate from_unit
        if not from_unit_normalized:
            supported_units = get_all_supported_units()
            all_units_str = "\n".join([
                f"  {category}: {', '.join(units)}"
                for category, units in supported_units.items()
            ])
            click.echo(f"Error: Invalid from-unit '{from_unit}'. Supported units:", err=True)
            click.echo(all_units_str, err=True)
            sys.exit(1)
        
        # Validate to_unit
        if not to_unit_normalized:
            supported_units = get_all_supported_units()
            all_units_str = "\n".join([
                f"  {category}: {', '.join(units)}"
                for category, units in supported_units.items()
            ])
            click.echo(f"Error: Invalid to-unit '{to_unit}'. Supported units:", err=True)
            click.echo(all_units_str, err=True)
            sys.exit(1)
        
        # Perform the conversion
        converted_value = electromagnetic_conversion(value, from_unit_normalized, to_unit_normalized)
        
        # Format the output: numeric value followed by target unit
        # Use scientific notation if the value is very small or very large
        if abs(converted_value) < 1e-6 or abs(converted_value) > 1e6:
            formatted_value = f"{converted_value:.6e}"
        else:
            # For reasonable-sized numbers, show up to 10 significant figures
            formatted_value = f"{converted_value:.10g}"
        
        click.echo(f"{formatted_value} {to_unit_normalized}")
        
        if ctx.obj.get('verbose', False):
            click.echo(f"Conversion details:", err=True)
            click.echo(f"  Input: {value} {from_unit_normalized}", err=True)
            click.echo(f"  Output: {formatted_value} {to_unit_normalized}", err=True)
            from_category = get_unit_category(from_unit_normalized)
            to_category = get_unit_category(to_unit_normalized)
            click.echo(f"  Category conversion: {from_category} → {to_category}", err=True)
    
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: Unexpected error during conversion: {e}", err=True)
        if ctx.obj.get('log_level', LogLevel.NORMAL) == LogLevel.DEBUG:
            import traceback
            traceback.print_exc()
        sys.exit(1)




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
