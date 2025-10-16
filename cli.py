from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

import pandas as pd

from pyVAMDC.spectral import species as species_module
from pyVAMDC.spectral import vamdcQuery


DEFAULT_LAMBDA_MIN = 0.0
DEFAULT_LAMBDA_MAX = 1.0e9


@dataclass
class CacheConfig:
    directory: Path
    force_refresh: bool


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command is None:
        parser.print_help()
        return 0

    try:
        if args.command == "get":
            return _handle_get(args)
        if args.command == "count":
            return _handle_count(args)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:  # pragma: no cover - defensive
        print(f"vamdc: error: {exc}", file=sys.stderr)
        return 1

    parser.print_help()
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="vamdc",
        description="Command line interface for interacting with the VAMDC infrastructure.",
    )
    subparsers = parser.add_subparsers(dest="command")

    get_parser = subparsers.add_parser("get", help="Retrieve data from VAMDC services")
    get_subparsers = get_parser.add_subparsers(dest="get_command")

    nodes_parser = get_subparsers.add_parser("nodes", help="Fetch the VAMDC nodes registry")
    _add_cache_arguments(nodes_parser)

    species_parser = get_subparsers.add_parser("species", help="Fetch the species catalogue")
    _add_cache_arguments(species_parser)

    lines_parser = get_subparsers.add_parser("lines", help="Fetch spectroscopic lines for a species")
    _add_cache_arguments(lines_parser, include_force=False)
    _add_lines_query_arguments(lines_parser, include_output=True)

    count_parser = subparsers.add_parser("count", help="Inspect metadata without downloading data")
    count_subparsers = count_parser.add_subparsers(dest="count_command")

    count_lines_parser = count_subparsers.add_parser(
        "lines", help="Inspect HEAD metadata for spectroscopic line queries"
    )
    _add_cache_arguments(count_lines_parser, include_force=False)
    _add_lines_query_arguments(count_lines_parser, include_output=False)

    return parser


def _add_cache_arguments(parser: argparse.ArgumentParser, include_force: bool = True) -> None:
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Directory used to cache fetched data (default: $VAMDC_CACHE_DIR or ~/.cache/vamdc).",
    )
    if include_force:
        parser.add_argument(
            "--force-refresh",
            action="store_true",
            help="Ignore cached data and fetch a fresh copy.",
        )


def _add_lines_query_arguments(parser: argparse.ArgumentParser, include_output: bool) -> None:
    parser.add_argument("--inchikey", required=True, help="InChIKey of the target species")
    parser.add_argument(
        "--node",
        required=True,
        help=(
            "Node identifier. Matches against the node shortname, ivoIdentifier, or TAP endpoint."
        ),
    )
    parser.add_argument(
        "--lambda-min",
        type=float,
        default=DEFAULT_LAMBDA_MIN,
        help=f"Lower wavelength bound in Angstrom (default: {DEFAULT_LAMBDA_MIN:g}).",
    )
    parser.add_argument(
        "--lambda-max",
        type=float,
        default=DEFAULT_LAMBDA_MAX,
        help=f"Upper wavelength bound in Angstrom (default: {DEFAULT_LAMBDA_MAX:g}).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable HTTP diagnostics from the underlying library.",
    )
    if include_output:
        parser.add_argument(
            "--output", type=Path, help="Optional path for the XSAMS output file."
        )


def _handle_get(args: argparse.Namespace) -> int:
    if args.get_command == "nodes":
        return _cmd_get_nodes(_make_cache_config(args))
    if args.get_command == "species":
        return _cmd_get_species(_make_cache_config(args))
    if args.get_command == "lines":
        return _cmd_get_lines(args)

    raise ValueError("unknown get subcommand")


def _handle_count(args: argparse.Namespace) -> int:
    if args.count_command == "lines":
        return _cmd_count_lines(args)
    raise ValueError("unknown count subcommand")


def _make_cache_config(args: argparse.Namespace) -> CacheConfig:
    cache_dir = _resolve_cache_dir(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    force_refresh = getattr(args, "force_refresh", False)
    return CacheConfig(directory=cache_dir, force_refresh=force_refresh)


def _resolve_cache_dir(explicit: Optional[Path]) -> Path:
    if explicit is not None:
        return explicit.expanduser()
    env_value = os.environ.get("VAMDC_CACHE_DIR")
    if env_value:
        return Path(env_value).expanduser()
    return Path.home() / ".cache" / "vamdc"


def _cmd_get_nodes(cache: CacheConfig) -> int:
    cache_file = cache.directory / "nodes.csv"
    if cache_file.exists() and not cache.force_refresh:
        df = pd.read_csv(cache_file)
        print(f"Loaded {len(df)} nodes from cache at {cache_file}")
        return 0

    df = species_module.getNodeHavingSpecies()
    df.to_csv(cache_file, index=False)
    print(f"Fetched {len(df)} nodes and cached at {cache_file}")
    return 0


def _cmd_get_species(cache: CacheConfig) -> int:
    species_file = cache.directory / "species.csv"
    nodes_file = cache.directory / "species_nodes.csv"

    if species_file.exists() and nodes_file.exists() and not cache.force_refresh:
        df = pd.read_csv(species_file)
        print(f"Loaded {len(df)} species from cache at {species_file}")
        return 0

    species_df, nodes_df = species_module.getAllSpecies()
    species_df.to_csv(species_file, index=False)
    nodes_df.to_csv(nodes_file, index=False)
    print(f"Fetched {len(species_df)} species and cached at {species_file}")
    return 0


def _cmd_get_lines(args: argparse.Namespace) -> int:
    cache_dir = _resolve_cache_dir(getattr(args, "cache_dir", None))
    cache_dir.mkdir(parents=True, exist_ok=True)

    inchikey = args.inchikey.strip()
    node_hint = args.node.strip()
    lambda_min = args.lambda_min
    lambda_max = args.lambda_max

    if lambda_max <= lambda_min:
        raise ValueError("--lambda-max must be greater than --lambda-min")

    species_df, nodes_df = _load_species_data(cache_dir)
    node_endpoint, species_type = _resolve_node_and_species(
        inchikey, node_hint, species_df, nodes_df
    )

    queries = []
    vamdcQuery.VamdcQuery(
        nodeEndpoint=node_endpoint,
        lambdaMin=lambda_min,
        lambdaMax=lambda_max,
        InchiKey=inchikey,
        speciesType=species_type,
        totalListOfQueries=queries,
        verbose=args.verbose,
    )

    if not queries:
        print("No matching data were found for the specified criteria.")
        return 0

    if args.output and len(queries) != 1:
        raise ValueError(
            "Explicit --output requires the query to resolve to a single XSAMS file. "
            "Consider narrowing the wavelength range."
        )

    xsams_paths = []
    line_frames = []

    for query in queries:
        query.getXSAMSData()
        if args.output:
            xsams_path = args.output.expanduser()
            xsams_path.parent.mkdir(parents=True, exist_ok=True)
            Path(query.XSAMSFileName).replace(xsams_path)
            query.XSAMSFileName = str(xsams_path)
        xsams_paths.append(query.XSAMSFileName)
        query.convertToDataFrame()
        if getattr(query, "lines_df", None) is not None:
            line_frames.append(query.lines_df)

    if line_frames:
        combined = pd.concat(line_frames, ignore_index=True)
        print(f"Fetched {len(combined)} spectral lines from node {node_endpoint}")
    else:
        print("Fetched XSAMS data but no tabular lines were produced.")

    for path in xsams_paths:
        print(f"XSAMS written to {path}")

    return 0


def _load_species_data(cache_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    species_file = cache_dir / "species.csv"
    nodes_file = cache_dir / "species_nodes.csv"

    if species_file.exists() and nodes_file.exists():
        species_df = pd.read_csv(species_file)
        nodes_df = pd.read_csv(nodes_file)
        return species_df, nodes_df

    species_df, nodes_df = species_module.getAllSpecies()
    cache_dir.mkdir(parents=True, exist_ok=True)
    species_df.to_csv(species_file, index=False)
    nodes_df.to_csv(nodes_file, index=False)
    return species_df, nodes_df


def _resolve_node_and_species(
    inchikey: str, node_hint: str, species_df: pd.DataFrame, nodes_df: pd.DataFrame
) -> Tuple[str, str]:
    normalized_inchikey = inchikey.strip().upper()
    normalized_hint = node_hint.strip().lower()

    inchikey_series = species_df["InChIKey"].astype(str).str.upper()
    species_matches = species_df[inchikey_series == normalized_inchikey]
    if species_matches.empty:
        raise ValueError(f"No species with InChIKey '{inchikey}' were found.")

    node_candidates = species_matches[
        species_matches["tapEndpoint"].str.lower() == normalized_hint
    ]

    if node_candidates.empty:
        node_candidates = species_matches[
            species_matches["ivoIdentifier"].str.lower() == normalized_hint
        ]

    if node_candidates.empty and "shortname" in species_matches.columns:
        node_candidates = species_matches[
            species_matches["shortname"].astype(str).str.lower() == normalized_hint
        ]

    if node_candidates.empty:
        node_candidates = _match_against_node_table(species_matches, nodes_df, normalized_hint)

    if node_candidates.empty:
        raise ValueError(
            f"No node matching '{node_hint}' for species '{inchikey}' was found in cached metadata."
        )

    row = node_candidates.iloc[0]
    endpoint = row["tapEndpoint"]
    species_type = row["speciesType"]
    if species_type not in {"atom", "molecule"}:
        raise ValueError(f"Unsupported species type '{species_type}' for lines retrieval.")
    return endpoint, species_type


def _match_against_node_table(
    species_matches: pd.DataFrame, nodes_df: pd.DataFrame, normalized_hint: str
) -> pd.DataFrame:
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


def _cmd_count_lines(args: argparse.Namespace) -> int:
    cache_dir = _resolve_cache_dir(getattr(args, "cache_dir", None))
    cache_dir.mkdir(parents=True, exist_ok=True)

    inchikey = args.inchikey.strip()
    node_hint = args.node.strip()
    lambda_min = args.lambda_min
    lambda_max = args.lambda_max

    if lambda_max <= lambda_min:
        raise ValueError("--lambda-max must be greater than --lambda-min")

    species_df, nodes_df = _load_species_data(cache_dir)
    node_endpoint, species_type = _resolve_node_and_species(
        inchikey, node_hint, species_df, nodes_df
    )

    queries = []
    vamdcQuery.VamdcQuery(
        nodeEndpoint=node_endpoint,
        lambdaMin=lambda_min,
        lambdaMax=lambda_max,
        InchiKey=inchikey,
        speciesType=species_type,
        totalListOfQueries=queries,
        verbose=args.verbose,
    )

    if not queries:
        print("No matching data were found for the specified criteria.")
        return 0

    aggregated_counts: dict[str, float] = {}

    for idx, query in enumerate(queries, start=1):
        print(
            f"Sub-query {idx}: node={query.nodeEndpoint} "
            f"lambda_min={query.lambdaMin} lambda_max={query.lambdaMax}"
        )
        if query.counts:
            for key, value in sorted(query.counts.items()):
                print(f"  {key}: {value}")
                numeric = _coerce_numeric(value)
                if numeric is not None:
                    aggregated_counts[key] = aggregated_counts.get(key, 0.0) + numeric
        else:
            print("  No VAMDC count headers returned.")

    if aggregated_counts:
        print("Aggregated numeric headers:")
        for key, value in sorted(aggregated_counts.items()):
            print(f"  {key}: {_format_numeric(value)}")

    return 0


def _coerce_numeric(value: str) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric


def _format_numeric(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return str(value)


if __name__ == "__main__":
    sys.exit(main())
