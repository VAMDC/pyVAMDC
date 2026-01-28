import pandas as pd
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore, Lock
from collections import defaultdict
import re
from typing import Tuple, Optional
import numpy as np
from tqdm import tqdm
from datetime import datetime
from pathlib import Path
import duckdb
import pyVAMDC.spectral.species as species
import pyVAMDC.spectral.vamdcQuery as vamdcQuery
from pyVAMDC.spectral.logging_config import get_logger
from pyVAMDC.spectral.energyConverter import electromagnetic_conversion

LOGGER = get_logger(__name__)


def _sanitize_node_name(node_endpoint, for_directory=False):
    """
    Create a sanitized name from a node endpoint URL or IVO identifier.
    
    Args:
        node_endpoint: The node endpoint URL or IVO identifier
        for_directory: If True, extracts meaningful parts for directory naming.
                      If False, creates a full sanitized filename with underscores.
    
    Returns:
        Sanitized string suitable for use as directory or filename
    """
    # Remove common prefixes
    clean_name = node_endpoint.replace("http://", "").replace("https://", "").replace("ivo://", "")
    
    if for_directory:
        # Handle IVO identifiers and URLs differently
        if node_endpoint.startswith("ivo://"):
            # For IVO identifiers like "ivo://vamdc/vald/uu/django" or "ivo://vamdc/vald-Moscow"
            # Extract meaningful parts after "vamdc"
            parts = clean_name.split("/")
            if len(parts) >= 2 and parts[0] == "vamdc":
                # Use the database name (e.g., "vald", "topbase") as the base
                db_name = parts[1]
                # If there are additional parts, include them to ensure uniqueness
                if len(parts) > 2:
                    additional_parts = "_".join(parts[2:])
                    return f"{db_name}_{additional_parts}".replace("-", "_")
                else:
                    return db_name.replace("-", "_")
            else:
                # Fallback: use all parts joined with underscores
                return "_".join(parts).replace("-", "_").replace(":", "_")
        else:
            # For regular URLs, extract the domain name
            return clean_name.split("/")[0].split(".")[0]
    else:
        # Create a full sanitized filename with underscores
        return clean_name.replace("/", "_").replace(":", "_").replace("-", "_")


def _build_aggregated_parquet_name(species_type, node_endpoint, lambda_min, lambda_max):
    """
    Generate a unique filename for aggregated parquet files containing query parameters and timestamp.
    
    Args:
        species_type: Type of species (e.g., 'atomic' or 'molecular')
        node_endpoint: The node endpoint URL or IVO identifier
        lambda_min: Minimum wavelength value
        lambda_max: Maximum wavelength value
    
    Returns:
        str: Formatted filename for the parquet file
        
    Example:
        >>> _build_aggregated_parquet_name('atomic', 'ivo://vamdc/vald/uu/django', 2.60e+07, 3.60e+07)
        'atomic_vald_uu_django_2.60e+07_3.60e+07_20250127T143022.parquet'
    """
    # Get sanitized node identifier
    node_id = _sanitize_node_name(node_endpoint, for_directory=True)
    
    # Generate timestamp
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    
    # Format lambda values in scientific notation
    lambda_min_str = f"{lambda_min:.2e}"
    lambda_max_str = f"{lambda_max:.2e}"
    
    # Return formatted filename
    return f"{species_type}_{node_id}_{lambda_min_str}_{lambda_max_str}_{timestamp}.parquet"


def _get_query_results_dir():
    """
    Get the QueryResults directory, creating it if it doesn't exist.
    
    Returns:
        Path: Path object to the QueryResults directory in the current working directory
    """
    query_results_dir = Path.cwd() / "QueryResults"
    query_results_dir.mkdir(exist_ok=True)
    return query_results_dir


def _build_individual_parquet_path(uuid):
    """
    Build the full path for an individual parquet file based on UUID.
    
    Args:
        uuid: Unique identifier for the parquet file
    
    Returns:
        Path: Full path to the parquet file in QueryResults directory
    """
    query_results_dir = _get_query_results_dir()
    return query_results_dir / f"{uuid}.parquet"


class telescopeBands(Enum):
    """
    This class defines the bands for the main telescopes.
    Bands are named with the official telescope conventions. 
    Wavelengths are expressend in Angstrom. 
    """

    #TODO to enrich this list with the bands of the main telescopes

    # Bands for Alma
    Alma_band1 = [60000000, 86000000]
    Alma_band2 = [26000000, 45000000]
    Alma_band3 = [26000000, 36000000]
    Alma_band4 = [18000000, 24000000]
    Alma_band5 = [14000000, 18000000]
    Alma_band6 = [11000000, 14000000]
    Alma_band7 = [8000000, 11000000]
    Alma_band8 = [6000000, 8000000]
    Alma_band9 = [4000000, 5000000]
    Alma_band10= [3000000, 4000000]

    # Bands for Noema
    NOEMA_band1 = [2.500938e+07, 4.259384e+07]
    NOEMA_band2 = [1.639357e+07, 2.360571e+07]
    NOEMA_band3 = [1.086205e+07, 1.528555e+07]

    # Bands for Green Bank Observatory
    GBT_PF1_342 = [7.589682e+09, 1.033767e+10]
    GBT_PF1_450 = [5.765240e+09, 7.786817e+09]
    GBT_PF1_600 = [4.344818e+09, 5.878283e+09]
    GBT_PF1_800 = [3.258614e+09, 4.408713e+09]
    GBT_PF2 = [2.437337e+09, 3.294423e+09]
    GBT_L_band = [1.732904e+09, 2.606891e+09]
    GBT_S_band = [1.153048e+09, 1.732904e+09]
    GBT_C_band = [3.747406e+08, 7.589682e+08]
    GBT_X_band = [2.584418e+08, 3.747406e+08]
    GBT_Ku_band = [1.946704e+08, 2.498270e+08]
    GBT_KFPA = [1.090154e+08, 1.665514e+08]
    GBT_Ka_band_MM_F1 = [9.670724e+07, 1.153048e+08]
    GBT_Ka_band_MM_F2 = [8.102499e+07, 9.829261e+07]
    GBT_Ka_band_MM_F3 = [7.589682e+07, 8.327568e+07]
    GBT_Q_band = [6.019929e+07, 7.847970e+07]
    GBT_W_band_MM_F1 = [4.051249e+07, 4.474514e+07]
    GBT_W_band_MM_F2 = [3.747406e+07, 4.106746e+07]
    GBT_W_band_MM_F3 = [3.485959e+07, 3.794841e+07]
    GBT_W_band_MM_F4 = [3.213210e+07, 3.526970e+07]
    GBT_Mustang2 = [2.997925e+07, 3.747406e+07]
    GBT_ARGUS = [2.600108e+07, 3.747406e+07]

    @property 
    def lambdaMin(self) -> float:
        return self.value[0]
    
    @property
    def lambdaMax(self) -> float:
        return self.value[1]


   
def getTelescopeBandFromLine(wavelength):
    """
    Find all telescope bands for a given wavelength, handling overlaps.
    
    Args:
        wavelength: float, the wavelength to check
    
    Return:
        matching_bands: list(str) names of the bands that match or empty list if not found
    """
    telescope_bands_dict = {band.name: band.value for band in telescopeBands}
    matching_bands = []
    for band_name, range_values in telescope_bands_dict.items():
        if range_values[0] <= wavelength <= range_values[1]:
            matching_bands.append(band_name)
    return matching_bands



def getLinesByTelescopeBand(band:telescopeBands, species_dataframe = None, nodes_dataframe = None):
    """
    Extract all the spectroscopic lines for a given telescope band. 

    Args:
        band : telescopeBands
            defines the telescope band to request data for. 
            Bands are simply identified by their name: for example the function calling argument will be "telescopeBands.Alma_band3" for the 3rd band of Alma)
             

        species_dataframe : dataframe
            restrict the extraction of the lines to the chemical species contained into the species_dataframe. 
            The species_dataframe is typically built by functions in the 'species' module (getAllSpecies, getSpeciesWithSearchCriteria), 
            optionally filtered using the functions in the 'filters' module and/or Pandas functionalities. 
            Default None. In this case there is no restriction on the species. 

        nodes_dataframe : dataframe
            restrict the extraction of the lines to the databases (VAMDC nodes) contained into the nodes_dataframe.
            The nodes_dataframe is typically built by the function 'getNodeHavingSpecies' in the 'species' module, optionally 
            filtered using the functions in the 'filters' module and/or Pandas functionalities. 
            Default None. In this case there is no restriction on the Nodes.
        
        acceptTruncation : boolean
            If True, queries that are reported as truncated will be accepted (not split)
            and included and exectuted as-is. If False (default), truncated queries are recursively split into sub-queries
            until sub-queries are not truncated.
  
    Returns:
        atomic_results_dict : dictionary
            A dictionary containing paths to parquet files with atomic spectroscopic lines,
            grouped by databases. Keys are node identifiers, values are file paths.
            Use getLinesAsDataFramesByTelescopeBand() if you need DataFrames directly.
        
        molecular_results_dict : dictionary
            A dictionary containing paths to parquet files with molecular spectroscopic lines,
            grouped by databases. Keys are node identifiers, values are file paths.
        
        queries_metadata_list : list
            A list of dictionaries with metadata about each query.
    """
    return getLines(band.value[0], band.value[1], species_dataframe=species_dataframe, nodes_dataframe=nodes_dataframe)



def _create_single_head_query(species_row, lambdaMin, lambdaMax, acceptTruncation, semaphore):
    """
    Create a VamdcQuery instance (which executes HEAD request in __init__).
    Uses a semaphore to limit concurrent HEAD requests to the same node.
    
    Args:
        species_row: dictionary containing species data (tapEndpoint, InChIKey, speciesType)
        lambdaMin: float, minimum wavelength boundary
        lambdaMax: float, maximum wavelength boundary
        acceptTruncation: boolean, whether to accept truncated results
        semaphore: Semaphore to control concurrent access to the node
    
    Returns:
        list of VamdcQuery instances (may be multiple due to recursive splitting)
    """
    with semaphore:
        listOfQueries = []
        try:
            nodeEndpoint = species_row["tapEndpoint"]
            InChIKey = species_row["InChIKey"]
            speciesType = species_row["speciesType"]
            
            # Create VamdcQuery instance (HEAD request executed in __init__)
            vamdcQuery.VamdcQuery(nodeEndpoint, lambdaMin, lambdaMax, InChIKey, speciesType, listOfQueries, acceptTruncation)
            
            LOGGER.debug(f"Created HEAD query for {InChIKey} on node {nodeEndpoint}")
        except Exception as e:
            LOGGER.error(
                f"Error creating HEAD query for {species_row.get('InChIKey', 'unknown')} on node {species_row.get('tapEndpoint', 'unknown')}",
                exception=e,
                show_traceback=True
            )
        return listOfQueries


def _process_single_query(query, semaphore):
    """
    Process a single query: fetch XSAMS data and convert to parquet file.
    Uses a semaphore to limit concurrent requests to the same node.
    
    After processing, the query will have:
    - query.parquet_path: Path to the individual parquet file
    - query.lines_df: None (memory released after writing parquet)
    
    Args:
        query: VamdcQuery instance to process
        semaphore: Semaphore to control concurrent access to the node
    
    Returns:
        query: The processed VamdcQuery instance
    """
    with semaphore:
        try:
            # get the data
            query.getXSAMSData()
            # convert the data to parquet (harmonizes wavelength, writes parquet, releases memory)
            query.convertToDataFrame()
            LOGGER.debug(f"Successfully processed query {query.localUUID} for node {query.nodeEndpoint}")
        except Exception as e:
            LOGGER.error(
                f"Error processing query {query.localUUID} for node {query.nodeEndpoint}",
                exception=e,
                show_traceback=True
            )
    return query



def _process_queries_parallel(listOfAllQueries, max_concurrent_per_node=3):
    """
    Process queries in parallel with controlled concurrency per node.
    
    This function groups queries by node endpoint and uses semaphores to limit
    the number of concurrent requests to each node. This prevents overwhelming
    individual nodes while maximizing overall throughput.
    
    Args:
        listOfAllQueries: list of VamdcQuery instances to process
        max_concurrent_per_node: maximum number of concurrent requests per node (default: 3)
    
    Returns:
        listOfAllQueries: the same list with all queries processed
    """
    if not listOfAllQueries:
        return listOfAllQueries
    
    # Group queries by node endpoint
    queries_by_node = defaultdict(list)
    for query in listOfAllQueries:
        queries_by_node[query.nodeEndpoint].append(query)
    
    # Create a semaphore for each node to limit concurrent requests
    semaphores = {node: Semaphore(max_concurrent_per_node) for node in queries_by_node.keys()}
    
    # Calculate total number of workers: max_concurrent_per_node * number_of_nodes
    num_nodes = len(queries_by_node)
    max_workers = max_concurrent_per_node * num_nodes
    
    LOGGER.info(f"Processing {len(listOfAllQueries)} data queries with {max_workers} workers ({max_concurrent_per_node} per node, {num_nodes} nodes)")
    
    # Process all queries in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all queries to the executor
        future_to_query = {
            executor.submit(_process_single_query, query, semaphores[query.nodeEndpoint]): query
            for query in listOfAllQueries
        }
        
        # Wait for all queries to complete with progress bar
        with tqdm(total=len(listOfAllQueries), desc="Fetching data", unit="query") as pbar:
            for future in as_completed(future_to_query):
                try:
                    future.result()
                except Exception as e:
                    query = future_to_query[future]
                    LOGGER.error(
                        f"Query {query.localUUID} failed with exception",
                        exception=e,
                        show_traceback=True
                    )
                pbar.update(1)
    
    return listOfAllQueries


def _build_and_run_wrappings(lambdaMin, lambdaMax, species_dataframe, nodes_dataframe, accept_truncation, max_concurrent_per_node=5) -> list:
    """
    Build and execute HEAD queries in parallel with controlled concurrency per node.
    
    This function creates VamdcQuery instances (which execute HEAD requests) for all
    species/node combinations. It uses thread-based parallelism with semaphores to
    limit concurrent HEAD requests per node.
    
    Args:
        lambdaMin: float, minimum wavelength boundary
        lambdaMax: float, maximum wavelength boundary
        species_dataframe: dataframe with species data (or None for all species)
        nodes_dataframe: dataframe with node data (or None for all nodes)
        accept_truncation: boolean, whether to accept truncated results
        max_concurrent_per_node: int, maximum concurrent HEAD requests per node (default: 3)
    
    Returns:
        list of VamdcQuery instances ready for data fetching
    """
    # if the provided species_dataframe is not provided, we build it by taking all the species
    if species_dataframe is None:
        species_dataframe, _ = species.getAllSpecies()

    # if the provided node_dataframe is None
    if nodes_dataframe is None:
        nodes_dataframe = species.getNodeHavingSpecies()
    
    # Getting the list of the nodes passed as argument
    selectedNodeList = nodes_dataframe["ivoIdentifier"].to_list()

    # filter the list of species by selecting only the node from the selectedNodeList
    filtered_species_df = species_dataframe[species_dataframe["ivoIdentifier"].isin(selectedNodeList)]

    if filtered_species_df.empty:
        LOGGER.info("No species found for selected nodes")
        return []
    
    # Group species by node endpoint
    species_by_node = defaultdict(list)
    for _, row in filtered_species_df.iterrows():
        species_by_node[row["tapEndpoint"]].append(row.to_dict())
    
    # Create a semaphore for each node to limit concurrent HEAD requests
    semaphores = {node: Semaphore(max_concurrent_per_node) for node in species_by_node.keys()}
    
    # Calculate total number of workers
    num_nodes = len(species_by_node)
    total_species = len(filtered_species_df)
    max_workers = max_concurrent_per_node * num_nodes
    
    LOGGER.info(f"Creating HEAD queries for {total_species} species across {num_nodes} nodes with {max_workers} workers ({max_concurrent_per_node} per node)")
    
    # Prepare all tasks: (species_row, node_endpoint) pairs
    all_tasks = []
    for node_endpoint, species_list in species_by_node.items():
        for species_row in species_list:
            all_tasks.append((species_row, node_endpoint))
    
    # Process all HEAD queries in parallel using ThreadPoolExecutor
    listOfAllQueries = []
    list_lock = Lock()  # Lock to protect listOfAllQueries from concurrent modifications
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all HEAD query creation tasks
        future_to_species = {
            executor.submit(
                _create_single_head_query,
                species_row,
                lambdaMin,
                lambdaMax,
                accept_truncation,
                semaphores[node_endpoint]
            ): species_row
            for species_row, node_endpoint in all_tasks
        }
        
        # Wait for all HEAD queries to complete and collect results with progress bar
        with tqdm(total=len(future_to_species), desc="Creating queries", unit="species") as pbar:
            for future in as_completed(future_to_species):
                try:
                    queries = future.result()
                    # Thread-safe list extension
                    with list_lock:
                        listOfAllQueries.extend(queries)
                except Exception as e:
                    species_row = future_to_species[future]
                    LOGGER.error(
                        f"Failed to create HEAD query for species {species_row.get('InChIKey', 'unknown')}",
                        exception=e,
                        show_traceback=True
                    )
                pbar.update(1)
    
    LOGGER.info(f"Created {len(listOfAllQueries)} HEAD queries (including splits from truncation)")
    return listOfAllQueries


def get_metadata_for_lines(lambdaMin, lambdaMax, species_dataframe = None, nodes_dataframe = None, max_concurrent_per_node = 3):
    """
        Collect metadata for queries in a wavelength interval.

        This function prepares and runs the set of VAMDC HEAD queries (via the internal
        parallel wrapping) for the requested wavelength interval, and returns a list of
        metadata dictionaries for each sub-query.

        Args:
            lambdaMin : float
                the inf boundary (in Angstrom) of the wavelength interval

            lambdaMax : float
                the sup boundary (in Angstrom) of the wavelength interval

            species_dataframe : dataframe, optional
                restrict the processing to the chemical species contained into this dataframe.
                If None, all species are used.

            nodes_dataframe : dataframe, optional
                restrict the processing to the databases (VAMDC nodes) contained into this dataframe.
                If None, all nodes that have species are used.
            
            max_concurrent_per_node : int
                Maximum number of concurrent HEAD requests allowed per node (default: 3).
                Total parallelism will be max_concurrent_per_node * number_of_nodes.

        Returns:
            metadata_list : list
                A list of dictionaries, one per sub-query, where each dictionary contains:
                    - 'query': the query URL/string that will be executed
                    - 'response': the HEAD response metadata (as stored on the VamdcQuery instance)
        """
    listOfAllQueries = _build_and_run_wrappings(lambdaMin, lambdaMax, species_dataframe, nodes_dataframe, True, max_concurrent_per_node)

    # build list of dictionaries with keys 'query' and 'response'
    metadata_list = []
    for currentQuery in listOfAllQueries:
        metadata_list.append({
            "query": currentQuery.vamdcCall,
            "metadata": currentQuery.counts
        })

    return metadata_list




def getLines(lambdaMin, lambdaMax, species_dataframe = None, nodes_dataframe = None, acceptTruncation = False, max_concurrent_per_node = 3):
    """
    Extract all the spectroscopic lines in a given wavelenght interval. 

    Args:
        lambdaMin : float
            the inf boundary (in Angstrom) of the wavelenght interval
        
         lambdaMax : float
            the sup boundary (in Angstrom) of the wavelenght interval  

        species_dataframe : dataframe
            restrict the extraction of the lines to the chemical species contained into the species_dataframe. 
            The species_dataframe is typically built by functions in the 'species' module (getAllSpecies, getSpeciesWithSearchCriteria), 
            optionally filtered using the functions in the 'filters' module and/or Pandas functionalities. 
            Default None. In this case there is no restriction on the species. 

        nodes_dataframe : dataframe
            restrict the extraction of the lines to the databases (VAMDC nodes) contained into the nodes_dataframe.
            The nodes_dataframe is typically built by the function 'getNodeHavingSpecies' in the 'species' module, optionally 
            filtered using the functions in the 'filters' module and/or Pandas functionalities. 
            Default None. In this case there is no restriction on the Nodes.
        
        acceptTruncation : boolean
            If True, accept truncated query results. If False, split queries recursively.
        
        max_concurrent_per_node : int
            Maximum number of concurrent requests allowed per node (default: 3).
            Total parallelism will be max_concurrent_per_node * number_of_nodes.
  
    Returns:
        atomic_results_dict : dictionary
            A dictionary containing paths to aggregated parquet files for atomic species, grouped by databases. 
            The keys of this dictionary are the database identifiers (nodeEndpoint) and the values are paths 
            (strings) to the aggregated parquet files containing the spectroscopic lines extracted from that database.
        
        molecular_results_dict : dictionary
            A dictionary containing paths to aggregated parquet files for molecular species, grouped by databases. 
            The keys of this dictionary are the database identifiers (nodeEndpoint) and the values are paths 
            (strings) to the aggregated parquet files containing the spectroscopic lines extracted from that database.
        
        queries_metadata_list : list
            A list of dictionaries, one per query in listOfAllQueries, containing metadata about each query with the following fields:
                - 'nodeEndpoint': the VAMDC node endpoint
                - 'lambdaMin': the minimum wavelength boundary
                - 'lambdaMax': the maximum wavelength boundary
                - 'InchiKey': the InChI Key identifier of the chemical species
                - 'vamdcCall': the VAMDC query URL
                - 'XSAMS_file_path': the path to the downloaded XSAMS file
                - 'parquet_path': the path to the individual parquet file
    """
    # Build all HEAD queries (this will show progress bar for query creation)
    listOfAllQueries = _build_and_run_wrappings(lambdaMin, lambdaMax, species_dataframe, nodes_dataframe, acceptTruncation, max_concurrent_per_node)

    # Show summary of what will be processed
    if listOfAllQueries:
        nodes_in_queries = set(q.nodeEndpoint for q in listOfAllQueries)
        print(f"\n{'='*70}")
        print(f"Processing Summary:")
        print(f"  Wavelength range: {lambdaMin:.2f} - {lambdaMax:.2f} Angstrom")
        print(f"  Total queries to process: {len(listOfAllQueries)}")
        print(f"  Nodes involved: {len(nodes_in_queries)}")
        print(f"  Parallel workers per node: {max_concurrent_per_node}")
        print(f"{'='*70}\n")
    else:
        print("No queries to process.")
        return {}, {}, []

    # At this point the list listOfAllQueries contains all the query that can be run without truncation
    # Process all queries in parallel with controlled concurrency per node
    _process_queries_parallel(listOfAllQueries, max_concurrent_per_node)
    
    # Group parquet paths by node and species type
    atomic_parquets_by_node = defaultdict(list)
    molecular_parquets_by_node = defaultdict(list)
    queries_metadata_list = []
    
    for currentQuery in listOfAllQueries:
        if currentQuery.parquet_path and Path(currentQuery.parquet_path).exists():
            if currentQuery.speciesType == "atom":
                atomic_parquets_by_node[currentQuery.nodeEndpoint].append(currentQuery.parquet_path)
            elif currentQuery.speciesType == "molecule":
                molecular_parquets_by_node[currentQuery.nodeEndpoint].append(currentQuery.parquet_path)
        
        # Build metadata dictionary for this query
        metadata_dict = {
            "nodeEndpoint": currentQuery.nodeEndpoint,
            "lambdaMin": currentQuery.lambdaMin,
            "lambdaMax": currentQuery.lambdaMax,
            "InchiKey": currentQuery.InchiKey,
            "vamdcCall": currentQuery.vamdcCall,
            "XSAMS_file_path": currentQuery.XSAMSFileName,
            "parquet_path": str(currentQuery.parquet_path) if currentQuery.parquet_path else None
        }
        queries_metadata_list.append(metadata_dict)
    
    # Aggregate atomic parquets using DuckDB
    atomic_results_dict = {}
    for node_endpoint, parquet_paths in atomic_parquets_by_node.items():
        aggregated_filename = _build_aggregated_parquet_name("atomic", node_endpoint, lambdaMin, lambdaMax)
        aggregated_path = _get_query_results_dir() / aggregated_filename
        
        # DuckDB aggregation with union_by_name for schema flexibility
        paths_list = [str(p) for p in parquet_paths]
        duckdb.execute(f"""
            COPY (SELECT * FROM read_parquet({paths_list}, union_by_name=true))
            TO '{aggregated_path}' (FORMAT PARQUET)
        """)
        
        atomic_results_dict[node_endpoint] = str(aggregated_path)
        LOGGER.info(f"Aggregated {len(parquet_paths)} atomic parquet files for {node_endpoint} into {aggregated_path}")
    
    # Aggregate molecular parquets using DuckDB
    molecular_results_dict = {}
    for node_endpoint, parquet_paths in molecular_parquets_by_node.items():
        aggregated_filename = _build_aggregated_parquet_name("molecular", node_endpoint, lambdaMin, lambdaMax)
        aggregated_path = _get_query_results_dir() / aggregated_filename
        
        # DuckDB aggregation with union_by_name for schema flexibility
        paths_list = [str(p) for p in parquet_paths]
        duckdb.execute(f"""
            COPY (SELECT * FROM read_parquet({paths_list}, union_by_name=true))
            TO '{aggregated_path}' (FORMAT PARQUET)
        """)
        
        molecular_results_dict[node_endpoint] = str(aggregated_path)
        LOGGER.info(f"Aggregated {len(parquet_paths)} molecular parquet files for {node_endpoint} into {aggregated_path}")
    
    
    if not(atomic_results_dict) :
        LOGGER.info("No atomic data to fetch")

    if not(molecular_results_dict):
        LOGGER.info("No molecular data to fetch")

    # we return the three results: atomic, molecular dictionaries and queries metadata list
    return atomic_results_dict, molecular_results_dict, queries_metadata_list


def getLinesAsDataFrames(lambdaMin, lambdaMax, species_dataframe=None, nodes_dataframe=None, acceptTruncation=False, max_concurrent_per_node=3):
    """
    Extract all spectroscopic lines in a given wavelength interval and return as DataFrames.
    
    This is a convenience wrapper around getLines() that reads the aggregated parquet files
    back into pandas DataFrames. Use getLines() directly if you want to work with parquet
    files for better memory efficiency with large datasets.

    Args:
        lambdaMin : float
            the inf boundary (in Angstrom) of the wavelength interval
        
        lambdaMax : float
            the sup boundary (in Angstrom) of the wavelength interval  

        species_dataframe : dataframe
            restrict the extraction of the lines to the chemical species contained into the species_dataframe. 
            Default None. In this case there is no restriction on the species. 

        nodes_dataframe : dataframe
            restrict the extraction of the lines to the databases (VAMDC nodes) contained into the nodes_dataframe.
            Default None. In this case there is no restriction on the Nodes.
        
        acceptTruncation : boolean
            If True, accept truncated query results. If False, split queries recursively.
        
        max_concurrent_per_node : int
            Maximum number of concurrent requests allowed per node (default: 3).

    Returns:
        atomic_results_dict : dictionary
            Dictionary with node identifiers as keys and pandas DataFrames as values,
            containing atomic spectroscopic lines.
        
        molecular_results_dict : dictionary
            Dictionary with node identifiers as keys and pandas DataFrames as values,
            containing molecular spectroscopic lines.
        
        queries_metadata_list : list
            A list of dictionaries containing metadata about each query.
    """
    atomic_paths, molecular_paths, queries_metadata = getLines(
        lambdaMin, lambdaMax, 
        species_dataframe=species_dataframe,
        nodes_dataframe=nodes_dataframe,
        acceptTruncation=acceptTruncation,
        max_concurrent_per_node=max_concurrent_per_node
    )
    
    atomic_dfs = {}
    for node, path in atomic_paths.items():
        if path and Path(path).exists():
            atomic_dfs[node] = pd.read_parquet(path)
            LOGGER.debug(f"Loaded atomic DataFrame for {node} from {path}")
    
    molecular_dfs = {}
    for node, path in molecular_paths.items():
        if path and Path(path).exists():
            molecular_dfs[node] = pd.read_parquet(path)
            LOGGER.debug(f"Loaded molecular DataFrame for {node} from {path}")
    
    return atomic_dfs, molecular_dfs, queries_metadata


def getLinesAsDataFramesByTelescopeBand(band: telescopeBands, species_dataframe=None, nodes_dataframe=None):
    """
    Extract all spectroscopic lines for a given telescope band and return as DataFrames.
    
    This is a convenience wrapper that returns DataFrames instead of parquet paths.
    
    Args:
        band : telescopeBands
            defines the telescope band to request data for.
        
        species_dataframe : dataframe
            restrict the extraction of the lines to the chemical species. Default None.
        
        nodes_dataframe : dataframe
            restrict the extraction to specific VAMDC nodes. Default None.
    
    Returns:
        atomic_results_dict : dictionary
            Dictionary with node identifiers as keys and pandas DataFrames as values.
        
        molecular_results_dict : dictionary
            Dictionary with node identifiers as keys and pandas DataFrames as values.
        
        queries_metadata_list : list
            A list of dictionaries containing metadata about each query.
    """
    return getLinesAsDataFrames(band.value[0], band.value[1], 
                                 species_dataframe=species_dataframe, 
                                 nodes_dataframe=nodes_dataframe)

