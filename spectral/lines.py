import pandas as pd
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore, Lock
from collections import defaultdict
import re
from typing import Tuple, Optional
import numpy as np
import pyVAMDC.spectral.species as species
import pyVAMDC.spectral.vamdcQuery as vamdcQuery
from pyVAMDC.spectral.logging_config import get_logger
from pyVAMDC.spectral.energyConverter import electromagnetic_conversion

LOGGER = get_logger(__name__)


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
            A dictionary containing the extracted lines for atomic species, grouped by databases. The keys of this dictionary is the database 
            identifier (nodeIdentifier) and the value is a datafrale containing the spectroscopic lines extracted from that database. 
        
        molecular_results_dict : dictionary
            A dictionary containing the extracted lines for molecular species, grouped by databases. The keys of this dictionary is the database 
            identifier (nodeIdentifier) and the value is a datafrale containing the spectroscopic lines extracted from that database.
    """
    return getLines(band.lambdaMin, band.lambdaMax, species_dataframe=species_dataframe, nodes_dataframe=nodes_dataframe)



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
    Process a single query: fetch XSAMS data and convert to DataFrame.
    Uses a semaphore to limit concurrent requests to the same node.
    
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
            # convert the data 
            query.convertToDataFrame()
            LOGGER.debug(f"Successfully processed query {query.localUUID} for node {query.nodeEndpoint}")
        except Exception as e:
            LOGGER.error(
                f"Error processing query {query.localUUID} for node {query.nodeEndpoint}",
                exception=e,
                show_traceback=True
            )
    return query


def _ensure_common_wavelength_column(lines_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the dataframe has a common 'Wavelength (m)' column in meters for concatenation.

    This method checks for wavelength, energy, or frequency columns in the dataframe
    and converts them to wavelength in meters if necessary. It handles column names
    with units in the format "Column_name (unit)".

    Args:
        lines_df (pd.DataFrame): Input lines dataframe from VAMDC.

    Returns:
        pd.DataFrame: DataFrame with 'Wavelength (m)' column in meters.

    Raises:
        ValueError: If no wavelength, energy, or frequency data is found.
    """
    if lines_df is None or lines_df.empty:
        return lines_df
        
    df = lines_df.copy()

    # Pattern to extract unit from column name like "ColumnName (unit)"
    unit_pattern = r'\(([^)]+)\)$'

    # Check if Wavelength (m) already exists
    if 'Wavelength (m)' in df.columns:
        LOGGER.debug("Wavelength (m) column already present")
        return df

    # Helper function to normalize unit names for energyConverter
    def normalize_unit(unit_str: str) -> str:
        """Normalize unit name to match energyConverter expectations."""
        if unit_str is None:
            return None
        # Special cases that need to maintain case
        if unit_str.upper() == 'EV':
            return 'eV'
        # Handle common variations
        unit_lower = unit_str.lower().strip()
        if unit_lower in ['a','å', 'ang']:
            return 'angstrom'
        if unit_lower in ['hz']:
            return 'hertz'
        if unit_lower in ['m']:
            return 'meter'
        if unit_lower in ['nm']:
            return 'nanometer'
        if unit_lower in ['cm']:
            return 'centimeter'
        if unit_lower in ['mm']:
            return 'millimeter'
        if unit_lower in ['um', 'μm']:
            return 'micrometer'
        if unit_lower in ['ghz']:
            return 'gigahertz'
        if unit_lower in ['mhz']:
            return 'megahertz'
        if unit_lower in ['khz']:
            return 'kilohertz'
        if unit_lower in ['thz']:
            return 'terahertz'
        if unit_lower in ['cm-1', 'cm^-1']:
            return 'cm-1'
        # Return as lowercase by default
        return unit_lower

    # Helper function to parse column name and extract unit
    def parse_column_with_unit(col_name: str) -> Tuple[str, Optional[str]]:
        """Parse column name to extract base name and unit."""
        match = re.search(unit_pattern, col_name)
        if match:
            unit = match.group(1).strip()
            base_name = col_name[:match.start()].strip()
            return base_name, unit
        return col_name, None

    # Search for wavelength, energy, and frequency columns
    wavelength_columns = {}
    energy_columns = {}
    frequency_columns = {}

    for col in df.columns:
        base_name, unit = parse_column_with_unit(col)
        base_lower = base_name.lower()

        # Wavelength variants
        if any(wl in base_lower for wl in ['wavelength', 'wave', 'wl']):
            wavelength_columns[col] = unit
        # Energy variants (including wavenumber)
        elif any(en in base_lower for en in ['energy', 'wavenumber']):
            energy_columns[col] = unit
        # Frequency variants
        elif any(fr in base_lower for fr in ['frequency', 'freq']):
            frequency_columns[col] = unit

    # Conversion logic - preference order: wavelength > frequency > energy
    try:
        if wavelength_columns:
            # Use the first wavelength column found
            wl_col = list(wavelength_columns.keys())[0]
            wl_unit = wavelength_columns[wl_col]

            if wl_unit is None:
                # Assume Angstrom if no unit specified (common in VAMDC)
                LOGGER.warning(
                    f"Wavelength column '{wl_col}' has no unit specified, assuming Angstrom"
                )
                wl_unit = 'angstrom'
            else:
                # Normalize unit name for energyConverter
                wl_unit = normalize_unit(wl_unit)

            LOGGER.debug(f"Converting wavelength from {wl_unit} to meter using column '{wl_col}'")
            df['Wavelength (m)'] = df[wl_col].apply(
                lambda x: electromagnetic_conversion(x, wl_unit, 'meter') if pd.notna(x) else np.nan
            )

        elif frequency_columns:
            # Use the first frequency column found
            freq_col = list(frequency_columns.keys())[0]
            freq_unit = frequency_columns[freq_col]

            if freq_unit is None:
                # Assume Hertz if no unit specified
                LOGGER.warning(
                    f"Frequency column '{freq_col}' has no unit specified, assuming hertz"
                )
                freq_unit = 'hertz'
            else:
                # Normalize unit name for energyConverter
                freq_unit = normalize_unit(freq_unit)

            LOGGER.debug(f"Converting frequency from {freq_unit} to wavelength in meters using column '{freq_col}'")
            df['Wavelength (m)'] = df[freq_col].apply(
                lambda x: electromagnetic_conversion(x, freq_unit, 'meter') if pd.notna(x) else np.nan
            )

        elif energy_columns:
            # Use the first energy column found
            energy_col = list(energy_columns.keys())[0]
            energy_unit = energy_columns[energy_col]

            if energy_unit is None:
                # Assume eV if no unit specified
                LOGGER.warning(
                    f"Energy column '{energy_col}' has no unit specified, assuming eV"
                )
                energy_unit = 'eV'
            else:
                # Normalize unit name for energyConverter
                energy_unit = normalize_unit(energy_unit)

            LOGGER.debug(f"Converting energy from {energy_unit} to wavelength in meters using column '{energy_col}'")
            df['Wavelength (m)'] = df[energy_col].apply(
                lambda x: electromagnetic_conversion(x, energy_unit, 'meter') if pd.notna(x) else np.nan
            )

        else:
            # If no suitable column found, create a placeholder column with NaN values
            LOGGER.warning(
                "No wavelength, energy, or frequency columns found in the dataframe. "
                "Creating placeholder 'Wavelength (m)' column with NaN values for concatenation compatibility."
            )
            df['Wavelength (m)'] = np.nan

        return df

    except Exception as e:
        LOGGER.error(f"Failed to convert wavelength data: {str(e)}")
        # Create placeholder column to allow concatenation
        LOGGER.warning("Creating placeholder 'Wavelength (m)' column with NaN values due to conversion error")
        df['Wavelength (m)'] = np.nan
        return df


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
    
    LOGGER.info(f"Processing queries with {max_workers} workers ({max_concurrent_per_node} per node, {num_nodes} nodes)")
    
    # Process all queries in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all queries to the executor
        future_to_query = {
            executor.submit(_process_single_query, query, semaphores[query.nodeEndpoint]): query
            for query in listOfAllQueries
        }
        
        # Wait for all queries to complete and log progress
        completed = 0
        total = len(listOfAllQueries)
        for future in as_completed(future_to_query):
            completed += 1
            if completed % 10 == 0 or completed == total:
                LOGGER.info(f"Progress: {completed}/{total} queries completed")
            try:
                future.result()
            except Exception as e:
                query = future_to_query[future]
                LOGGER.error(
                    f"Query {query.localUUID} failed with exception",
                    exception=e,
                    show_traceback=True
                )
    
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
        
        # Wait for all HEAD queries to complete and collect results
        completed = 0
        total = len(future_to_species)
        for future in as_completed(future_to_species):
            completed += 1
            if completed % 10 == 0 or completed == total:
                LOGGER.info(f"HEAD query progress: {completed}/{total} species processed")
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
            A dictionary containing the extracted lines for atomic species, grouped by databases. The keys of this dictionary is the database 
            identifier (nodeIdentifier) and the value is a datafrale containing the spectroscopic lines extracted from that database. 
        
        molecular_results_dict : dictionary
            A dictionary containing the extracted lines for molecular species, grouped by databases. The keys of this dictionary is the database 
            identifier (nodeIdentifier) and the value is a datafrale containing the spectroscopic lines extracted from that database.
        
        queries_metadata_list : list
            A list of dictionaries, one per query in listOfAllQueries, containing metadata about each query with the following fields:
                - 'nodeEndpoint': the VAMDC node endpoint
                - 'lambdaMin': the minimum wavelength boundary
                - 'lambdaMax': the maximum wavelength boundary
                - 'InchiKey': the InChI Key identifier of the chemical species
                - 'vamdcCall': the VAMDC query URL
                - 'XSAMS_file_path': the path to the downloaded XSAMS file
    """
    listOfAllQueries = _build_and_run_wrappings(lambdaMin, lambdaMax, species_dataframe, nodes_dataframe, acceptTruncation, max_concurrent_per_node)

    LOGGER.info(f"Total amount of sub-queries to be submitted for data fetching: {len(listOfAllQueries)}")

    # At this point the list listOfAllQueries contains all the query that can be run without truncation
    # Process all queries in parallel with controlled concurrency per node
    _process_queries_parallel(listOfAllQueries, max_concurrent_per_node)
    
    # now we build two dictionaries, one with all the molecular data-frames, the other one with atomic data-frames
    atomic_results_dict = {}
    molecular_results_dict= {}
    queries_metadata_list = []
   

    # and we populate those two dictionaries by iterating over the queries that have been processed 
    for currentQuery in listOfAllQueries:
       
        nodeIdentifier = currentQuery.nodeEndpoint
        
        # Ensure the current query's dataframe has the common wavelength column
        if currentQuery.lines_df is not None and not currentQuery.lines_df.empty:
            harmonized_df = _ensure_common_wavelength_column(currentQuery.lines_df)
        else:
            harmonized_df = currentQuery.lines_df
       
        if currentQuery.speciesType == "atom":
            if nodeIdentifier in atomic_results_dict:
                # Ensure existing dataframe also has the common wavelength column
                existing_df = _ensure_common_wavelength_column(atomic_results_dict[nodeIdentifier])
                atomic_results_dict[nodeIdentifier] = pd.concat([existing_df, harmonized_df], ignore_index=True)
            else:
                atomic_results_dict[nodeIdentifier] = harmonized_df
        
        if currentQuery.speciesType == "molecule":
            if nodeIdentifier in molecular_results_dict:
                # Ensure existing dataframe also has the common wavelength column
                existing_df = _ensure_common_wavelength_column(molecular_results_dict[nodeIdentifier])
                molecular_results_dict[nodeIdentifier] = pd.concat([existing_df, harmonized_df], ignore_index=True)
            else:
                molecular_results_dict[nodeIdentifier] = harmonized_df
        
        # Build metadata dictionary for this query
        metadata_dict = {
            "nodeEndpoint": currentQuery.nodeEndpoint,
            "lambdaMin": currentQuery.lambdaMin,
            "lambdaMax": currentQuery.lambdaMax,
            "InchiKey": currentQuery.InchiKey,
            "vamdcCall": currentQuery.vamdcCall,
            "XSAMS_file_path": currentQuery.XSAMSFileName
        }
        queries_metadata_list.append(metadata_dict)
    
    
    if not(atomic_results_dict) :
        LOGGER.info("No atomic data to fetch")

    if not(molecular_results_dict):
        LOGGER.info("No molecular data to fetch")

    # we return the three results: atomic, molecular dictionaries and queries metadata list
    return atomic_results_dict, molecular_results_dict, queries_metadata_list
