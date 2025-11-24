import pandas as pd
import multiprocessing
from enum import Enum
import pyVAMDC.spectral.species as species
import pyVAMDC.spectral.vamdcQuery as vamdcQuery


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



def getLinesByTelescopeBand(band:telescopeBands, species_dataframe = None, nodes_dataframe = None, verbose = False):
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
        
        verbose : boolean
            If True, display verbose logs. 
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
    return getLines(band.lambdaMin, band.lambdaMax, species_dataframe=species_dataframe, nodes_dataframe=nodes_dataframe, verbose=verbose)



class _VAMDCQueryParallelWrapping:
    """
    This class is a wrapping for the VamdcQuery class (defined in the vamdcQuery module) to parallelize part of the processings: the instanciation
    of the VamdcQuery objects and the execution of the related HEAD queries. We will use this wrapping to have a distinct parallelized process instanciating 
    queries, a process for each database (VAMDC node)

    Args:
        localDataFrame : dataframe
            a dataframe containing species data. To run a parallel process for each VAMDC node, the field 
            tapEndpoint of the dataframe localDataFrame must contain a unique value (the same for each row)
            
        lambdaMin : float
            the inf boundary (in Angstrom) of the wavelenght interval
        
        lambdaMax : float
            the sup boundary (in Angstrom) of the wavelenght interval  
        
        verbose : boolean
            If True, display verbose logs. 
    """
    def __init__(self, localDataFrame, lambdaMin, lambdaMax, verbose, acceptTruncation):
        self.local_df = localDataFrame
        self.lambdaMin = lambdaMin
        self.lambdaMax = lambdaMax
        self.verbose = verbose
        self.acceptTruncation = acceptTruncation

    def parallelMethod(self):
        """
        Definition of the tasks that will be executed by each parallel process.
        This tasks are the instanciation of the VamdcQuery objects (and the execution of the HEAD queries). 
        """
        listOfQueries = []

        # looping over the content of the local data frame
        for index, row in self.local_df.iterrows():
            nodeEndpoint = row["tapEndpoint"]
            InChIKey = row["InChIKey"]
            speciesType = row["speciesType"]

            # for each row of the data-frame we create a VamdcQuery instance
            vamdcQuery.VamdcQuery(nodeEndpoint,self.lambdaMin,self.lambdaMax, InChIKey, speciesType, listOfQueries, self.verbose, self.acceptTruncation)

        return listOfQueries


def _process_instance(instance):
    """
    Definition of the mapping between the prallel processes and the 
    tasks executed by each process.
    """
    return instance.parallelMethod()


def _build_and_run_wrappings(lambdaMin, lambdaMax, species_dataframe, nodes_dataframe, verbose, accept_truncation) -> list:
     # if the provided species_dataframe is not provided, we build it by taking all the species
    if species_dataframe is None:
        species_dataframe , _ = species.getAllSpecies()

    # if the provided node_dataframe is None
    if nodes_dataframe is None:
        nodes_dataframe = species.getNodeHavingSpecies()
    
    # Getting the list of the nodes passed as argument
    selectedNodeList = nodes_dataframe["ivoIdentifier"].to_list()

    # fitler the list of species by selecting only the node from the selectedNodeList
    filtered_species_df = species_dataframe[species_dataframe["ivoIdentifier"].isin(selectedNodeList)]

    # Let us split the dataFrame, grouping by nodes
    df_list = [group for _, group in filtered_species_df.groupby('tapEndpoint')]

    # defining the list for storing the instances of the query wrapping
    wrappingInstances = []

    # Loop over the list of dataFrame, for each element we create an instance of the wrapper to be added to the list of wrapping
    for current_df in df_list:
        instance = _VAMDCQueryParallelWrapping(current_df, lambdaMin, lambdaMax, verbose, accept_truncation)

        wrappingInstances.append(instance) 
    
    # We define the number of parallel processes, one for each wrapping instance
    NbOfProcesses = len(wrappingInstances)

    # we launch the parallel processing using the wrapper objects
    # we will have a process for each datanode
    # Handle the case where no wrapping instances were created (empty species for selected nodes)
    if NbOfProcesses == 0:
        results = []
    else:
        with multiprocessing.Pool(processes=NbOfProcesses) as pool:
            # Apply the process_instance function to each instance and get the results
            results = pool.map(_process_instance, wrappingInstances)

     # defining an empty list, which will be used to store all the VamdcQuery instances returned by the parallel process
    listOfAllQueries = []

    for result in results:
        listOfAllQueries.extend(result)

    return listOfAllQueries


def get_metadata_for_lines(lambdaMin, lambdaMax, species_dataframe = None, nodes_dataframe = None, verbose = False):
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

            verbose : boolean, optional
                If True, display verbose logs.

        Returns:
            metadata_list : list
                A list of dictionaries, one per sub-query, where each dictionary contains:
                    - 'query': the query URL/string that will be executed
                    - 'response': the HEAD response metadata (as stored on the VamdcQuery instance)
        """
    listOfAllQueries = _build_and_run_wrappings(lambdaMin, lambdaMax, species_dataframe, nodes_dataframe, verbose, True)

    # build list of dictionaries with keys 'query' and 'response'
    metadata_list = []
    for currentQuery in listOfAllQueries:
        metadata_list.append({
            "query": currentQuery.vamdcCall,
            "metadata": currentQuery.counts
        })

    return metadata_list




def getLines(lambdaMin, lambdaMax, species_dataframe = None, nodes_dataframe = None, verbose = False, acceptTruncation = False):
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
        
        verbose : boolean
            If True, display verbose logs. 
  
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
    listOfAllQueries = _build_and_run_wrappings(lambdaMin, lambdaMax, species_dataframe, nodes_dataframe, verbose, acceptTruncation)

    print("total amount of sub-queries to be submitted "+str(len(listOfAllQueries)))

    # At this point the list listOfAllQueries contains all the query that can be run without truncation
    # For each query in the list, we get the data, and convert the data into a Pandas dataframe
    for currentQuery in listOfAllQueries:
        # get the data
        currentQuery.getXSAMSData()
        # convert the data 
        currentQuery.convertToDataFrame()
    
    # now we build two dictionaries, one with all the molecular data-frames, the other one with atomic data-frames
    atomic_results_dict = {}
    molecular_results_dict= {}
    queries_metadata_list = []
   

    # and we populate those two dictionaries by iterating over the queries that have been processed 
    for currentQuery in listOfAllQueries:
       
        nodeIdentifier = currentQuery.nodeEndpoint
       
        if currentQuery.speciesType == "atom":
            if nodeIdentifier in atomic_results_dict:
                atomic_results_dict[nodeIdentifier] = pd.concat([atomic_results_dict[nodeIdentifier], currentQuery.lines_df], ignore_index=True)
            else:
                atomic_results_dict[nodeIdentifier] = currentQuery.lines_df
        
        if currentQuery.speciesType == "molecule":
            if nodeIdentifier in molecular_results_dict:
                 molecular_results_dict[nodeIdentifier] = pd.concat([molecular_results_dict[nodeIdentifier], currentQuery.lines_df], ignore_index=True)
            else:
                molecular_results_dict[nodeIdentifier] = currentQuery.lines_df
        
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
        print("no atomic data to fetch")

    if not(molecular_results_dict):
        print("no molecular data to fetch")

    # we return the three results: atomic, molecular dictionaries and queries metadata list
    return atomic_results_dict, molecular_results_dict, queries_metadata_list
