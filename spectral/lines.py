import pandas as pd
import pyVAMDC.spectral.species as species
import pyVAMDC.spectral.vamdcQuery as vamdcQuery
import multiprocessing

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
    def __init__(self, localDataFrame, lambdaMin, lambdaMax, verbose):
        self.local_df = localDataFrame
        self.lambdaMin = lambdaMin
        self.lambdaMax = lambdaMax
        self.verbose = verbose

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
            vamdcQuery.VamdcQuery(nodeEndpoint,self.lambdaMin,self.lambdaMax, InChIKey, speciesType, listOfQueries, self.verbose)

        return listOfQueries


def _process_instance(instance):
    """
    Definition of the mapping between the prallel processes and the 
    tasks executed by each process.
    """
    return instance.parallelMethod()


def getLines(lambdaMin, lambdaMax, species_dataframe = None, nodes_dataframe = None, verbose = False):
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
    """
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
        instance = _VAMDCQueryParallelWrapping(current_df, lambdaMin, lambdaMax, verbose)
        wrappingInstances.append(instance)
    
    # We define the number of parallel processes, one for each wrapping instance
    NbOfProcesses = len(wrappingInstances)

    # we launch the parallel processing using the wrapper objects
    # we will have a process for each datanode
    with multiprocessing.Pool(processes=NbOfProcesses) as pool:
        # Apply the process_instance function to each instance and get the results
        results = pool.map(_process_instance, wrappingInstances)

     # defining an empty list, which will be used to store all the VamdcQuery instances returned by the parallel process
    listOfAllQueries = []

    for result in results:
        listOfAllQueries.extend(result)


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
    
    
    if not(atomic_results_dict) :
        print("no atomic data to fetch")

    if not(molecular_results_dict):
        print("no molecular data to fetch")

    # we return the two dictionaries
    return atomic_results_dict, molecular_results_dict




