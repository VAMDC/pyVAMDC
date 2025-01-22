import pandas as pd
import multiprocessing
from enum import Enum
import pyVAMDC.spectral.species as species
import pyVAMDC.spectral.vamdcQuery as vamdcQuery

from multiprocessing import Manager
import threading

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
    def __init__(self, localDataFrame, lambdaMin, lambdaMax, verbose, listOfQueries, numParallelWorkers):
        self.local_df = localDataFrame
        self.lambdaMin = lambdaMin
        self.lambdaMax = lambdaMax
        self.verbose = verbose
        self.listOfQueries = listOfQueries
        self.numParallelWorkers = numParallelWorkers
        self.parallelMethod()


    def parallelMethod(self):
        """
        Definition of the tasks that will be executed by each parallel process.
        This tasks are the instanciation of the VamdcQuery objects (and the execution of the HEAD queries). 
        """
        # looping over the content of the local data frame
        for index, row in self.local_df.iterrows():
            nodeEndpoint = row["tapEndpoint"]
            InChIKey = row["InChIKey"]
            speciesType = row["speciesType"]

            # for each row of the data-frame we create a VamdcQuery instance
            vamdcQuery.VamdcQuery(nodeEndpoint,self.lambdaMin,self.lambdaMax, InChIKey, speciesType, self.listOfQueries, self.verbose, self.numParallelWorkers)


def getLines(lambdaMin, lambdaMax, species_dataframe = None, nodes_dataframe = None, verbose = False, numParallelWorkers = 2):
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
    manager = Manager()
    totalListOfQueries = manager.list()
    threadList = []
    # Loop over the list of dataFrame, for each element we create an instance of the wrapper to be added to the list of wrapping
    for current_df in df_list:
        thread =  threading.Thread(target=_VAMDCQueryParallelWrapping, args=(current_df, lambdaMin, lambdaMax, verbose, totalListOfQueries, numParallelWorkers))
        threadList.append(thread)
        thread.start()

    for thread in threadList:
        thread.join()
    
     # defining an empty list, which will be used to store all the VamdcQuery instances returned by the parallel process
    listOfAllQueries = list(totalListOfQueries)

    print("total amount of sub-queries to be submitted "+str(len(listOfAllQueries)))

    # At this point the list listOfAllQueries contains all the query that can be run without truncation
    # For each query in the list, we get the data, and convert the data into a Pandas dataframe
 #   for currentQuery in listOfAllQueries:
        # get the data
 #       currentQuery.getXSAMSData()
        # convert the data 
 #       currentQuery.convertToDataFrame()
    
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




