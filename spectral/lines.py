import pandas as pd
import pyVAMDC.spectral.species as species
import pyVAMDC.spectral.vamdcQuery as vamdcQuery

def getLines(lambdaMin, lambdaMax, species_dataframe = None, nodes_dataframe = None):
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

    # defining an empty list, which will be used to store all the VamdcQuery instances
    listOfAllQueries = []

    # looping over the content of the filtered data frame
    for index, row in filtered_species_df.iterrows():
        nodeEndpoint = row["tapEndpoint"]
        InChIKey = row["InChIKey"]
        speciesType = row["speciesType"]

        # for each row of the data-frame we create a VamdcQuery instance
        vamdcQuery.VamdcQuery(nodeEndpoint,lambdaMin,lambdaMax, InChIKey, speciesType, listOfAllQueries)

    print("total amount of sub-queries to be submitted "+str(len(listOfAllQueries)))

    # At this point the list listOfAllQueries contains all the query that can be run without truncation
    # For each query in the list, we get the data, and convert the data into a Pandas dataframe
    for currentQuery in listOfAllQueries:
        # get the data
        currentQuery.getXSAMSData()
        # convert the data 
        currentQuery.convertToDataFrame()
    
    # now we build two list, one with all the molecular data-frames, the other one with atomic data-frames
    list_molecular_df = []
    list_atomic_df = []

    # and we populate those two list by iterating over the queries that have been processed to generate its data-frame
    for currentQuery in listOfAllQueries:
        if currentQuery.speciesType == "atom":
            list_atomic_df.append(currentQuery.lines_df)
        if currentQuery.speciesType == "molecule":
            list_molecular_df.append(currentQuery.lines_df)
    
    # we concatenate the lists, if these are not empty, into ad-hoc data-frames
    atomic_df = None
    molecular_df = None
    
    if list_atomic_df:    
        atomic_df = pd.concat(list_atomic_df, ignore_index=True)
    else:
        print("no atomic data to fetch")

    if list_molecular_df:
        molecular_df = pd.concat(list_molecular_df, ignore_index=True)
    else:
        print("no molecular data to fetch")

    # we return the two data-frames
    return atomic_df, molecular_df



