import pandas as pd
import sys
import species

import vamdcQuery

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

    print(len(listOfAllQueries))



def main():

    nodes_df = species.getNodeHavingSpecies()
    # We just select the topbase node for testing the library
    row_indices = [27]
    filtered_df = nodes_df.iloc[row_indices]

    # we get a list of species built using the search API
    #species_df , _ = species.getSpeciesWithSearchCriteria(name="Fe", charge_min=11, charge_max=11)
    species_df , _ = species.getSpeciesWithSearchCriteria(text_search="DOBFQOMOKMYPDT-UHFFFAOYSA-N")

    print(species_df)

    lambdaMin = 1
    lambdaMax = 50

 #   getLines(lambdaMin, lambdaMax, nodes_dataframe=filtered_df, species_dataframe=species_df)

if __name__=='__main__':
    main()

