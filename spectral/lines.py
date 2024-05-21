import pandas as pd
import sys
import species

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
    
    

    print('iti')
    




def main():

    nodes_df = species.getNodeHavingSpecies()
    row_indices = [0, 2, 4]
    filtered_df = nodes_df.iloc[row_indices]

    getLines(10,20, nodes_dataframe=filtered_df)

if __name__=='__main__':
    main()

