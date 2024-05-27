import os
import pandas as pd
from pyVAMDC.spectral.species import getSpeciesWithSearchCriteria, getNodeHavingSpecies
from pyVAMDC.spectral.lines import getLines
#from spectral import vamdcQuery


def main():

    nodes_df = getNodeHavingSpecies()
  
    # We just select the CDMS (number 8) and topbase (number 27) nodes for testing the library
    row_indices = [8,27]

    filtered_nodes_df = nodes_df.iloc[row_indices]

    # we get a list of species built using the search API
    #species_df , _ = species.getSpeciesWithSearchCriteria(name="Fe", charge_min=11, charge_max=11)

    #this species is Carbon
    species_df , _ = getSpeciesWithSearchCriteria(text_search="OKTJSMMVPCPJKN-UHFFFAOYSA-N")

    #this species is Carbon-Monoxide
    #species_df , _ = species.getSpeciesWithSearchCriteria(text_search="UGFAIRIUMAVXCW-UHFFFAOYSA-N")
    
    

    print(species_df)
    print(filtered_nodes_df)

    lambdaMin = 1
    lambdaMax = 100000000

    atomicLines, molecularLines = getLines(lambdaMin, lambdaMax, nodes_dataframe=filtered_nodes_df, species_dataframe=species_df)

    atomicLines.to_html("./atom.html")
    molecularLines.to_html("./molecule.html")

if __name__=='__main__':
    main()