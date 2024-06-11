import pandas as pd
import random
import string

from pyVAMDC.spectral.species import getSpeciesWithSearchCriteria, getNodeHavingSpecies
from pyVAMDC.spectral.lines import getLines
from pyVAMDC.spectral.energyConverter import convert_dataframe_units
from pyVAMDC.spectral.filters import filterDataByColumnValues, filterDataHavingColumnContainingStrings, filterDataHavingColumnNotContainingStrings
#from spectral import vamdcQuery


def main():
    # Let us test filtering functions on some simple data-frame
    A = [random.uniform(0, 1) for _ in range(20)]
    B = [''.join(random.choices(string.ascii_uppercase + string.digits, k=5)) for _ in range(20)]
    C = [random.randint(0, 100) for _ in range(20)]


    df = pd.DataFrame({'A': A, 'B': B, 'C': C})

    # testing the unit conversion
    df = convert_dataframe_units(df, 'A', 'angstrom', 'D', 'kelvin', False)

    # testing the filter over some values of the dataframe
    df = filterDataByColumnValues(df, 'C', 30 , 80)

    # testing the filtering by string inlcusion 
    filtered_df = filterDataHavingColumnContainingStrings(df, 'B', ["A", "XB", "33"])

    # testing the filtering by string exclusion 
    filtered_df = filterDataHavingColumnNotContainingStrings(df, 'B', ["ZE", "ZY", "12"])

    print(filtered_df)

    # starting testing VAMDC functionalities
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

   

if __name__=='__main__':
    main()