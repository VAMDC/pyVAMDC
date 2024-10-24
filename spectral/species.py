import pandas as pd
import json
import urllib.request
from datetime import datetime
from io import StringIO

import numpy as np

from rdkit import Chem
from rdkit.Chem import Draw

def _getEndpoints():
    """
    get the two endpoints of the Species-database web services 
    (cf. https://doi.org/10.1140/epjd/s10053-024-00863-1 for a detailed description of those services)

    Returns:
        urlNodeEnpoint : str
            the url of the Node endpoint
        urlSpeciesEntpoint : str
            the url of the Species endpoint
    """
    urlNodeEnpoint = 'https://species.vamdc.org/web-service/api/v12.07/nodes'
    urlSpeciesEntpoint = 'https://species.vamdc.org/web-service/api/v12.07/species'
    return urlNodeEnpoint, urlSpeciesEntpoint


def getNodeHavingSpecies():
    """
   Returns a Pandas dataframe containing the information regarding the Nodes. Only the node having chemical-data are included in the dataframe 

    Returns:
        df_nodes : dataframe
            a Pandas dataframe containing the information regarding the Nodes.
    """
    # get the Node-service endpoint
    urlNodeEnpoint, _ =_getEndpoints()

    # interact with the service endpoint and format the result into json
    responseNode = urllib.request.urlopen(urlNodeEnpoint)
    json_list = responseNode.read()
    data = json.loads(json_list)

    # convert the json to Pandas dataframe
    df_nodes = pd.json_normalize(data)

    # return the generated dataframe
    return df_nodes


def getAllSpecies():
    """
    Gets all the chemical information available on the Species Database. 
    Returns two Pandas dataframe. One for the inforamtion regarding the Nodes, the other for the information regarding the chemical species within the Nodes. 

    Returns:
        AllSpeciedDF : dataframe
            a Pandas dataframe containing all the chemical information available on the Species database.

        df_nodes : dataframe
            a Pandas dataframe containing the information regarding the Nodes.
    """
    # get the Species-database service endpoints
    _, urlSpeciesEndpoint = _getEndpoints()
    # get the chemical information
    AllSpeciedDF, df_nodes = _getChemicalInfoFromEnpoint(urlSpeciesEndpoint)
    return AllSpeciedDF, df_nodes


def getSpeciesWithSearchCriteria(text_search = None, stoichiometric_formula = None, mass_min = None, mass_max = None, charge_min = None, charge_max = None, type = None, ivo_identifier = None, inchikey = None, name = None, structural_formula = None):
    """
    Gets the chemical informaton available on the Species Database, with restriction defined by the user. Restriction are defined via the calling arguments
    Args:
        text_search : str 
            Restricts the results to those having at least a protion of one ot the five fields 'stoichiometric_formula', 'formula', 
            'name', 'InChi', 'inchikey' equal to the user provided value. Default None.

        stoichiometric_formula : str
            Restricts the results to those having their stoichiometric  formulaequal equal to the provided valeu. Default is None. 
        
        mass_min : Integer
            Restricts the results to those having their mass number greater than mass_min, Default None. 
            If both mass_min nd mass_max are provided, the difference (mass_max - mass_min) is checked to be positive. Otherwise an exception is risen.
        
        mass_max : Integer 
            Restricts the results to those having their mass number smaller than mass_max, Default None. 
            If both mass_min nd mass_max are provided, the difference (mass_max - mass_min) is checked to be positive. Otherwise an exception is risen.
        
        charge_min : Integer
            Restricts the results to those having their electric charge greater than charge_min, Default None. 
            If both charge_min nd charge_max are provided, the difference (charge_max - charge_min) is checked to be positive. Otherwise an exception is risen. 
        
        charge_max = Integer
            Restricts the results to those having their electric charge smaller than charge_max, Default None. 
            If both charge_min nd charge_max are provided, the difference (charge_max - charge_min) is checked to be positive. Otherwise an exception is risen. 
        
        type : str
            Restricts the results to those having their type equal to the provided value. Default None. 
            Admitted Values are 'molecule', 'atom', 'particle' 
        
        ivo_identifier : str
            Restricts the results to the data-Node whose identifier is equal to the provided value. Default None.
        
        inchikey : str
            Restricts the results to those having the InchiKey equal to the provided value. Default None 

        name : str
            Restricts the results to those having the species name equal to the provided value. Default None 

        structural_formula : str
            Restricts the results to those having the structural formula equal to the provided value. Default None 

     Returns:
        species_df : dataframe
            a Pandas dataframe containing the chemical information available on the Species database and satisfying all the defined restrictions.

        df_nodes : dataframe
            a Pandas dataframe containing the information regarding the Nodes.
    """
    _, urlSpeciesEndpoint = _getEndpoints()
    
    urlSuffix = "?"
    urlSuffix = (urlSuffix+"text_search="+text_search+"&") if text_search is not None else urlSuffix
    urlSuffix = (urlSuffix+"stoichiometric_formula="+stoichiometric_formula+"&") if stoichiometric_formula is not None else urlSuffix
    urlSuffix = (urlSuffix+"ivo_identifier="+ivo_identifier+"&") if ivo_identifier is not None else urlSuffix
    urlSuffix = (urlSuffix+"inchikey="+inchikey+"&") if inchikey is not None else urlSuffix
    urlSuffix = (urlSuffix+"name="+name+"&") if name is not None else urlSuffix
    urlSuffix = (urlSuffix+"structural_formula="+structural_formula+"&") if structural_formula is not None else urlSuffix

    if type == "molecule" or type == "atom":
        urlSuffix = urlSuffix + "type="+type + "&"


    # testing the difference mass_max-mass_min to be positive 
    maxMinDifference = mass_max-mass_min if (mass_max is not None and mass_min is not None) else 0
    if maxMinDifference < 0:
        raise Exception("The difference (mass_max - mass_min) must be positive")

    # testing the difference charge_max-charge_min to be positive 
    maxMinDifference = charge_max - charge_min if (charge_max is not None and charge_min is not None) else 0
    if maxMinDifference < 0: 
        raise Exception("The difference (charge_max - charge_min) must be positive")

    urlSuffix = (urlSuffix+"mass_max="+str(mass_max)+"&") if mass_max is not None else urlSuffix
    urlSuffix = (urlSuffix+"mass_min="+str(mass_min)+"&") if mass_min is not None else urlSuffix

    urlSuffix = (urlSuffix+"charge_max="+str(charge_max)+"&") if charge_max is not None else urlSuffix
    urlSuffix = (urlSuffix+"charge_min="+str(charge_min)+"&") if charge_min is not None else urlSuffix

    fullUrl = urlSpeciesEndpoint + urlSuffix

    species_df , node_df = _getChemicalInfoFromEnpoint(fullUrl)
    
    return species_df, node_df

    
  


def getAllSpeciesInExcelFile(filePath):
    """
    Generate and save an Excel spreadsheet containing all the Chemical information extracted from the 
    Species database.

    Arg:
        filePath (str): The path of the folder where the file will be generate.
        
    Returns:
        filename (str): The absolute name (including the absolute path) of the generated file. This name will contain the creation timestamp.  
    """
    AllSpeciesDF, df_nodes = getAllSpecies()
    
    now = datetime.now()
    date_time = now.strftime("%Y%m%d_%H_%M")
    file_name = filePath+"/SpeciesDatabase"+date_time+".xlsx"

    writer = pd.ExcelWriter(file_name, engine='openpyxl')

    # convert the DataFrame to an Excel object
    AllSpeciesDF.to_excel(writer, index=False, sheet_name='species')

    # get the workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets['species']

    # iterate over the columns and set the width of each column based on the maximum length of the data in that column
    for i, column in enumerate(AllSpeciesDF.columns):
        column_width = max(AllSpeciesDF[column].astype(str).map(len).max(), len(column))
        worksheet.column_dimensions[worksheet.cell(row=1, column=i+1).column_letter].width = column_width

    #convert the Node DataFrame to an Excel object
    df_nodes.to_excel(writer, index=False, sheet_name='nodes')
    workbook = writer.book
    worksheet = writer.sheets['nodes']

    # iterate over the columns and set the width of each column based on the maximum length of the data in that column
    for i, column in enumerate(df_nodes.columns):
        column_width = max(df_nodes[column].astype(str).map(len).max(), len(column))
        worksheet.column_dimensions[worksheet.cell(row=1, column=i+1).column_letter].width = column_width

    # save the Excel file
    #import warnings
    #warnings.filterwarnings("ignore")
    writer.close()
    print("The excel file "+ file_name+" has been successfully created.")
    return file_name





def _getChemicalInfoFromEnpoint(specificSpeciesEndpoint):
    """
    This function factors some code common to the 'getSpeciesWithSearchCriteria' and 'getAllSpecies' functions.  
    It is not intended to be called directly, but only by the two functions we have just mentioned. 

    Arg:
        specificSpeciesEndpoint (str): the url of the endpoint to resolve to extract the chemical information
        
    Returns:
        AllSpeciesDF (dataframe): a Pandas dataframe containing the chemical information obtained while resolving the URL in the specificSpeciesEndpoint variable.
        df_nodes (dataframe):  a Pandas dataframe containing the information regarding the Nodes.
    """
    response = urllib.request.urlopen(specificSpeciesEndpoint)
    data = json.loads(response.read())

    # wrapping the results into a unique Pandas Dataframe (called AllSpeciesDF)
    dataFrames = []
    AllSpeciesDF = None
    for key, value in data.items():
        datum = json.dumps(value)
        df = pd.read_json(StringIO(datum), orient='records')
        df.insert(0,"ivoIdentifier", str(key))
        dataFrames.append(df)

    AllSpeciesDF = pd.concat(dataFrames)

    # getting a second dataframe with the information of Data-Nodes having chemical data
    df_nodes = getNodeHavingSpecies()

    # merging the raw chemical information with the ones from the Data-Nodes
    AllSpeciesDF = AllSpeciesDF.merge(df_nodes[['ivoIdentifier', 'shortName', 'tapEndpoint']], on='ivoIdentifier', how='left')

    col = AllSpeciesDF.pop('shortName')  # Remove the column and store it in col
    AllSpeciesDF.insert(0, 'shortName', col)  # Insert the column at the beginning

    # Split the column 'lastSeenDateTime' and expand into two separate columns
    AllSpeciesDF['lastIngestionScriptDate']= AllSpeciesDF["lastSeenDateTime"].apply(lambda x: x.split('||')[0])
    AllSpeciesDF['speciesLastSeenOn']= AllSpeciesDF["lastSeenDateTime"].apply(lambda x: x.split('||')[1])

    # Drop the original 'lastSeenDateTime' column
    AllSpeciesDF = AllSpeciesDF.drop('lastSeenDateTime', axis=1)

    # enrich the AllSpeciesDF with chemical information locally computed
    AllSpeciesDF = getChemicalInformationsFromInchi(AllSpeciesDF)

    return AllSpeciesDF, df_nodes


def getChemicalInformationsFromInchi(inchi):
    """
    Provides some chemical characterisation of a given species, from its Inchi 

    Args:
        inchi (str): The InChI string of the molecule.
        
    Returns:
        len(atom_set) (integer): the number of unique atoms in the species
        len(atom_list) (integer): the number of total atoms in the species
        total_charge (integer): the total electric charge of the species
        atom_set (set): a set containing unique atoms forming a given species.
        atom_list (list): a list containing the atoms forming a given species (with eventual duplications) 
    """
    mol = Chem.MolFromInchi(inchi, sanitize=False, removeHs=False)
    mol = Chem.AddHs(mol) 
    
    atoms_set = set()
    atoms_list = list()
    total_charge = 0

    for atom in mol.GetAtoms():
        atoms_set.add(atom.GetSymbol())
        atoms_list.append(atom.GetSymbol())
        total_charge += atom.GetFormalCharge()

    return len(atoms_set), len(atoms_list), total_charge, atoms_set, atoms_list




def addComputedChemicalInfo(input_df):
    """
    Adds some chemical characterisations to a dataframe containing species information.

    Args:
        input_df (dataframe): A dataframe containing chemical information. The input_df must contain at least 
        a column named 'InChI' and containing valid InChI identifiers. 
        

    Returns:
        input_df (dataframe): the input_df enriched with the chemical characterisation computed by the function. 
        The columns added to the dataframe and populated with the computed values are: '# unique atoms', '# total atoms', 'computed charge'
    """
    # if the dataframe provided as call argument has a column called InChI
    if "InChI" in input_df.columns:
        # let us iterate over the rows of the dataframe
        for index, row in input_df.iterrows():
            inchi = row['InChI']
            try:
                # get some chemical information locally, from the InChI
                number_unique_atoms, number_total_atoms, computed_charge , _ , _, = getChemicalInformationsFromInchi(inchi)
            except:
                # if the chemical information can not be deduced from the Inchi
                print("Exception in converting the InChI:" + str(inchi))
                number_unique_atoms = np.nan
                number_total_atoms = np.nan
                computed_charge = np.nan
            finally:
                # Enrich the input dataframe with the computed fields
                input_df.at[index, '# unique atoms'] = number_unique_atoms
                input_df.at[index, '# total atoms'] = number_total_atoms
                input_df.at[index, 'computed charge'] =  computed_charge
    # return the enriched dataframe
    return input_df


def generate_molecule_image(inchi, image_path_and_name, size=(300, 300)):
    """
    Generates and save the 2D depiction of a molecule from its InChI.

    Args:
        inchi (str): The InChI string of the molecule.
        image_path_and_name (str): The filename to save the molecule image.
        size (tuple, optional): The dimensions of the image in pixels. Defaults to (300, 300).

    Returns:
        image (a PIL Image object): the producted image 
    """
    try:
        # Convert the InChI to a RDKit molecule object
        mol = Chem.MolFromInchi(inchi, sanitize=False, removeHs=False)
        mol = Chem.AddHs(mol) 
        # Generate the 2D depiction of the molecule
        mol_image = Draw.MolToImage(mol, size=size)
        # Save the molecule image to a file
        mol_image.save(image_path_and_name)
    except Exception as error:
         print("Exception in converting the InChI:" + str(inchi))
         print(error)
         mol_image = None
    finally:
        return mol_image
    