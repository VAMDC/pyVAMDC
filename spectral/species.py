from logging import Logger

import pandas as pd
import json
import urllib.request
from datetime import datetime
from io import StringIO

import numpy as np
from enum import Enum

from rdkit import Chem
from rdkit.Chem import Descriptors
from rdkit.Chem import Draw

import logging
from logging import Logger

# Import the filters module
from pyVAMDC.spectral import filters

LOGGER = logging.getLogger(__name__)
class speciesByAstronomicalDomains(Enum):
    """
    This class defines some astronomical domain, together with their related species.
    Chemical species are identified by theri IncIKey
    """

    #TODO enrich the list of applications and related species

    planetary_atmospheres = ["MWUXSHHQAYIFBG-UHFFFAOYSA-N","RAHZWNYVWXNFOC-UHFFFAOYSA-N","OKKJLVBELUTLKV-UHFFFAOYSA-N","XRJCSTPFBZTAPK-UHFFFAOYSA-N"]
    hot_cores = ["WSFSSNUMVMOOMR-UHFFFAOYSA-N","WSFSSNUMVMOOMR-UHFFFAOYSA-N","LELOWRISYMNNSU-UHFFFAOYSA-N"]
    dark_clouds = ["WSFSSNUMVMOOMR-UHFFFAOYSA-N","LELOWRISYMNNSU-OUBTZVSYSA-N","BDAGIHXWWSANSR-UHFFFAOYSA-N","MWUXSHHQAYIFBG-UHFFFAOYSA-N"]
    diffuse_clouds = ["WSFSSNUMVMOOMR-UHFFFAOYSA-N","LELOWRISYMNNSU-UHFFFAOYSA-N","MWUXSHHQAYIFBG-UHFFFAOYSA-N"]
    comets = ["WSFSSNUMVMOOMR-UHFFFAOYSA-N","BDAGIHXWWSANSR-UHFFFAOYSA-N","MWUXSHHQAYIFBG-UHFFFAOYSA-N"]
    agb_ppn_pn = ["WSFSSNUMVMOOMR-UHFFFAOYSA-N","LNDJVIYUJOJFSO-UHFFFAOYSA-N","TUJKJAMUKRIRHC-UHFFFAOYSA-N","HBMJWWWQQXIZIP-UHFFFAOYSA-N"]
    extragalactic= ["WSFSSNUMVMOOMR-UHFFFAOYSA-N","MWUXSHHQAYIFBG-UHFFFAOYSA-N","WEVYAHXRMPXWCK-UHFFFAOYSA-N"]


def getSpeciesByAstronomicalDomain(domain = speciesByAstronomicalDomains):
    """
    Gets all the chemical species for a given astronomical domain.

    Arg:
        domain : speciesByAstronomicalDomains
        restriction to a given astronomical domain, defined in the "speciesByAstronomicalDomains" class

    Returns:
        AllSpeciedDF : dataframe
            a Pandas dataframe containing all the chemical information available on the Species database.
            The structure of the dataframe is the following:
                shortname: a human readable name for the node the current species is extracted from;
                ivoIdentifier: the unique identifier for the Node the current species is extracted from;
                InChI: the InChI chemical unique identifier for the current species;
                InChIKey: the InChIKey derived from the InChI;
                stoichiometricFormula: the stoichiometric Formula of the current species;
                massNumber: the mass number for the current species;
                charge: the electric charge for the current species;
                speciesType: the type of the current species. Available values are 'molecule', 'atom', 'particle';
                structuralFormula: the structural formula of the current species;
                name: a human readable name for the current species;
                did: an alternative unique identifier for the current species;
                tapEndpoint: the enpoint of the Data Node from which data related to the current species may be extracted;
                lastIngestionScriptDate: the last time the species database executed its ingestion script;
                speciesLastSeenOn: the last time the species database has been fed with information about the current species.
    """

    # we start by getting all the species available within VAMDC
    species_dataframe , _ = getAllSpecies()
    speciesByDomain = domain.value
    filtered_species_df = species_dataframe[species_dataframe["InChIKey"].isin(speciesByDomain)]
    return filtered_species_df



def _getEndpoints():
    """
    Gets the two endpoints of the Species-database web services
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


def getSpeciesWithRestrictions(name = None, inchi = None, inchikey = None, ivo_identifier = None,
                               stoichiometric_formula = None, mass_min = None, mass_max = None,
                               charge_min = None, charge_max = None, type = None, number_unique_atoms_min = None,
                               number_unique_atoms_max = None, number_total_atoms_min = None, number_total_atoms_max = None,
                               computed_charge_min = None, computed_charge_max = None, computed_weight_min = None, computed_weight_max = None,
                               tap_endpoint = None):
    """
    Gets all the chemical species with restrictions defined by the user.

    Args:
        name (str): Filter species where stoichiometricFormula, structuralFormula, or name contains this string. Default None.
        inchi (str): Filter by exact InChI match. Default None.
        inchikey (str): Filter by exact InChIKey match. Default None.
        ivo_identifier (str): Filter by IVO identifier. Default None.
        stoichiometric_formula (str): Filter by exact stoichiometric formula match. Default None.
        mass_min (int): Minimum mass number. Default None.
        mass_max (int): Maximum mass number. Default None.
        charge_min (int): Minimum charge. Default None.
        charge_max (int): Maximum charge. Default None.
        type (str): Filter by species type ('molecule', 'atom', 'particle'). Default None.
        number_unique_atoms_min (int): Minimum number of unique atoms. Default None.
        number_unique_atoms_max (int): Maximum number of unique atoms. Default None.
        number_total_atoms_min (int): Minimum number of total atoms. Default None.
        number_total_atoms_max (int): Maximum number of total atoms. Default None.
        computed_charge_min (int): Minimum computed charge. Default None.
        computed_charge_max (int): Maximum computed charge. Default None.
        computed_weight_min (float): Minimum computed molecular weight. Default None.
        computed_weight_max (float): Maximum computed molecular weight. Default None.
        tap_endpoint (str): Filter by TAP endpoint of the node. Default None.

    Returns:
        filtered_df (dataframe): Filtered species dataframe.
        df_nodes (dataframe): Nodes dataframe.
    """
    # Get all species
    species_df, df_nodes = getAllSpecies()

    # Apply filters based on provided arguments

    # Filter by name (search in stoichiometricFormula, structuralFormula, and name columns)
    if name is not None:
        # Create a combined filter for the three columns
        mask = (
            species_df['stoichiometricFormula'].astype(str).str.contains(name, case=False, na=False) |
            species_df['structuralFormula'].astype(str).str.contains(name, case=False, na=False) |
            species_df['name'].astype(str).str.contains(name, case=False, na=False)
        )
        species_df = species_df[mask]

    # Filter by inchi (exact match)
    if inchi is not None:
        species_df = species_df[species_df['InChI'] == inchi]

    # Filter by inchikey (exact match)
    if inchikey is not None:
        species_df = species_df[species_df['InChIKey'] == inchikey]

    # Filter by ivo_identifier
    if ivo_identifier is not None:
        species_df = species_df[species_df['ivoIdentifier'] == ivo_identifier]

    # Filter by stoichiometric_formula (exact match)
    if stoichiometric_formula is not None:
        species_df = species_df[species_df['stoichiometricFormula'] == stoichiometric_formula]

    # Filter by tap_endpoint
    if tap_endpoint is not None:
        species_df = species_df[species_df['tapEndpoint'] == tap_endpoint]

    # Filter by type
    if type is not None:
        species_df = species_df[species_df['speciesType'] == type]

    # Filter by mass range
    if mass_min is not None or mass_max is not None:
        species_df = filters.filterDataByColumnValues(
            species_df, 'massNumber', minValue=mass_min, maxValue=mass_max
        )

    # Filter by charge range
    if charge_min is not None or charge_max is not None:
        species_df = filters.filterDataByColumnValues(
            species_df, 'charge', minValue=charge_min, maxValue=charge_max
        )

    # Filter by number of unique atoms
    if number_unique_atoms_min is not None or number_unique_atoms_max is not None:
        species_df = filters.filterDataByColumnValues(
            species_df, '# unique atoms', minValue=number_unique_atoms_min, maxValue=number_unique_atoms_max
        )

    # Filter by number of total atoms
    if number_total_atoms_min is not None or number_total_atoms_max is not None:
        species_df = filters.filterDataByColumnValues(
            species_df, '# total atoms', minValue=number_total_atoms_min, maxValue=number_total_atoms_max
        )

    # Filter by computed charge
    if computed_charge_min is not None or computed_charge_max is not None:
        species_df = filters.filterDataByColumnValues(
            species_df, 'computed charge', minValue=computed_charge_min, maxValue=computed_charge_max
        )

    # Filter by computed weight
    if computed_weight_min is not None or computed_weight_max is not None:
        species_df = filters.filterDataByColumnValues(
            species_df, 'computed mol_weight', minValue=computed_weight_min, maxValue=computed_weight_max
        )

    return species_df, df_nodes


def getAllSpecies():
    """
    Gets all the chemical information available on the Species Database.
    Returns two Pandas dataframe. One for the inforamtion regarding the Nodes, the other for the information regarding the chemical species within the Nodes.

    Returns:
        AllSpeciedDF : dataframe
            a Pandas dataframe containing all the chemical information available on the Species database.
            The structure of the dataframe is the following:
                shortname: a human readable name for the node the current species is extracted from;

                ivoIdentifier: the unique identifier for the Node the current species is extracted from;

                InChI: the InChI chemical unique identifier for the current species;

                InChIKey: the InChIKey derived from the InChI;

                stoichiometricFormula: the stoichiometric Formula of the current species;

                massNumber: the mass number for the current species;

                charge: the electric charge for the current species;

                speciesType: the type of the current species. Available values are 'molecule', 'atom', 'particle';

                structuralFormula: the structural formula of the current species;

                name: a human readable name for the current species;

                did: an alternative unique identifier for the current species;

                tapEndpoint: the enpoint of the Data Node from which data related to the current species may be extracted;

                lastIngestionScriptDate: the last time the species database executed its ingestion script;

                speciesLastSeenOn: the last time the species database has been fed with information about the current species.


        df_nodes : dataframe
            a Pandas dataframe containing the information regarding the Nodes which contain chemical information.
            The structure of the dataframe is the following:
                shortName: a human readable name for the Data Node;
                description: a human readable description og the Node;
                contactEmail: the e-mail of the Node's mantainer;
                ivoIdentifier: the unique identifier for the Node;
                tapEndPoint: the endpoint to retrive data from;
                referenceUrl: the webpage describing the node;
                lastUpdate: the last time the chemical information has beed updated from this node;
                lastSeen: the last time the node responded positevly to an update pull;
                topics: a list of keywords describing the node.
    """
    # get the Species-database service endpoints
    _, urlSpeciesEndpoint = _getEndpoints()
    # get the chemical information
    AllSpeciedDF, df_nodes = _getChemicalInfoFromEnpoint(urlSpeciesEndpoint)
    return AllSpeciedDF, df_nodes


def getSpeciesWithSearchCriteria(text_search = None, stoichiometric_formula = None, mass_min = None, mass_max = None, charge_min = None, charge_max = None, type = None, ivo_identifier = None, inchikey = None, name = None, structural_formula = None):
    """
    Gets the chemical informaton available on the Species Database, with restriction defined by the user. Restrictions are defined via the calling arguments
    Args:
        text_search: str
            Restricts the results to those having at least a protion of one ot the five fields 'stoichiometric_formula', 'formula',
            'name', 'InChi', 'inchikey' equal to the user provided value. Default None.

        stoichiometric_formula: str
            Restricts the results to those having their stoichiometric formula equal to the provided value. Default is None.

        mass_min: Integer
            Restricts the results to those having their mass number greater than mass_min. Default None.
            If both mass_min and mass_max are provided, the difference (mass_max - mass_min) is checked to be positive. Otherwise an exception is risen.

        mass_max: Integer
            Restricts the results to those having their mass number smaller than mass_max. Default None.
            If both mass_min and mass_max are provided, the difference (mass_max - mass_min) is checked to be positive. Otherwise an exception is risen.

        charge_min: Integer
            Restricts the results to those having their electric charge greater than charge_min. Default None.
            If both charge_min nd charge_max are provided, the difference (charge_max - charge_min) is checked to be positive. Otherwise an exception is risen.

        charge_max: Integer
            Restricts the results to those having their electric charge smaller than charge_max. Default None.
            If both charge_min nd charge_max are provided, the difference (charge_max - charge_min) is checked to be positive. Otherwise an exception is risen.

        type: str
            Restricts the results to those having their type equal to the provided value. Default None.
            Admitted Values are 'molecule', 'atom', 'particle'.

        ivo_identifier: str
            Restricts the results to the data-Node whose identifier is equal to the provided value. Default None.

        inchikey: str
            Restricts the results to those having their InchiKey equal to the provided value. Default None.

        name: str
            Restricts the results to those having their species name equal to the provided value. Default None.

        structural_formula : str
            Restricts the results to those having their structural formula equal to the provided value. Default None.

     Returns:
        species_df: dataframe
            a Pandas dataframe containing the chemical information available on the Species database and satisfying all the defined restrictions.
            The structure of the dataframe is the following:
                shortname: a human readable name for the node the current species is extracted from;
                ivoIdentifier: the unique identifier for the Node the current species is extracted from;
                InChI: the InChI chemical unique identifier for the current species;
                InChIKey: the InChIKey derived from the InChI;
                stoichiometricFormula: the stoichiometric Formula of the current species;
                massNumber: the mass number for the current species;
                charge: the electric charge for the current species;
                speciesType: the type of the current species. Available values are 'molecule', 'atom', 'particle';
                structuralFormula: the structural formula of the current species;
                name: a human readable name for the current species;
                did: an alternative unique identifier for the current species;
                tapEndpoint: the enpoint of the Data Node from which data related to the current species may be extracted;
                lastIngestionScriptDate: the last time the species database executed its ingestion script;
                speciesLastSeenOn: the last time the species database has been fed with information about the current species.

        df_nodes: dataframe
            a Pandas dataframe containing the information regarding the Nodes.
            The structure of the dataframe is the following:
                shortname: a human readable name for the node the current species is extracted from;
                ivoIdentifier: the unique identifier for the Node the current species is extracted from;
                InChI: the InChI chemical unique identifier for the current species;
                InChIKey: the InChIKey derived from the InChI;
                stoichiometricFormula: the stoichiometric Formula of the current species;
                massNumber: the mass number for the current species;
                charge: the electric charge for the current species;
                speciesType: the type of the current species. Available values are 'molecule', 'atom', 'particle';
                structuralFormula: the structural formula of the current species;
                name: a human readable name for the current species;
                did: an alternative unique identifier for the current species;
                tapEndpoint: the enpoint of the Data Node from which data related to the current species may be extracted;
                lastIngestionScriptDate: the last time the species database executed its ingestion script;
                speciesLastSeenOn: the last time the species database has been fed with information about the current species.
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
    AllSpeciesDF = addComputedChemicalInfo(AllSpeciesDF)

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
        molWeight (integer) : the molecular mass of the species.
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

    mol_weight = Descriptors.ExactMolWt(mol)

    return len(atoms_set), len(atoms_list), total_charge, atoms_set, atoms_list, mol_weight




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
                number_unique_atoms, number_total_atoms, computed_charge, _, _, computed_weight = getChemicalInformationsFromInchi(inchi)
            except:
                # if the chemical information can not be deduced from the Inchi
                LOGGER.error("Exception in converting the InChI: %s from %s" % (str(inchi), row['ivoIdentifier']))
                #print("Exception in converting the InChI:" + str(inchi))
                number_unique_atoms = np.nan
                number_total_atoms = np.nan
                computed_charge = np.nan
                computed_weight = np.nan
            finally:
                # Enrich the input dataframe with the computed fields
                input_df.at[index, '# unique atoms'] = number_unique_atoms
                input_df.at[index, '# total atoms'] = number_total_atoms
                input_df.at[index, 'computed charge'] =  computed_charge
                input_df.at[index, 'computed mol_weight'] =  computed_weight

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

    return mol_image

