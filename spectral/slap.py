"""
SLAP2 (Simple Line Access Protocol v2.0) VOTable generation module.

This module provides functionality to convert species data from getSpeciesWithRestrictions
into SLAP2-compliant VOTable XML files, grouped by data nodes (service providers).

The module follows the SLAP2 specification for the {species} resource, creating VOTable
documents with proper metadata, field definitions, and species data organized by node.

Reference: SLAP2 specification - Simple Line Access Protocol v2.0
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import tempfile

import pandas as pd
import numpy as np
from lxml import etree

from pyVAMDC.spectral.species import getSpeciesWithRestrictions

LOGGER = logging.getLogger(__name__)


# SLAP2 VOTable XML Namespaces
VOTABLE_NS = "http://www.ivoa.net/xml/VOTable/v1.3"
XSAMS_NS = "http://vamdc.org/xml/xsams/1.0"
SLAP_NS = "http://www.ivoa.net/xml/SLAPExtension/v1.0"

# Map of SLAP2 {species} fields with their metadata
SPECIES_FIELDS_METADATA = {
    "shortname": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "meta.id;meta.main",
        "unit": None,
        "description": "Short name of the data node",
        "required": True,
    },
    "ivoIdentifier": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "meta.id",
        "unit": None,
        "description": "IVO identifier of the data node",
        "required": True,
    },
    "InChIKey": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "phys.atmol.element",
        "unit": None,
        "description": "IUPAC International Chemical Identifier Key",
        "required": True,
    },
    "InChI": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "phys.atmol.element",
        "unit": None,
        "description": "IUPAC International Chemical Identifier",
        "required": False,
    },
    "stoichiometricFormula": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "phys.atmol.element",
        "unit": None,
        "description": "Stoichiometric formula of the species",
        "required": True,
    },
    "massNumber": {
        "datatype": "float",
        "arraysize": None,
        "ucd": "phys.mass",
        "unit": "u",
        "description": "Mass number in Unified Atomic Mass Units",
        "required": False,
    },
    "charge": {
        "datatype": "int",
        "arraysize": None,
        "ucd": "phys.atmol.ionization",
        "unit": None,
        "description": "Electric charge of the species",
        "required": False,
    },
    "speciesType": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "meta.code.class",
        "unit": None,
        "description": "Type of species: molecule, atom, or particle",
        "required": False,
    },
    "structuralFormula": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "phys.atmol.element",
        "unit": None,
        "description": "Structural formula of the species",
        "required": False,
    },
    "name": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "meta.title",
        "unit": None,
        "description": "Human-readable name of the species",
        "required": False,
    },
    "did": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "meta.id.alternative",
        "unit": None,
        "description": "Alternative unique identifier for the species",
        "required": False,
    },
    "tapEndpoint": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "meta.ref.url",
        "unit": None,
        "description": "TAP endpoint URL of the data node",
        "required": False,
    },
    "lastIngestionScriptDate": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "meta.version.date",
        "unit": None,
        "description": "Last time the species database ingestion script ran",
        "required": False,
    },
    "speciesLastSeenOn": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "meta.version.date",
        "unit": None,
        "description": "Last time the species was seen in the database",
        "required": False,
    },
    "# unique atoms": {
        "datatype": "int",
        "arraysize": None,
        "ucd": "meta.number",
        "unit": None,
        "description": "Number of unique atoms in the species",
        "required": False,
    },
    "# total atoms": {
        "datatype": "int",
        "arraysize": None,
        "ucd": "meta.number",
        "unit": None,
        "description": "Total number of atoms in the species",
        "required": False,
    },
    "computed charge": {
        "datatype": "int",
        "arraysize": None,
        "ucd": "phys.atmol.ionization",
        "unit": None,
        "description": "Computed electric charge of the species",
        "required": False,
    },
    "computed mol_weight": {
        "datatype": "float",
        "arraysize": None,
        "ucd": "phys.mass",
        "unit": "u",
        "description": "Computed molecular weight of the species",
        "required": False,
    },
}


class SLAP2VOTableGenerator:
    """
    Generates SLAP2-compliant VOTable XML documents from species data grouped by nodes.

    This class handles the creation of VOTable files following the SLAP2 specification
    for the {species} resource, including proper XML structure, metadata, field definitions,
    and data rows.
    """

    def __init__(self, output_directory: Optional[str] = None):
        """
        Initialize the SLAP2VOTableGenerator.

        Args:
            output_directory (str, optional): Directory where VOTable files will be saved.
                If None, a temporary directory is used. Defaults to None.
        """
        if output_directory is None:
            self.output_directory = tempfile.mkdtemp(prefix="slap2_votable_")
        else:
            self.output_directory = str(output_directory)
            Path(self.output_directory).mkdir(parents=True, exist_ok=True)

    def generate_votables_for_nodes(
        self,
        name: Optional[str] = None,
        inchi: Optional[str] = None,
        inchikey: Optional[str] = None,
        ivo_identifier: Optional[str] = None,
        stoichiometric_formula: Optional[str] = None,
        mass_min: Optional[float] = None,
        mass_max: Optional[float] = None,
        charge_min: Optional[int] = None,
        charge_max: Optional[int] = None,
        type: Optional[str] = None,
        number_unique_atoms_min: Optional[int] = None,
        number_unique_atoms_max: Optional[int] = None,
        number_total_atoms_min: Optional[int] = None,
        number_total_atoms_max: Optional[int] = None,
        computed_charge_min: Optional[int] = None,
        computed_charge_max: Optional[int] = None,
        computed_weight_min: Optional[float] = None,
        computed_weight_max: Optional[float] = None,
        tap_endpoint: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate SLAP2-compliant VOTable files grouped by data nodes.

        This method retrieves species data using getSpeciesWithRestrictions with the provided
        filter parameters, then groups the results by node (data provider) and creates
        a separate VOTable file for each node, following the SLAP2 specification
        which requires that species VO-tables contain only data for a given service.

        Args:
            name (str, optional): Filter species where stoichiometricFormula, structuralFormula, 
                or name contains this string. Default None.
            inchi (str, optional): Filter by exact InChI match. Default None.
            inchikey (str, optional): Filter by exact InChIKey match. Default None.
            ivo_identifier (str, optional): Filter by IVO identifier. Default None.
            stoichiometric_formula (str, optional): Filter by exact stoichiometric formula match. 
                Default None.
            mass_min (float, optional): Minimum mass number. Default None.
            mass_max (float, optional): Maximum mass number. Default None.
            charge_min (int, optional): Minimum charge. Default None.
            charge_max (int, optional): Maximum charge. Default None.
            type (str, optional): Filter by species type ('molecule', 'atom', 'particle'). 
                Default None.
            number_unique_atoms_min (int, optional): Minimum number of unique atoms. Default None.
            number_unique_atoms_max (int, optional): Maximum number of unique atoms. Default None.
            number_total_atoms_min (int, optional): Minimum number of total atoms. Default None.
            number_total_atoms_max (int, optional): Maximum number of total atoms. Default None.
            computed_charge_min (int, optional): Minimum computed charge. Default None.
            computed_charge_max (int, optional): Maximum computed charge. Default None.
            computed_weight_min (float, optional): Minimum computed molecular weight. Default None.
            computed_weight_max (float, optional): Maximum computed molecular weight. Default None.
            tap_endpoint (str, optional): Filter by TAP endpoint of the node. Default None.

        Returns:
            List[Dict[str, Any]]: List of dictionaries, each containing:
                - 'query_params': Dictionary of filters used
                - 'node_ivoidentifier': IVO identifier of the node
                - 'node_shortname': Short name of the node
                - 'node_endpoint': TAP endpoint of the node (if available)
                - 'species_count': Number of species in this node's VOTable
                - 'votable_filepath': Absolute path to the generated VOTable XML file
                - 'generation_timestamp': When the VOTable was generated

        Raises:
            ValueError: If no species match the provided filters or if required columns are missing.
            IOError: If the VOTable file cannot be written.

        Example:
            >>> from pyVAMDC.spectral.slap import SLAP2VOTableGenerator
            >>>
            >>> # Generate VOTables for water molecules
            >>> generator = SLAP2VOTableGenerator(output_directory='/path/to/output')
            >>> results = generator.generate_votables_for_nodes(
            ...     stoichiometric_formula='H2O',
            ...     type='molecule'
            ... )
            >>>
            >>> # Inspect results
            >>> for result in results:
            ...     print(f"Node: {result['node_shortname']}")
            ...     print(f"Species count: {result['species_count']}")
            ...     print(f"VOTable: {result['votable_filepath']}")
        """
        # Retrieve species data using provided filters
        LOGGER.info("Fetching species from VAMDC database with provided filters...")
        species_df, node_info_df = getSpeciesWithRestrictions(
            name=name,
            inchi=inchi,
            inchikey=inchikey,
            ivo_identifier=ivo_identifier,
            stoichiometric_formula=stoichiometric_formula,
            mass_min=mass_min,
            mass_max=mass_max,
            charge_min=charge_min,
            charge_max=charge_max,
            type=type,
            number_unique_atoms_min=number_unique_atoms_min,
            number_unique_atoms_max=number_unique_atoms_max,
            number_total_atoms_min=number_total_atoms_min,
            number_total_atoms_max=number_total_atoms_max,
            computed_charge_min=computed_charge_min,
            computed_charge_max=computed_charge_max,
            computed_weight_min=computed_weight_min,
            computed_weight_max=computed_weight_max,
            tap_endpoint=tap_endpoint,
        )

        if species_df.empty:
            raise ValueError("No species found matching the provided filters")

        if "ivoIdentifier" not in species_df.columns:
            raise ValueError("Species dataframe must contain 'ivoIdentifier' column")

        # Create query_params dictionary for metadata
        query_params = {
            'name': name,
            'inchi': inchi,
            'inchikey': inchikey,
            'ivo_identifier': ivo_identifier,
            'stoichiometric_formula': stoichiometric_formula,
            'mass_min': mass_min,
            'mass_max': mass_max,
            'charge_min': charge_min,
            'charge_max': charge_max,
            'type': type,
            'number_unique_atoms_min': number_unique_atoms_min,
            'number_unique_atoms_max': number_unique_atoms_max,
            'number_total_atoms_min': number_total_atoms_min,
            'number_total_atoms_max': number_total_atoms_max,
            'computed_charge_min': computed_charge_min,
            'computed_charge_max': computed_charge_max,
            'computed_weight_min': computed_weight_min,
            'computed_weight_max': computed_weight_max,
            'tap_endpoint': tap_endpoint,
        }

        results = []
        nodes = species_df["ivoIdentifier"].unique()

        LOGGER.info(
            f"Generating VOTables for {len(nodes)} node(s) with "
            f"{len(species_df)} total species"
        )

        for node_ivo_id in nodes:
            try:
                # Filter species for this node
                node_species_df = species_df[
                    species_df["ivoIdentifier"] == node_ivo_id
                ].copy()

                # Get node metadata
                node_shortname = node_species_df["shortName"].iloc[0]
                node_endpoint = (
                    node_species_df["tapEndpoint"].iloc[0]
                    if "tapEndpoint" in node_species_df.columns
                    else None
                )

                LOGGER.debug(
                    f"Processing node {node_shortname} ({node_ivo_id}) "
                    f"with {len(node_species_df)} species"
                )

                # Generate VOTable for this node
                votable_filepath = self._create_votable_for_node(
                    node_species_df, node_ivo_id, query_params
                )

                # Collect result information
                result = {
                    "query_params": {k: v for k, v in query_params.items() if v is not None},
                    "node_ivoidentifier": node_ivo_id,
                    "node_shortname": node_shortname,
                    "node_endpoint": node_endpoint,
                    "species_count": len(node_species_df),
                    "votable_filepath": votable_filepath,
                    "generation_timestamp": datetime.now().isoformat(),
                }

                results.append(result)
                LOGGER.info(
                    f"Successfully generated VOTable for {node_shortname}: "
                    f"{votable_filepath}"
                )

            except Exception as e:
                LOGGER.error(
                    f"Error processing node {node_ivo_id}: {str(e)}", exc_info=True
                )
                raise

        return results

    def _create_votable_for_node(
        self, node_species_df: pd.DataFrame, node_ivo_id: str, query_params: Dict[str, Any]
    ) -> str:
        """
        Create a SLAP2-compliant VOTable XML file for a single node.

        Generates an XML VOTable following the SLAP2 specification with proper structure,
        metadata (RESOURCE element with INFO tags), field definitions, and data rows.

        Args:
            node_species_df (pd.DataFrame): Species dataframe for a single node.
            node_ivo_id (str): IVO identifier of the node.
            query_params (Dict[str, Any]): Query parameters used for filtering.

        Returns:
            str: Absolute path to the generated VOTable XML file.

        Raises:
            IOError: If the file cannot be written.
        """
        # Create VOTable root element
        votable = etree.Element(
            "VOTABLE",
            version="1.3",
            xmlns=VOTABLE_NS,
            attrib={"{http://www.w3.org/2001/XMLSchema-instance}schemaLocation": VOTABLE_NS},
        )

        # Create RESOURCE element for results
        resource = etree.SubElement(votable, "RESOURCE", name="species", type="results")

        # Add RESOURCE metadata
        self._add_resource_metadata(resource, node_species_df, node_ivo_id, query_params)

        # Create TABLE element
        table = etree.SubElement(resource, "TABLE")

        # Add FIELD elements (column definitions)
        self._add_field_elements(table, node_species_df)

        # Add DATA element with TABLEDATA
        self._add_data_tabledata(table, node_species_df)

        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        node_shortname = node_species_df["shortName"].iloc[0]
        safe_nodename = node_shortname.replace(" ", "_").replace("/", "_")
        filename = f"slap2_species_{safe_nodename}_{timestamp}.xml"
        filepath = str(Path(self.output_directory) / filename)

        # Write VOTable to file
        try:
            tree = etree.ElementTree(votable)
            tree.write(filepath, encoding="UTF-8", xml_declaration=True, pretty_print=True)
            LOGGER.debug(f"VOTable written to {filepath}")
        except IOError as e:
            LOGGER.error(f"Failed to write VOTable to {filepath}: {str(e)}")
            raise

        return filepath

    def _add_resource_metadata(
        self,
        resource: etree._Element,
        node_species_df: pd.DataFrame,
        node_ivo_id: str,
        query_params: Dict[str, Any],
    ) -> None:
        """
        Add SLAP2-compliant metadata INFO elements to the RESOURCE element.

        According to SLAP2 specification, the RESOURCE element MUST contain:
        - QUERY_STATUS (OK or OVERFLOW)
        - Recommended: request_date, service_protocol, publisher, last_update_date

        Args:
            resource (etree._Element): RESOURCE element to add metadata to.
            node_species_df (pd.DataFrame): Species dataframe for this node.
            node_ivo_id (str): IVO identifier of the node.
            query_params (Dict[str, Any]): Query parameters used.
        """
        # MUST: QUERY_STATUS
        query_status = etree.SubElement(resource, "INFO")
        query_status.set("name", "QUERY_STATUS")
        query_status.set("value", "OK")

        # SHOULD: request_date
        request_date = etree.SubElement(resource, "INFO")
        request_date.set("name", "request_date")
        request_date.set("value", datetime.now().isoformat())

        # SHOULD: service_protocol
        service_protocol = etree.SubElement(resource, "INFO")
        service_protocol.set("name", "service_protocol")
        service_protocol.set("value", "SLAP 2.0")

        # SHOULD: publisher
        publisher = etree.SubElement(resource, "INFO")
        publisher.set("name", "publisher")
        node_shortname = node_species_df["shortName"].iloc[0]
        publisher.set("value", f"VAMDC Data Node: {node_shortname}")

        # SHOULD: last_update_date
        if "speciesLastSeenOn" in node_species_df.columns:
            last_update = node_species_df["speciesLastSeenOn"].iloc[0]
        else:
            last_update = datetime.now().isoformat()

        last_update_date = etree.SubElement(resource, "INFO")
        last_update_date.set("name", "last_update_date")
        last_update_date.set("value", str(last_update))

        # Add query parameters as INFO elements
        for param_name, param_value in query_params.items():
            if param_value is not None:
                query_param = etree.SubElement(resource, "INFO")
                query_param.set("name", f"query_param_{param_name}")
                query_param.set("value", str(param_value))

    def _add_field_elements(self, table: etree._Element, node_species_df: pd.DataFrame) -> None:
        """
        Add FIELD elements (column definitions) to the TABLE element.

        Creates a FIELD element for each column in the species dataframe,
        using metadata from SPECIES_FIELDS_METADATA when available.

        Args:
            table (etree._Element): TABLE element to add FIELD elements to.
            node_species_df (pd.DataFrame): Species dataframe.
        """
        for col in node_species_df.columns:
            if col not in SPECIES_FIELDS_METADATA:
                LOGGER.warning(
                    f"Column '{col}' not in SPECIES_FIELDS_METADATA, using default mapping"
                )
                # Create default field metadata
                field_meta = {
                    "datatype": "char",
                    "arraysize": "*",
                    "ucd": "meta.main",
                    "unit": None,
                }
            else:
                field_meta = SPECIES_FIELDS_METADATA[col]

            field = etree.SubElement(table, "FIELD")
            field.set("name", col)
            field.set("datatype", field_meta["datatype"])

            if field_meta["arraysize"]:
                field.set("arraysize", field_meta["arraysize"])

            if field_meta["ucd"]:
                field.set("ucd", field_meta["ucd"])

            if field_meta["unit"]:
                field.set("unit", field_meta["unit"])

            # Add DESCRIPTION element
            description = etree.SubElement(field, "DESCRIPTION")
            description.text = field_meta.get("description", f"Column: {col}")

    def _add_data_tabledata(
        self, table: etree._Element, node_species_df: pd.DataFrame
    ) -> None:
        """
        Add DATA element with TABLEDATA rows to the TABLE element.

        Creates a TR (table row) and TD (table data) elements for each row
        in the species dataframe.

        Args:
            table (etree._Element): TABLE element to add DATA to.
            node_species_df (pd.DataFrame): Species dataframe.
        """
        data = etree.SubElement(table, "DATA")
        tabledata = etree.SubElement(data, "TABLEDATA")

        for _, row in node_species_df.iterrows():
            tr = etree.SubElement(tabledata, "TR")

            for col in node_species_df.columns:
                td = etree.SubElement(tr, "TD")
                value = row[col]

                # Handle NaN and None values
                if pd.isna(value):
                    td.text = ""
                else:
                    td.text = str(value)

    def get_output_directory(self) -> str:
        """
        Get the output directory path.

        Returns:
            str: Absolute path to the output directory.
        """
        return self.output_directory


def create_slap2_votables_from_species(
    name: Optional[str] = None,
    inchi: Optional[str] = None,
    inchikey: Optional[str] = None,
    ivo_identifier: Optional[str] = None,
    stoichiometric_formula: Optional[str] = None,
    mass_min: Optional[float] = None,
    mass_max: Optional[float] = None,
    charge_min: Optional[int] = None,
    charge_max: Optional[int] = None,
    type: Optional[str] = None,
    number_unique_atoms_min: Optional[int] = None,
    number_unique_atoms_max: Optional[int] = None,
    number_total_atoms_min: Optional[int] = None,
    number_total_atoms_max: Optional[int] = None,
    computed_charge_min: Optional[int] = None,
    computed_charge_max: Optional[int] = None,
    computed_weight_min: Optional[float] = None,
    computed_weight_max: Optional[float] = None,
    tap_endpoint: Optional[str] = None,
    output_directory: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Convenience function to generate SLAP2-compliant VOTable files from species filters.

    This is the main entry point for generating SLAP2-compliant VOTable XML files grouped 
    by data nodes. The function retrieves species data using the provided filter parameters 
    and creates a separate VOTable for each node.

    Args:
        name (str, optional): Filter species where stoichiometricFormula, structuralFormula, 
            or name contains this string. Default None.
        inchi (str, optional): Filter by exact InChI match. Default None.
        inchikey (str, optional): Filter by exact InChIKey match. Default None.
        ivo_identifier (str, optional): Filter by IVO identifier. Default None.
        stoichiometric_formula (str, optional): Filter by exact stoichiometric formula match. 
            Default None.
        mass_min (float, optional): Minimum mass number. Default None.
        mass_max (float, optional): Maximum mass number. Default None.
        charge_min (int, optional): Minimum charge. Default None.
        charge_max (int, optional): Maximum charge. Default None.
        type (str, optional): Filter by species type ('molecule', 'atom', 'particle'). 
            Default None.
        number_unique_atoms_min (int, optional): Minimum number of unique atoms. Default None.
        number_unique_atoms_max (int, optional): Maximum number of unique atoms. Default None.
        number_total_atoms_min (int, optional): Minimum number of total atoms. Default None.
        number_total_atoms_max (int, optional): Maximum number of total atoms. Default None.
        computed_charge_min (int, optional): Minimum computed charge. Default None.
        computed_charge_max (int, optional): Maximum computed charge. Default None.
        computed_weight_min (float, optional): Minimum computed molecular weight. Default None.
        computed_weight_max (float, optional): Maximum computed molecular weight. Default None.
        tap_endpoint (str, optional): Filter by TAP endpoint of the node. Default None.
        output_directory (str, optional): Directory where VOTable files will be saved.
            If None, a temporary directory is used. Defaults to None.

    Returns:
        List[Dict[str, Any]]: List of dictionaries, each containing:
            - 'query_params': Dictionary of filters used (non-None only)
            - 'node_ivoidentifier': IVO identifier of the node
            - 'node_shortname': Short name of the node
            - 'node_endpoint': TAP endpoint of the node (if available)
            - 'species_count': Number of species in this node's VOTable
            - 'votable_filepath': Absolute path to the generated VOTable XML file
            - 'generation_timestamp': When the VOTable was generated

    Raises:
        ValueError: If no species match the provided filters.
        IOError: If VOTable files cannot be written.

    Example:
        >>> from pyVAMDC.spectral.slap import create_slap2_votables_from_species
        >>>
        >>> # Generate SLAP2 VOTables for water molecules
        >>> results = create_slap2_votables_from_species(
        ...     stoichiometric_formula='H2O',
        ...     type='molecule',
        ...     output_directory='/path/to/output'
        ... )
        >>>
        >>> # Use results
        >>> for result in results:
        ...     print(f"Node: {result['node_shortname']}")
        ...     print(f"  Species: {result['species_count']}")
        ...     print(f"  VOTable: {result['votable_filepath']}")
    """
    generator = SLAP2VOTableGenerator(output_directory=output_directory)
    return generator.generate_votables_for_nodes(
        name=name,
        inchi=inchi,
        inchikey=inchikey,
        ivo_identifier=ivo_identifier,
        stoichiometric_formula=stoichiometric_formula,
        mass_min=mass_min,
        mass_max=mass_max,
        charge_min=charge_min,
        charge_max=charge_max,
        type=type,
        number_unique_atoms_min=number_unique_atoms_min,
        number_unique_atoms_max=number_unique_atoms_max,
        number_total_atoms_min=number_total_atoms_min,
        number_total_atoms_max=number_total_atoms_max,
        computed_charge_min=computed_charge_min,
        computed_charge_max=computed_charge_max,
        computed_weight_min=computed_weight_min,
        computed_weight_max=computed_weight_max,
        tap_endpoint=tap_endpoint,
    )
