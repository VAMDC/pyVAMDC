"""
SLAP2 (Simple Line Access Protocol v2.0) VOTable generation module.

This module provides functionality to convert species and spectroscopic lines data 
into SLAP2-compliant VOTable XML files, grouped by data nodes (service providers).

The module follows the SLAP2 specification for both {species} and {lines} resources,
creating VOTable documents with proper metadata, field definitions, and data organized by node.

Usage:
    For species data:
        - Use create_slap2_votables_from_species() with filter parameters
    
    For spectroscopic lines:
        - Use getLinesAsDataFrames() to get DataFrames, then create_slap2_votables_from_lines()
        - Or use getLines() to get parquet paths, then create_slap2_votables_from_parquet_paths()

Reference: SLAP2 specification - Simple Line Access Protocol v2.0
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import tempfile
import re

import pandas as pd
import numpy as np
from lxml import etree

from pyVAMDC.spectral.species import getSpeciesWithRestrictions
from pyVAMDC.spectral.energyConverter import electromagnetic_conversion

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


# Map of SLAP2 {lines} fields with their metadata
SLAP2_LINES_FIELDS_METADATA = {
    "vacuum_wavelength": {
        "datatype": "double",
        "arraysize": None,
        "ucd": "em.wl",
        "unit": "m",
        "description": "Wavelength in vacuum of the transition originating the line in meters",
        "required": True,
    },
    "line_title": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "meta.title",
        "unit": None,
        "description": "Short description identifying the line (e.g., 'H I', 'N III 992.873 A')",
        "required": True,
    },
    "chemical_element_name": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "phys.atmol.element",
        "unit": None,
        "description": "Chemical element name or molecular formula",
        "required": False,
    },
    "chemical_element_mass": {
        "datatype": "float",
        "arraysize": None,
        "ucd": "phys.atmol.element;phys.mass",
        "unit": "u",
        "description": "Atomic mass or molecular weight in Unified Atomic Mass Units",
        "required": False,
    },
    "inchikey": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "phys.atmol.element",
        "unit": None,
        "description": "IUPAC International Chemical Identifier Key (27-character hash)",
        "required": False,
    },
    "inchi": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "phys.atmol.element",
        "unit": None,
        "description": "IUPAC International Chemical Identifier",
        "required": False,
    },
    "ion_charge": {
        "datatype": "int",
        "arraysize": None,
        "ucd": "phys.atmol.ionization",
        "unit": None,
        "description": "Electric charge of the species (positive, negative, or zero for neutral)",
        "required": False,
    },
    "lower_level_description": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "phys.atmol.level;meta.title",
        "unit": None,
        "description": "Full description of the lower energy level of the transition",
        "required": False,
    },
    "upper_level_description": {
        "datatype": "char",
        "arraysize": "*",
        "ucd": "phys.atmol.level;meta.title",
        "unit": None,
        "description": "Full description of the upper energy level of the transition",
        "required": False,
    },
    "lower_level_energy": {
        "datatype": "double",
        "arraysize": None,
        "ucd": "phys.energy;phys.atmol.level",
        "unit": "J",
        "description": "Energy of the lower level of the transition in Joules",
        "required": False,
    },
    "upper_level_energy": {
        "datatype": "double",
        "arraysize": None,
        "ucd": "phys.energy;phys.atmol.level",
        "unit": "J",
        "description": "Energy of the upper level of the transition in Joules",
        "required": False,
    },
    "einstein_a": {
        "datatype": "double",
        "arraysize": None,
        "ucd": "phys.atmol.transProb",
        "unit": "1/s",
        "description": "Einstein A coefficient: probability per unit time for spontaneous emission",
        "required": False,
    },
    "observed_wavelength": {
        "datatype": "double",
        "arraysize": None,
        "ucd": "em.wl",
        "unit": "m",
        "description": "Observed wavelength in vacuum of the transition in meters",
        "required": False,
    },
}


class SLAP2LinesVOTableGenerator:
    """
    Generates SLAP2-compliant VOTable XML documents from spectroscopic lines data grouped by nodes.

    This class handles the creation of VOTable files following the SLAP2 specification
    for the {lines} resource, converting dataframes from VAMDC queries into properly
    structured VOTable documents with SLAP2-compliant field definitions and metadata.

    The input is expected to come from the getLines() function in the 'lines' module,
    which returns atomic_results_dict, molecular_results_dict, and queries_metadata_list.
    """

    def __init__(self, output_directory: Optional[str] = None):
        """
        Initialize the SLAP2LinesVOTableGenerator.

        Args:
            output_directory (str, optional): Directory where VOTable files will be saved.
                If None, a temporary directory is used. Defaults to None.
        """
        if output_directory is None:
            self.output_directory = tempfile.mkdtemp(prefix="slap2_lines_votable_")
        else:
            self.output_directory = str(output_directory)
            Path(self.output_directory).mkdir(parents=True, exist_ok=True)

    def _ensure_wavelength_in_meters(self, lines_df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure the dataframe has a 'vacuum_wavelength' column in meters for SLAP2 compliance.

        This method checks for wavelength, energy, or frequency columns in the dataframe
        and converts them to wavelength in meters if necessary. It handles column names
        with units in the format "Column_name (unit)".

        SLAP2 specification requires the vacuum wavelength in meters to be present.

        Args:
            lines_df (pd.DataFrame): Input lines dataframe from VAMDC.

        Returns:
            pd.DataFrame: DataFrame with 'vacuum_wavelength' column in meters.

        Raises:
            ValueError: If no wavelength, energy, or frequency data is found.

        Logic:
            1. Check if 'vacuum_wavelength' exists (in meters) - use as-is
            2. Check for wavelength columns with various units
            3. Check for energy columns with various units
            4. Check for frequency columns with various units
            5. Use electromagnetic_conversion to convert to meters
        """
        df = lines_df.copy()

        # Pattern to extract unit from column name like "ColumnName (unit)"
        unit_pattern = r'\((.*?)\)$'

        # Check if vacuum_wavelength already exists in meters
        if 'vacuum_wavelength' in df.columns:
            LOGGER.debug("vacuum_wavelength column already present")
            return df

        # Helper function to normalize unit names for energyConverter
        def normalize_unit(unit_str: str) -> str:
            """Normalize unit name to match energyConverter expectations."""
            if unit_str is None:
                return None
            # Special cases that need to maintain case
            if unit_str.upper() == 'EV':
                return 'eV'
            # All others lowercase
            return unit_str.lower()

        # Helper function to parse column name and extract unit
        def parse_column_with_unit(col_name: str) -> Tuple[str, Optional[str]]:
            """Parse column name to extract base name and unit."""
            match = re.search(unit_pattern, col_name)
            if match:
                unit = match.group(1).strip()
                base_name = col_name[:match.start()].strip()
                return base_name, unit
            return col_name, None

        # Search for wavelength columns
        wavelength_columns = {}
        energy_columns = {}
        frequency_columns = {}

        for col in df.columns:
            base_name, unit = parse_column_with_unit(col)
            base_lower = base_name.lower()

            # Wavelength variants
            if any(wl in base_lower for wl in ['wavelength', 'wave', 'wl']):
                wavelength_columns[col] = unit
            # Energy variants
            elif any(en in base_lower for en in ['energy', 'wavenumber']):
                energy_columns[col] = unit
            # Frequency variants
            elif any(fr in base_lower for fr in ['frequency', 'freq']):
                frequency_columns[col] = unit

        # Conversion logic - preference order: wavelength > frequency > energy
        try:
            if wavelength_columns:
                # Use the first wavelength column found
                wl_col = list(wavelength_columns.keys())[0]
                wl_unit = wavelength_columns[wl_col]

                if wl_unit is None:
                    # Assume Angstrom if no unit specified
                    LOGGER.warning(
                        f"Wavelength column '{wl_col}' has no unit specified, assuming Angstrom"
                    )
                    wl_unit = 'angstrom'
                else:
                    # Normalize unit name for energyConverter
                    wl_unit = normalize_unit(wl_unit)

                LOGGER.info(f"Converting wavelength from {wl_unit} to meter using column '{wl_col}'")
                df['vacuum_wavelength'] = df[wl_col].apply(
                    lambda x: electromagnetic_conversion(x, wl_unit, 'meter') if pd.notna(x) else np.nan
                )

            elif frequency_columns:
                # Use the first frequency column found
                freq_col = list(frequency_columns.keys())[0]
                freq_unit = frequency_columns[freq_col]

                if freq_unit is None:
                    # Assume Hertz if no unit specified
                    LOGGER.warning(
                        f"Frequency column '{freq_col}' has no unit specified, assuming hertz"
                    )
                    freq_unit = 'hertz'
                else:
                    # Normalize unit name for energyConverter
                    freq_unit = normalize_unit(freq_unit)

                LOGGER.info(f"Converting frequency from {freq_unit} to wavelength in meters using column '{freq_col}'")
                df['vacuum_wavelength'] = df[freq_col].apply(
                    lambda x: electromagnetic_conversion(x, freq_unit, 'meter') if pd.notna(x) else np.nan
                )

            elif energy_columns:
                # Use the first energy column found
                energy_col = list(energy_columns.keys())[0]
                energy_unit = energy_columns[energy_col]

                if energy_unit is None:
                    # Assume eV if no unit specified
                    LOGGER.warning(
                        f"Energy column '{energy_col}' has no unit specified, assuming eV"
                    )
                    energy_unit = 'eV'
                else:
                    # Normalize unit name for energyConverter
                    energy_unit = normalize_unit(energy_unit)

                LOGGER.info(f"Converting energy from {energy_unit} to wavelength in meters using column '{energy_col}'")
                df['vacuum_wavelength'] = df[energy_col].apply(
                    lambda x: electromagnetic_conversion(x, energy_unit, 'meter') if pd.notna(x) else np.nan
                )

            else:
                raise ValueError(
                    "No wavelength, energy, or frequency columns found in the dataframe. "
                    "Cannot ensure SLAP2 compliance (vacuum_wavelength in meters required)."
                )

            LOGGER.info("Successfully ensured vacuum_wavelength column in meters")
            return df

        except Exception as e:
            LOGGER.error(f"Failed to convert wavelength data: {str(e)}")
            raise

    def generate_votables_for_lines(
        self,
        atomic_results_dict: Dict[str, pd.DataFrame],
        molecular_results_dict: Dict[str, pd.DataFrame],
        queries_metadata_list: List[Dict[str, Any]],
        lambdaMin: Optional[float] = None,
        lambdaMax: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate SLAP2-compliant VOTable files for spectroscopic lines grouped by nodes.

        This method takes the output from getLinesAsDataFrames() and creates a separate VOTable
        file for each database (VAMDC node), following the SLAP2 specification which
        requires that line VOTables contain only data for a given service.
        
        Note: If you have the output from getLines() (which returns parquet file paths),
        use getLinesAsDataFrames() to convert to DataFrames first, or let the
        convenience function create_slap2_votables_from_lines() handle it automatically.

        Args:
            atomic_results_dict (Dict[str, pd.DataFrame]): Dictionary with node endpoints as keys
                and DataFrames of atomic lines as values. From getLinesAsDataFrames().
            molecular_results_dict (Dict[str, pd.DataFrame]): Dictionary with node endpoints as keys
                and DataFrames of molecular lines as values. From getLinesAsDataFrames().
            queries_metadata_list (List[Dict[str, Any]]): List of query metadata dictionaries
                containing information about each query execution. From getLinesAsDataFrames().
            lambdaMin (float, optional): Minimum wavelength boundary in Angstroms for metadata.
                Defaults to None.
            lambdaMax (float, optional): Maximum wavelength boundary in Angstroms for metadata.
                Defaults to None.

        Returns:
            List[Dict[str, Any]]: List of dictionaries, each containing:
                - 'species_type': Type of species ('atomic' or 'molecular')
                - 'node_endpoint': Node TAP endpoint identifier
                - 'lambdaMin': Minimum wavelength boundary
                - 'lambdaMax': Maximum wavelength boundary
                - 'lines_count': Number of lines in this VOTable
                - 'votable_filepath': Absolute path to the generated VOTable XML file
                - 'generation_timestamp': ISO format timestamp of when VOTable was generated

        Raises:
            ValueError: If both result dictionaries are empty.
            IOError: If VOTable files cannot be written.

        Example:
            >>> from pyVAMDC.spectral.lines import getLinesAsDataFrames
            >>> from pyVAMDC.spectral.slap import SLAP2LinesVOTableGenerator
            >>>
            >>> # Get lines from VAMDC as DataFrames
            >>> atomic_dict, molecular_dict, metadata = getLinesAsDataFrames(
            ...     lambdaMin=1000.0,  # 1000 Angstroms
            ...     lambdaMax=2000.0
            ... )
            >>>
            >>> # Generate VOTables
            >>> generator = SLAP2LinesVOTableGenerator(output_directory='/path/to/output')
            >>> results = generator.generate_votables_for_lines(
            ...     atomic_dict, molecular_dict, metadata,
            ...     lambdaMin=1000.0,
            ...     lambdaMax=2000.0
            ... )
            >>>
            >>> for result in results:
            ...     print(f"Species: {result['species_type']}")
            ...     print(f"Lines: {result['lines_count']}")
            ...     print(f"VOTable: {result['votable_filepath']}")
        """
        if not atomic_results_dict and not molecular_results_dict:
            raise ValueError("Both atomic and molecular results dictionaries are empty")

        results = []

        # Process atomic lines
        for node_endpoint, lines_df in atomic_results_dict.items():
            try:
                LOGGER.info(
                    f"Generating atomic lines VOTable for node {node_endpoint} "
                    f"with {len(lines_df)} lines"
                )

                # Ensure wavelength in meters for SLAP2 compliance
                lines_df = self._ensure_wavelength_in_meters(lines_df)

                votable_filepath = self._create_votable_for_lines(
                    lines_df, node_endpoint, "atomic", lambdaMin, lambdaMax
                )

                result = {
                    "species_type": "atomic",
                    "node_endpoint": node_endpoint,
                    "lambdaMin": lambdaMin,
                    "lambdaMax": lambdaMax,
                    "lines_count": len(lines_df),
                    "votable_filepath": votable_filepath,
                    "generation_timestamp": datetime.now().isoformat(),
                }

                results.append(result)
                LOGGER.info(
                    f"Successfully generated atomic lines VOTable for {node_endpoint}: "
                    f"{votable_filepath}"
                )

            except Exception as e:
                LOGGER.error(
                    f"Error processing atomic lines for node {node_endpoint}: {str(e)}",
                    exc_info=True,
                )
                raise

        # Process molecular lines
        for node_endpoint, lines_df in molecular_results_dict.items():
            try:
                LOGGER.info(
                    f"Generating molecular lines VOTable for node {node_endpoint} "
                    f"with {len(lines_df)} lines"
                )

                # Ensure wavelength in meters for SLAP2 compliance
                lines_df = self._ensure_wavelength_in_meters(lines_df)

                votable_filepath = self._create_votable_for_lines(
                    lines_df, node_endpoint, "molecular", lambdaMin, lambdaMax
                )

                result = {
                    "species_type": "molecular",
                    "node_endpoint": node_endpoint,
                    "lambdaMin": lambdaMin,
                    "lambdaMax": lambdaMax,
                    "lines_count": len(lines_df),
                    "votable_filepath": votable_filepath,
                    "generation_timestamp": datetime.now().isoformat(),
                }

                results.append(result)
                LOGGER.info(
                    f"Successfully generated molecular lines VOTable for {node_endpoint}: "
                    f"{votable_filepath}"
                )

            except Exception as e:
                LOGGER.error(
                    f"Error processing molecular lines for node {node_endpoint}: {str(e)}",
                    exc_info=True,
                )
                raise

        return results

    def _create_votable_for_lines(
        self,
        lines_df: pd.DataFrame,
        node_endpoint: str,
        species_type: str,
        lambdaMin: Optional[float] = None,
        lambdaMax: Optional[float] = None,
    ) -> str:
        """
        Create a SLAP2-compliant VOTable XML file for spectroscopic lines from a single node.

        Generates an XML VOTable following the SLAP2 specification with proper structure,
        metadata (RESOURCE element with INFO tags), field definitions, and data rows
        populated from the lines dataframe.

        Args:
            lines_df (pd.DataFrame): Dataframe containing spectroscopic lines from VAMDC query.
            node_endpoint (str): TAP endpoint identifier of the VAMDC node.
            species_type (str): Type of species ('atomic' or 'molecular').
            lambdaMin (float, optional): Minimum wavelength boundary in Angstroms.
            lambdaMax (float, optional): Maximum wavelength boundary in Angstroms.

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
        resource = etree.SubElement(votable, "RESOURCE", name="lines", type="results")

        # Add RESOURCE metadata
        self._add_resource_metadata_for_lines(
            resource, node_endpoint, species_type, lambdaMin, lambdaMax, len(lines_df)
        )

        # Create TABLE element
        table = etree.SubElement(resource, "TABLE")

        # Add FIELD elements (column definitions)
        self._add_field_elements_for_lines(table, lines_df)

        # Add DATA element with TABLEDATA
        self._add_data_tabledata_for_lines(table, lines_df)

        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_endpoint = node_endpoint.replace("://", "_").replace("/", "_").replace(".", "_")
        filename = f"slap2_lines_{species_type}_{safe_endpoint}_{timestamp}.xml"
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

    def _add_resource_metadata_for_lines(
        self,
        resource: etree._Element,
        node_endpoint: str,
        species_type: str,
        lambdaMin: Optional[float],
        lambdaMax: Optional[float],
        lines_count: int,
    ) -> None:
        """
        Add SLAP2-compliant metadata INFO elements to the RESOURCE element for lines.

        According to SLAP2 specification, the RESOURCE element MUST contain:
        - QUERY_STATUS (OK or OVERFLOW)
        - Recommended: request_date, service_protocol, publisher, last_update_date

        Args:
            resource (etree._Element): RESOURCE element to add metadata to.
            node_endpoint (str): TAP endpoint of the VAMDC node.
            species_type (str): Type of species ('atomic' or 'molecular').
            lambdaMin (float, optional): Minimum wavelength boundary.
            lambdaMax (float, optional): Maximum wavelength boundary.
            lines_count (int): Number of lines in the result.
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
        publisher.set("value", f"VAMDC Node: {node_endpoint}")

        # Add query parameters as INFO elements
        if lambdaMin is not None:
            info_lambda_min = etree.SubElement(resource, "INFO")
            info_lambda_min.set("name", "wavelength_min")
            info_lambda_min.set("value", str(lambdaMin))

        if lambdaMax is not None:
            info_lambda_max = etree.SubElement(resource, "INFO")
            info_lambda_max.set("name", "wavelength_max")
            info_lambda_max.set("value", str(lambdaMax))

        # Add species type
        info_species = etree.SubElement(resource, "INFO")
        info_species.set("name", "species_type")
        info_species.set("value", species_type)

        # Add lines count
        info_count = etree.SubElement(resource, "INFO")
        info_count.set("name", "lines_count")
        info_count.set("value", str(lines_count))

    def _add_field_elements_for_lines(self, table: etree._Element, lines_df: pd.DataFrame) -> None:
        """
        Add FIELD elements (column definitions) to the TABLE element for lines.

        Creates a FIELD element for each column in the lines dataframe,
        using metadata from SLAP2_LINES_FIELDS_METADATA when available.
        Handles mapping between VAMDC field names and SLAP2 standard field names.

        Args:
            table (etree._Element): TABLE element to add FIELD elements to.
            lines_df (pd.DataFrame): Lines dataframe containing the columns.
        """
        # Track which SLAP2 field names have already been added to avoid duplicates
        added_field_names = set()

        for col in lines_df.columns:
            # Map VAMDC column names to SLAP2 standard names if needed
            slap2_field_name = self._map_column_to_slap2_field(col)

            # Skip if this SLAP2 field has already been added (avoid duplicates)
            if slap2_field_name in added_field_names:
                LOGGER.debug(f"Skipping column '{col}' (mapped to '{slap2_field_name}') - already added")
                continue

            if slap2_field_name not in SLAP2_LINES_FIELDS_METADATA:
                LOGGER.warning(
                    f"Column '{col}' (mapped to '{slap2_field_name}') not in SLAP2_LINES_FIELDS_METADATA, "
                    f"using default mapping"
                )
                # Create default field metadata
                field_meta = {
                    "datatype": "char",
                    "arraysize": "*",
                    "ucd": "meta.main",
                    "unit": None,
                }
            else:
                field_meta = SLAP2_LINES_FIELDS_METADATA[slap2_field_name]

            field = etree.SubElement(table, "FIELD")
            field.set("name", slap2_field_name)
            field.set("datatype", field_meta["datatype"])

            if field_meta["arraysize"]:
                field.set("arraysize", field_meta["arraysize"])

            if field_meta["ucd"]:
                field.set("ucd", field_meta["ucd"])

            if field_meta["unit"]:
                field.set("unit", field_meta["unit"])

            # Add DESCRIPTION element
            description = etree.SubElement(field, "DESCRIPTION")
            description.text = field_meta.get("description", f"Column: {slap2_field_name}")

            # Mark this field as added
            added_field_names.add(slap2_field_name)

    def _add_data_tabledata_for_lines(
        self, table: etree._Element, lines_df: pd.DataFrame
    ) -> None:
        """
        Add DATA element with TABLEDATA rows to the TABLE element for lines.

        Creates a TR (table row) and TD (table data) elements for each row
        in the lines dataframe, with proper handling of VAMDC field names
        and conversion to SLAP2 standard fields. Data order matches FIELD elements.

        Args:
            table (etree._Element): TABLE element to add DATA to.
            lines_df (pd.DataFrame): Lines dataframe containing the data rows.
        """
        data = etree.SubElement(table, "DATA")
        tabledata = etree.SubElement(data, "TABLEDATA")

        # Get the list of FIELD elements to match data order and avoid duplicates
        field_elements = table.findall("FIELD")
        field_names = [f.get("name") for f in field_elements]

        # Build a mapping from SLAP2 field names back to source column names
        # This is needed because we mapped columns to SLAP2 names
        slap2_to_source_col = {}
        for col in lines_df.columns:
            slap2_field_name = self._map_column_to_slap2_field(col)
            if slap2_field_name not in slap2_to_source_col:
                slap2_to_source_col[slap2_field_name] = col

        for _, row in lines_df.iterrows():
            tr = etree.SubElement(tabledata, "TR")

            # Add data in the same order as FIELD elements
            for field_name in field_names:
                td = etree.SubElement(tr, "TD")

                # Get the source column name for this field
                if field_name in slap2_to_source_col:
                    col = slap2_to_source_col[field_name]
                    value = row[col]
                else:
                    # Field might not exist in source data (shouldn't happen)
                    value = None

                # Handle NaN and None values
                if pd.isna(value):
                    td.text = ""
                else:
                    td.text = str(value)

    @staticmethod
    def _map_column_to_slap2_field(column_name: str) -> str:
        """
        Map VAMDC/HTML table column names to SLAP2 standard field names.

        This mapping handles common variations in column names from different
        VAMDC nodes and their XSAMS to HTML conversions.

        Special case: 'vacuum_wavelength' is already the standard name and is not mapped.
        Original wavelength columns (with units) are kept as-is to allow both the original
        and the converted vacuum_wavelength to coexist.

        Args:
            column_name (str): Original column name from the VAMDC dataframe.

        Returns:
            str: Mapped SLAP2-standard field name.
        """
        # If the column is already the standard vacuum_wavelength, return it as-is
        if column_name == "vacuum_wavelength":
            return "vacuum_wavelength"

        # Mapping dictionary for common VAMDC field names to SLAP2 standard names
        mapping = {
            # Line title/identification
            "Title": "line_title",
            "Transition": "line_title",
            "Line": "line_title",
            # Chemical element/species
            "Species": "chemical_element_name",
            "Element": "chemical_element_name",
            "Atom": "chemical_element_name",
            "Molecule": "chemical_element_name",
            # Charge
            "Charge": "ion_charge",
            "Ion Charge": "ion_charge",
            # Energy levels
            "Lower Energy": "lower_level_energy",
            "Upper Energy": "upper_level_energy",
            "Lower Level": "lower_level_description",
            "Upper Level": "upper_level_description",
            # Einstein A
            "Einstein A": "einstein_a",
            "A": "einstein_a",
            # InChI related
            "InChIKey": "inchikey",
            "InChI": "inchi",
            # Mass
            "Mass": "chemical_element_mass",
        }

        # Return mapped name if found, otherwise return original
        return mapping.get(column_name, column_name)

    def get_output_directory(self) -> str:
        """
        Get the output directory path.

        Returns:
            str: Absolute path to the output directory.
        """
        return self.output_directory


def create_slap2_votables_from_lines(
    atomic_results_dict: Dict[str, pd.DataFrame],
    molecular_results_dict: Dict[str, pd.DataFrame],
    queries_metadata_list: List[Dict[str, Any]],
    lambdaMin: Optional[float] = None,
    lambdaMax: Optional[float] = None,
    output_directory: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Convenience function to generate SLAP2-compliant VOTable files from spectroscopic lines.

    This is the main entry point for generating SLAP2-compliant VOTable XML files for
    spectroscopic lines grouped by data nodes. The function is typically used with the
    output of getLinesAsDataFrames() from the 'lines' module.
    
    Note: If you have the output from getLines() (which returns parquet file paths),
    use getLinesAsDataFrames() instead, or pass the parquet paths to 
    create_slap2_votables_from_parquet_paths() which will load them automatically.

    Args:
        atomic_results_dict (Dict[str, pd.DataFrame]): Dictionary with node endpoints as keys
            and DataFrames of atomic lines as values. Output from getLinesAsDataFrames().
        molecular_results_dict (Dict[str, pd.DataFrame]): Dictionary with node endpoints as keys
            and DataFrames of molecular lines as values. Output from getLinesAsDataFrames().
        queries_metadata_list (List[Dict[str, Any]]): List of query metadata dictionaries
            containing information about each query execution. Output from getLinesAsDataFrames().
        lambdaMin (float, optional): Minimum wavelength boundary in Angstroms for metadata.
            Defaults to None.
        lambdaMax (float, optional): Maximum wavelength boundary in Angstroms for metadata.
            Defaults to None.
        output_directory (str, optional): Directory where VOTable files will be saved.
            If None, a temporary directory is used. Defaults to None.

    Returns:
        List[Dict[str, Any]]: List of dictionaries, each containing:
            - 'species_type': Type of species ('atomic' or 'molecular')
            - 'node_endpoint': Node TAP endpoint identifier
            - 'lambdaMin': Minimum wavelength boundary
            - 'lambdaMax': Maximum wavelength boundary
            - 'lines_count': Number of lines in this VOTable
            - 'votable_filepath': Absolute path to the generated VOTable XML file
            - 'generation_timestamp': ISO format timestamp

    Raises:
        ValueError: If both result dictionaries are empty.
        IOError: If VOTable files cannot be written.

    Example:
        >>> from pyVAMDC.spectral.lines import getLinesAsDataFrames
        >>> from pyVAMDC.spectral.slap import create_slap2_votables_from_lines
        >>>
        >>> # Get spectroscopic lines from VAMDC as DataFrames
        >>> atomic_dict, molecular_dict, metadata = getLinesAsDataFrames(
        ...     lambdaMin=1000.0,  # 1000 Angstroms
        ...     lambdaMax=2000.0
        ... )
        >>>
        >>> # Generate SLAP2 VOTables
        >>> results = create_slap2_votables_from_lines(
        ...     atomic_dict,
        ...     molecular_dict,
        ...     metadata,
        ...     lambdaMin=1000.0,
        ...     lambdaMax=2000.0,
        ...     output_directory='/path/to/output'
        ... )
        >>>
        >>> # Use results
        >>> for result in results:
        ...     print(f"Species type: {result['species_type']}")
        ...     print(f"Lines count: {result['lines_count']}")
        ...     print(f"VOTable path: {result['votable_filepath']}")
    """
    generator = SLAP2LinesVOTableGenerator(output_directory=output_directory)
    return generator.generate_votables_for_lines(
        atomic_results_dict, molecular_results_dict, queries_metadata_list, lambdaMin, lambdaMax
    )


def create_slap2_votables_from_parquet_paths(
    atomic_parquet_paths: Dict[str, str],
    molecular_parquet_paths: Dict[str, str],
    queries_metadata_list: List[Dict[str, Any]],
    lambdaMin: Optional[float] = None,
    lambdaMax: Optional[float] = None,
    output_directory: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Convenience function to generate SLAP2-compliant VOTable files from parquet file paths.

    This function accepts the direct output from getLines() (which returns parquet file paths)
    and automatically loads the parquet files as DataFrames before generating VOTable files.
    
    For better memory efficiency with very large datasets, consider loading and processing
    one node at a time instead of loading all data into memory at once.

    Args:
        atomic_parquet_paths (Dict[str, str]): Dictionary with node endpoints as keys
            and parquet file paths (strings) as values for atomic lines. Direct output from getLines().
        molecular_parquet_paths (Dict[str, str]): Dictionary with node endpoints as keys
            and parquet file paths (strings) as values for molecular lines. Direct output from getLines().
        queries_metadata_list (List[Dict[str, Any]]): List of query metadata dictionaries.
            Output from getLines().
        lambdaMin (float, optional): Minimum wavelength boundary in Angstroms for metadata.
            Defaults to None.
        lambdaMax (float, optional): Maximum wavelength boundary in Angstroms for metadata.
            Defaults to None.
        output_directory (str, optional): Directory where VOTable files will be saved.
            If None, a temporary directory is used. Defaults to None.

    Returns:
        List[Dict[str, Any]]: List of dictionaries, each containing:
            - 'species_type': Type of species ('atomic' or 'molecular')
            - 'node_endpoint': Node TAP endpoint identifier
            - 'lambdaMin': Minimum wavelength boundary
            - 'lambdaMax': Maximum wavelength boundary
            - 'lines_count': Number of lines in this VOTable
            - 'votable_filepath': Absolute path to the generated VOTable XML file
            - 'generation_timestamp': ISO format timestamp

    Raises:
        ValueError: If both parquet path dictionaries are empty.
        FileNotFoundError: If parquet files don't exist at specified paths.
        IOError: If parquet files cannot be read or VOTable files cannot be written.

    Example:
        >>> from pyVAMDC.spectral.lines import getLines
        >>> from pyVAMDC.spectral.slap import create_slap2_votables_from_parquet_paths
        >>>
        >>> # Get spectroscopic lines from VAMDC (returns parquet paths)
        >>> atomic_paths, molecular_paths, metadata = getLines(
        ...     lambdaMin=1000.0,  # 1000 Angstroms
        ...     lambdaMax=2000.0
        ... )
        >>>
        >>> # Generate SLAP2 VOTables directly from parquet paths
        >>> results = create_slap2_votables_from_parquet_paths(
        ...     atomic_paths,
        ...     molecular_paths,
        ...     metadata,
        ...     lambdaMin=1000.0,
        ...     lambdaMax=2000.0,
        ...     output_directory='/path/to/output'
        ... )
        >>>
        >>> # Use results
        >>> for result in results:
        ...     print(f"Species type: {result['species_type']}")
        ...     print(f"Lines count: {result['lines_count']}")
        ...     print(f"VOTable path: {result['votable_filepath']}")
    """
    LOGGER.info("Loading parquet files into DataFrames for VOTable generation")
    
    # Load atomic parquet files into DataFrames
    atomic_dfs = {}
    for node, path in atomic_parquet_paths.items():
        if path and Path(path).exists():
            LOGGER.debug(f"Loading atomic parquet for {node} from {path}")
            atomic_dfs[node] = pd.read_parquet(path)
        else:
            LOGGER.warning(f"Atomic parquet file not found for {node}: {path}")
    
    # Load molecular parquet files into DataFrames
    molecular_dfs = {}
    for node, path in molecular_parquet_paths.items():
        if path and Path(path).exists():
            LOGGER.debug(f"Loading molecular parquet for {node} from {path}")
            molecular_dfs[node] = pd.read_parquet(path)
        else:
            LOGGER.warning(f"Molecular parquet file not found for {node}: {path}")
    
    # Generate VOTables from DataFrames
    return create_slap2_votables_from_lines(
        atomic_dfs,
        molecular_dfs,
        queries_metadata_list,
        lambdaMin=lambdaMin,
        lambdaMax=lambdaMax,
        output_directory=output_directory,
    )
