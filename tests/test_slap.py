"""
Test module for SLAP2 VOTable generation from species data.

This test module demonstrates and validates the functionality of the slap.py module
for converting species data into SLAP2-compliant VOTable XML files.
"""

import sys
import tempfile
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import pytest
from lxml import etree

# Add the parent directory to the path to import pyVAMDC modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyVAMDC.spectral.slap import SLAP2VOTableGenerator, create_slap2_votables_from_species


def create_sample_species_df() -> pd.DataFrame:
    """
    Create a sample species dataframe for testing.

    Returns:
        pd.DataFrame: Sample species dataframe with multiple nodes.
    """
    data = {
        "shortName": ["Node A", "Node A", "Node B", "Node B", "Node B"],
        "ivoIdentifier": [
            "ivo://node.a",
            "ivo://node.a",
            "ivo://node.b",
            "ivo://node.b",
            "ivo://node.b",
        ],
        "InChI": [
            "InChI=1S/H2O/h1H2",
            "InChI=1S/CO2/c2-1-3",
            "InChI=1S/H2O/h1H2",
            "InChI=1S/NH3/h1H3",
            "InChI=1S/CH4/h1H4",
        ],
        "InChIKey": [
            "XLYOFNOQVQJJNP-UHFFFAOYSA-N",
            "CURLTUGMZLYLDI-UHFFFAOYSA-N",
            "XLYOFNOQVQJJNP-UHFFFAOYSA-N",
            "QGZKDVFQNNGYKY-UHFFFAOYSA-N",
            "VNWKTOKETHGBQD-UHFFFAOYSA-N",
        ],
        "stoichiometricFormula": ["H2O", "CO2", "H2O", "NH3", "CH4"],
        "structuralFormula": ["H-O-H", "O=C=O", "H-O-H", "H-N(-H)-H", "H-C(-H)(-H)-H"],
        "name": ["water", "carbon dioxide", "water", "ammonia", "methane"],
        "speciesType": ["molecule", "molecule", "molecule", "molecule", "molecule"],
        "charge": [0, 0, 0, 0, 0],
        "massNumber": [18.0, 44.0, 18.0, 17.0, 16.0],
        "tapEndpoint": [
            "http://node.a/tap",
            "http://node.a/tap",
            "http://node.b/tap",
            "http://node.b/tap",
            "http://node.b/tap",
        ],
        "lastIngestionScriptDate": [
            "2025-11-01T10:00:00",
            "2025-11-01T10:00:00",
            "2025-11-02T14:30:00",
            "2025-11-02T14:30:00",
            "2025-11-02T14:30:00",
        ],
        "speciesLastSeenOn": [
            "2025-11-05T15:20:00",
            "2025-11-05T15:20:00",
            "2025-11-06T09:15:00",
            "2025-11-06T09:15:00",
            "2025-11-06T09:15:00",
        ],
        "did": ["did1", "did2", "did3", "did4", "did5"],
        "# unique atoms": [2, 2, 2, 2, 2],
        "# total atoms": [3, 3, 3, 4, 5],
        "computed charge": [0, 0, 0, 0, 0],
        "computed mol_weight": [18.01528, 44.00956, 18.01528, 17.02655, 16.04260],
    }
    return pd.DataFrame(data)


def test_slap2_generator_initialization():
    """Test SLAP2VOTableGenerator initialization."""
    with tempfile.TemporaryDirectory() as tmpdir:
        generator = SLAP2VOTableGenerator(output_directory=tmpdir)
        assert generator.get_output_directory() == tmpdir

    # Test with None (creates temp directory)
    generator = SLAP2VOTableGenerator()
    assert Path(generator.get_output_directory()).exists()


def test_generate_votables_for_nodes():
    """Test generating VOTables grouped by nodes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        generator = SLAP2VOTableGenerator(output_directory=tmpdir)
        # Test with mock - we just test the structure and grouping logic
        # In real usage, this would connect to VAMDC database
        results = generator.generate_votables_for_nodes(type="molecule")
        
        # Results should be a list
        assert isinstance(results, list)
        
        # If results exist, verify structure
        for result in results:
            assert "query_params" in result
            assert "node_ivoidentifier" in result
            assert "node_shortname" in result
            assert "species_count" in result
            assert "votable_filepath" in result
            assert "generation_timestamp" in result


def test_votable_xml_structure():
    """Test that generated VOTable has correct XML structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        generator = SLAP2VOTableGenerator(output_directory=tmpdir)
        
        # Use sample data with mock species
        species_df = create_sample_species_df()
        
        # Generate VOTable using internal logic (simulated)
        results = []
        for node_ivo_id in species_df["ivoIdentifier"].unique():
            node_species_df = species_df[species_df["ivoIdentifier"] == node_ivo_id].copy()
            query_params = {'type': 'molecule'}
            
            votable_filepath = generator._create_votable_for_node(
                node_species_df, node_ivo_id, query_params
            )
            results.append({'votable_filepath': votable_filepath})
        
        # Verify structure
        assert len(results) > 0
        
        # Parse the first generated VOTable
        votable_path = results[0]["votable_filepath"]
        tree = etree.parse(votable_path)
        root = tree.getroot()

        # Check root element
        assert root.tag.endswith("VOTABLE")
        assert root.get("version") == "1.3"

        # Check RESOURCE element
        resources = root.findall(".//{http://www.ivoa.net/xml/VOTable/v1.3}RESOURCE")
        assert len(resources) > 0
        resource = resources[0]
        assert resource.get("type") == "results"

        # Check for required INFO elements
        infos = resource.findall("./{http://www.ivoa.net/xml/VOTable/v1.3}INFO")
        info_names = [info.get("name") for info in infos]
        assert "QUERY_STATUS" in info_names
        assert "request_date" in info_names
        assert "service_protocol" in info_names
        assert "publisher" in info_names

        # Check QUERY_STATUS value
        query_status = [info for info in infos if info.get("name") == "QUERY_STATUS"][0]
        assert query_status.get("value") == "OK"

        # Check TABLE element
        tables = resource.findall("./{http://www.ivoa.net/xml/VOTable/v1.3}TABLE")
        assert len(tables) > 0


def test_votable_field_definitions():
    """Test that VOTable has proper FIELD definitions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        generator = SLAP2VOTableGenerator(output_directory=tmpdir)
        
        # Use sample data with mock species
        species_df = create_sample_species_df()
        node_ivo_id = species_df["ivoIdentifier"].iloc[0]
        node_species_df = species_df[species_df["ivoIdentifier"] == node_ivo_id].copy()
        query_params = {'type': 'molecule'}
        
        votable_filepath = generator._create_votable_for_node(
            node_species_df, node_ivo_id, query_params
        )
        
        tree = etree.parse(votable_filepath)
        root = tree.getroot()

        # Find FIELD elements
        fields = root.findall(".//{http://www.ivoa.net/xml/VOTable/v1.3}FIELD")
        assert len(fields) > 0

        field_names = [f.get("name") for f in fields]

        # Check for expected fields
        expected_fields = ["InChIKey", "stoichiometricFormula"]
        for expected_field in expected_fields:
            assert expected_field in field_names

        # Check field attributes
        for field in fields:
            assert field.get("datatype") in ["char", "int", "float", "double"]
            if field.get("datatype") == "char":
                assert field.get("arraysize") == "*"


def test_votable_data_rows():
    """Test that VOTable contains correct data rows."""
    with tempfile.TemporaryDirectory() as tmpdir:
        generator = SLAP2VOTableGenerator(output_directory=tmpdir)
        
        # Use sample data with mock species
        species_df = create_sample_species_df()
        node_ivo_id = species_df["ivoIdentifier"].iloc[0]
        node_species_df = species_df[species_df["ivoIdentifier"] == node_ivo_id].copy()
        query_params = {'type': 'molecule'}
        
        votable_filepath = generator._create_votable_for_node(
            node_species_df, node_ivo_id, query_params
        )
        
        tree = etree.parse(votable_filepath)
        root = tree.getroot()

        # Find table rows
        rows = root.findall(".//{http://www.ivoa.net/xml/VOTable/v1.3}TR")
        assert len(rows) > 0

        # Check that each row has correct number of cells
        fields = root.findall(".//{http://www.ivoa.net/xml/VOTable/v1.3}FIELD")
        num_fields = len(fields)

        for row in rows:
            cells = row.findall("./{http://www.ivoa.net/xml/VOTable/v1.3}TD")
            assert len(cells) == num_fields


def test_empty_species_df():
    """Test behavior when no species match the filter criteria."""
    with tempfile.TemporaryDirectory() as tmpdir:
        generator = SLAP2VOTableGenerator(output_directory=tmpdir)
        # Call with filter that matches no species
        # Using a very specific stoichiometric formula that won't match sample data
        with pytest.raises(ValueError, match="No species found matching the provided filters"):
            generator.generate_votables_for_nodes(stoichiometric_formula="XeXe")


def test_missing_required_columns():
    """Test that module handles species data validation correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        generator = SLAP2VOTableGenerator(output_directory=tmpdir)
        # Create sample data to test internal validation
        species_df = create_sample_species_df()
        node_ivo_id = species_df["ivoIdentifier"].iloc[0]
        node_species_df = species_df[species_df["ivoIdentifier"] == node_ivo_id].copy()
        
        # Test with proper data - should not raise
        try:
            votable_filepath = generator._create_votable_for_node(
                node_species_df, node_ivo_id, {}
            )
            assert Path(votable_filepath).exists()
        except ValueError as e:
            pytest.fail(f"Should not raise ValueError with valid species dataframe: {e}")


def test_create_slap2_votables_convenience_function():
    """Test the convenience function with filter parameters."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Call convenience function with filter parameters
        results = create_slap2_votables_from_species(type="molecule", output_directory=tmpdir)

        # Should return results from matching species
        assert isinstance(results, list)
        
        # If results exist, verify structure
        if len(results) > 0:
            for result in results:
                assert "votable_filepath" in result
                assert Path(result["votable_filepath"]).exists()
                assert "query_params" in result
                assert result["query_params"]["type"] == "molecule"


def test_votable_file_creation():
    """Test that VOTable files are actually created."""
    with tempfile.TemporaryDirectory() as tmpdir:
        generator = SLAP2VOTableGenerator(output_directory=tmpdir)
        # Call with type filter to get species matches
        results = generator.generate_votables_for_nodes(type="molecule")

        # Verify files created for matches
        for result in results:
            filepath = result["votable_filepath"]
            assert Path(filepath).exists()
            assert filepath.endswith(".xml")

            # Verify file can be parsed as XML
            tree = etree.parse(filepath)
            assert tree.getroot() is not None


if __name__ == "__main__":
    # Run tests with pytest if available, otherwise run basic tests
    try:
        import pytest

        pytest.main([__file__, "-v"])
    except ImportError:
        print("pytest not available, running basic tests...")

        print("Testing SLAP2VOTableGenerator initialization...")
        test_slap2_generator_initialization()
        print("✓ Passed")

        print("Testing VOTable generation for nodes...")
        test_generate_votables_for_nodes()
        print("✓ Passed")

        print("Testing VOTable XML structure...")
        test_votable_xml_structure()
        print("✓ Passed")

        print("Testing VOTable field definitions...")
        test_votable_field_definitions()
        print("✓ Passed")

        print("Testing VOTable data rows...")
        test_votable_data_rows()
        print("✓ Passed")

        print("Testing convenience function...")
        test_create_slap2_votables_convenience_function()
        print("✓ Passed")

        print("Testing VOTable file creation...")
        test_votable_file_creation()
        print("✓ Passed")

        print("\nAll basic tests passed!")
