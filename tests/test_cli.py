"""
Lightweight tests for the core `vamdc` command-line workflows.

These tests never hit the network: every function that would otherwise call
out to VAMDC/RADEX services is monkeypatched with an in-memory stand-in, and
the cache directory is isolated to a temporary folder for every test.
"""

import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

# Point the cache at a throwaway directory before the cli module is imported,
# since importing it eagerly creates/uses CACHE_DIR as a side effect.
os.environ["VAMDC_CACHE_DIR"] = tempfile.mkdtemp(prefix="vamdc_test_cache_")

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyVAMDC.spectral import cli as cli_mod


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    """Redirect the CLI's cache paths to a fresh temporary directory."""
    monkeypatch.setattr(cli_mod, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(cli_mod, "NODES_CACHE_FILE", tmp_path / "nodes.csv")
    monkeypatch.setattr(cli_mod, "SPECIES_CACHE_FILE", tmp_path / "species.csv")
    monkeypatch.setattr(cli_mod, "SPECIES_NODES_CACHE_FILE", tmp_path / "species_nodes.csv")
    return tmp_path


def _sample_nodes_df() -> pd.DataFrame:
    return pd.DataFrame({
        "shortName": ["CDMS", "VALD"],
        "ivoIdentifier": ["ivo://vamdc/cdms", "ivo://vamdc/vald"],
        "tapEndpoint": ["http://cdms.example/tap/", "http://vald.example/tap/"],
    })


def _sample_species_df() -> pd.DataFrame:
    return pd.DataFrame({
        "shortName": ["CDMS", "CDMS"],
        "ivoIdentifier": ["ivo://vamdc/cdms", "ivo://vamdc/cdms"],
        "InChI": ["InChI=1S/CO/c1-2", "InChI=1S/H2O/h1H2"],
        "InChIKey": ["UGFAIRIUMAVXCW-UHFFFAOYSA-N", "XLYOFNOQVPJJNP-UHFFFAOYSA-N"],
        "stoichiometricFormula": ["CO", "H2O"],
        "massNumber": [28, 18],
        "charge": [0, 0],
        "speciesType": ["molecule", "molecule"],
        "structuralFormula": ["CO", "H2O"],
        "name": ["Carbon monoxide", "Water"],
        "tapEndpoint": ["http://cdms.example/tap/", "http://cdms.example/tap/"],
    })


# --- get nodes / get species -------------------------------------------------

def test_get_nodes_csv(runner, isolated_cache, monkeypatch):
    monkeypatch.setattr(cli_mod.species_module, "getNodeHavingSpecies", lambda: _sample_nodes_df())

    result = runner.invoke(cli_mod.cli, ["get", "nodes", "--format", "csv"])

    assert result.exit_code == 0
    assert "CDMS" in result.output
    assert "VALD" in result.output


def test_get_species_filter_by_name(runner, isolated_cache, monkeypatch):
    monkeypatch.setattr(
        cli_mod.species_module, "getAllSpecies",
        lambda: (_sample_species_df(), _sample_nodes_df())
    )

    result = runner.invoke(
        cli_mod.cli, ["get", "species", "--format", "csv", "--filter-by", "name:Carbon"]
    )

    assert result.exit_code == 0
    assert "Carbon monoxide" in result.output
    assert "Water" not in result.output


# --- get/count lines: input validation and aggregation -----------------------

def test_get_lines_rejects_invalid_wavelength_range(runner, isolated_cache):
    result = runner.invoke(
        cli_mod.cli, ["get", "lines", "--lambda-min", "5000", "--lambda-max", "3000"]
    )

    assert result.exit_code == 1
    assert "--lambda-max must be greater than --lambda-min" in result.output


def test_count_lines_rejects_invalid_wavelength_range(runner, isolated_cache):
    result = runner.invoke(
        cli_mod.cli, ["count", "lines", "--lambda-min", "5000", "--lambda-max", "3000"]
    )

    assert result.exit_code == 1
    assert "--lambda-max must be greater than --lambda-min" in result.output


def test_count_lines_aggregates_counts(runner, isolated_cache, monkeypatch):
    metadata_list = [
        {"query": "q1", "metadata": {"vamdc-count-radiative": "5"}},
        {"query": "q2", "metadata": {"vamdc-count-radiative": "3"}},
    ]
    monkeypatch.setattr(
        cli_mod.lines_module, "get_metadata_for_lines", lambda **kwargs: metadata_list
    )

    result = runner.invoke(
        cli_mod.cli, ["count", "lines", "--lambda-min", "3000", "--lambda-max", "5000"]
    )

    assert result.exit_code == 0
    assert "vamdc-count-radiative: 8" in result.output


def test_get_lines_marks_explicitly_accepted_truncation_as_partial(
    runner, isolated_cache, monkeypatch
):
    monkeypatch.setattr(
        cli_mod,
        "load_species_data",
        lambda: (_sample_species_df(), _sample_nodes_df()),
    )
    received = {}

    def fake_get_lines(**kwargs):
        received.update(kwargs)
        return (
            {},
            {"http://cdms.example/tap/": pd.DataFrame({"wavelength": [42.0]})},
            [{"truncated": True, "accepted_truncation": True}],
        )

    monkeypatch.setattr(cli_mod.lines_module, "getLinesAsDataFrames", fake_get_lines)

    result = runner.invoke(cli_mod.cli, ["get", "lines", "--accept-truncation"])

    assert result.exit_code == 0
    assert received["acceptTruncation"] is True
    assert "CAUTION: --accept-truncation" in result.output
    assert "PARTIAL RESULT:" in result.output
    assert "the output is incomplete" in result.output


# --- get radex ----------------------------------------------------------------

def test_get_radex_requires_target_or_collider(runner):
    result = runner.invoke(cli_mod.cli, ["get", "radex"])

    assert result.exit_code == 1
    assert "At least one of --target or --collider" in result.output


# --- cache management ----------------------------------------------------------

def test_cache_status_reports_not_cached(runner, isolated_cache):
    result = runner.invoke(cli_mod.cli, ["cache", "status"])

    assert result.exit_code == 0
    assert "NOT CACHED" in result.output


def test_cache_clear_empties_directory(runner, isolated_cache):
    (isolated_cache / "dummy.txt").write_text("data")

    result = runner.invoke(cli_mod.cli, ["cache", "clear"])

    assert result.exit_code == 0
    assert "Cache cleared." in result.output
    assert isolated_cache.exists()
    assert not any(isolated_cache.iterdir())


def test_cache_clear_refuses_home_directory(runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "CACHE_DIR", Path.home())

    result = runner.invoke(cli_mod.cli, ["cache", "clear"])

    assert result.exit_code != 0
    assert "Refusing to clear" in result.output


# --- unit conversion ------------------------------------------------------------

def test_convert_energy_wavelength_to_wavelength(runner):
    result = runner.invoke(
        cli_mod.cli, ["convert", "energy", "500", "--from-unit", "nanometer", "--to-unit", "angstrom"]
    )

    assert result.exit_code == 0
    assert result.output.strip() == "5000 angstrom"


def test_convert_energy_invalid_unit(runner):
    result = runner.invoke(
        cli_mod.cli, ["convert", "energy", "1", "--from-unit", "bogus", "--to-unit", "eV"]
    )

    assert result.exit_code == 1
    assert "Invalid from-unit" in result.output
