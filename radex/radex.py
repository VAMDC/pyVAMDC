from pyVAMDC.radex.logger import logger

import pandas as pd
import tempfile
import os
import requests
import base64
from urllib.parse import quote
from typing import Optional, Dict, List, Any

try:
    from pyVAMDC.radex.config import API_BASE_URL
except ImportError:
    # Default fallback if config file is not found
    API_BASE_URL = "http://127.0.0.1:8000"


class Radex:
    """
    Client for RADEX API to access and extract molecular collision data.

    This class provides a Python interface to query the RADEX web service,
    allowing users to search for molecular species collision data and export results.
    """

    def __init__(self, base_url: Optional[str] = None):
        """
        Initialize the RADEX API client.

        Parameters:
            base_url (str, optional): Base URL of the RADEX API server.
                                     If None, uses the value from radex_config.py
        """
        if base_url is None:
            base_url = API_BASE_URL
        self.base_url = base_url.rstrip('/')

    def _decode_base64_blobs(self, df: pd.DataFrame, blob_columns: list = None) -> pd.DataFrame:
        """
        Decode base64-encoded BLOB columns back to bytes.

        Parameters:
            df (pd.DataFrame): DataFrame with base64-encoded BLOBs
            blob_columns (list, optional): List of column names containing encoded BLOBs.
                                          If None, auto-detects 'radex' column.

        Returns:
            pd.DataFrame: DataFrame with BLOBs decoded to bytes
        """
        if df.empty:
            return df

        # Auto-detect blob columns if not specified
        if blob_columns is None:
            blob_columns = ['radex'] if 'radex' in df.columns else []

        df_copy = df.copy()
        for col in blob_columns:
            if col in df_copy.columns:
                # Decode base64 string to bytes
                df_copy[col] = df_copy[col].apply(
                    lambda x: base64.b64decode(x) if isinstance(x, str) else x
                )

        return df_copy
    
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Make an HTTP GET request to the API and return a DataFrame.

        Parameters:
            endpoint (str): API endpoint to call
            params (dict): Query parameters

        Returns:
            pd.DataFrame: Response data as DataFrame, or empty DataFrame on error
        """
        # Filter out None values and encode special characters
        filtered_params = {
            key: quote(str(value), safe='') if isinstance(value, str) else value
            for key, value in params.items()
            if value is not None
        }

        url = f"{self.base_url}{endpoint}"

        try:
            response = requests.get(url, params=filtered_params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                df = pd.DataFrame(data)
                # Decode base64-encoded BLOBs back to bytes
                df = self._decode_base64_blobs(df)
                return df
            else:
                logger.warning(f"Unexpected response format: {type(data)}")
                return pd.DataFrame()

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {endpoint}: {e}")
            return pd.DataFrame()

    def _cross_species(self, df_target: pd.DataFrame, df_collider: pd.DataFrame) -> pd.DataFrame:
        """
        Create a cartesian product between target and collider species DataFrames.

        Removes duplicates based on InChIKey before creating the cross product.

        Parameters:
            df_target (pd.DataFrame): DataFrame of target species
            df_collider (pd.DataFrame): DataFrame of collider species

        Returns:
            pd.DataFrame: Cross product with suffixed columns (_Target, _Collider)
        """
        # Remove duplicates
        df_target = df_target.drop_duplicates(subset="InChIKey").reset_index(drop=True)
        df_collider = df_collider.drop_duplicates(subset="InChIKey").reset_index(drop=True)

        # Cartesian product with suffixes
        return df_target.merge(df_collider, how="cross", suffixes=("Target", "Collider"))

    def _aggregate_results(self, results_list: List[Dict]) -> pd.DataFrame:
        """
        Aggregate and deduplicate a list of result dictionaries.

        Parameters:
            results_list (list): List of dictionaries containing RADEX entries

        Returns:
            pd.DataFrame: Deduplicated DataFrame, or empty DataFrame if no results
        """
        if not results_list:
            logger.info("No RADEX entries found.")
            return pd.DataFrame()

        df = pd.DataFrame(results_list)
        return df.drop_duplicates().reset_index(drop=True)

    def getRadexWithRole(
        self,
        df_species: pd.DataFrame,
        role: str = "any",
        symmetry_target: Optional[str] = None,
        symmetry_collider: Optional[str] = None,
        db_collision: Optional[str] = None,
        db_spectro: Optional[str] = None,
        doi: Optional[str] = None,
        quantum_numbers: Optional[str] = None,
        id_case: Optional[str] = None,
        process: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get RADEX entries for molecular species with flexible role assignment.

        Queries the API for each species in the DataFrame, searching for matches
        as target, collider, or both depending on the role parameter.

        Parameters:
            df_species (pd.DataFrame): DataFrame with species info (stoichiometricFormula, InChIKey, nameEspece)
            role (str): Species role - "target", "collider", or "any" (default: "any")
            symmetry_target (str, optional): Target symmetry filter
            symmetry_collider (str, optional): Collider symmetry filter
            db_collision (str, optional): Collisional database filter
            db_spectro (str, optional): Spectroscopic database filter
            doi (str, optional): DOI filter
            quantum_numbers (str, optional): Quantum number filter
            id_case (str, optional): Case ID filter
            process (str, optional): Process type filter
            limit (int, optional): Maximum results per species

        Returns:
            pd.DataFrame: Deduplicated DataFrame of matching RADEX entries
        """
        if df_species is None or df_species.empty:
            logger.info("The species DataFrame is empty")
            return pd.DataFrame()

        all_results = []

        for _, row in df_species.iterrows():
            params = {
                "role": role,
                "specie": row.get("nameEspece"),
                "stoichiometric": row.get("stoichiometricFormula"),
                "inchikey": row.get("InChIKey"),
                "symmetryTarget": symmetry_target,
                "symmetryCollider": symmetry_collider,
                "dbCollision": db_collision,
                "dbSpectro": db_spectro,
                "doi": doi,
                "quantumNumbers": quantum_numbers,
                "idCase": id_case,
                "process": process,
                "limit": limit
            }

            entries = self._make_request("/entries/by_role", params)

            if not entries.empty:
                all_results.extend(entries.to_dict(orient="records"))

        return self._aggregate_results(all_results)

    def getRadexCrossProduct(
        self,
        df_species_target: pd.DataFrame,
        df_species_collider: pd.DataFrame,
        symmetry_target: Optional[str] = None,
        symmetry_collider: Optional[str] = None,
        db_collision: Optional[str] = None,
        db_spectro: Optional[str] = None,
        doi: Optional[str] = None,
        quantum_numbers: Optional[str] = None,
        id_case: Optional[str] = None,
        process: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get RADEX entries for all target-collider species combinations (cartesian product).

        Creates all possible pairs between target and collider species (N × M combinations),
        then queries the API for each pair to retrieve collision data.

        Example:
            If df_species_target contains [H2O, CO] and df_species_collider contains [He, H2],
            this will query for: H2O-He, H2O-H2, CO-He, CO-H2 (4 queries total).

        Parameters:
            df_species_target (pd.DataFrame): Target species DataFrame
            df_species_collider (pd.DataFrame): Collider species DataFrame
            symmetry_target (str, optional): Target symmetry filter
            symmetry_collider (str, optional): Collider symmetry filter
            db_collision (str, optional): Collisional database filter
            db_spectro (str, optional): Spectroscopic database filter
            doi (str, optional): DOI filter
            quantum_numbers (str, optional): Quantum number filter
            id_case (str, optional): Case ID filter
            process (str, optional): Process type filter
            limit (int, optional): Maximum results per species pair

        Returns:
            pd.DataFrame: Deduplicated DataFrame of matching RADEX entries for all pairs
        """
        if (df_species_collider is None or df_species_collider.empty or
            df_species_target is None or df_species_target.empty):
            logger.info("Target or collider species DataFrame is empty")
            return pd.DataFrame()

        df_cross_species = self._cross_species(df_species_target, df_species_collider)

        all_results = []

        for _, row in df_cross_species.iterrows():
            params = {
                "stoichiometricTarget": row.get("stoichiometricFormulaTarget"),
                "stoichiometricCollider": row.get("stoichiometricFormulaCollider"),
                "inchikeyTarget": row.get("InChIKeyTarget"),
                "inchikeyCollider": row.get("InChIKeyCollider"),
                "symmetryTarget": symmetry_target,
                "symmetryCollider": symmetry_collider,
                "dbCollision": db_collision,
                "dbSpectro": db_spectro,
                "doi": doi,
                "quantumNumbers": quantum_numbers,
                "idCase": id_case,
                "process": process,
                "limit": limit
            }

            entries = self._make_request("/entries/filter", params)

            if not entries.empty:
                all_results.extend(entries.to_dict(orient="records"))

        return self._aggregate_results(all_results)

    def getRadexDirect(
        self,
        specie_target: Optional[str] = None,
        specie_collider: Optional[str] = None,
        stoichiometric_target: Optional[str] = None,
        stoichiometric_collider: Optional[str] = None,
        symmetry_target: Optional[str] = None,
        symmetry_collider: Optional[str] = None,
        inchikey_target: Optional[str] = None,
        inchikey_collider: Optional[str] = None,
        quantum_numbers: Optional[str] = None,
        doi: Optional[str] = None,
        db_collision: Optional[str] = None,
        db_spectro: Optional[str] = None,
        id_case: Optional[str] = None,
        process: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get RADEX entries with direct parameter filtering (single API query).

        Use this method when you know exact values to search for. Makes only one API call
        with the provided parameters.

        Parameters:
            specie_target (str, optional): Target species name
            specie_collider (str, optional): Collider species name
            stoichiometric_target (str, optional): Target stoichiometric formula
            stoichiometric_collider (str, optional): Collider stoichiometric formula
            symmetry_target (str, optional): Target symmetry
            symmetry_collider (str, optional): Collider symmetry
            inchikey_target (str, optional): Target InChIKey
            inchikey_collider (str, optional): Collider InChIKey
            quantum_numbers (str, optional): Quantum numbers
            doi (str, optional): DOI reference
            db_collision (str, optional): Collisional database
            db_spectro (str, optional): Spectroscopic database
            id_case (str, optional): Case ID
            process (str, optional): Process type
            limit (int, optional): Maximum number of results

        Returns:
            pd.DataFrame: Matching RADEX entries
        """
        params = {
            "specieTarget": specie_target,
            "specieCollider": specie_collider,
            "stoichiometricTarget": stoichiometric_target,
            "stoichiometricCollider": stoichiometric_collider,
            "symmetryTarget": symmetry_target,
            "symmetryCollider": symmetry_collider,
            "inchikeyTarget": inchikey_target,
            "inchikeyCollider": inchikey_collider,
            "quantumNumbers": quantum_numbers,
            "doi": doi,
            "dbCollision": db_collision,
            "dbSpectro": db_spectro,
            "idCase": id_case,
            "process": process,
            "limit": limit
        }

        return self._make_request("/entries/filter", params)

    def getRadexAll(self, limit: int = 100) -> pd.DataFrame:
        """
        Get all RADEX entries from the database.

        Parameters:
            limit (int): Maximum number of results (default: 100)

        Returns:
            pd.DataFrame: All RADEX entries up to the limit
        """
        return self._make_request("/entries", {"limit": limit})

    def exportBlobConsole(
        self,
        df_radex: pd.DataFrame,
        filename_col: str = "fileName",
        blob_col: str = "radex",
        save_dir: Optional[str] = None
    ) -> None:
        """
        Interactive console menu to select and export a RADEX blob file.

        Parameters:
            df_radex (pd.DataFrame): DataFrame containing RADEX results
            filename_col (str): Column name containing the filename (default: "fileName")
            blob_col (str): Column name containing the blob data (default: "radex")
            save_dir (str, optional): Directory to save file. Uses temp dir if None
        """
        if df_radex.empty:
            print("No results to export")
            return

        print("Choose an index:")
        for idx in df_radex.index:
            filename = df_radex.loc[idx, filename_col]
            print(f"{idx}: {filename}")

        choice = input("Enter the line number to save: ")

        try:
            choice = int(choice)
            if choice < 0 or choice >= len(df_radex):
                print("Invalid choice.")
                return
        except ValueError:
            print("Please enter a valid number.")
            return

        row = df_radex.iloc[choice]
        filename = row[filename_col]
        blob_data = row.get(blob_col)

        # Check if blob data exists
        if blob_data is None:
            print(f"Error: No blob data found in column '{blob_col}'")
            print(f"Available columns: {list(df_radex.columns)}")
            return

        if not isinstance(blob_data, bytes):
            print(f"Error: Blob data is not in bytes format (type: {type(blob_data)})")
            return

        # Determine save directory
        if save_dir is None:
            save_dir = tempfile.gettempdir()
        os.makedirs(save_dir, exist_ok=True)

        save_path = os.path.join(save_dir, filename)
        with open(save_path, "wb") as f:
            f.write(blob_data)

        print(f"\nFile saved at: {save_path}")

    def displayFileUrls(
        self,
        df_radex: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """
        Display and return file URLs from all RADEX results.

        Parameters:
            df_radex (pd.DataFrame): DataFrame containing RADEX results

        Returns:
            list: List of dictionaries, each containing entry info and available URLs
        """
        if df_radex.empty:
            logger.error("The DataFrame is empty")
            return []

        all_urls = []

        for idx, row in df_radex.iterrows():
            urls = {
                'radex': row.get('radexFileUrl'),
                'collision': row.get('collisionFileUrl'),
                'spectro': row.get('spectroFileUrl')
            }

            print(f"\n{'=' * 60}")
            print(f"Entry {idx}:")
            print(f"  ID: {row.get('idRadex', 'N/A')}")
            print(f"  File: {row.get('fileName', 'N/A')}")
            print(f"  Target: {row.get('specieTarget', 'N/A')} (symmetry: {row.get('symmetryTarget', 'N/A')})")
            print(f"  Collider: {row.get('specieCollider', 'N/A')} (symmetry: {row.get('symmetryCollider', 'N/A')})")
            print(f"\n  Available URLs:")

            available_urls = {}
            for file_type, url in urls.items():
                if url is not None and not pd.isna(url):
                    print(f"    {file_type.capitalize()}: {url}")
                    available_urls[file_type] = url
                else:
                    print(f"    {file_type.capitalize()}: Not available")

            entry_info = {
                'index': idx,
                'idRadex': row.get('idRadex'),
                'fileName': row.get('fileName'),
                'specieTarget': row.get('specieTarget'),
                'specieCollider': row.get('specieCollider'),
                'urls': available_urls
            }
            all_urls.append(entry_info)

        print(f"\n{'=' * 60}")
        print(f"\nTotal entries: {len(all_urls)}")

        return all_urls

    def extractBlob(
        self,
        results: pd.DataFrame,
        index: int,
        output_path: Optional[str] = None
    ) -> Optional[bytes]:
        """
        Extract RADEX blob data from a specific result entry.

        Parameters:
            results (pd.DataFrame): DataFrame with RADEX results
            index (int): Index of the desired entry
            output_path (str, optional): File path to write blob. If None, prints to console

        Returns:
            bytes: Blob content if output_path is None, otherwise None
        """
        if results.empty:
            logger.info("The results DataFrame is empty.")
            return None

        if index < 0 or index >= len(results):
            logger.info(f"Index {index} is out of bounds (size: {len(results)}).")
            return None

        row = results.iloc[index]
        blob = row.get("radex")

        if blob is None:
            logger.info("No blob data found in the selected entry.")
            return None

        if output_path:
            with open(output_path, "wb") as f:
                f.write(blob)
            logger.info(f"Blob written to: {output_path}")
            return None
        else:
            try:
                print(blob.decode("utf-8"))
            except UnicodeDecodeError:
                logger.warning("Blob cannot be decoded as UTF-8")
            return blob

