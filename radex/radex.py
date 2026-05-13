from pyVAMDC.logging_config import get_logger

logger = get_logger(__name__)

import pandas as pd
import requests
import zipfile
import time
from pathlib import Path
from urllib.parse import urlparse, urlencode, quote
from typing import Optional, Dict, List, Any

try:
    from pyVAMDC.radex.config import API_BASE_URL
except ImportError:
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
                                     If None, uses API_BASE_URL from pyVAMDC.radex.config
        """
        if base_url is None:
            base_url = API_BASE_URL
        self.base_url = base_url.rstrip('/')

    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Make an HTTP GET request to the API and return a DataFrame.

        Parameters:
            endpoint (str): API endpoint to call
            params (dict): Query parameters

        Returns:
            pd.DataFrame: Response data as DataFrame, or empty DataFrame on error
        """
        filtered_params = {
            key: value
            for key, value in params.items()
            if value is not None
        }

        # Encode params but keep ':' and '/' unencoded for ivoIdentifier values
        query_string = urlencode(filtered_params, quote_via=lambda v, *_: quote(v, safe=":/"))
        url = f"{self.base_url}{endpoint}?{query_string}" if query_string else f"{self.base_url}{endpoint}"

        for attempt in range(3):
            try:
                response = requests.get(url, timeout=30)
                if response.status_code in (403, 429):
                    wait = 2 ** attempt
                    logger.warning(f"Rate limited ({response.status_code}), retrying in {wait}s")
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                data = response.json()
                if isinstance(data, list):
                    return pd.DataFrame(data)
                else:
                    logger.warning(f"Unexpected response format: {type(data)}")
                    return pd.DataFrame()
            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed for {endpoint}: {e}")
                return pd.DataFrame()

        logger.error(f"API request failed after 3 attempts for {endpoint}")
        return pd.DataFrame()

    def _cross_species(self, df_target: pd.DataFrame, df_collider: pd.DataFrame) -> pd.DataFrame:
        """
        Create a cartesian product between target and collider species DataFrames.

        Parameters:
            df_target (pd.DataFrame): DataFrame of target species
            df_collider (pd.DataFrame): DataFrame of collider species

        Returns:
            pd.DataFrame: Cross product with suffixed columns (Target, Collider)
        """
        if df_target is None or df_target.empty:
            if df_collider is None or df_collider.empty:
                return pd.DataFrame()
            return df_collider.drop_duplicates(subset="InChIKey").reset_index(drop=True).rename(columns=lambda c: c + "Collider")
        df_target = df_target.drop_duplicates(subset="InChIKey").reset_index(drop=True)
        if df_collider is None or df_collider.empty:
            return df_target.rename(columns=lambda c: c + "Target")
        df_collider = df_collider.drop_duplicates(subset="InChIKey").reset_index(drop=True)
        return df_target.merge(df_collider, how="cross", suffixes=("Target", "Collider"))

    def _aggregate_results(self, results_list: List[Dict]) -> pd.DataFrame:
        """
        Aggregate a list of result dictionaries into a DataFrame.

        Parameters:
            results_list (list): List of dictionaries containing RADEX entries

        Returns:
            pd.DataFrame: DataFrame, or empty DataFrame if no results
        """
        if not results_list:
            logger.info("No RADEX entries found.")
            return pd.DataFrame()

        return pd.DataFrame(results_list).reset_index(drop=True)

    def getRadex(
        self,
        target_df: pd.DataFrame,
        collider_df: pd.DataFrame,
        db_df_collision: Optional[pd.DataFrame] = None,
        db_df_spectro: Optional[pd.DataFrame] = None,
        doi_df: Optional[pd.DataFrame] = None,
        limit: Optional[int] = None,
        output_dir: str = "./QueryResults/RADEX"
    ) -> pd.DataFrame:
        """
        Get RADEX entries for all target-collider species combinations (cartesian product).

        Parameters:
            target_df (pd.DataFrame): Target species DataFrame
            collider_df (pd.DataFrame): Collider species DataFrame
            db_df_collision (pd.DataFrame, optional): DataFrame with ivoIdentifier column for collision database filter (→ dbCollision in API).
            db_df_spectro (pd.DataFrame, optional): DataFrame with ivoIdentifier column for spectroscopic database filter (→ dbSpectro in API).
            doi_df (pd.DataFrame, optional): DataFrame with a 'doi' column to filter by DOI.
            limit (int, optional): Maximum results per individual API call

        Returns:
            pd.DataFrame: DataFrame with the following columns:
                - inchikeyTarget, inchikeyCollider: InChIKey identifiers
                - symmetryTarget, symmetryCollider: symmetry classifications
                - doi: DOI reference (if present)
                - zipFile: local path to the zip archive containing the RADEX file
                  ({base}.radex) and the original collision/spectro files
                  with their server-side filenames
            output_dir (str): Directory where zip files will be saved (default: "./QueryResults/RADEX")
        """
        if (target_df is None or target_df.empty) and (collider_df is None or collider_df.empty):
            logger.warning("No species provided, returning all entries")
        elif target_df is None or target_df.empty:
            logger.warning("Target species DataFrame is empty, querying on collider only")
        elif collider_df is None or collider_df.empty:
            logger.warning("Collider species DataFrame is empty, querying on target only")

        def _node_names(db_df):
            if db_df is None or db_df.empty:
                return [None]
            col = "ivoIdentifier" if "ivoIdentifier" in db_df.columns else "shortName"
            return db_df[col].dropna().unique().tolist() or [None]

        collision_nodes = _node_names(db_df_collision)
        spectro_nodes = _node_names(db_df_spectro)

        doi = None
        if doi_df is not None and not doi_df.empty and "doi" in doi_df.columns:
            dois = doi_df["doi"].dropna().unique().tolist()
            doi = dois[0] if len(dois) == 1 else None

        df_cross_species = self._cross_species(target_df, collider_df)
        rows = [{}] if df_cross_species.empty else [row.to_dict() for _, row in df_cross_species.iterrows()]

        all_results = []
        for row in rows:
            for db_collision in collision_nodes:
                for db_spectro in spectro_nodes:
                    params = {
                        "inchikeyTarget": row.get("InChIKeyTarget"),
                        "inchikeyCollider": row.get("InChIKeyCollider"),
                        "dbCollision": db_collision,
                        "dbSpectro": db_spectro,
                        "doi": doi,
                        "limit": limit,
                    }
                    entries = self._make_request("/entries/filter", params)
                    if not entries.empty:
                        all_results.extend(entries.to_dict(orient="records"))

        df_raw = self._aggregate_results(all_results)
        if df_raw.empty:
            return pd.DataFrame()

        keep = ["inchikeyTarget", "inchikeyCollider", "symmetryTarget", "symmetryCollider",
                "fileName", "radexFileUrl", "collisionFileUrl", "spectroFileUrl", "doi"]
        return self._downloadRadexFiles(df_raw[[c for c in keep if c in df_raw.columns]].copy(), output_dir=output_dir)

    def _downloadRadexFiles(
        self,
        df_radex: pd.DataFrame,
        output_dir: str = "./QueryResults/RADEX"
    ) -> pd.DataFrame:
        """
        Download files from URLs in df_radex, group them into a zip per entry,
        and return a DataFrame with a zipFile column.

        Each zip contains:
            - {base}.radex   (from radexFileUrl, name derived from fileName)
            - <server name>  (from collisionFileUrl, original sanitized filename)
            - <server name>  (from spectroFileUrl, original sanitized filename)

        Parameters:
            df_radex (pd.DataFrame): DataFrame with file URL columns and fileName
            output_dir (str): Directory where zip files will be saved (default: "./QueryResults/RADEX")

        Returns:
            pd.DataFrame: Same structure with URL columns and fileName replaced by zipFile path
        """
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True, parents=True)

        url_to_entry = {
            "radexFileUrl":     ("fixed", ".radex"),
            "collisionFileUrl": ("url",   ".xsams"),
            "spectroFileUrl":   ("url",   ".xsams"),
        }

        records = []
        for _, row in df_radex.iterrows():
            record = row.to_dict()
            filename = row.get("fileName")

            # Sanitize base name and strip leading M_
            raw = Path(Path(filename).name).stem if filename else "unknown"
            raw = "".join(c for c in raw if c.isalnum() or c in "_-.")
            base = raw[2:] if raw.startswith("M_") else raw

            zip_path = (output_path / f"{base}.zip").resolve()
            if not str(zip_path).startswith(str(output_path.resolve())):
                logger.error(f"Rejected path traversal attempt: {zip_path}")
                record["zipFile"] = None
                for url_col in url_to_entry:
                    record.pop(url_col, None)
                record.pop("fileName", None)
                records.append(record)
                continue

            downloaded = {}
            for url_col, (naming, suffix) in url_to_entry.items():
                url = row.get(url_col)
                if url is None or (isinstance(url, float) and pd.isna(url)):
                    continue

                # SSRF: only allow http/https downloads
                parsed = urlparse(str(url))
                if parsed.scheme not in ("http", "https"):
                    logger.error(f"Rejected URL with disallowed scheme: {parsed.scheme}")
                    continue

                # use original filename from URL for spectro, constructed name for others
                if naming == "url":
                    raw_name = Path(parsed.path).name
                    stem = "".join(c for c in Path(raw_name).stem if c.isalnum() or c in "_-.")
                    ext = "".join(c for c in Path(raw_name).suffix if c.isalnum() or c == ".")
                    entry_name = f"{stem}{ext}" or f"{base}{suffix}"
                else:
                    entry_name = f"{base}{suffix}"

                MAX_BYTES = 50 * 1024 * 1024  # 50 MB
                try:
                    with requests.get(url, timeout=30, stream=True) as response:
                        response.raise_for_status()
                        content_length = response.headers.get("Content-Length")
                        if content_length and int(content_length) > MAX_BYTES:
                            logger.error(f"Rejected oversized download ({content_length} bytes) for {url_col}")
                            continue
                        chunks = []
                        total = 0
                        for chunk in response.iter_content(chunk_size=65536):
                            total += len(chunk)
                            if total > MAX_BYTES:
                                logger.error(f"Rejected oversized download for {url_col}")
                                chunks = None
                                break
                            chunks.append(chunk)
                        if chunks is not None:
                            downloaded[entry_name] = b"".join(chunks)
                except requests.exceptions.RequestException as e:
                    logger.error(f"Failed to download {url_col}: {e}")

            if downloaded:
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                    for name, content in downloaded.items():
                        zf.writestr(name, content)
                record["zipFile"] = str(zip_path)
                print(f"Saved: {zip_path}")
            else:
                record["zipFile"] = None

            for url_col in url_to_entry:
                record.pop(url_col, None)
            record.pop("fileName", None)

            records.append(record)

        return pd.DataFrame(records).reset_index(drop=True)
