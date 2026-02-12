import requests
import lxml.etree as ET
import pandas as pd
import os
from pathlib import Path
import uuid
import json
import re
import numpy as np
import time
from typing import Tuple, Optional
from pyVAMDC.spectral.logging_config import get_logger
from pyVAMDC.spectral.energyConverter import electromagnetic_conversion

LOGGER = get_logger(__name__)

class VamdcQuery:
    """
    This class is used to submit spectroscopic queries to the VAMDC infrastructure. 
    It implements low level mechanisms: unless you are a developer whishing to modify this low-level behaviours, it is suggested to use
    the high level functions and classes which interact with the VAMDCQuery class while hiding the complexity (e.g. getLines in 'lines' module). 
    The VAMDCQuery class wraps spectroscopic queries having the following pattern:
    'select * where (RadTransWavelength >= lambdMin AND RadTransWavelength <= lambdaMax) AND ((InchiKey = 'InchiKey'))format(lambdaMin, lambdaMax, InchiKey)'
    and submit this type of queries to a specif VAMDC Node.
    
    Attributes
    ----------
    nodeEndpoint : str
      the string containing the Node TAP endpoint to submit query to
    
    lambdaMin : float
      the inf boundary (in Angstrom) of the wavelenght interval

    
    lambdaMax : float
      the sup boundary (in Angstrom) of the wavelenght interval  


    InchiKey : str
      the InchiKey identifier of the chemical species we want to retrieve data on

    speciesType : str
      this attribute may take two values: 'molecule' or 'atom'. We need this flag because the processing 
      to convert the VAMDC output to a Pandas dataframe depends on the species type. 

    
    hasData : Boolean 
      this flag is true if, while executed, the query will generate data

    truncated : Boolean
      this flag is true if, while executed, the query will be truncated (the result of the given query won't contain all the data available on a given data node)    
    
    XSAMSFileName : str
      the name of the XSAMS output file generated while executing the query
     
    localUUID : str
      A local (client side) unique identifer for the query
    
    acceptTruncation : boolean
      If this flag is false and the query is truncated, the query is recursively split into sub-queries until the sub-queries are not truncated anymore.
      If this flas is false and the query is truncated, the query is not split. 
      This flag has no effect on queries which are not truncated. 

    Methods
    -------
    getXSAMSData()
      Run the GET method on the query to dowloand the data. The extracted data are stored using the XSAMSFileName name.
    
    
    convertToDataFrame()
      Convert the result of the query into a Pandas dataframe. This conversion is performed using the VAMDC-molecular
      or VAMDC-atomic conversion processors (cf. https://github.com/VAMDC/Processors), depending on the species involved in the query. 

    """

    # Constants for HTTP headers
    USER_AGENT_QUERY_STORE = 'VAMDC Query store'
    #DEFAULT_USER_AGENT = 'VAMDC Query store'
    DEFAULT_USER_AGENT = 'pyVAMDC v0.1'

    def __init__(self, nodeEndpoint, lambdaMin, lambdaMax, InchiKey, speciesType, totalListOfQueries, acceptTruncation = False):
      """ This is the constructor of the VAMDCQuery class. 
      The subtlety consists in the fact that this constructor is recursive and takes as argument a list of VAMDCQuery instances already instanciated. 
      This design copes with a particularity of the VAMDC infrastructure: if the result of a given query generates too much data, the result may be truncated. 
      HEAD requests associated with the Query give the information about this truncation. In this constructor we instanciate an initial query and we verify, by executing 
      the corresponding HEAD request, that this is not truncated and that this contain data (thus avoiding empty results). In that case, we add the current instance to the list of 
      the existing instances (passed as an call argument to the constructor). If it is truncated, we split the query into two sub-queries. 
      This mechanism being recursive, at the end the list of instances will contain only queries which will not be truncated or empty while executed. 

      Arguments
      ----------
      nodeEndpoint : str
        the string containing the Node TAP endpoint to submit query to
    
      lambdaMin : float
        the inf boundary (in Angstrom) of the wavelenght interval

      lambdaMax : float
        the sup boundary (in Angstrom) of the wavelenght interval  
  
      InchiKey : str
        the InchiKey identifier of the chemical species we want to retrieve data on

      speciesType : str
        this attribute may take two values: 'molecule' or 'atom'. We need this flag because the processing 
        to convert the VAMDC output to a Pandas dataframe depends on the species type.
      
      acceptTruncation : boolean
        If False, truncated queries are recursively split. If True, truncation is accepted.
      """

      self.nodeEndpoint = nodeEndpoint
      self.lambdaMin = lambdaMin
      self.lambdaMax =lambdaMax
      self.InchiKey = InchiKey
      self.speciesType = speciesType
      self.hasData = False
      self.truncated = None
      self.XSAMSFileName = None
      self.localUUID = None
      self.acceptTruncation = acceptTruncation
      self.counts = {}
      self.parquet_path = None

      self.localUUID = str(uuid.uuid4())

      LOGGER.debug(f"Creating {self.localUUID} ; l_min={lambdaMin} ; l_max={lambdaMax} ; node={nodeEndpoint} ; inchi={InchiKey}")

      query = "select * where (RadTransWavelength >= {0} AND RadTransWavelength <= {1}) AND ((InchiKey = '{2}'))".format(lambdaMin, lambdaMax, InchiKey)
      self.vamdcCall = self.nodeEndpoint + "sync?LANG=VSS2&REQUEST=doQuery&FORMAT=XSAMS&QUERY="+query

      
      if(self.acceptTruncation):
        headers = {'User-Agent' : self.DEFAULT_USER_AGENT}
      else: 
        headers = {'User-Agent': self.USER_AGENT_QUERY_STORE}
         
      try:
          response = requests.head(self.vamdcCall, headers=headers)
          headers_json = {key: value for key, value in response.headers.items()}
          self.counts = {
              key.lower(): value
              for key, value in response.headers.items()
              if key.lower().startswith("vamdc-")
          }

          if response.status_code == 200:
              self.hasData = True
               
              queryTruncation = response.headers.get("VAMDC-TRUNCATED")
              if queryTruncation is None or queryTruncation == '100' or  queryTruncation == "None":
                  self.truncated = False
                  LOGGER.debug(f"Status {self.localUUID}: not truncated")
              else:
                  self.truncated = True
                  LOGGER.debug(f"Status {self.localUUID}: truncated")
          else:
              LOGGER.debug(f"Status {self.localUUID}: no data")
            
          # if the query has data
          if self.hasData is True:
            # if the query is not truncated
            if (self.truncated is False) or self.acceptTruncation:
              # we add to the total list
              totalListOfQueries.append(self)
              LOGGER.debug(f"Query {self.localUUID} added to execution list")

            else:
              #if the query is truncated we split it in two
              newFirstLambdaMin = self.lambdaMin
              newFirstLambdaMax = 0.5*(self.lambdaMax + self.lambdaMin)
              newSecondLambdaMin = newFirstLambdaMax
              newSecondLambdaMax = self.lambdaMax
              LOGGER.debug(f"Splitting {self.localUUID}: l1=[{newFirstLambdaMin}, {newFirstLambdaMax}], l2=[{newSecondLambdaMin}, {newSecondLambdaMax}]")
              VamdcQuery(self.nodeEndpoint, newFirstLambdaMin, newFirstLambdaMax, self.InchiKey, self.speciesType, totalListOfQueries, self.acceptTruncation)
              VamdcQuery(self.nodeEndpoint, newSecondLambdaMin, newSecondLambdaMax, self.InchiKey, self.speciesType, totalListOfQueries, self.acceptTruncation)
                

      except TimeoutError as e:
        LOGGER.error(
            f"Query timeout for {self.nodeEndpoint} (wavelength {lambdaMin}-{lambdaMax}, InChIKey {InchiKey})",
            exception=e,
            show_traceback=False
        )
      except Exception as e:
        LOGGER.error(
            f"Unexpected error querying {self.nodeEndpoint} (wavelength {lambdaMin}-{lambdaMax}, InChIKey {InchiKey})",
            exception=e,
            show_traceback=True
        )
  

    def getXSAMSData(self):
      """
      This method executes a GET request on the current query instance to extract data from the VAMDC infrastructure.
      The data extraction is performed only if the query will contain data and will not be truncated 
      (those states are checked running HEAD request in the object constructor).
      The dowloaded data are stored with the filename from the attribute XSAMSFileName.
      
      Implements retry logic with exponential backoff (3 attempts) for transient network errors.
      If all attempts fail, the query is marked as failed (queryToken=None, XSAMSFileName=None)
      and will be skipped during DataFrame conversion.
      """
      # to be changed in the final version of the lib. This option desactivate the Query Store notifications
      headers = {'User-Agent': self.DEFAULT_USER_AGENT}
      self.queryToken = None
      
      # we get the data only if there is data and the request is not truncated
      if self.hasData is True and (self.truncated is False or self.acceptTruncation):
        max_attempts = 3
        queryResult = None
        
        for attempt in range(max_attempts):
            try:
                queryResult = requests.get(self.vamdcCall, headers=headers, timeout=300)
                # If successful, break out of retry loop
                break
            except (requests.exceptions.ChunkedEncodingError, 
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                if attempt < max_attempts - 1:
                    wait_time = 2 ** attempt  # 1, 2, 4 seconds
                    LOGGER.warning(
                        f"Request failed for {self.InchiKey} "
                        f"(wavelength {self.lambdaMin}-{self.lambdaMax} Å) "
                        f"(attempt {attempt + 1}/{max_attempts}), "
                        f"retrying in {wait_time}s... Error: {type(e).__name__}\n"
                        f"Query: {self.vamdcCall}"
                    )
                    time.sleep(wait_time)
                else:
                    LOGGER.error(
                        f"All {max_attempts} attempts failed for {self.InchiKey} "
                        f"(wavelength {self.lambdaMin}-{self.lambdaMax} Å) on {self.nodeEndpoint}\n"
                        f"Query: {self.vamdcCall}",
                        exception=e,
                        show_traceback=False
                    )
                    # Mark as failed - convertToDataFrame will skip this query
                    self.XSAMSFileName = None
                    return
        
        # If we get here without a queryResult, something went wrong
        if queryResult is None:
            LOGGER.error(f"No response received for {self.InchiKey} on {self.nodeEndpoint}")
            self.XSAMSFileName = None
            return
        
        self.queryToken = queryResult.headers.get('VAMDC-REQUEST-TOKEN')
        
        # Store XSAMS files in QueryResults directory alongside parquet files
        query_results_dir = Path.cwd() / "QueryResults"
        query_results_dir.mkdir(exist_ok=True, parents=True)
        
        if self.queryToken:
           self.XSAMSFileName = str(query_results_dir / f"{self.queryToken}.xsams")
        else:
           self.XSAMSFileName = str(query_results_dir / f"{self.localUUID}.xsams")

        output_file = Path(self.XSAMSFileName)
        output_file.write_bytes(queryResult.content)
       
        #with open(filename, "wb") as file:
        #Write the content of the response to the file
        # file.write(queryResult.content.decode("utf-8"))

#        file = codecs.open(filename, "w", "utf-8")
#        file.write(queryResult.content.encode("utf-8"))
#        file.close


    def _harmonize_wavelength_column(self):
        """
        Ensure self.lines_df has a common 'Wavelength (m)' column in meters.

        This method checks for wavelength, energy, or frequency columns in the dataframe
        and converts them to wavelength in meters if necessary. It handles column names
        with units in the format "Column_name (unit)".

        Raises:
            ValueError: If no wavelength, energy, or frequency data is found.
        """
        if self.lines_df is None or self.lines_df.empty:
            return
            
        df = self.lines_df

        # Pattern to extract unit from column name like "ColumnName (unit)"
        unit_pattern = r'\(([^)]+)\)$'

        # Check if Wavelength (m) already exists
        if 'Wavelength (m)' in df.columns:
            LOGGER.debug("Wavelength (m) column already present")
            return

        # Helper function to normalize unit names for energyConverter
        def normalize_unit(unit_str: str) -> str:
            """Normalize unit name to match energyConverter expectations."""
            if unit_str is None:
                return None
            # Special cases that need to maintain case
            if unit_str.upper() == 'EV':
                return 'eV'
            # Handle common variations
            unit_lower = unit_str.lower().strip()
            if unit_lower in ['a','å', 'ang']:
                return 'angstrom'
            if unit_lower in ['hz']:
                return 'hertz'
            if unit_lower in ['m']:
                return 'meter'
            if unit_lower in ['nm']:
                return 'nanometer'
            if unit_lower in ['cm']:
                return 'centimeter'
            if unit_lower in ['mm']:
                return 'millimeter'
            if unit_lower in ['um', 'μm']:
                return 'micrometer'
            if unit_lower in ['ghz']:
                return 'gigahertz'
            if unit_lower in ['mhz']:
                return 'megahertz'
            if unit_lower in ['khz']:
                return 'kilohertz'
            if unit_lower in ['thz']:
                return 'terahertz'
            if unit_lower in ['cm-1', 'cm^-1']:
                return 'cm-1'
            # Return as lowercase by default
            return unit_lower

        # Helper function to parse column name and extract unit
        def parse_column_with_unit(col_name: str) -> Tuple[str, Optional[str]]:
            """Parse column name to extract base name and unit."""
            match = re.search(unit_pattern, col_name)
            if match:
                unit = match.group(1).strip()
                base_name = col_name[:match.start()].strip()
                return base_name, unit
            return col_name, None

        # Search for wavelength, energy, and frequency columns
        wavelength_columns = {}
        energy_columns = {}
        frequency_columns = {}

        for col in df.columns:
            base_name, unit = parse_column_with_unit(col)
            base_lower = base_name.lower()

            # Energy variants (including wavenumber) - check FIRST to avoid 'wavenumber' matching 'wave'
            if any(en in base_lower for en in ['energy', 'wavenumber']):
                energy_columns[col] = unit
            # Wavelength variants
            elif any(wl in base_lower for wl in ['wavelength', 'wave', 'wl']):
                wavelength_columns[col] = unit
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
                    # Assume Angstrom if no unit specified (common in VAMDC)
                    LOGGER.warning(
                        f"Wavelength column '{wl_col}' has no unit specified, assuming Angstrom"
                    )
                    wl_unit = 'angstrom'
                else:
                    # Normalize unit name for energyConverter
                    wl_unit = normalize_unit(wl_unit)

                LOGGER.debug(f"Converting wavelength from {wl_unit} to meter using column '{wl_col}'")
                df['Wavelength (m)'] = df[wl_col].apply(
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

                LOGGER.debug(f"Converting frequency from {freq_unit} to wavelength in meters using column '{freq_col}'")
                df['Wavelength (m)'] = df[freq_col].apply(
                    lambda x: electromagnetic_conversion(x, freq_unit, 'meter') if pd.notna(x) else np.nan
                )

            elif energy_columns:
                # Use the first energy column found
                energy_col = list(energy_columns.keys())[0]
                energy_unit = energy_columns[energy_col]

                if energy_unit is None:
                    # Check if column name contains 'wavenumber' - assume cm-1
                    base_name, _ = parse_column_with_unit(energy_col)
                    if 'wavenumber' in base_name.lower():
                        LOGGER.warning(
                            f"Wavenumber column '{energy_col}' has no unit specified, assuming cm-1"
                        )
                        energy_unit = 'cm-1'
                    else:
                        # Assume eV if no unit specified for other energy columns
                        LOGGER.warning(
                            f"Energy column '{energy_col}' has no unit specified, assuming eV"
                        )
                        energy_unit = 'eV'
                else:
                    # Normalize unit name for energyConverter
                    energy_unit = normalize_unit(energy_unit)

                LOGGER.debug(f"Converting energy from {energy_unit} to wavelength in meters using column '{energy_col}'")
                df['Wavelength (m)'] = df[energy_col].apply(
                    lambda x: electromagnetic_conversion(x, energy_unit, 'meter') if pd.notna(x) else np.nan
                )

            else:
                # If no suitable column found, create a placeholder column with NaN values
                LOGGER.warning(
                    "No wavelength, energy, or frequency columns found in the dataframe. "
                    "Creating placeholder 'Wavelength (m)' column with NaN values for concatenation compatibility."
                )
                df['Wavelength (m)'] = np.nan

        except Exception as e:
            LOGGER.error(f"Failed to convert wavelength data: {str(e)}")
            # Create placeholder column to allow concatenation
            LOGGER.warning("Creating placeholder 'Wavelength (m)' column with NaN values due to conversion error")
            df['Wavelength (m)'] = np.nan
       

    def convertToDataFrame(self):
       """
       This method convert the result dowloaded (while executing the getXSAMSData on the current instance) from the
       XSAMS data-format to Pandas dataframe. 
       This conversion is performed by locally applying the VAMDC processors (https://github.com/VAMDC/Processors): 
       the atomic processor if the species in the query is an atom, the molecular processor if the species in the query is a molecule
       """
       self.lines_df = None

       # if the data are there (we chek the presence with the Query Token)
       if self.queryToken is not None or os.path.exists(self.XSAMSFileName):
          # Get the full path of the current script
          script_path = Path(__file__).resolve()

          # Get the parent directory of the script
          parent_dir = script_path.parent.parent

          # Determine XSL file path based on species type
          if self.speciesType == "atom":
            xsl_path = str(parent_dir) + "/xsl/atomicxsams2html.xsl"
          elif self.speciesType == "molecule":
            xsl_path = str(parent_dir) + "/xsl/molecularxsams2html.xsl"
          else:
            LOGGER.error(
                f"Unknown speciesType '{self.speciesType}' for query. "
                f"Expected 'atom' or 'molecule'.\n"
                f"  InchiKey: {self.InchiKey}\n"
                f"  Wavelength: {self.lambdaMin}-{self.lambdaMax} Å\n"
                f"  Node: {self.nodeEndpoint}\n"
                f"  XSAMS file: {self.XSAMSFileName}"
            )
            return

          # Parse XSAMS XML file
          try:
              xml_doc = ET.parse(self.XSAMSFileName)
          except ET.XMLSyntaxError as e:
              LOGGER.error(
                  f"XML syntax error parsing XSAMS file.\n"
                  f"  XSAMS file: {self.XSAMSFileName}\n"
                  f"  InchiKey: {self.InchiKey}\n"
                  f"  Wavelength: {self.lambdaMin}-{self.lambdaMax} Å\n"
                  f"  Node: {self.nodeEndpoint}\n"
                  f"  Query: {self.vamdcCall}\n"
                  f"  Error: {e}",
                  exception=e,
                  show_traceback=False
              )
              return
          except Exception as e:
              LOGGER.error(
                  f"Failed to parse XSAMS file.\n"
                  f"  XSAMS file: {self.XSAMSFileName}\n"
                  f"  InchiKey: {self.InchiKey}\n"
                  f"  Wavelength: {self.lambdaMin}-{self.lambdaMax} Å\n"
                  f"  Node: {self.nodeEndpoint}\n"
                  f"  Query: {self.vamdcCall}",
                  exception=e,
                  show_traceback=True
              )
              return

          # Parse XSL stylesheet
          try:
              xslt_doc = ET.parse(xsl_path)
              transform = ET.XSLT(xslt_doc)
          except Exception as e:
              LOGGER.error(
                  f"Failed to load or compile XSL stylesheet.\n"
                  f"  XSL file: {xsl_path}\n"
                  f"  Species type: {self.speciesType}\n"
                  f"  InchiKey: {self.InchiKey}",
                  exception=e,
                  show_traceback=True
              )
              return

          # Perform the transformation
          try:
              result = transform(xml_doc)
              
              # Check for XSLT transformation errors
              if transform.error_log:
                  for entry in transform.error_log:
                      if entry.level_name in ('ERROR', 'FATAL'):
                          LOGGER.warning(
                              f"XSLT transformation error: {entry.message}\n"
                              f"  Line {entry.line}, Column {entry.column}\n"
                              f"  XSAMS file: {self.XSAMSFileName}\n"
                              f"  XSL file: {xsl_path}"
                          )
          except ET.XSLTApplyError as e:
              LOGGER.error(
                  f"XSL transformation failed.\n"
                  f"  XSAMS file: {self.XSAMSFileName}\n"
                  f"  XSL file: {xsl_path}\n"
                  f"  InchiKey: {self.InchiKey}\n"
                  f"  Wavelength: {self.lambdaMin}-{self.lambdaMax} Å\n"
                  f"  Node: {self.nodeEndpoint}\n"
                  f"  Query: {self.vamdcCall}",
                  exception=e,
                  show_traceback=True
              )
              return

          # Save the transformed output to an HTML temporary file
          tempHTMLFileName = self.queryToken+".html" if self.queryToken is not None else self.localUUID+".html"
          try:
              with open(tempHTMLFileName, "wb") as output_file:
                  output_file.write(result)
          except Exception as e:
              LOGGER.error(
                  f"Failed to write HTML output file.\n"
                  f"  Output file: {tempHTMLFileName}\n"
                  f"  InchiKey: {self.InchiKey}",
                  exception=e,
                  show_traceback=True
              )
              return
          
          # reading the html file to produce a data-frame
          # pd.read_html uses lxml by default, which fails with XPathEvalError
          # on very large HTML files (>~300 MB). In that case we fall back to
          # html5lib, a streaming parser that handles arbitrarily large files.
          try:
              tableHTML = pd.read_html(tempHTMLFileName)
          except ValueError as e:
              # read_html raises ValueError when no tables are found
              LOGGER.error(
                  f"No tables found in transformed HTML output.\n"
                  f"  HTML file: {tempHTMLFileName}\n"
                  f"  XSAMS file: {self.XSAMSFileName}\n"
                  f"  InchiKey: {self.InchiKey}\n"
                  f"  Wavelength: {self.lambdaMin}-{self.lambdaMax} Å\n"
                  f"  Node: {self.nodeEndpoint}\n"
                  f"  Query: {self.vamdcCall}",
                  exception=e,
                  show_traceback=False
              )
              os.remove(tempHTMLFileName)
              return
          except Exception as lxml_err:
              LOGGER.warning(
                  f"lxml HTML parser failed ({type(lxml_err).__name__}), "
                  f"retrying with html5lib parser...\n"
                  f"  HTML file: {tempHTMLFileName}"
              )
              try:
                  tableHTML = pd.read_html(tempHTMLFileName, flavor='html5lib')
              except Exception as html5lib_err:
                  LOGGER.error(
                      f"Failed to parse HTML output with both lxml and html5lib.\n"
                      f"  HTML file: {tempHTMLFileName}\n"
                      f"  XSAMS file: {self.XSAMSFileName}\n"
                      f"  InchiKey: {self.InchiKey}\n"
                      f"  Wavelength: {self.lambdaMin}-{self.lambdaMax} Å\n"
                      f"  Node: {self.nodeEndpoint}\n"
                      f"  Query: {self.vamdcCall}\n"
                      f"  lxml error: {lxml_err}\n"
                      f"  html5lib error: {html5lib_err}",
                      exception=html5lib_err,
                      show_traceback=True
                  )
                  os.remove(tempHTMLFileName)
                  return
          
          # removing the temporary HTML file
          os.remove(tempHTMLFileName)
          
          # Check that we have at least 2 tables (expected format)
          if len(tableHTML) < 2:
              LOGGER.error(
                  f"Unexpected HTML table structure: found {len(tableHTML)} table(s), expected at least 2.\n"
                  f"  XSAMS file: {self.XSAMSFileName}\n"
                  f"  InchiKey: {self.InchiKey}\n"
                  f"  Wavelength: {self.lambdaMin}-{self.lambdaMax} Å\n"
                  f"  Node: {self.nodeEndpoint}\n"
                  f"  Query: {self.vamdcCall}"
              )
              return
          
          self.lines_df = tableHTML[1]

          # adding to the data-frame the information about the queryToken. If not available, we use the local query UUID.
          self.lines_df["queryToken"]= self.queryToken if self.queryToken is not None else (self.localUUID+self.nodeEndpoint)

          # Harmonize wavelength column to ensure 'Wavelength (m)' exists
          self._harmonize_wavelength_column()

          # Write DataFrame to parquet file in QueryResults directory
          query_results_dir = Path.cwd() / "QueryResults"
          query_results_dir.mkdir(exist_ok=True)
          
          # Use queryToken if available, otherwise use localUUID (same logic as XSAMS files)
          parquet_filename = self.queryToken if self.queryToken else self.localUUID
          self.parquet_path = query_results_dir / f"{parquet_filename}.parquet"
          
          self.lines_df.to_parquet(self.parquet_path, index=False)
          
          # Log parquet file information
          file_size = self.parquet_path.stat().st_size
          file_size_mb = file_size / (1024 * 1024)
          LOGGER.info(f"Saved parquet file: {self.parquet_path} ({file_size_mb:.2f} MB)")
          
          # Release memory by setting lines_df to None
          self.lines_df = None



# def main():
#     node = "http://topbase.obspm.fr/12.07/vamdc/tap/"
#     inchi="DONWDOGXJBIXRQ-UHFFFAOYSA-N"
#     lambda_min = 0
#     lambda_max = 900090769000 #~90km 
#     totalListOfQueries = []
#     speciesType = "molecule"
#     VamdcQuery(nodeEndpoint=node, lambdaMin=lambda_min, lambdaMax=lambda_max, InchiKey=inchi, totalListOfQueries=totalListOfQueries, speciesType=speciesType, verbose = True, acceptTruncation=True),
#     #totalListOfQueries[0].getXSAMSData()
#     #totalListOfQueries[0].convertToDataFrame()
#     #print(totalListOfQueries[0].lines_df)



# if __name__ == "__main__":
#     main()
