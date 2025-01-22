import requests
import lxml.etree as ET
import pandas as pd
import os
from pathlib import Path
import uuid
from multiprocessing import Manager
import threading


def _display_message(messageToDisplay, wannaDisplay):
  """
  This function is used to display log messages in the console. 

  Arguments
  ---------

  messageToDisplay : str
    the log message to display

  wannaDisplay : boolean
    If true, the message is displayed. 

  """
  if wannaDisplay:
     print(messageToDisplay)

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
    
    verbose : boolean
      If this flag is true, detailed log information are displayed
    
    Methods
    -------
    getXSAMSData()
      Run the GET method on the query to dowloand the data. The extracted data are stored using the XSAMSFileName name.
    
    
    convertToDataFrame()
      Convert the result of the query into a Pandas dataframe. This conversion is performed using the VAMDC-molecular
      or VAMDC-atomic conversion processors (cf. https://github.com/VAMDC/Processors), depending on the species involved in the query. 

    """

    def __init__(self, nodeEndpoint, lambdaMin, lambdaMax, InchiKey, speciesType, totalListOfQueries, verbose = False, numberSubIntervals = 2):
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

      verbose : boolean
      If this flag is true, detaile log information are displayed  
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
      self.verbose = verbose
      self.numberSubInterval = numberSubIntervals

      self.localUUID = str(uuid.uuid4())

      message = f"\nCreating {self.localUUID} ; l_min={lambdaMin} ; l_max={lambdaMax} ;  node ={nodeEndpoint} ; inchi={InchiKey}"
      _display_message(message,verbose)

      query = "select * where (RadTransWavelength >= {0} AND RadTransWavelength <= {1}) AND ((InchiKey = '{2}'))".format(lambdaMin, lambdaMax, InchiKey)
      self.vamdcCall = self.nodeEndpoint + "sync?LANG=VSS2&REQUEST=doQuery&FORMAT=XSAMS&QUERY="+query

      # to be changed in the final version of the lib. This option desactivate the Query Store notifications
      headers = {'User-Agent':'VAMDC Query store'}
         
      try:
          response = requests.head(self.vamdcCall, headers=headers)
            
          if response.status_code == 200:
              self.hasData = True
               
              queryTruncation = response.headers.get("VAMDC-TRUNCATED")
              if queryTruncation is None or queryTruncation == '100' or  queryTruncation == "None":
                  self.truncated = False
                  message = f"__status {self.localUUID} is not truncated"
                    
                  _display_message(message,verbose)
              else:
                  self.truncated = True
                  message = f"__status {self.localUUID} is truncated"
                  _display_message(message,verbose)
          else:
              message = f"__status {self.localUUID} has no data"
              _display_message(message,verbose)
            
          # if the query has data
          if self.hasData is True:
            # if the query is not truncated
            if self.truncated is False :
              # we add to the total list
              totalListOfQueries.append(self)
              message = f"++++++++ {self.localUUID} added to the list of queries to execute"
              _display_message(message,verbose)

            else:
              # Calculate the width of each part
              width = (self.lambdaMax - self.lambdaMin)/self.numberSubInterval
              
              # Generate the boundaries of each part
              boundaries = [self.lambdaMin + i*width for i in range(self.numberSubInterval+1)]

              intervals = [(boundaries[i], boundaries[i+1]) for i in range(self.numberSubInterval)]

              threadList = []
              for interval in intervals:
                thread =  threading.Thread(target=VamdcQuery, args=(self.nodeEndpoint, interval[0], interval[1], self.InchiKey, self.speciesType, totalListOfQueries, self.verbose, self.numberSubInterval))
                threadList.append(thread)
                thread.start()
                # VamdcQuery(self.nodeEndpoint, interval[0], interval[1], self.InchiKey, self.speciesType, totalListOfQueries, self.verbose)

              for thread in  threadList:
                thread.join()
                

      except TimeoutError as e:
        print("TimeOut error")
  

    def getXSAMSData(self):
      """
      This method executes a GET request on the current query instance to extract data from the VAMDC infrastructure.
      The data extraction is performed only if the query will contain data and will not be truncated 
      (those states are checked running HEAD request in the object constructor).
      The dowloaded data are stored with the filename from the attribute XSAMSFileName
      """
      # to be changed in the final version of the lib. This option desactivate the Query Store notifications
      headers = {'User-Agent':'VAMDC Query store'}
      self.queryToken = None
      
      # we get the data only if there is data and the request is not truncated
      if self.hasData is True and self.truncated is False:
        queryResult = requests.get(self.vamdcCall, headers=headers)
        
        self.queryToken = queryResult.headers.get('VAMDC-REQUEST-TOKEN')
        
        if self.queryToken:
           self.XSAMSFileName = "./XSAMS/"+self.queryToken+".xsams"
        else:
           self.XSAMSFileName = "./XSAMS/"+self.localUUID+".xsams"

        output_file = Path(self.XSAMSFileName)
        output_file.parent.mkdir(exist_ok=True, parents=True)
        output_file.write_bytes(queryResult.content)

        message = f"++++++++ {self.localUUID} -----> File downloaded to {self.XSAMSFileName}"
        _display_message(message,self.verbose)
       

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
          #xsltfile = ET.XSLT(ET.parse("/home/zwolf/Work/PythonDev/pyVAMDC/xsl/atomicxsams2html.xsl"))
          #xmlfile = ET.parse(resultFileName)
          #output = xsltfile(xmlfile).write_output('test1.html')

          #tableHTML = pd.read_html("test1.html")

          #print(tableHTML[0])
          #print(tableHTML[1])
         
          xml_doc = ET.parse(self.XSAMSFileName)

          # Get the full path of the current script
          script_path = Path(__file__).resolve()

          # Get the parent directory of the script
          parent_dir = script_path.parent.parent

          # Load the XSL file, according to the type of transormation needed 
          if self.speciesType == "atom":
            xslt_doc = ET.parse(str(parent_dir)+"/xsl/atomicxsams2html.xsl")

          if self.speciesType == "molecule":
            xslt_doc = ET.parse(str(parent_dir)+"/xsl/molecularxsams2html.xsl")

          transform = ET.XSLT(xslt_doc)

          # Perform the transformation
          result = transform(xml_doc)

          # Save the transformed output to an HTML temporary file

          tempHTMLFileName = self.queryToken+".html" if self.queryToken is not None else self.localUUID+".html"
          with open(tempHTMLFileName, "wb") as output_file:
           output_file.write(result)
          
          # reading the html file to produce a data-frame
          tableHTML = pd.read_html(tempHTMLFileName)
          
          # removing the temporart HTML file
          os.remove(tempHTMLFileName)
          
          self.lines_df = tableHTML[1]

          # adding to the data-frame the information about the queryToken. If not available, we use the local query UUID.
          self.lines_df["queryToken"]= self.queryToken if self.queryToken is not None else (self.localUUID+self.nodeEndpoint)

          message = f"++++++++ {self.localUUID} ---- {self.XSAMSFileName} correctly converted to dataframe"
          _display_message(message, self.verbose)



def main():
     node = "http://sesam.obspm.fr/12.07/vamdc/tap/"
     inchi="UFHFLCQGNIYNRP-OUBTZVSYSA-N"
     lambda_min = 970
     lambda_max = 1000
     manager = Manager()
     totalListOfQueries = manager.list()
     speciesType = "molecule"
     VamdcQuery(nodeEndpoint=node, lambdaMin=lambda_min, lambdaMax=lambda_max, InchiKey=inchi, totalListOfQueries=totalListOfQueries, speciesType=speciesType, verbose = True, numberSubIntervals=4)

     print(len(totalListOfQueries))
     totalListOfQueries = list(totalListOfQueries)


     for currentQuery in totalListOfQueries:
        print("current Query == "+str(currentQuery.localUUID))
        # get the data
        currentQuery.getXSAMSData()
        # convert the data 
        currentQuery.convertToDataFrame()

     # now we build two dictionaries, one with all the molecular data-frames, the other one with atomic data-frames
     atomic_results_dict = {}
     molecular_results_dict= {}


    # and we populate those two dictionaries by iterating over the queries that have been processed 
     for currentQuery in totalListOfQueries:
        print("******* current query ID == " +str(currentQuery.localUUID))
        nodeIdentifier = currentQuery.nodeEndpoint
       
        if currentQuery.speciesType == "atom":
            if nodeIdentifier in atomic_results_dict:
                atomic_results_dict[nodeIdentifier] = pd.concat([atomic_results_dict[nodeIdentifier], currentQuery.lines_df], ignore_index=True)
            else:
                atomic_results_dict[nodeIdentifier] = currentQuery.lines_df
        

        if currentQuery.speciesType == "molecule":
            if nodeIdentifier in molecular_results_dict:
                 molecular_results_dict[nodeIdentifier] = pd.concat([molecular_results_dict[nodeIdentifier], currentQuery.lines_df], ignore_index=True)
            else:
                molecular_results_dict[nodeIdentifier] = currentQuery.lines_df 


if __name__ == "__main__":
   main()