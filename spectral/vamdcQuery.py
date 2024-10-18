import requests
import lxml.etree as ET
import pandas as pd
import os
from pathlib import Path
import uuid


def display_message(messageToDisplay, wannaDisplay):
  if wannaDisplay:
     print(messageToDisplay)

class VamdcQuery:

    def __init__(self, nodeEndpoint, lambdaMin, lambdaMax, InchiKey, speciesType, totalListOfQueries, verbose = False):
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

        self.localUUID = str(uuid.uuid4())

        message = f"\nCreating {self.localUUID} ; l_min={lambdaMin} ; l_max={lambdaMax} ;  node ={nodeEndpoint} ; inchi={InchiKey}"
        display_message(message,verbose)

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
                    
                    display_message(message,verbose)
                else:
                   self.truncated = True
                   message = f"__status {self.localUUID} is truncated"
                   display_message(message,verbose)
            else:
               message = f"__status {self.localUUID} has no data"
               display_message(message,verbose)
            
            # if the query has data
            if self.hasData is True:
              # if the query is not truncated
              if self.truncated is False :
                # we add to the total list
                totalListOfQueries.append(self)
                message = f"++++++++ {self.localUUID} added to the list of queries to execute"
                display_message(message,verbose)

              else:
                #if the query is truncated we split it in two
                newFirstLambdaMin = self.lambdaMin
                newFirstLambdaMax = 0.5*(self.lambdaMax + self.lambdaMin)
                newSecondLambdaMin = newFirstLambdaMax
                newSecondLambdaMax = self.lambdaMax
                message = f"-------> {self.localUUID} splitting ; l1_min ={newFirstLambdaMin}; l1_max={newFirstLambdaMax}; l2_min={newSecondLambdaMin}; l2_max={newSecondLambdaMax}"
                display_message(message,verbose)
                VamdcQuery(self.nodeEndpoint, newFirstLambdaMin, newFirstLambdaMax, self.InchiKey, self.speciesType, totalListOfQueries, verbose=self.verbose)
                VamdcQuery(self.nodeEndpoint, newSecondLambdaMin, newSecondLambdaMax, self.InchiKey, self.speciesType, totalListOfQueries, verbose=self.verbose)
                

        except TimeoutError as e:
            print("TimeOut error")
  

    def getXSAMSData(self):
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
       
        #with open(filename, "wb") as file:
        #Write the content of the response to the file
        # file.write(queryResult.content.decode("utf-8"))

#        file = codecs.open(filename, "w", "utf-8")
#        file.write(queryResult.content.encode("utf-8"))
#        file.close
       

    def convertToDataFrame(self):
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

          # adding to the data-frame the information about the queryToken
          self.lines_df["queryToken"]= self.queryToken if self.queryToken is not None else (self.localUUID+self.nodeEndpoint)



def main():
    node = "http://sesam.obspm.fr/12.07/vamdc/tap/"
    inchi="UFHFLCQGNIYNRP-UHFFFAOYSA-N"
    lambda_min = 10
    lambda_max = 99076900
    totalListOfQueries = []
    speciesType = "molecule"
    VamdcQuery(nodeEndpoint=node, lambdaMin=lambda_min, lambdaMax=lambda_max, InchiKey=inchi, totalListOfQueries=totalListOfQueries, speciesType=speciesType, verbose = True)



if __name__ == "__main__":
    main()