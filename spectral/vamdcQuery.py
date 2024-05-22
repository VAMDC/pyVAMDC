import requests
import lxml.etree as ET
import pandas as pd
import os
from pathlib import Path

class VamdcQuery:

    def __init__(self, nodeEndpoint, lambdaMin, lambdaMax, InchiKey, speciesType, totalListOfQueries):
        self.nodeEndpoint = nodeEndpoint
        self.lambdaMin = lambdaMin
        self.lambdaMax =lambdaMax
        self.InchiKey = InchiKey
        self.speciesType = speciesType
        self.hasData = False
        self.truncated = None

        query = "select * where (RadTransWavelength >= {0} AND RadTransWavelength <= {1}) AND ((InchiKey = '{2}'))".format(lambdaMin, lambdaMax, InchiKey)
        self.vamdcCall = self.nodeEndpoint + "sync?LANG=VSS2&REQUEST=doQuery&FORMAT=XSAMS&QUERY="+query

        # to be changed in the final version of the lib. This option desactivate the Query Store notifications
        headers = {'User-Agent':'VAMDC Query store'}
         
        try:
            response = requests.head(self.vamdcCall, headers=headers)

            if response.status_code == 200:
                self.hasData = True
               
                queryTruncation = response.headers.get("VAMDC-TRUNCATED")
                if queryTruncation is None or queryTruncation == '100':
                    self.truncated = False
                else:
                   self.truncated = True

            # if the query has data
            if self.hasData is True:
              # if the query is not truncated
              if self.truncated is False :
                # we add to the total list
                totalListOfQueries.append(self)
              else:
                #if the query is truncated we split it in two
                newFirstLambdaMin = self.lambdaMin
                newFirstLambdaMax = 0.5*(self.lambdaMax + self.lambdaMin)
                newSecondLambdaMin = newFirstLambdaMax
                newSecondLambdaMax = self.lambdaMax
                VamdcQuery(self.nodeEndpoint, newFirstLambdaMin, newFirstLambdaMax, self.InchiKey, self.speciesType, totalListOfQueries)
                VamdcQuery(self.nodeEndpoint, newSecondLambdaMin, newSecondLambdaMax, self.InchiKey, self.speciesType, totalListOfQueries)

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
        filename = "./XSAMS/"+self.queryToken+".xsams"
       
        print("Creating file "+filename)
       

        output_file = Path(filename)
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
       if self.queryToken is not None:
          #xsltfile = ET.XSLT(ET.parse("/home/zwolf/Work/PythonDev/pyVAMDC/xsl/atomicxsams2html.xsl"))
          resultFileName = "./XSAMS/"+self.queryToken+".xsams"
          #xmlfile = ET.parse(resultFileName)
          #output = xsltfile(xmlfile).write_output('test1.html')

          #tableHTML = pd.read_html("test1.html")

          #print(tableHTML[0])
          #print(tableHTML[1])
         
          xml_doc = ET.parse(resultFileName)

          # Get the full path of the current script
          script_path = Path(__file__).resolve()

          # Get the parent directory of the script
          parent_dir = script_path.parent.parent

          # Load the XSL file, according to the type of transormation needed 
          if self.speciesType == "atom":
            xslt_doc = ET.parse(str(parent_dir)+"/xsl/atomicxsams2html.xsl")
            print("atom")

          if self.speciesType == "molecule":
            xslt_doc = ET.parse(str(parent_dir)+"/xsl/molecularxsams2html.xsl")

          transform = ET.XSLT(xslt_doc)

          # Perform the transformation
          result = transform(xml_doc)

          # Save the transformed output to an HTML temporary file
          with open(self.queryToken+".html", "wb") as output_file:
           output_file.write(result)
          
          # reading the html file to produce a data-frame
          tableHTML = pd.read_html(self.queryToken+".html")
          
          # removing the temporart HTML file
          os.remove(self.queryToken+".html")
          
          self.lines_df = tableHTML[1]

          # adding to the data-frame the information about the queryToken
          self.lines_df["queryToken"]= self.queryToken






#lambdaMin = 1
#lambdaMax = 50
#InchiKey =  "DOBFQOMOKMYPDT-UHFFFAOYSA-N"
#nodeEndpoint = "http://topbase.obspm.fr/12.07/vamdc/tap/"

#listOfAllQueries = []
#VamdcQuery(nodeEndpoint,lambdaMin,lambdaMax, InchiKey,"atom", listOfAllQueries)
#print(len(listOfAllQueries))
#vamdcQueryTest = listOfAllQueries[0]
#vamdcQueryTest.getXSAMSData()
#vamdcQueryTest.convertToDataFrame()