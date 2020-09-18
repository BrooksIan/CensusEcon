from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import trim
import cdsw
import time
import sys 

# Check NVidia Rapids Jars
!ls -l $SPARK_RAPIDS_DIR

RapidJarDir = os.path.join(os.environ["SPARK_RAPIDS_DIR"])
RapidJars = [os.path.join(RapidJarDir, x) for x in os.listdir(RapidJarDir)]
RapidgetGPURespource = os.environ["SPARK_RAPIDS_DIR"] + "/getGpusResources.sh" 

print(RapidJarDir)
print(RapidJars)
print(RapidgetGPURespource)


# Initialize Spark Session 
spark = SparkSession.builder \
      .appName("CenusEcon") \
      .master("k8s://https://172.20.0.1:443") \
      .config("spark.plugins","com.nvidia.spark.SQLPlugin") \
      .config("spark.rapids.sql.format.csv.read.enabled", "false") \
      .config("spark.rapids.sql.enabled", "false") \
      .config("spark.executor.resource.gpu.discoveryScript", RapidgetGPURespource) \
      .config("spark.executor.resource.gpu.vendor","nvidia") \
      .config("spark.task.resource.gpu.amount",".25") \
      .config("spark.executor.cores","4") \
      .config("spark.executor.resource.gpu.amount","1") \
      .config("spark.executor.memory","1G") \
      .config("spark.task.cpus","1") \
      .config("spark.rapids.memory.pinnedPool.size","1G") \
      .config("spark.locality.wait","0s") \
      .config("spark.sql.files.maxPartitionBytes","256m") \
      .config("spark.sql.shuffle.partitions","10") \
      .config("spark.jars", ",".join(RapidJars))\
      .config("spark.files", RapidgetGPURespource)\
      .getOrCreate()

      
## Spark + Rapids on K8s      
#      --master "k8s://https://172.20.0.1:443" \
#     --conf spark.rapids.sql.concurrentGpuTasks=1 \
#     --driver-memory 2G \
#     --conf spark.executor.memory=4G \
#     --conf spark.executor.cores=4 \
#     --conf spark.task.cpus=1 \
#     --conf spark.task.resource.gpu.amount=0.25 \
#     --conf spark.rapids.memory.pinnedPool.size=2G \
#     --conf spark.locality.wait=0s \
#     --conf spark.sql.files.maxPartitionBytes=512m \
#     --conf spark.sql.shuffle.partitions=10 \
#     --conf spark.plugins=com.nvidia.spark.SQLPlugin \
#     --conf spark.executor.resource.gpu.discoveryScript=/opt/sparkRapidsPlugin/getGpusResources.sh \
#     --conf spark.executor.resource.gpu.vendor=nvidia.com \
#     --conf spark.kubernetes.container.image=$IMAGE_NAME
#
#     --files ${SPARK_RAPIDS_DIR}/getGpusResources.sh \
#     --jars  ${SPARK_CUDF_JAR},${SPARK_RAPIDS_PLUGIN_JAR}
#
      
      
spark.version

### Start Timer
startTime = time.process_time()


#Load Data Files
#Create a data frame from CSV File 
#df_WholeSetRaw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("cfs_2012_pumf_csv.txt")

#Create Table from DataFrame
#df_WholeSetRaw.createOrReplaceTempView("CensusECON")

#Display resulting Infered schema 
#df_WholeSetRaw.printSchema()


#Load Helper Files and Build Tables

#CFS Area
df_Table_CFSArea = spark.read.format("csv").option("header", "true").load("CFS_2012_table_CFSArea.csv")
#option("inferSchema", "true").
#df_Table_CFSArea = spark.read.load("CFS_2012_table_CFSArea.csv", format="csv", sep=",", inferSchema="true", header="true")



#Mode of Transportation 
df_Table_ModeofTrans = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("CFS_2012_table_ModeofTrans.csv")

#NAICS - North American Industry Classificatio System Codes
df_Table_NAICS = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("CFS_2012_table_NAICS.csv")

#SCTG - Standard Classification of Transported Goods Codes
df_Table_SCTG = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("CFS_2012_table_SCTG.csv")
#State Codes
df_Table_StateCodes = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("CFS_2012_table_StateCodes.csv")

#Create Table from DataFrames
df_Table_CFSArea.createOrReplaceTempView("CensusECON_CFSArea")
df_Table_ModeofTrans.createOrReplaceTempView("CensusECON_MoT")
df_Table_NAICS.createOrReplaceTempView("CensusECON_NAICS")
df_Table_SCTG.createOrReplaceTempView("CensusECON_SCTG")
df_Table_StateCodes.createOrReplaceTempView("CensusECON_StateCodes")


#Merge Datasets

mergedatasets = spark.sql("select A.shipmt_id, A.orig_state, H.state as OrigState, A.orig_ma, A.orig_cfs_area, B.description as OrgCSFArea, A.dest_state, H.state as DestState, A.dest_ma, A.dest_cfs_area,  C.description as DestCSFArea, A.naics, D.description as NAICSDesc, A.quarter, A.sctg, E.description as SCTGDesc, A.mode, F.description as ModeDesc, A.shipmt_value, A.shipmt_wght, A.shipmt_dist_gc, A.shipmt_dist_routed, A.temp_cntl_yn, A.export_yn, A.export_cntry, A.hazmat, A.wgt_factor, A.quarter from censusecon as A, censusecon_cfsarea as B, censusecon_cfsarea as C, censusecon_naics as D, censusecon_sctg as E, censusecon_mot as F, censusecon_statecodes as G, censusecon_statecodes as H where A.orig_cfs_area = B.CFSArea and  A.dest_cfs_area = C.CFSArea and A.naics = D.naics and A.sctg = E.sctg and A.mode = F.modecode and A.orig_state = G.code and A.dest_state = H.code")
#PDFmerged = mergedatasets.toPandas()

#Table B1 - Shipments by Mode of Transportation

B1ShipModeTran = spark.sql("select mode, count(shipmt_id) as count, format_number( sum(shipmt_value*wgt_factor), 0) as TotalValue, format_number( sum( wgt_factor*(shipmt_wght/2000)), 0) as TotalTonnage, format_number( (sum( wgt_factor * shipmt_dist_routed)) / sum(wgt_factor), 0) as AvgerageMilesPerShip from CensusEcon group by mode order by mode")
B1ShipModeTran.show()

#Table B1 - Transportation Mode Count

B1TranModeCount = spark.sql("select F.description as ModeDesc, count(A.shipmt_id) as count, format_number( sum(A.shipmt_value*A.wgt_factor), 0) as TotalValue, format_number( sum( A.wgt_factor*(A.shipmt_wght/2000)), 0) as TotalTonnage, format_number( (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor), 0) as AvgerageMilesPerShip from CensusEcon as A, censusecon_mot as F where  A.mode = F.modecode group by  F.description  order by F.description")
B1TranModeCount.show()

#Table B1 - Transportation Mode by Tonnage

B1TranModeTonnage = spark.sql("select F.description as ModeDesc, format_number( sum(A.shipmt_value*A.wgt_factor), 0) as TotalValue,  sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, format_number( (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor), 0) as AvgerageMilesPerShip from CensusEcon as A, censusecon_mot as F where  A.mode = F.modecode group by  F.description order by F.description")
B1TranModeTonnage.show()

#Table B1 - Average Miles Per Shipment by Mode of Transportation

B1AvgMilePerShip = spark.sql("select F.description as ModeDesc, count(A.shipmt_id) as count, format_number( sum(A.shipmt_value*A.wgt_factor), 0) as TotalValue, format_number( sum( A.wgt_factor*(A.shipmt_wght/2000)), 0) as TotalTonnage, format_number( (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor), 0) as AvgerageMilesPerShip from CensusEcon as A, censusecon_mot as F where  A.mode = F.modecode group by  F.description  order by F.description")
B1AvgMilePerShip.show()

#Table B1 - Transportation Mode by Shipment Value

B1TranModeByShipValue = spark.sql("select F.description as ModeDesc, sum(A.shipmt_value*A.wgt_factor) as TotalValue,  sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, format_number( (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor), 0) as AvgerageMilesPerShip from CensusEcon as A, censusecon_mot as F where  A.mode = F.modecode group by  F.description order by F.description")
B1TranModeByShipValue.show()

#Table B2 - Shipments by Classification of Goods

B2ShipByClass = spark.sql("select sctg, count(shipmt_id) as count, format_number( sum(shipmt_value*wgt_factor), 0) as TotalValue, format_number(sum( wgt_factor*(shipmt_wght/2000)),0) as TotalTonnage, format_number((sum( wgt_factor * shipmt_dist_routed)) / sum(wgt_factor),0) as AvgerageMilesPerShip from CensusEcon as A group by sctg order by sctg")
B2ShipByClass.show()

#Table B2 - Shipments by Classification of Goods by Count

B2ShipByClassCount = spark.sql("select E.description as SCTGDesc, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from CensusEcon as A, censusecon_sctg as E where A.sctg = E.sctg group by E.description order by count")
B2ShipByClassCount.show()

#Table B2 - Shipments by Classification of Goods by Value

B2ShipByClassValue = spark.sql("select E.description as SCTGDesc, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from CensusEcon as A, censusecon_sctg as E where A.sctg = E.sctg group by E.description order by TotalValue")
B2ShipByClassValue.show()

#Table B2 - Shipments by Classification of Goods by Tonnage

B2ShipByClassTonnage = spark.sql("select E.description as SCTGDesc, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from CensusEcon as A, censusecon_sctg as E where A.sctg = E.sctg group by E.description order by totaltonnage")
B2ShipByClassTonnage.show()

#Table B2- Shipments by Classification of Goods by Average Miles Per Shipment

B2ShipByClassAvgMilePerShip = spark.sql("select E.description as SCTGDesc, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from CensusEcon as A, censusecon_sctg as E where A.sctg = E.sctg group by E.description order by E.description")
B2ShipByClassAvgMilePerShip.show()

#Table B3

B3Base = spark.sql("select orig_state, count(shipmt_id) as count, format_number(sum(shipmt_value*wgt_factor),0) as TotalValue, format_number(sum( wgt_factor*(shipmt_wght/2000)),0) as TotalTonnage, format_number((sum( wgt_factor * shipmt_dist_routed)) / sum(wgt_factor),0) as AvgerageMilesPerShip from CensusEcon group by orig_state order by orig_state")
B3Base.show()

#Table B3 - Shipment Count by State of Origin

B3ShipCountStateOrigin = spark.sql("select B.state, A.orig_state, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum(A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum(A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from censusecon as A, censusecon_statecodes as B where A.orig_state = B.code group by orig_state, B.state order by count(A.shipmt_id)")
B3ShipCountStateOrigin.show()

#Table B3 - Shipment Value by State of Origin

B3ShipValueStateOrigin = spark.sql("select B.state, A.orig_state, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum(A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from censusecon as A, censusecon_statecodes as B where A.orig_state = B.code group by orig_state, B.state order by TotalValue")
B3ShipValueStateOrigin.show()

#Table B3 - Shipment Count By State of Destination
B3ShipCountStateDest = spark.sql("select B.state, A.dest_state, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum(A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum(A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from censusecon as A, censusecon_statecodes as B where A.dest_state = B.code group by dest_state, B.state order by count(A.shipmt_id)")
B3ShipCountStateDest.show()

#Table B3 - Shipment Value by State of Destination

B3ShipValueStateDest = spark.sql("select B.state, A.dest_state, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum(A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from censusecon as A, censusecon_statecodes as B where A.dest_state = B.code group by dest_state, B.state order by TotalValue")
B3ShipValueStateDest.show()

#Table C1 - Variance by Mode
C1VarianceByMode = spark.sql(" select mode, format_number(exp( 3.844 + 0.039 * log(2.71828, count(shipmt_id) ) - 0.020 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as Value, format_number(exp( 3.761 + 0.076 * log(2.71828, count(shipmt_id) ) - 0.019 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as WGHT, format_number(exp( 4.092 - 0.015 * log(2.71828, count(shipmt_id) ) - 0.012 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as TonMiles, format_number(exp( 5.168 - 0.084 * log(2.71828, count(shipmt_id) ) - 0.376 * log(2.71828, sum(wgt_factor * shipmt_dist_routed) / sum(wgt_factor))), 2)  as AvgMilesShipped from CensusEcon group by mode order by mode")
C1VarianceByMode.show()

#Table C2 - Variance by SCTG

C1VarianceBySTCG = spark.sql("select sctg, format_number(exp( 3.844 + 0.039 * log(2.71828, count(shipmt_id) ) - 0.020 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as Value, format_number(exp( 3.761 + 0.076 * log(2.71828, count(shipmt_id) ) - 0.019 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as WGHT, format_number(exp( 4.092 - 0.015 * log(2.71828, count(shipmt_id) ) - 0.012 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as TonMiles, format_number(exp( 5.168 - 0.084 * log(2.71828, count(shipmt_id) ) - 0.376 * log(2.71828, sum(wgt_factor * shipmt_dist_routed) / sum(wgt_factor))), 2)  as AvgMilesShipped from CensusEcon group by sctg order by sctg")
C1VarianceBySTCG.show()

#Table C3 - Variance by Original State

C1VarianceByOriginalState = spark.sql("select orig_state, format_number(exp( 3.844 + 0.039 * log(2.71828, count(shipmt_id) ) - 0.020 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as Value, format_number(exp( 3.761 + 0.076 * log(2.71828, count(shipmt_id) ) - 0.019 * pow( log(2.71828,count(shipmt_id)), 2 )), 2) as WGHT, format_number(exp( 4.092 - 0.015 * log(2.71828, count(shipmt_id) ) - 0.012 * pow( log(2.71828,count(shipmt_id)), 2 )), 2) as TonMiles, format_number(exp( 5.168 - 0.084 * log(2.71828, count(shipmt_id) ) - 0.376 * log(2.71828, sum(wgt_factor * shipmt_dist_routed) / sum(wgt_factor))), 2)  as AvgMilesShipped from CensusEcon group by orig_state order by orig_state")
C1VarianceByOriginalState.show()

#Shipments by Hazards
HazardsShips = spark.sql("select hazmat, count(hazmat) from CensusEcon group by hazmat order by count(hazmat)")
HazardsShips.show()

#Hazards / Non-Hazards Shipments by Quarter
HazardsShipsByQtr = spark.sql("select quarter, hazmat, count(quarter) from CensusEcon group by quarter, hazmat order by count(quarter)")
HazardsShipsByQtr.show()

#Hazmat Shipments By State of Origin
HazardsShipsByStateOrigin = spark.sql("select B.state, hazmat, count(hazmat) from CensusEcon as A, CensusEcon_Statecodes as B where A.orig_state = B.code and A.hazmat != 'N' group by B.state, A.hazmat order by count(hazmat)")
HazardsShipsByStateOrigin.show()

#Hazmat Shipments by State of Destination 
HazardsShipsByStateDest = spark.sql(" select B.state, hazmat, count(hazmat) from CensusEcon as A, CensusEcon_Statecodes as B where A.dest_state = B.code and A.hazmat != 'N' group by B.state, A.hazmat order by count(hazmat)")
HazardsShipsByStateDest.show()

#States of Origin Shipment Counts by International Destinations
HazardsShipsCountIntOrigin = spark.sql("select B.state, A.export_cntry, count(A.export_cntry) as count from censusecon as A, censusecon_statecodes as B where export_yn = 'Y' and A.orig_state = B.code group by B.state, A.export_cntry order by count")
HazardsShipsCountIntOrigin.show()

#Shipment Count by International Destination
HazardsShipsCountIntDest = spark.sql("select A.export_cntry, count(A.export_cntry) as count from censusecon as A where export_yn = 'Y' group by export_cntry order by count")
HazardsShipsCountIntDest.show()

### Stop Timer
stopTime = time.process_time()
elapsedTime = stopTime-startTime
"Elapsed Process Time: %0.8f" % (elapsedTime)

#Return Paramaters to CDSW User Interface
cdsw.track_metric("ProcTime", elapsedTime)

spark.stop()