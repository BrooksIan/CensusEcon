//import Dep

//Load Dependencies
%AddDeps com.databricks spark-csv_2.11 1.5.0
//%AddDeps org.vegas-viz vegas-spark_2.11 0.3.11 
%AddDeps org.vegas-viz vegas_2.11 0.3.11 

import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer


//Initialize Spark Session 
import org.apache.spark.sql.SparkSession
val spark: SparkSession = SparkSession.builder.appName("CensusMash").master("local[*]").enableHiveSupport().config("spark.sql.warehouse.dir", "target/spark-warehouse").getOrCreate()

spark.version


//Load Data Files
//Create a data frame from CSV File 
val df_WholeSetRaw = spark.sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("/tmp/cfs_2012_pumf_csv.txt")

//Create Table from DataFrame
df_WholeSetRaw.createOrReplaceTempView("CensusECON")

//Display resulting Infered schema 
df_WholeSetRaw.printSchema()


//Load Helper Files and Build Tables

//CFS Area
val df_Table_CFSArea = spark.sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("/tmp/CFS_2012_table_CFSArea.csv")

//Mode of Transportation 
val df_Table_ModeofTrans = spark.sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("/tmp/CFS_2012_table_ModeofTrans.csv")

//NAICS - North American Industry Classificatio System Codes
val df_Table_NAICS = spark.sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("/tmp/CFS_2012_table_NAICS.csv")

//SCTG - Standard Classification of Transported Goods Codes
val df_Table_SCTG = spark.sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("/tmp/CFS_2012_table_SCTG.csv")

//State Codes
val df_Table_StateCodes = spark.sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("/tmp/CFS_2012_table_StateCodes.csv")

//Create Table from DataFrames
df_Table_CFSArea.createOrReplaceTempView("CensusECON_CFSArea")
df_Table_ModeofTrans.createOrReplaceTempView("CensusECON_MoT")
df_Table_NAICS.createOrReplaceTempView("CensusECON_NAICS")
df_Table_SCTG.createOrReplaceTempView("CensusECON_SCTG")
df_Table_StateCodes.createOrReplaceTempView("CensusECON_StateCodes")


//Merge Datasets

val mergedatasets = spark.sqlContext.sql("select A.shipmt_id, A.orig_state, H.state as OrigState, A.orig_ma, A.orig_cfs_area, B.description as OrgCSFArea, A.dest_state, H.state as DestState, A.dest_ma, A.dest_cfs_area,  C.description as DestCSFArea, A.naics, D.description as NAICSDesc, A.quarter, A.sctg, E.description as SCTGDesc, A.mode, F.description as ModeDesc, A.shipmt_value, A.shipmt_wght, A.shipmt_dist_gc, A.shipmt_dist_routed, A.temp_cntl_yn, A.export_yn, A.export_cntry, A.hazmat, A.wgt_factor, A.quarter from censusecon as A, censusecon_cfsarea as B, censusecon_cfsarea as C, censusecon_naics as D, censusecon_sctg as E, censusecon_mot as F, censusecon_statecodes as G, censusecon_statecodes as H where A.orig_cfs_area = B.CFSArea and  A.dest_cfs_area = C.CFSArea and A.naics = D.naics and A.sctg = E.sctg and A.mode = F.modecode and A.orig_state = G.code and A.dest_state = H.code")

// Table B1 - Shipments by Mode of Transportation

val B1ShipModeTran = spark.sqlContext.sql("select mode, count(shipmt_id) as count, format_number( sum(shipmt_value*wgt_factor), 0) as TotalValue, format_number( sum( wgt_factor*(shipmt_wght/2000)), 0) as TotalTonnage, format_number( (sum( wgt_factor * shipmt_dist_routed)) / sum(wgt_factor), 0) as AvgerageMilesPerShip from CensusEcon group by mode order by mode")

// Table B1 - Transportation Mode Count

val B1TranModeCount = spark.sqlContext.sql("select F.description as ModeDesc, count(A.shipmt_id) as count, format_number( sum(A.shipmt_value*A.wgt_factor), 0) as TotalValue, format_number( sum( A.wgt_factor*(A.shipmt_wght/2000)), 0) as TotalTonnage, format_number( (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor), 0) as AvgerageMilesPerShip from CensusEcon as A, censusecon_mot as F where  A.mode = F.modecode group by  F.description  order by F.description")

// Table B1 - Transportation Mode by Tonnage

val B1TranModeTonnage = spark.sqlContext.sql("select F.description as ModeDesc, format_number( sum(A.shipmt_value*A.wgt_factor), 0) as TotalValue,  sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, format_number( (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor), 0) as AvgerageMilesPerShip from CensusEcon as A, censusecon_mot as F where  A.mode = F.modecode group by  F.description order by F.description")

// Table B1 - Average Miles Per Shipment by Mode of Transportation

val B1AvgMilePerShip = spark.sqlContext.sql("select F.description as ModeDesc, count(A.shipmt_id) as count, format_number( sum(A.shipmt_value*A.wgt_factor), 0) as TotalValue, format_number( sum( A.wgt_factor*(A.shipmt_wght/2000)), 0) as TotalTonnage, format_number( (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor), 0) as AvgerageMilesPerShip from CensusEcon as A, censusecon_mot as F where  A.mode = F.modecode group by  F.description  order by F.description")

// Table B1 - Transportation Mode by Shipment Value

val B1TranModeByShipValue = spark.sqlContext.sql("select F.description as ModeDesc, sum(A.shipmt_value*A.wgt_factor) as TotalValue,  sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, format_number( (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor), 0) as AvgerageMilesPerShip from CensusEcon as A, censusecon_mot as F where  A.mode = F.modecode group by  F.description order by F.description")

// Table B2 - Shipments by Classification of Goods

val B2ShipByClass = spark.sqlContext.sql("select sctg, count(shipmt_id) as count, format_number( sum(shipmt_value*wgt_factor), 0) as TotalValue, format_number(sum( wgt_factor*(shipmt_wght/2000)),0) as TotalTonnage, format_number((sum( wgt_factor * shipmt_dist_routed)) / sum(wgt_factor),0) as AvgerageMilesPerShip from CensusEcon as A group by sctg order by sctg")

// Table B2 - Shipments by Classification of Goods by Count

val B2ShipByClassCount = spark.sqlContext.sql("select E.description as SCTGDesc, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from CensusEcon as A, censusecon_sctg as E where A.sctg = E.sctg group by E.description order by count")

// Table B2 - Shipments by Classification of Goods by Value

val B2ShipByClassValue = spark.sqlContext.sql("select E.description as SCTGDesc, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from CensusEcon as A, censusecon_sctg as E where A.sctg = E.sctg group by E.description order by TotalValue")

// Table B2 - Shipments by Classification of Goods by Tonnage

val B2ShipByClassTonnage = spark.sqlContext.sql("select E.description as SCTGDesc, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from CensusEcon as A, censusecon_sctg as E where A.sctg = E.sctg group by E.description order by totaltonnage")

// Table B2- Shipments by Classification of Goods by Average Miles Per Shipment

val B2ShipByClassAvgMilePerShip = spark.sqlContext.sql("select E.description as SCTGDesc, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum( A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from CensusEcon as A, censusecon_sctg as E where A.sctg = E.sctg group by E.description order by E.description")

// Table B3

val B3Base = spark.sqlContext.sql("select orig_state, count(shipmt_id) as count, format_number(sum(shipmt_value*wgt_factor),0) as TotalValue, format_number(sum( wgt_factor*(shipmt_wght/2000)),0) as TotalTonnage, format_number((sum( wgt_factor * shipmt_dist_routed)) / sum(wgt_factor),0) as AvgerageMilesPerShip from CensusEcon group by orig_state order by orig_state")

// Table B3 - Shipment Count by State of Origin

val B3ShipCountStateOrigin = spark.sqlContext.sql("select B.state, A.orig_state, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum(A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum(A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from censusecon as A, censusecon_statecodes as B where A.orig_state = B.code group by orig_state, B.state order by count(A.shipmt_id)")

// Table B3 - Shipment Value by State of Origin

val B3ShipValueStateOrigin = spark.sqlContext.sql("select B.state, A.orig_state, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum(A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from censusecon as A, censusecon_statecodes as B where A.orig_state = B.code group by orig_state, B.state order by TotalValue")

// Table B3 - Shipment Count By State of Destination
val B3ShipCountStateDest = spark.sqlContext.sql("select B.state, A.dest_state, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum(A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum(A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from censusecon as A, censusecon_statecodes as B where A.dest_state = B.code group by dest_state, B.state order by count(A.shipmt_id)")

// Table B3 - Shipment Value by State of Destination

val B3ShipValueStateDest = spark.sqlContext.sql("select B.state, A.dest_state, count(A.shipmt_id) as count, sum(A.shipmt_value*A.wgt_factor) as TotalValue, sum( A.wgt_factor*(A.shipmt_wght/2000)) as TotalTonnage, (sum(A.wgt_factor * A.shipmt_dist_routed)) / sum(A.wgt_factor) as AvgerageMilesPerShip from censusecon as A, censusecon_statecodes as B where A.dest_state = B.code group by dest_state, B.state order by TotalValue")

// Table C1 - Variance by Mode
val C1VarianceByMode = spark.sqlContext.sql(" select mode, format_number(exp( 3.844 + 0.039 * log(2.71828, count(shipmt_id) ) - 0.020 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as Value, format_number(exp( 3.761 + 0.076 * log(2.71828, count(shipmt_id) ) - 0.019 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as WGHT, format_number(exp( 4.092 - 0.015 * log(2.71828, count(shipmt_id) ) - 0.012 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as TonMiles, format_number(exp( 5.168 - 0.084 * log(2.71828, count(shipmt_id) ) - 0.376 * log(2.71828, sum(wgt_factor * shipmt_dist_routed) / sum(wgt_factor))), 2)  as AvgMilesShipped from CensusEcon group by mode order by mode")

// Table C2 - Variance by SCTG

val C1VarianceBySTCG = spark.sqlContext.sql("select sctg, format_number(exp( 3.844 + 0.039 * log(2.71828, count(shipmt_id) ) - 0.020 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as Value, format_number(exp( 3.761 + 0.076 * log(2.71828, count(shipmt_id) ) - 0.019 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as WGHT, format_number(exp( 4.092 - 0.015 * log(2.71828, count(shipmt_id) ) - 0.012 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as TonMiles, format_number(exp( 5.168 - 0.084 * log(2.71828, count(shipmt_id) ) - 0.376 * log(2.71828, sum(wgt_factor * shipmt_dist_routed) / sum(wgt_factor))), 2)  as AvgMilesShipped from CensusEcon group by sctg order by sctg")

// Table C3 - Variance by Original State

val C1VarianceByOriginalState = spark.sqlContext.sql("select orig_state, format_number(exp( 3.844 + 0.039 * log(2.71828, count(shipmt_id) ) - 0.020 * pow( log(2.71828,count(shipmt_id)), 2)), 2) as Value, format_number(exp( 3.761 + 0.076 * log(2.71828, count(shipmt_id) ) - 0.019 * pow( log(2.71828,count(shipmt_id)), 2 )), 2) as WGHT, format_number(exp( 4.092 - 0.015 * log(2.71828, count(shipmt_id) ) - 0.012 * pow( log(2.71828,count(shipmt_id)), 2 )), 2) as TonMiles, format_number(exp( 5.168 - 0.084 * log(2.71828, count(shipmt_id) ) - 0.376 * log(2.71828, sum(wgt_factor * shipmt_dist_routed) / sum(wgt_factor))), 2)  as AvgMilesShipped from CensusEcon group by orig_state order by orig_state")

// Shipments by Hazards
val HazardsShips = spark.sqlContext.sql("select hazmat, count(hazmat) from CensusEcon group by hazmat order by count(hazmat)")

// Hazards / Non-Hazards Shipments by Quarter
val HazardsShipsByQtr = spark.sqlContext.sql("select quarter, hazmat, count(quarter) from CensusEcon group by quarter, hazmat order by count(quarter)")

// Hazmat Shipments By State of Origin
val HazardsShipsByStateOrigin = spark.sqlContext.sql("select B.state, hazmat, count(hazmat) from CensusEcon as A, CensusEcon_Statecodes as B where A.orig_state = B.code and A.hazmat != 'N' group by B.state, A.hazmat order by count(hazmat)")

// Hazmat Shipments by State of Destination 
val HazardsShipsByStateDest = spark.sqlContext.sql(" select B.state, hazmat, count(hazmat) from CensusEcon as A, CensusEcon_Statecodes as B where A.dest_state = B.code and A.hazmat != 'N' group by B.state, A.hazmat order by count(hazmat)")

// States of Origin Shipment Counts by International Destinations
val HazardsShipsCountIntOrigin = spark.sqlContext.sql("select B.state, A.export_cntry, count(A.export_cntry) as count from censusecon as A, censusecon_statecodes as B where export_yn = 'Y' and A.orig_state = B.code group by B.state, A.export_cntry order by count")

// Shipment Count by International Destination
val HazardsShipsCountIntDest = spark.sqlContext.sql("select A.export_cntry, count(A.export_cntry) as count from censusecon as A where export_yn = 'Y' group by export_cntry order by count")

