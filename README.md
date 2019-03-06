## Data Science in Apache Spark
### Census - Econ Workbook
#### Report Building

**Level**: Easy
**Language**: Scala

**Requirements**: 
- [HDP 2.6.X]
- Spark 2.x

**Author**: Ian Brooks
**Follow** [LinkedIn - Ian Brooks PhD] (https://www.linkedin.com/in/ianrbrooksphd/)

![Census](https://yt3.ggpht.com/a-/AN66SAwtUzDFUsvm7MJszIS3ccgglaFGrw1_Ye16ew=s900-mo-c-c0xffffffff-rj-k-no "Census")


## File Description

The PUM file includes 20 variables for all usable shipment records collected by the CFS – a total of 4,547,661 shipments from approximately 60,000 responding establishments.  The information included on each shipment record is:
*	Shipment Origin
    *	State 
    *	Metropolitan Area
*	Shipment Destination (in US)
    *	State
    *	Metropolitan Area
*	NAICS industry classification of the shipper
*	Quarter in 2012 in which the shipment was made
*	Type of commodity 
*	Mode of transportation
*	The value of the shipment (dollars)
*	The weight of the shipment (pounds)
*	The great circle distance between the shipment origin and US destination (in miles)
*	The routed distance between the shipment origin and US destination (in miles) 
*	Whether or not the shipment required temperature control during transportation
*	Whether or not the shipment was an export
*	If an export, the final export destination country
*	Hazardous material code
*	Shipment tabulation weighting factor 

##Column Descriptions 

* SHIPMT_ID   Shipment identifier	
* ORIG_STATE	FIPS state code of shipment origin	
* ORIG_MA	Metro area of shipment origin	
* ORIG_CFS_AREA	CFS Area of shipment origin	Concatenation of ORIG_STATE and ORIG_MA (ex: 24-12580)
* DEST_STATE	FIPS state code of shipment destination	01-56
* DEST_MA	Metro area of shipment destination	See Note (1)
* DEST_CFS_AREA	CFS Area of shipment destination	Concatenation of DEST_STATE and DEST_MA (ex: 01-142)
* NAICS	Industry classification of shipper	See Note (2)
* QUARTER	Quarter of 2012 in which the shipment occurred	1, 2, 3, 4
* SCTG	2-digit SCTG commodity code of the shipment	See Note (3)
* MODE	Mode of transportation of the shipment	See Note (4)
* SHIPMT_VALUE	Value of the shipment in dollars	0 - 999,999,999
* SHIPMT_WGHT	Weight of the shipment in pounds	0 - 999,999,999
* SHIPMT_DIST_GC	Great circle distance between ship-ment origin and destination (in miles)	0 - 99,999
* SHIPMT_DIST_ROUTED	Routed distance between shipment origin and destination (in miles)	0 - 99,999
* TEMP_CNTL_YN	Temperature controlled shipment - Yes or No	Y, N
* EXPORT_YN	Export shipment - Yes or No	Y, N
* EXPORT_CNTRY	Export final destination	
	* C = Canada
	* M = Mexico
	* O = Other country
	* N = Not an export
* HAZMAT	Hazardous material (HAZMAT) code	
	* P = Class 3.0 Hazmat (flammable liquids)
	* H = Other Hazmat
	* N = Not Hazmat
* WGT_FACTOR	Shipment tabulation weighting factor.  (This factor is also an estimate of the total number of shipments represent-ted by the PUM file shipment.)	0 – 975,000.0

## Pre-Run Instructions

### For HDP with Apache Zeppelin
1. Log into Apache Ambari 

2. In Ambari, select "Files View" and upload all of the CSV files to the /tmp/ directory.  For assistance, please use the following [tutorial.](https://fr.hortonworks.com/tutorial/loading-and-querying-data-with-hadoop/)

3. Upload the source data file  [CFS 2012 csv] (https://www.census.gov/econ/cfs/pums.html) to HDFS in the /tmp directory 

4. Upload helper files to the HDFS in the /tmp directory 
Upload all of the helper files to HDFS in the /tmp directory 

a. CFS_2012_table_CFSArea.csv

b. CFS_2012_table_ModeofTrans.csv

c. CFS_2012_table_NAICS.csv

d. CFS_2012_table_SCTG.csv

e. CFS_2012_table_StateCodes.csv


5. In Zeppelin, download the Zeppelin Note [JSON file.](https://github.com/BrooksIan/CensusEcon) For assistance, please use the following [tutorial](https://hortonworks.com/tutorial/getting-started-with-apache-zeppelin/)

### For Cloudera Data Science Workbench
1. Log into CDSW and upload the project
2. Open a terminal on a session and run the loaddata.sh

## License
Unlike all other Apache projects which use Apache license, this project uses an advanced and modern license named The Star And Thank Author License (SATA). Please see the [LICENSE](LICENSE) file for more information.