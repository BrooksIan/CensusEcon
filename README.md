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

## Pre-Run Instructions


1. Upload the source data file  [CFS 2012 csv] (https://www.census.gov/econ/cfs/pums.html) to HDFS in the /tmp directory 

2. Upload helper files to the HDFS in the /tmp directory 
Upload all of the helper files to HDFS in the /tmp directory 

a. CFS_2012_table_CFSArea.csv

b. CFS_2012_table_ModeofTrans.csv

c. CFS_2012_table_NAICS.csv

d. CFS_2012_table_SCTG.csv

e. CFS_2012_table_StateCodes.csv


3. Log into Apache Ambari 

4. In Ambari, select "Files View" and upload all of the CSV files to the /tmp/ directory.  For assistance, please use the following [tutorial.](https://fr.hortonworks.com/tutorial/loading-and-querying-data-with-hadoop/)

5. In Zeppelin, download the Zeppelin Note [JSON file.](https://github.com/BrooksIan/CensusEcon) For assistance, please use the following [tutorial](https://hortonworks.com/tutorial/getting-started-with-apache-zeppelin/)

## License
Unlike all other Apache projects which use Apache license, this project uses an advanced and modern license named The Star And Thank Author License (SATA). Please see the [LICENSE](LICENSE) file for more information.