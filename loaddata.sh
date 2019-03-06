# Download data from Github
wget https://raw.githubusercontent.com/BrooksIan/CensusEcon/master/CFS_2012_table_CFSArea.csv -O /tmp/CFS_2012_table_CFSArea.csv
wget https://raw.githubusercontent.com/BrooksIan/CensusEcon/master/CFS_2012_table_ModeofTrans.csv -O /tmp/CFS_2012_table_ModeofTrans.csv
wget https://raw.githubusercontent.com/BrooksIan/CensusEcon/master/CFS_2012_table_NAICS.csv -O /tmp/CFS_2012_table_NAICS.csv
wget https://raw.githubusercontent.com/BrooksIan/CensusEcon/master/CFS_2012_table_SCTG.csv -O /tmp/CFS_2012_table_SCTG.csv
wget https://raw.githubusercontent.com/BrooksIan/CensusEcon/master/CFS_2012_table_StateCodes.csv -O /tmp/CFS_2012_table_StateCodes.csv

# Download data from Census
wget https://www2.census.gov/programs-surveys/cfs/datasets/2012/2012-pums-files/cfs-2012-pumf-csv.zip -O /tmp/cfs-2012-pumf-csv.zip -o /dev/null

# Unzip Census Data
cd /tmp/
unzip cfs-2012-pumf-csv.zip

ls /tmp/CFS*

# Make HDFS Directory and Load CSV files into HDFS
hadoop fs -mkdir /tmp/
hadoop fs -put /tmp/CFS_2012_table_CFSArea.csv /tmp/CFS_2012_table_CFSArea.csv
hadoop fs -put /tmp/CFS_2012_table_ModeofTrans.csv /tmp/CFS_2012_table_ModeofTrans.csv
hadoop fs -put /tmp/CFS_2012_table_NAICS.csv /tmp/CFS_2012_table_NAICS.csv
hadoop fs -put /tmp/CFS_2012_table_SCTG.csv /tmp/CFS_2012_table_SCTG.csv
hadoop fs -put /tmp/CFS_2012_table_StateCodes.csv /tmp/CFS_2012_table_StateCodes.csv
hadoop fs -put /tmp/cfs_2012_pumf_csv.txt /tmp/cfs_2012_pumf_csv.txt

# List files in HDFS
hadoop fs -ls /tmp/CFS_2012*
hadoop fs -ls /tmp/cfs*.txt