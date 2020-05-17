#!/bin/bash

source ../mysql_config.sh
datadir=../01_preprocessing/

echo "------------------------------------------------------------"
echo "-- Before loading data from stagng file system; this script will drop all the tables and views in ${DBHOST}.${DBNAME}"
echo "------------------------------------------------------------"

if [[ "no" == $(ask_yes_or_no "Are you sure?") || \
      "no" == $(ask_yes_or_no "Are you *really* sure?") ]]
then
    echo "Exiting"
    exit 0
fi


echo "------------------------------------------------------------"
echo "-- Dropping all objects in ${DBHOST} ${DBNAME}"
echo "------------------------------------------------------------"
views_exist=$(runSQL_check_and_get "select count(*) from information_schema.views WHERE table_schema = '"${DBNAME}"'")
echo "  Found ${views_exist} views existing"
if [[ ${views_exist} > 0 ]]; then 
   echo "  Generating Drop Views"
   stmts=$(runSQL_check_and_get "SELECT concat('DROP VIEW ', table_name, ';') as stmt FROM information_schema.views WHERE table_schema = '"${DBNAME}"'")
   runSQL_and_check "${stmts}"
fi

tables_exist=$(runSQL_check_and_get  "select count(*) from information_schema.tables where table_schema = '"${DBNAME}"' and TABLE_TYPE = 'BASE TABLE'")
echo "  Found ${tables_exist} tables existing"
if [[ ${tables_exist} > 0 ]]; then 
   echo "  Generating Drop Tables"
   stmts=$(runSQL_check_and_get "SELECT concat('SET FOREIGN_KEY_CHECKS = 0; DROP TABLE ', table_name, '; SET FOREIGN_KEY_CHECKS = 1;') as stmt FROM information_schema.tables WHERE table_schema = '"${DBNAME}"' and TABLE_TYPE = 'BASE TABLE'")
   echo "  Dropping Tables"
   runSQL_and_check "${stmts}"
fi


echo "------------------------------------------------------------"
echo "-- Creating Datamodel tables in ${DBHOST} ${DBNAME}"
echo "------------------------------------------------------------"
runSQLFile_and_check ../datamodel/datamodel.sql

runSQL_and_check "SHOW GLOBAL VARIABLES LIKE 'local_infile';"
runSQL_and_check "SET GLOBAL local_infile = 'ON';"
runSQL_and_check "SHOW GLOBAL VARIABLES LIKE 'local_infile';"

loadThis ${datadir}ref_colors/ref_colors.txt           "ref_colors"
loadThis ${datadir}ref_first_names/ref_first_names.txt "ref_first_names"
loadThis ${datadir}ref_last_names/ref_last_names.txt   "ref_last_names"
loadThis ${datadir}ref_vehicle/ref_vehicle.txt         "ref_vehicle"
loadThis ${datadir}ref_codes/recovery_type.txt         "recovery_type"
loadThis ${datadir}ref_codes/claim_status.txt          "claim_status"
loadThis ${datadir}ref_codes/policy_status.txt         "policy_status"
loadThis ${datadir}ref_codes/party_role.txt            "party_role"
loadThis ${datadir}ref_address/ref_address.txt         "ref_address"


echo "------------------------------------------------------------"
echo "Looking for tables to run Analyze"
echo "------------------------------------------------------------"
tables_exist=$(runSQL_check_and_get "select count(*) from information_schema.tables where table_schema = '"${DBNAME}"' and TABLE_TYPE = 'BASE TABLE'")
echo "  Found ${tables_exist} tables existing"
if [[ ${tables_exist} > 0 ]]; then
   echo "  Generating Analyze Table statements"
   stmts=$(runSQL_check_and_get "SELECT concat('ANALYZE TABLE ', table_name, ';') as stmt FROM information_schema.tables WHERE table_schema = '"${DBNAME}"' and TABLE_TYPE = 'BASE TABLE'")
   echo "  Running Analyze tables"
   runSQL_and_check "${stmts}"
fi


echo "------------------------------------------------------------"
echo "Looking for tables to run Count"
echo "------------------------------------------------------------"
tables_exist=$(runSQL_check_and_get "select count(*) from information_schema.tables where table_schema = '"${DBNAME}"' and TABLE_TYPE = 'BASE TABLE'")
echo "  Found ${tables_exist} tables existing"
if [[ ${tables_exist} > 0 ]]; then 
   echo "  Generating Count statements"
   stmts=$(runSQL_check_and_get "SELECT concat('SELECT ''', table_name, ''' ,COUNT(*) FROM ', table_name, ';') as stmt FROM information_schema.tables WHERE table_schema = '"${DBNAME}"' and TABLE_TYPE = 'BASE TABLE'")
   echo "  Running Count tables"
   runSQL_and_check "${stmts}"
fi

echo "------------------------------------------------------------"
echo "Removing Staged data"
echo "------------------------------------------------------------"
rm -rf ${datadir}ref_colors
rm -rf ${datadir}ref_first_names
rm -rf ${datadir}ref_last_names
rm -rf ${datadir}ref_vehicle
rm -rf ${datadir}ref_codes
rm -rf ${datadir}ref_address

echo "## End ${0}"