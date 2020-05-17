#!/bin/bash

source ../mysql_config.sh

export rawDir=../00_rawdata/
export rawFilename=feb20_gnaf_pipeseparatedvalue.zip

export outputDir=./ref_address/
export outputFilename=ref_address.txt

echo "rawDir        : ${rawDir}"
echo "rawFilename   : ${rawFilename}"
echo "outputDir     : ${outputDir}"
echo "outputFilename: ${outputFilename}"

echo "Resetting outputDir Directory : ${outputDir}"
rm -rf ${outputDir}
mkdir ${outputDir}

echo "Copying ${rawDir}${rawFilename} to ${outputDir}"
cp ${rawDir}${rawFilename} ${outputDir}

echo "Unzipping ${outputDir}${rawFilename}"
unzip ${outputDir}${rawFilename} -d ${outputDir}

echo "------------------------------------------------------------"
echo "-- CREATING TABLES in ${DBHOST} ${DBNAME}"
echo "------------------------------------------------------------"

echo "------------------------------------------------------------"
echo "Drop existing objects"
echo "------------------------------------------------------------"
python3 getObjects.py ${outputDir}/G-NAF/Extras/GNAF_View_Scripts/address_view.sql | while read -r line ; do
    echo ${line}
    runSQL_and_check "${line}"
done

python3 getObjects.py ${outputDir}/G-NAF/Extras/GNAF_TableCreation_Scripts/add_fk_constraints.sql | while read -r line ; do
    echo ${line}  
    runSQL_and_check "${line}"
done

python3 getObjects.py ${outputDir}/G-NAF/Extras/GNAF_TableCreation_Scripts/create_tables_ansi.sql | while read -r line ; do
    echo ${line}  
    runSQL_and_check "${line}"
done


echo "------------------------------------------------------------"
echo "Creating Tables"
echo "------------------------------------------------------------"
runSQLFile_and_check ${outputDir}/G-NAF/Extras/GNAF_TableCreation_Scripts/create_tables_ansi.sql

echo "------------------------------------------------------------"
echo "Creating Views"
echo "------------------------------------------------------------"
runSQLFile_and_check ${outputDir}/G-NAF/Extras/GNAF_View_Scripts/address_view.sql

echo "------------------------------------------------------------"
echo "Loading Data (only those tables required for ADDRESS_VIEW"
echo "------------------------------------------------------------"
runSQL_and_check "SHOW GLOBAL VARIABLES LIKE 'local_infile';"
runSQL_and_check "SET GLOBAL local_infile = 'ON';"
runSQL_and_check "SHOW GLOBAL VARIABLES LIKE 'local_infile';"

# Only the tables that the ADDRESS VIEW uses
IFS=$'\n'

find "${outputDir}/G-NAF/" -type f -name *.psv -print0 | while IFS= read -r -d '' f; do
      filename=${f}
      tablename=$(basename -- ${f})
      tablename=${tablename%.psv}
      tablename=${tablename/_psv/}
      tablename=${tablename/Authority_Code_/}
      tablename=${tablename/WA_/}
      tablename=${tablename/VIC_/}
      tablename=${tablename/QLD_/}
      tablename=${tablename/SA_/}
      tablename=${tablename/TAS_/}
      tablename=${tablename/ACT_/}
      tablename=${tablename/NSW_/}
      tablename=${tablename/NT_/}
      tablename=${tablename/OT_/}

      if [[ ${tablename} == *"ADDRESS_DETAIL"* ]] || [[ ${tablename} == *"FLAT_TYPE_AUT"* ]] || [[ ${tablename} == *"LEVEL_TYPE_AUT"* ]] || [[ ${tablename} == *"STREET_LOCALITY"* ]] || [[ ${tablename} == *"STREET_SUFFIX_AUT"* ]] || [[ ${tablename} == *"STREET_CLASS_AUT"* ]] || [[ ${tablename} == *"LOCALITY"* ]] || [[ ${tablename} == *"ADDRESS_DEFAULT_GEOCODE"* ]] || [[ ${tablename} == *"GEOCODE_TYPE_AUT"* ]] || [[ ${tablename} == *"STATE"* ]]; then
         echo "------------------------------------------------------------"
         echo "Loading ${tablename} from ${filename}"
         loadThis ${filename} ${tablename} "IGNORE 1 LINES"
         
       else 
         echo "------------------------------------------------------------"
         echo "Skipping : ${filename}"
       fi
done


echo "------------------------------------------------------------"
echo "Creating Indexs for View"
echo "------------------------------------------------------------"
echo "------------------------------------------------------------"
echo "Looking for indexes to clear out first"
echo "------------------------------------------------------------"
indexes_exist=$(runSQL_check_and_get "select count(*) from information_schema.STATISTICS where table_schema = '"${DBNAME}"' and index_name like 'U%'")
echo "  Found ${indexes_exist} indexes existing"
if [[ ${indexes_exist} > 0 ]]; then 
   echo "  Generating alter Table statements"
   stmts=$(runSQL_check_and_get "SELECT CONCAT('ALTER TABLE ', TABLE_NAME, ' DROP INDEX ', INDEX_NAME, ';') FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '"${DBNAME}"' and index_name like 'U%'")
   echo "  Running alter drop indexs"
   #echo "${stmts}"
   runSQL_and_check "${stmts}"
fi

echo "Creating Indexes"
runSQL_and_check "CREATE UNIQUE INDEX UI01_FLAT_TYPE_AUT           ON FLAT_TYPE_AUT           (CODE)                  ;"
runSQL_and_check "CREATE UNIQUE INDEX UI01_LEVEL_TYPE_AUT          ON LEVEL_TYPE_AUT          (CODE)                  ;"
runSQL_and_check "CREATE UNIQUE INDEX UI01_STREET_LOCALITY         ON STREET_LOCALITY         (STREET_LOCALITY_PID)   ;"
runSQL_and_check "CREATE UNIQUE INDEX UI01_STREET_SUFFIX_AUT       ON STREET_SUFFIX_AUT       (CODE)                  ;"
runSQL_and_check "CREATE UNIQUE INDEX UI01_STREET_CLASS_AUT        ON STREET_CLASS_AUT        (CODE)                  ;"
runSQL_and_check "CREATE UNIQUE INDEX UI01_STREET_TYPE_AUT         ON STREET_TYPE_AUT         (CODE)                  ;"
runSQL_and_check "CREATE UNIQUE INDEX UI01_LOCALITY                ON LOCALITY                (LOCALITY_PID)          ;"
runSQL_and_check "CREATE UNIQUE INDEX UI01_ADDRESS_DEFAULT_GEOCODE ON ADDRESS_DEFAULT_GEOCODE (ADDRESS_DETAIL_PID)    ;"
runSQL_and_check "CREATE UNIQUE INDEX UI01_GEOCODE_TYPE_AUT        ON GEOCODE_TYPE_AUT        (CODE)                  ;"
runSQL_and_check "CREATE UNIQUE INDEX UI01_GEOCODED_LEVEL_TYPE_AUT ON GEOCODED_LEVEL_TYPE_AUT (CODE)                  ;"
runSQL_and_check "CREATE UNIQUE INDEX UI01_STATE                   ON STATE                   (STATE_PID)             ;"

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
echo "-- Generating 'My' Address Table"
echo "------------------------------------------------------------"
echo "------------------------------------------------------------"
echo "Creating My Address Table"
echo "------------------------------------------------------------"
runSQL_and_check "
DROP TABLE IF EXISTS ADDRESS CASCADE;
"
runSQL_and_check "CREATE TABLE ADDRESS (
  ADDRESS_ID bigint(20) NOT NULL,
  ADDRESS_GNAF varchar(15) DEFAULT NULL,
  ADDRESS_BUILDING_NAME varchar(200) DEFAULT NULL,
  ADDRESS_LOT varchar(32) DEFAULT NULL,
  ADDRESS_FLAT varchar(89) DEFAULT NULL,
  ADDRESS_LEVEL varchar(89) DEFAULT NULL,
  ADDRESS_STREET varchar(212) DEFAULT NULL,
  ADDRESS_SUBURB varchar(100) DEFAULT NULL,
  ADDRESS_STATE varchar(3) DEFAULT NULL,
  ADDRESS_POSTCODE varchar(4) DEFAULT NULL,
  PRIMARY KEY (ADDRESS_ID)
) ;
"

echo "------------------------------------------------------------"
echo "Inserting into 'My' Address Table"
echo "------------------------------------------------------------"
runSQL_and_check "
INSERT INTO ADDRESS 
select @i:=@i+1          AS ADDRESS_ID
   , ADDRESS_DETAIL_PID  AS ADDRESS_GNAF
   , BUILDING_NAME       AS ADDRESS_BUILDING_NAME
   , TRIM(REPLACE(CONCAT(LOT_NUMBER_PREFIX , ' ' , LOT_NUMBER         , ' ', LOT_NUMBER_SUFFIX), '  ', ' '))                                   AS ADDRESS_LOT
   , TRIM(REPLACE(REPLACE(CONCAT(FLAT_TYPE , ' ' , FLAT_NUMBER_PREFIX , ' ', CASE WHEN FLAT_NUMBER  ='0' THEN '' ELSE FLAT_NUMBER  END , ' ', FLAT_NUMBER_SUFFIX) , '  ', ' '),'  ', ' '))   AS ADDRESS_FLAT
   , TRIM(REPLACE(REPLACE(CONCAT(LEVEL_TYPE, ' ' , LEVEL_NUMBER_PREFIX, ' ', CASE WHEN LEVEL_NUMBER ='0' THEN '' ELSE LEVEL_NUMBER END, ' ', LEVEL_NUMBER_SUFFIX), '  ', ' '),'  ', ' '))   AS ADDRESS_LEVEL
   , TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CONCAT(NUMBER_FIRST_PREFIX, ' ',NUMBER_FIRST, ' ',NUMBER_FIRST_SUFFIX, ' ',NUMBER_LAST_PREFIX, ' ',CASE WHEN NUMBER_LAST  ='0' THEN '' ELSE NUMBER_LAST  END, ' ',NUMBER_LAST_SUFFIX, ' ',STREET_NAME, ' ',STREET_TYPE_CODE, ' ' , STREET_SUFFIX_CODE), '  ', ' '),'  ', ' '), '  ', ' '),'  ', ' '), '  ', ' '),'  ', ' ')) AS ADDRESS_STREET
   ,LOCALITY_NAME      AS ADDRESS_SUBURB
   ,STATE_ABBREVIATION AS ADDRESS_STATE
   ,POSTCODE           AS ADDRESS_POSTCODE   
   
FROM ADDRESS_VIEW
,    (SELECT @i:=0) AS foo
;
"

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
echo "Drop existing objects"
echo "------------------------------------------------------------"
python3 getObjects.py ${outputDir}/G-NAF/Extras/GNAF_View_Scripts/address_view.sql | while read -r line ; do
    echo ${line}
    runSQL_and_check "${line}"
done

python3 getObjects.py ${outputDir}/G-NAF/Extras/GNAF_TableCreation_Scripts/add_fk_constraints.sql | while read -r line ; do
    echo ${line}  
    runSQL_and_check "${line}"
done

python3 getObjects.py ${outputDir}/G-NAF/Extras/GNAF_TableCreation_Scripts/create_tables_ansi.sql | while read -r line ; do
    echo ${line}  
    runSQL_and_check "${line}"
done

echo "------------------------------------------------------------"
echo "Resetting Working Directory : ${outputDir}"
echo "------------------------------------------------------------"
rm -rf ${outputDir}
mkdir ${outputDir}


echo "------------------------------------------------------------"
echo "Export to pipe delimited"
echo "------------------------------------------------------------"
runSQL_and_check "SELECT * FROM ADDRESS" | tr '\t' '|' > ${outputDir}/${outputFilename}


ls -al ${outputDir}
head ${outputDir}/${outputFilename}

echo "## End ${0}"