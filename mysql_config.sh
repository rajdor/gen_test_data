#!/bin/bash
echo "################################################################################"
echo "## Start ${0}"

export DATADIR=${workingDir}/G-NAF
export pathToOptionsFile=~/projects-gitea/gen_data_mysql/mysql.cnf
export DBHOST=$(awk -F '=' '{if (! ($0 ~ /^;/) && $0 ~ /host/) print $2}' ~/projects-gitea/gen_data_mysql/mysql.cnf)
export DBNAME=$(awk -F '=' '{if (! ($0 ~ /^;/) && $0 ~ /database/) print $2}' ~/projects-gitea/gen_data_mysql/mysql.cnf)

runSQL_check_and_get()
{

 sql=${1}   
 
 values=$(mysql --defaults-extra-file=${pathToOptionsFile} --silent --execute "${sql}")
 retVal=$?
 if [ ${retVal} -ne 0 ]; then
    printf "Error in ${sql}\nReturn Code : ${retVal}\nExiting..."
    exit ${retVal}
 fi
 echo ${values}
}

runSQL_and_check()
{
  sql=${1}   
  mysql --defaults-extra-file=${pathToOptionsFile} --silent --raw --execute "${sql}"
  retVal=$?
  if [ ${retVal} -ne 0 ]; then
     printf "Error in ${sql}\nReturn Code : ${retVal}\nExiting..."
     exit ${retVal}
  fi
}

runSQLFile_and_check()
{
  sql=${1}
  mysql --defaults-extra-file=${pathToOptionsFile} --verbose < "${sql}"
  retVal=$?
  if [ ${retVal} -ne 0 ]; then
     printf "Error in ${sql}\nReturn Code : ${retVal}\nExiting..."
     exit ${retVal}
  fi
}

function ask_yes_or_no() {
    read -p "$1 ([y]es or [N]o): "
    case $(echo $REPLY | tr '[A-Z]' '[a-z]') in
        y|yes) echo "yes" ;;
        *)     echo "no" ;;
    esac
}

function loadThis() {
  inputFile=$1
  tableName=$2
  
  additionalOptions=""
  if [[ $# -eq 3 ]]; then
    additionalOptions=$3
  fi
  
  echo "------------------------------------------------------------"
  echo "-- Loading ${inputFile} to ${DBHOST} ${DBNAME} ${tableName}"
  echo "------------------------------------------------------------"
  mysql  --defaults-extra-file=${pathToOptionsFile} --verbose --local-infile=1 --execute="LOAD DATA LOCAL INFILE '${inputFile}' INTO TABLE ${tableName}  FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"' ${additionalOptions} ; SHOW WARNINGS"
}


echo "## End ${0}"
