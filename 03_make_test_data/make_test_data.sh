#!/bin/bash

source ../mysql_config.sh

python3 generic_gen.py --config=../mysql.cnf --schema=staging --table=employee     --iterations=20     --insert=True
python3 generic_gen.py --config=../mysql.cnf --schema=staging --table=customer     --iterations=2000   --insert=True
python3 generic_gen.py --config=../mysql.cnf --schema=staging --table=motor_policy --iterations=20000  --insert=True
python3 generic_gen.py --config=../mysql.cnf --schema=staging --table=party        --iterations=2500   --insert=True
python3 generic_gen.py --config=../mysql.cnf --schema=staging --table=portfolio    --iterations=500    --insert=True
python3 generic_gen.py --config=../mysql.cnf --schema=staging --table=motor_claim  --iterations=10000  --insert=True
python3 generic_gen.py --config=../mysql.cnf --schema=staging --table=recovery     --iterations=50000  --insert=True


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