#import ibm_db
import mysql.connector
import logging
from time import sleep
import math
import os
import sys
import inspect
import optparse

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from mysql_helpers import getRandomLoadDate
from mysql_helpers import getRandomQRMAccount
from mysql_helpers import getMaxID
from mysql_helpers import stringafy
from mysql_helpers import makeRandomValue
from mysql_helpers import getDDL
from mysql_helpers import getPrimaryKey
from mysql_helpers import countRows
from mysql_helpers import getRandomFirstName
from mysql_helpers import getRandomLastName
from mysql_helpers import getRandomID
from mysql_helpers import getRandomAddress
from mysql_helpers import getRandomRegistration
from mysql_helpers import getRandomColor
from mysql_helpers import getRandomWeather
from mysql_helpers import getRandomLocation
from mysql_helpers import getRandomChoice
from mysql_helpers import getRandomClaimDescription
from mysql_helpers import deleteRandomRecord
from mysql_helpers import myLogFormatter
logger = logging.getLogger('')
logger.setLevel(logging.INFO)

#use my formatter for date and time
formatter = myLogFormatter(fmt='%(asctime)s %(levelname)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S.%f')

#create an additional handler to also log to stdout (Console Handler)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

logger.addHandler(ch)

#Example Usage:
#Note, usage with an empty table is a problem when also doing deletes and updates.  ?? Some commit problem with mysql??? 
#      First run with Inserts only
#      python3 generic_gen.py --config=../mysql.cnf --schema=staging --table=employee --iterations=10  --insert=True
#
#python3 generic_gen.py --config=../mysql.cnf --schema=staging --table=employee --iterations=10  --sleepamt=3 --insert=True --update=True  --updatefreq=3 --deletefreq=4 --delete=True
parser = optparse.OptionParser()
parser.add_option('-c', '--config'     , action="store", dest="mysql_option_file" , help="mysql cnf file", default="../mysql.cnf")
parser.add_option('-n', '--iterations' , action="store", dest="iterations"        , help="Number of iterations to perform", default="10000")
parser.add_option('-k', '--kickoff'    , action="store", dest="kickoff"           , help="Starting id to be used, default is max id+1 from the specified table;  Use this for running for the same table in parellel and defined ranges.", default="-1")
parser.add_option('-i', '--insert'     , action="store", dest="doInserts"         , help="For each iteration perform an Insert", default=True)
parser.add_option('-u', '--update'     , action="store", dest="doUpdates"         , help="For each iteration AND frequency combination perform an Update on a random record; Note, where large gaps in sequential ID's exist, this may cause many retries to find an existing record.", default=False)
parser.add_option('-d', '--delete'     , action="store", dest="doDeletes"         , help="For each iteration AND frequency combination perform a Delete on a random record; Note, where large gaps in sequential ID's exist, this may cause many retries to find an existing record.", default=False)
parser.add_option('-a', '--sleepamt'   , action="store", dest="sleepAmt"          , help="Seconds to sleep between iterations; use this to control the amount of activity/simulate activity on the database table", default=0)
parser.add_option('-s', '--schema'     , action="store", dest="dbschema"          , help="Database Schema where the table exists", default=0)
parser.add_option('-t', '--table'      , action="store", dest="dbtable"           , help="Database Table name", default=0)
parser.add_option('-D', '--deletefreq' , action="store", dest="deletefreq"        , help="Delete frequency in iterations; if doDelete = True and current iteration number modulus this value = 0 then a Delete will be performed.", default=20)
parser.add_option('-U', '--updatefreq' , action="store", dest="updatefreq"        , help="Update frequency in iterations; if doUpdate = True and current iteration number modulus this value = 0 then an Update will be performed.", default=3)

options, args = parser.parse_args()
logger.info("mysql_option_file : " + options.mysql_option_file)
logger.info("iterations        : " +  str(options.iterations))
logger.info("kickoff           : " +  str(options.kickoff))
logger.info("insert            : " +  str(options.doInserts))
logger.info("update            : " +  str(options.doUpdates))
logger.info("updatefreq        : " +  str(options.updatefreq))
logger.info("delete            : " +  str(options.doDeletes))
logger.info("deletefreq        : " +  str(options.deletefreq))
logger.info("sleepamt          : " +  str(options.sleepAmt))
logger.info("dbschema          : " +  str(options.dbschema))
logger.info("table             : " +  str(options.dbtable))

conn = mysql.connector.connect(option_files=options.mysql_option_file)


def makeSQL(mode, dbschema, dbtable, ddl, pk, id):
  logger.debug("Making " + mode + " Statement for " + dbtable + " new id : " + str(id))
  #dependent columns
  # person
  tempgender    = None
  tempfirstname = None

  #address
  tempidaddress        = None
  tempaddress_1        = None
  tempaddress_2        = None
  tempaddress_3        = None
  tempaddress_state    = None
  tempaddress_postcode = None

  #location
  templocation          = None
  templocation_postcode = None

  for c in ddl:

      if c["COLUMN_NAME"].upper() == pk.upper() and mode == "INSERT":
          c["VALUE"] = stringafy(c["DATA_TYPE"], None, id)
          continue

      if c["COLUMN_NAME"].upper() == "CREATE_IDEMPLOYEE":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomID(conn, dbschema, "employee", "idemployee"))
          continue

      if c["COLUMN_NAME"].upper() == "UPDATE_IDEMPLOYEE":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomID(conn, dbschema, "employee", "idemployee"))
          continue

      if c["COLUMN_NAME"].upper() == "IDVEHICLE":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomID(conn, dbschema, "ref_vehicle", "idvehicle"))
          continue

      if c["COLUMN_NAME"].upper() == "IDPOLICY":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomID(conn, dbschema, "motor_policy", "idpolicy"))
          continue

      if c["COLUMN_NAME"].upper() == "IDPOLICY_STATUS":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomID(conn, dbschema, "policy_status", "idpolicy_status"))
          continue

      if c["COLUMN_NAME"].upper() == "IDCLAIM_STATUS":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomID(conn, dbschema, "claim_status", "idclaim_status"))
          continue

      if c["COLUMN_NAME"].upper() == "IDCUSTOMER":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomID(conn, dbschema, "customer", "idcustomer"))
          continue

      if c["COLUMN_NAME"].upper() == "IDPARTY_ROLE":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomID(conn, dbschema, "party_role", "idparty_role"))
          continue

      if c["COLUMN_NAME"].upper() == "IDCLAIM":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomID(conn, dbschema, "motor_claim", "idmotor_claim"))
          continue

      if c["COLUMN_NAME"].upper() == "IDRECOVERY_TYPE":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomID(conn, dbschema, "recovery_type", "idrecovery_type"))
          continue

      if c["COLUMN_NAME"].upper() == "DESCRIPTION":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomClaimDescription()[0:c["LENGTH"]])
          continue

      if c["COLUMN_NAME"].upper() == "REGISTRATION":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomRegistration())
          continue

      if c["COLUMN_NAME"].upper() == "COLOR":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomColor(conn, dbschema))
          continue

      if c["COLUMN_NAME"].upper() == "WEATHER_CONDITIONS":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomWeather())
          continue

      if c["COLUMN_NAME"].upper() in ("INJURED_FLAG","POLICE_SERVICES_ATTEND","FIRE_SERVICES_ATTEND"):
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomChoice())
          continue

      if c["COLUMN_NAME"].upper() == "INCIDENT_DESCRIPTION":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomClaimDescription())
          continue

      if c["COLUMN_NAME"].upper() == 'LAST_NAME':
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomLastName(conn, options.dbschema))
          continue

      if c["COLUMN_NAME"] == "DATA_DATE":
          c["VALUE"] = stringafy("date", None, unique_col2)
          continue

      if c["COLUMN_NAME"] == "ID_NUMBER":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], unique_col3)
          continue

      if c["COLUMN_NAME"] == "PORTFOLIO_DATE":
          c["VALUE"] = stringafy("date", None, unique_col4)
          continue

      if c["COLUMN_NAME"] == "QRM_ACCOUNT":
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], getRandomQRMAccount())
          continue
          
      if c["COLUMN_NAME"].upper() == "INCIDENT_LOCATION":
          if templocation == "" or templocation is None :
             (templocation, templocation_postcode) = getRandomLocation(conn, options.dbschema)
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], templocation)
          continue

      if c["COLUMN_NAME"].upper() == "INCIDENT_POSTCODE":
          if templocation_postcode == "" or templocation_postcode is None :
             (templocation, templocation_postcode) = getRandomLocation(conn, options.dbschema)
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], templocation_postcode)
          continue

      if c["COLUMN_NAME"].upper() == 'IDADDRESS':
          if tempidaddress == "" or tempidaddress is None :
             (tempidaddress, tempaddress_1, tempaddress_2, tempaddress_3, tempaddress_state, tempaddress_postcode) = getRandomAddress(conn, options.dbschema)
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], tempidaddress)
          continue

      if c["COLUMN_NAME"].upper() == 'ADDRESS_1':
          if tempaddress_1 == "" or tempaddress_1 is None :
             (tempidaddress, tempaddress_1, tempaddress_2, tempaddress_3, tempaddress_state, tempaddress_postcode) = getRandomAddress(conn, options.dbschema)
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], tempaddress_1)
          continue

      if c["COLUMN_NAME"].upper() == 'ADDRESS_2':
          if tempaddress_2 == "" or tempaddress_2 is None :
             (tempidaddress, tempaddress_1, tempaddress_2, tempaddress_3, tempaddress_state, tempaddress_postcode) = getRandomAddress(conn, options.dbschema)
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], tempaddress_2)
          continue

      if c["COLUMN_NAME"].upper() == 'ADDRESS_3':
          if tempaddress_3 == "" or tempaddress_3 is None :
             (tempidaddress, tempaddress_1, tempaddress_2, tempaddress_3, tempaddress_state, tempaddress_postcode) = getRandomAddress(conn, options.dbschema)
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], tempaddress_3)
          continue

      if c["COLUMN_NAME"].upper() == 'ADDRESS_STATE':
          if tempaddress_state == "" or tempaddress_state is None :
             (tempidaddress, tempaddress_1, tempaddress_2, tempaddress_3, tempaddress_state, tempaddress_postcode) = getRandomAddress(conn, options.dbschema)
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], tempaddress_state)
          continue

      if c["COLUMN_NAME"].upper() == 'ADDRESS_POSTCODE':
          if tempaddress_postcode == "" or tempaddress_postcode is None :
             (tempidaddress, tempaddress_1, tempaddress_2, tempaddress_3, tempaddress_state, tempaddress_postcode) = getRandomAddress(conn, options.dbschema)
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], tempaddress_postcode)
          continue

      if c["COLUMN_NAME"].upper() == 'GENDER':
          if tempgender == "" or tempgender is None :
             (tempgender, tempfirstname) = getRandomFirstName(conn, options.dbschema)
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], tempgender)
          continue

      if c["COLUMN_NAME"].upper() == 'FIRST_NAME':
          if tempfirstname == "" or tempfirstname is None :
             (tempgender, tempfirstname) = getRandomFirstName(conn, options.dbschema)
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], tempfirstname)
          continue

      if c["DEFAULT"] != None:
          c["VALUE"] = stringafy(c["DATA_TYPE"], c["DEFAULT"], c["DEFAULT"])
          continue

      c["VALUE"] = makeRandomValue(c["DATA_TYPE"], c["LENGTH"], c["SCALE"])

  if mode == "INSERT":
    sql  = "INSERT INTO " + dbschema + "." + dbtable + " ("
    vals = ") VALUES ("
    i = 0
    for c in ddl:
      if i == 0:
          sql  = sql  + c["COLUMN_NAME"]
          vals = vals + c["VALUE"]
      else:
          sql  = sql  + ", " + c["COLUMN_NAME"]
          vals = vals + ", " + c["VALUE"]
      i = i + 1
    sql = sql + vals + ")"
  else:
      sql  = "UPDATE " + dbschema + "." + dbtable + " SET "
      i = 0
      for c in ddl:
          if c["COLUMN_NAME"].upper() == pk.upper():
             continue
          if c["COLUMN_NAME"].upper() == "CREATE_IDEMPLOYEE":
             continue
          if c["COLUMN_NAME"].upper() == "CREATE_TIMESTAMP":
             continue
          if i == 0:
             sql  = sql  + c["COLUMN_NAME"] + " = " + c["VALUE"]
          else:
             sql  = sql  + ", " + c["COLUMN_NAME"] + " = " + c["VALUE"]
          i = i + 1
      sql = sql + " WHERE " + pk + " = " + str(id)

  return (sql)

def main():
  global logger

  rows = int(options.iterations)
  tenPercent = math.ceil(rows/10)

  ddl= getDDL(conn, options.dbschema, options.dbtable)
  pk = getPrimaryKey(conn, options.dbschema, options.dbtable)
  pk = pk['COLUMN_NAME']

  #rather than looking up the next primary key value/id for the table each time, we assume that this is the only script running.
  # Note, use kickoff parm to run multiple in parrallel each with it's own range
  # WARNING: gaps in contiguous id numbers will result in performance when attempting to find a random record for update/delete; use batches with insert only
  if int(options.kickoff) >= 0:
      id_number = int(options.kickoff)
      # decrement 1 because the first thing we do is add one below
      id_number = id_number -1
  else:
    id_number      = getMaxID(conn, options.dbschema, options.dbtable, pk)
  logger.info("Existing max id: " + str(id_number))

  for i in range(rows):
      if ((i%tenPercent == 0) and (i > 0)):
         logger.info(str(math.ceil((i/rows)*100)) + "% id_number : " + str(id_number))

      id_number = id_number + 1

      if options.doInserts == "True" or options.doInserts == True:
          insertStatement = makeSQL("INSERT", options.dbschema, options.dbtable, ddl, pk, id_number)
          logger.debug(insertStatement)

          cursor = conn.cursor()
          cursor.execute(insertStatement)
          conn.commit()
          cursor.close()

      if (options.doUpdates == "True" or options.doUpdates == True) and i > 1 and i % int(options.updatefreq) == 0 :
          randID = getRandomID(conn, options.dbschema, options.dbtable, pk)
          updateStatement = makeSQL("UPDATE", options.dbschema , options.dbtable, ddl, pk, randID)
          logger.debug(updateStatement)

          cursor = conn.cursor()
          cursor.execute(updateStatement)
          conn.commit()
          cursor.close()

      if (options.doDeletes == "True" or options.doDeletes == True) and i > 1 and i % int(options.deletefreq) == 0 :
          deleteRandomRecord(conn, options.dbschema , options.dbtable, pk)
          
      sleep(int(options.sleepAmt))

  logger.info("Iterations : " + str(i+1))
  logger.info("end id_number : " + str(id_number))

  num = countRows(conn, options.dbschema, options.dbtable)
  conn.close()
  logger.info("total rows in " + options.dbschema + "." + options.dbtable + " : " + str(num))


if __name__ == "__main__":
   main()