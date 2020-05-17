import random
from random import randint
import logging
import datetime

class myLogFormatter(logging.Formatter):
    converter=datetime.datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%03d" % (t, record.msecs)
        return s

logger    = logging.getLogger(__name__)

def nyi(s):
    logger.info(s + " :  Not Yet Implemented")
    return

def makeRandomValue(c, l, s):
    if "CHAR" in c.upper():
      return stringafy(c, None, getRandomString(l).strip())

    if "DATE" in c.upper():
      return stringafy(c, None, getRandomDate())

    if "TIMEST" in c.upper():
      return stringafy(c, None, getRandomTimestamp())

    if "TIME" in c.upper():
      return stringafy(c, None, getRandomTime())

    if "DECIMAL" in c.upper():
      return stringafy(c, None, getRandomDecimal(l , s))

    if "DOUBLE" in c.upper():
      return stringafy(c, None, getRandomDecimal(l , s))

    if "INT" in c.upper():
      return stringafy(c, None, getRandomInteger())

def getRandomDecimal(l, s):
    val = random.random()
    val = val * (10 * (l - 1))
    val = round(val, s)
    return val

def getRandomInteger():
    return random.randrange(32768)

def stringafy(coltype, default, val):
    v = str(val)
    if "CHAR" in coltype.upper() and default == None:
        v = "'" + escapeStr(val) + "'"
        return v

    if "TIME" in coltype.upper() and default == None:
        v = "'" + escapeStr(val) + "'"
        return v

    if "DATE" in coltype.upper() and default == None:
         v = "'" + escapeStr(val) + "'"
         return v
    return v

def escapeStr(s):
  s = str(s)
  s = s.replace("\'", "\'\'")
  return s

def countRows(conn, dbschema, dbtable):
    logger.debug("countRows : " + dbschema + "." + dbtable)
    sql    = "select count(*) from " + dbschema + "." + dbtable
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchone()
    cursor.close()

    num = int(result[0])
    logger.info("getMaxID: " + dbschema + '.' + dbtable + " : number of rows : " + str(num))

    return num

def getRandomFirstName(conn, dbschema):
    logger.debug("getRandomFirstName")
    randRecord = getRandomID(conn, dbschema, "ref_first_names", "idfirst_name")
    sql    = "select gender, replace(replace(first_name, HEX(232),'e' ) ,HEX(224),'a') as first_name from " + dbschema + ".ref_first_names where idfirst_name = %s"
    vals = (randRecord,)
    cursor = conn.cursor(prepared=True)
    cursor.execute(sql, vals)
    result = cursor.fetchone()
    cursor.close()

    return (result[0], result[1])

def getRandomLastName(conn, dbschema):
    logger.debug("getRandomLastName")
    randRecord = getRandomID(conn, dbschema, "ref_last_names", "idlast_name")
    sql    = "select replace(replace(last_name, HEX(232),'e' ) ,HEX(224),'a') as last_name from " + dbschema + ".ref_last_names where idlast_name = %s"
    vals = (randRecord,)
    cursor = conn.cursor(prepared=True)
    cursor.execute(sql, vals)
    result = cursor.fetchone()
    cursor.close()

    return (result[0])

def getRandomAddress(conn, dbschema):
    logger.debug("getRandomAddress")
    randID = getRandomID(conn, dbschema, "ref_address", "idaddress")

    sql = """select idaddress
                  , replace(replace(replace(trim(coalesce(building_name,'') || ' ' || coalesce(lot,'') || ' ' || coalesce(flat,'') || ' ' || coalesce(level,'')), '  ', ' '), '  ', ' '), '  ', ' ') as address_1
                  , trim(coalesce(street  ,'')) as address_2
                  , trim(coalesce(suburb  ,'')) as address_3
                  , trim(coalesce(state   ,'') )
                  , trim(coalesce(postcode,''))
             from  """ + dbschema + """.ref_address where idaddress = %s
          """
    cursor = conn.cursor(prepared=True)
    vals = (randID,)
    cursor.execute(sql, vals)
    result = cursor.fetchone()
    cursor.close()

    addressID       = result[0]
    addressLine1    = result[1]
    addressLine2    = result[2]
    addressLine3    = result[3]
    addressState    = result[4]
    addressPostcode = result[5]

    return (addressID ,addressLine1 ,addressLine2 ,addressLine3 ,addressState ,addressPostcode)

def getRandomID(conn, dbschema, dbtable, col):
    logger.debug("getRandomID : " + dbschema + "." + dbtable + "." + col)
    maxID  = getMaxID(conn, dbschema, dbtable, col)
    if maxID == 0:
        logger.warn("Unable to locate a random record on an empty table: " + dbtable + "." + col)
        logger.warn("Assuming 0")
        return 0

    foundOne = False
    tryCount = 1
    while foundOne == False:
        randID = random.randint(1,int(maxID))
        sql = "select 1 from " + dbschema + "." + dbtable + " where " + col + " = %s"
        cursor = conn.cursor(prepared=True)
        vals = (randID,)
        cursor.execute(sql, vals)
        result = cursor.fetchone()
        cursor.close()

        if result is None:
            logger.debug("getRandomID: Unable to validate that random ID exists (a) : " + dbschema + "." + dbtable + " : " + col  + " " + str(randID) + "  trying for another")
            tryCount = tryCount + 1
            continue
        if isinstance(result, bool) == True:
            logger.debug("getRandomID: Unable to validate that random ID exists (b) : " + dbschema + "." + dbtable + " : " + col  + " " + str(randID) + "  trying for another")
            tryCount = tryCount + 1
        else:
            if int(result[0]) == 1:
                foundOne = True
                logger.debug("getRandomID: Random ID chosen exists " + dbschema + "." + dbtable + " : " + col  + " : " + str(randID) + " Found after " + str(tryCount) + " tries")
                return randID
            else:
                logger.debug("getRandomID: Unable to validate that random ID exists (c) : " + dbschema + "." + dbtable + " : " + col  + " " + str(randID) + "  trying for another")
                tryCount = tryCount + 1

def getMaxID(conn, dbschema, dbtable, col):
    logger.debug("getMaxID : " + dbschema + "." + dbtable + "." + col)
    sql = "  select coalesce(max(" + col + "),0) from " + dbschema + '.' + dbtable
    logger.debug(sql)

    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchone()
    cursor.close()

    maxid = int(result[0])
    logger.debug("getMaxID : " + dbschema + '.' + dbtable + " : " + col  + " : " + str(maxid))
    return maxid

def getRandomColor(conn, dbschema):
    logger.debug("getRandomColor")
    randID = getRandomID(conn, dbschema, 'ref_colors', 'idcolor')

    sql = "select substr(colorname,1,64) from " + dbschema + ".ref_colors where idcolor = ?"

    cursor = conn.cursor(prepared=True)
    vals = (randID,)
    cursor.execute(sql, vals)
    result = cursor.fetchone()
    cursor.close()

    randomColor     = result[0]
    logger.debug("getRandomColor: " + randomColor)
    return randomColor

def getRandomString(l):
    logger.debug("getRandomString")
    letters = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'
            ,'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'
            ,'0','1','2','3','4','5','6','7','8','9'
            ,' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ','-','_'
            ]
    s = ""
    length = l - random.randrange(l)
    for i in range(length):
        s = s + str(random.choice(letters))
    return s

def getRandomRegistration():
    logger.debug("getRandomRegistration")
    letters = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']
    char1 = random.choice(letters)
    char2 = random.choice(letters)
    char3 = random.choice(letters)

    numbers = ['0','1','2','3','4','5','6','7','8','9']
    random.choice(numbers)
    char4 = random.choice(numbers)
    char5 = random.choice(numbers)
    char6 = random.choice(numbers)

    return char1 + char2 + char3 + '-' + str(char4) + str(char5) + str(char6)

def getRandomLoadDate():
    logger.debug("getRandomLoadDate")
    years = [2018,2019]
    year = random.choice(years)
    try:
        d = datetime.datetime.strptime('{} {}'.format(random.randint(1, 366), year), '%j %Y')
        d = d.strftime('%Y-%m-%d')
        return d
    # if the value happens to be in the leap year range, try again
    except ValueError:
        get_ragetRandomDatendom_date(year)

def getRandomQRMAccount():
    logger.debug("getRandomQRMAccount")
    numbers1 = ['0','1','2']
    numbers2 = ['0','1','2','3','4','5','6','7','8','9']
    numbers3 = ['0','1','2','3','4','5','6','7','8','9']
    char1 = random.choice(random.choice(numbers1))
    char2 = random.choice(random.choice(numbers2))
    char3 = random.choice(random.choice(numbers3))
    return "QRM-0000" + str(char1) + str(char2) + str(char3)

def getRandomDate():
    logger.debug("getRandomDate")
    years = [1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019]
    year = random.choice(years)
    try:
        d = datetime.datetime.strptime('{} {}'.format(random.randint(1, 366), year), '%j %Y')
        d = d.strftime('%Y-%m-%d')
        return d
    # if the value happens to be in the leap year range, try again
    except ValueError:
        get_ragetRandomDatendom_date(year)

def getRandomChoice():
    logger.debug("getRandomChoice")
    letters = ['Y','N']
    return random.choice(letters)

def getRandomTime():
    logger.debug("getRandomTime")
    start = datetime.datetime(2000, 1, 1, 00, 00, 00)
    years = 1
    end = start + datetime.timedelta(days=365 * years)
    t = start + (end - start) * random.random()
    t = t.strftime('%H:%M:%S')
    return t

def getCurrentTimestamp():
    logger.info("getCurrentTimestamp")
    ts = datetime.datetime.now()
    ts = ts.strftime('%Y-%m-%d %H:%M:%S.%f')
    return ts

def getRandomTimestamp():
    logger.debug("getRandomTimestamp")
    randDays  = randint(1,14)
    randSeconds = 86400 * randDays
    ts = datetime.datetime.now() - datetime.timedelta(seconds = randSeconds)
    ts = ts.strftime('%Y-%m-%d %H:%M:%S.%f')
    return ts

def getCurrentTimestampSerial():
    logger.debug("getCurrentTimestampSerial")
    ts = datetime.datetime.now()
    ts = ts.strftime('%Y%m%d%H%M%S%f')
    return ts

def getRandomAccidentReason():
    logger.debug("getRandomAccidentReason")
    reasons = ['Damage whilst Parked','Hit in Rear','Feel asleep','Distracted while drivign','Brake failure','At Fault','Not at Fault']
    return random.choice(reasons)

def getRandomLocation(conn, dbschema):
    logger.debug("getRandomLocation")
    (addressID ,addressLine1 ,addressLine2 ,addressLine3 ,addressState ,addressPostcode)  = getRandomAddress(conn, dbschema)
    location = addressLine1 + " " + addressLine2 + " " + addressLine3 + " " + addressState
    location = ' '.join(location.split())
    return (location, addressPostcode)

def getRandomWeather():
    logger.debug("getRandomWeather")
    w = ['chilly','clear','cloudy','cold','cool','dry','foggy','frosty','hot','icy','misty','rainy','snowy','stormy','sunny','warm','wet','windy']
    return random.choice(w)

def getRandomRecoveryDescription():
    logger.debug("getRandomRecoveryDescription")
    d = ['recovery from third party','recovery from Insurer','payout to Insurer','payout to customer', 'payout to third party', 'cost of repairer', 'pay to supplier']
    return random.choice(d)


def getRandomClaimDescription():
    logger.debug("getRandomClaimDescription")
    li = ['Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec tortor mauris, ornare et tincidunt eget, fermentum vel ligula. Ut condimentum dignissim sagittis. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Phasellus imperdiet nec purus in viverra. Nam vitae metus eu diam sagittis sagittis. Etiam consectetur porta fringilla. Aenean non pharetra dolor. Nulla eu sollicitudin ipsum. Donec eget eros id risus rutrum fermentum. Quisque nec turpis at tellus venenatis ultrices quis id nibh. Etiam vel erat quis mi efficitur congue ac eget lacus. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In non odio felis. Integer scelerisque in ipsum ut porta. Sed et nisi ligula.'
         ,'Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Nullam sit amet mollis leo. Fusce sed justo viverra, sagittis mauris nec, consequat mauris. In a sapien ex. Proin eu placerat diam. Suspendisse quam erat, sodales id eros sit amet, interdum congue lacus. Cras sed consequat turpis. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Nunc auctor ligula nisl, ac suscipit lorem cursus eu. Mauris maximus, urna ac fermentum consectetur, lorem erat viverra felis, eget faucibus mi massa eleifend tortor. Donec pharetra venenatis massa, id commodo augue vehicula non.'
         ,'In id elementum elit. Sed consequat dictum urna, vitae ullamcorper odio tincidunt vel. Quisque quis felis eros. Ut quis augue sit amet quam placerat auctor condimentum quis diam. Ut sodales id tortor a efficitur. Phasellus condimentum, sapien a posuere sollicitudin, nibh diam dignissim quam, in volutpat metus est in lacus. Sed scelerisque, metus vel pharetra vulputate, quam diam placerat dui, ut elementum tortor justo ac massa. Morbi pellentesque risus nec commodo porttitor. Cras vulputate ultricies erat vitae eleifend. Nam id felis non velit tincidunt efficitur ut non elit. Donec ac cursus leo, vel commodo mauris. Cras viverra quam vel tortor ultrices, in convallis orci tempus. Aenean vel ex non nisl malesuada vehicula nec pharetra ipsum. Aliquam erat volutpat.'
         ,'Maecenas imperdiet rutrum metus, congue molestie diam scelerisque eget. Proin pretium scelerisque iaculis. Sed accumsan, tellus eget dapibus auctor, mi est tincidunt elit, at vehicula nisi metus sed nunc. Suspendisse potenti. Morbi sit amet justo ut nisl malesuada ullamcorper in eu purus. Nulla pharetra pulvinar diam eget ultrices. Nulla tristique erat felis, nec facilisis nibh efficitur ac. Nunc in gravida augue, ut efficitur enim. Vestibulum placerat mi in euismod porttitor. Duis neque mauris, finibus in iaculis eu, tempus tempor enim.'
         ,'Aenean lobortis enim eget lacus hendrerit elementum. Nunc mi purus, dignissim sit amet vestibulum ac, aliquet vel tellus. Ut odio nibh, fringilla id urna vitae, placerat semper ex. Vestibulum tincidunt eleifend nunc, at dictum tortor condimentum eget. Donec semper ornare diam id euismod. Vivamus vestibulum leo sem, vel maximus nisl suscipit id. Maecenas ornare, ante ac imperdiet fermentum, mauris dui eleifend nunc, sed iaculis dolor nibh sit amet nulla. Nulla quis enim at diam tincidunt bibendum. Aliquam dolor sapien, mollis quis varius id, blandit vitae ex. Nullam faucibus dolor non ipsum semper, nec lobortis purus volutpat.'
         ,'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Curabitur efficitur leo porta, facilisis risus sit amet, mattis nibh. Fusce eget tellus lacinia, congue nisi vel, dignissim mauris. Pellentesque ullamcorper sem sem. Nulla leo risus, dignissim ac massa tempor, egestas placerat ex. Maecenas et eros magna. Donec ut elit euismod, semper leo quis, pulvinar ligula. Nulla semper non tortor et feugiat. Cras tempus magna elit, eget blandit nisl suscipit et.'
         ,'Sed luctus metus nec quam malesuada pellentesque. Integer tincidunt malesuada ipsum sit amet aliquet. Nulla facilisi. Praesent scelerisque in magna finibus feugiat. Donec at tincidunt ex. Cras suscipit, risus ut maximus congue, metus ex aliquam nulla, in cursus justo nibh ut risus. Morbi sed ex suscipit, pellentesque mi eget, tincidunt sapien. Donec interdum rhoncus odio eget tempor. Vivamus sit amet quam eget metus lobortis aliquam non mattis metus. Proin non libero elementum, finibus quam sit amet, iaculis neque. Donec mattis ornare leo posuere ultrices. Cras arcu lorem, placerat id dolor venenatis, consectetur efficitur metus. Pellentesque a pellentesque erat, a tincidunt erat. Integer in placerat est, ut venenatis erat.'
         ,'Curabitur eu gravida lacus, in ultricies turpis. Morbi euismod ex eu aliquam efficitur. Sed ultricies elementum quam at sodales. Donec dui sem, varius laoreet erat a, pellentesque faucibus urna. Donec neque tortor, scelerisque ut dictum in, venenatis eu turpis. Aenean a ultrices libero. Praesent non consequat lacus. Ut pulvinar viverra augue, a vestibulum enim posuere sed.'
         ,'Etiam interdum ante eget arcu mollis fringilla. Proin quis sem dignissim est interdum tincidunt id sed eros. Integer laoreet leo vehicula dapibus ornare. Nullam consequat, mi a tristique dictum, ante mi eleifend ligula, fermentum convallis tellus diam at ipsum. Donec a sem auctor, elementum purus sit amet, scelerisque felis. Duis non convallis nisi. Cras libero ipsum, venenatis sit amet molestie sit amet, egestas id nisi. Nunc aliquet ornare semper. Cras risus tortor, imperdiet eu justo sed, semper faucibus nisi. Nulla pharetra magna quam, eget condimentum nisl blandit et. Donec ligula eros, sollicitudin eget mauris vitae, bibendum imperdiet est.'
         ,'Maecenas at scelerisque mi. Suspendisse enim lacus, cursus eu interdum vitae, sodales porta orci. Morbi molestie tristique quam id vulputate. Maecenas vel tristique tortor. Cras vitae mauris tempor, sodales libero vitae, auctor ipsum. Aliquam eget ultrices arcu. Nam mattis sagittis porta. Fusce rutrum fringilla nunc, quis imperdiet odio vestibulum quis. Proin mattis risus sapien, eu mattis turpis aliquet maximus.'
         ,'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Tincidunt augue interdum velit euismod in pellentesque massa placerat duis. Leo a diam sollicitudin tempor id. Pretium vulputate sapien nec sagittis aliquam. Arcu non odio euismod lacinia. Sollicitudin aliquam ultrices sagittis orci a scelerisque. Nec nam aliquam sem et. Faucibus nisl tincidunt eget nullam non. At varius vel pharetra vel. Consequat nisl vel pretium lectus quam id leo. Viverra vitae congue eu consequat ac felis. Morbi leo urna molestie at elementum eu. Imperdiet nulla malesuada pellentesque elit eget gravida cum sociis natoque.'
         ,'Ac felis donec et odio pellentesque diam volutpat. Suspendisse ultrices gravida dictum fusce ut placerat orci nulla. Nec feugiat in fermentum posuere urna nec tincidunt praesent. Enim ut sem viverra aliquet. Sociis natoque penatibus et magnis dis parturient montes. Odio tempor orci dapibus ultrices in. Sit amet nisl suscipit adipiscing bibendum est ultricies integer. Nunc scelerisque viverra mauris in aliquam sem. Non tellus orci ac auctor augue. Nisl nunc mi ipsum faucibus vitae aliquet nec ullamcorper. Vulputate enim nulla aliquet porttitor lacus luctus accumsan. Facilisi nullam vehicula ipsum a arcu cursus. Iaculis at erat pellentesque adipiscing. Quam vulputate dignissim suspendisse in est ante in nibh. Rhoncus urna neque viverra justo nec ultrices.'
         ,'Placerat vestibulum lectus mauris ultrices eros in. Amet nulla facilisi morbi tempus iaculis urna id. Nec feugiat nisl pretium fusce id velit. Sit amet commodo nulla facilisi nullam vehicula ipsum a arcu. Non diam phasellus vestibulum lorem sed risus ultricies.'
         ,'Egestas sed sed risus pretium quam vulputate dignissim suspendisse in. Hendrerit dolor magna eget est. Sagittis aliquam malesuada bibendum arcu vitae elementum curabitur vitae nunc. Malesuada fames ac turpis egestas. Adipiscing enim eu turpis egestas pretium. Nam libero justo laoreet sit amet cursus. Faucibus a pellentesque sit amet porttitor eget. Platea dictumst quisque sagittis purus sit amet volutpat consequat. Tellus in hac habitasse platea dictumst vestibulum rhoncus. Quam id leo in vitae turpis. Sed velit dignissim sodales ut eu sem integer vitae justo. Dolor magna eget est lorem ipsum dolor sit amet consectetur. Non sodales neque sodales ut etiam sit.'
         ,'Pulvinar mattis nunc sed blandit. Sapien et ligula ullamcorper malesuada proin libero nunc consequat interdum. Velit sed ullamcorper morbi tincidunt ornare massa eget. Quam vulputate dignissim suspendisse in est ante in nibh. Auctor urna nunc id cursus metus. Dignissim cras tincidunt lobortis feugiat vivamus. Maecenas pharetra convallis posuere morbi leo urna.'
         ,'Velit laoreet id donec ultrices tincidunt. Dictumst quisque sagittis purus sit amet volutpat. Dolor sed viverra ipsum nunc aliquet bibendum enim facilisis gravida. Pellentesque elit eget gravida cum sociis natoque. Sit amet facilisis magna etiam. Turpis cursus in hac habitasse platea. Sagittis orci a scelerisque purus. Amet porttitor eget dolor morbi non arcu risus quis. Et ultrices neque ornare aenean euismod elementum nisi. Morbi tincidunt augue interdum velit euismod. Id interdum velit laoreet id donec. Viverra ipsum nunc aliquet bibendum enim facilisis gravida. Ornare massa eget egestas purus viverra accumsan in. Leo a diam sollicitudin tempor id.'
         ,'Sit amet porttitor eget dolor. Arcu non sodales neque sodales ut etiam sit amet. Sed velit dignissim sodales ut. Est placerat in egestas erat imperdiet sed euismod nisi porta. Parturient montes nascetur ridiculus mus mauris vitae ultricies leo integer.'
         ,'Magna etiam tempor orci eu lobortis elementum. Quis enim lobortis scelerisque fermentum dui faucibus in ornare. Etiam non quam lacus suspendisse faucibus. Bibendum enim facilisis gravida neque convallis a cras. Dui ut ornare lectus sit amet. Ut eu sem integer vitae justo eget magna. Elit sed vulputate mi sit amet. Proin sagittis nisl rhoncus mattis rhoncus. Vitae tortor condimentum lacinia quis vel eros donec ac. Imperdiet dui accumsan sit amet nulla facilisi morbi tempus iaculis. Maecenas pharetra convallis posuere morbi leo urna molestie at. Enim facilisis gravida neque convallis. Pellentesque pulvinar pellentesque habitant morbi. Erat nam at lectus urna duis convallis. Sit amet purus gravida quis.'
         ,'Facilisis gravida neque convallis a cras semper auctor neque vitae. Velit laoreet id donec ultrices tincidunt arcu.'
         ]
    return random.choice(li)



  
def deleteRandomRecord(conn, dbschema, dbtable, colname):
    logger.debug("deleteRandomRecord: " + dbschema + "." + dbtable + "." + colname)

    recordID = getRandomID(conn, dbschema, dbtable, colname)
    logger.info("Deleting Random record      " + dbtable + " id : " + str(recordID))

    sql = "DELETE FROM " + dbschema + "." +  dbtable + " WHERE " + colname + " = %s"
    cursor = conn.cursor(prepared=True)
    vals = (recordID,)
    cursor.execute(sql, vals)
    conn.commit()
    cursor.close()


def getPrimaryKey(conn, dbschema, dbtable):
    logger.debug("getPrimaryKey: " + dbschema + "." + dbtable)
    sql    = """SELECT
                     COLS.ORDINAL_POSITION    AS COL_NO
                    ,COLS.COLUMN_NAME         AS COLUMN_NAME
                    ,CONST.TABLE_SCHEMA       AS SCHEMA_NAME
                    ,CONST.TABLE_NAME         AS TABLE_NAME
                    ,CONST.CONSTRAINT_SCHEMA  AS PK_SCHEMA
                    ,CONST.CONSTRAINT_NAME    AS PK_NAME

              FROM  information_schema.table_constraints CONST

                    INNER JOIN information_schema.key_column_usage COLS
                            ON CONST.CONSTRAINT_SCHEMA = COLS.CONSTRAINT_SCHEMA
                           AND CONST.CONSTRAINT_NAME   = COLS.CONSTRAINT_NAME
                           AND CONST.TABLE_SCHEMA      = COLS.TABLE_SCHEMA
                           AND CONST.TABLE_NAME        = COLS.TABLE_NAME

             WHERE     CONST.constraint_type='PRIMARY KEY'
                   and CONST.TABLE_SCHEMA = %s
                   and CONST.TABLE_NAME = %s

             ORDER BY CONST.TABLE_SCHEMA
                     ,CONST.TABLE_NAME
                     ,CONST.CONSTRAINT_SCHEMA
                     ,CONST.CONSTRAINT_NAME
                     ,COLS.ORDINAL_POSITION
           """
    cursor = conn.cursor(prepared=True)
    vals = (dbschema, dbtable)
    cursor.execute(sql, vals)

    for row in cursor:
        result = dict(zip(cursor.column_names, row))
        cursor.close()
        return result


def getDDL(conn, dbschema, dbtable):
    logger.debug("getDDL: " + dbschema + "." + dbtable)
    sql    = """select
                      ordinal_position  as 'ORDINAL_POSITION'
                     ,column_name       as 'COLUMN_NAME'
                     ,data_type         as 'DATA_TYPE'
                     ,substr(is_nullable,1,1) as 'NULLS'
                     ,coalesce(case when data_type like '%char%' then character_maximum_length else numeric_precision end,0) as 'LENGTH'
                     ,coalesce(numeric_scale,0)         as 'SCALE'
                     ,cast(column_default as char(256)) as 'DEFAULT'

                from information_schema.columns

               where table_schema = ?
                 and table_name = ?

               order by table_schema
                       ,table_name
                       ,ordinal_position
            """
    cursor = conn.cursor(prepared=True)
    vals = (dbschema, dbtable)
    cursor.execute(sql, vals)

    ddl = []
    for row in cursor:
        result = dict(zip(cursor.column_names, row))
        ddl.append(result)
    cursor.close()
    return ddl