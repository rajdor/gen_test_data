import sys
startingID=sys.argv[1]
gender1=sys.argv[2]
fname1=sys.argv[3]

i = int(startingID)

with open(fname1) as fin: 
   lines = fin.readlines()

for line in lines:

    i = i + 1
    line = line.rstrip()

    id        = str(i)
    col1      = gender1.upper()
    col2      = line[:15]
    col2      = col2.strip()
    col2      = col2.title()
  
    record = id + "|" + col1 + "|" + col2
    
    print (record)