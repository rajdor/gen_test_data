import sys
fname=sys.argv[1]

with open(fname) as fin: 
   lines = fin.readlines()

i = 0
for line in lines:

    i = i + 1
    line = line.rstrip()

    id        = str(i)
    col1      = line[:15]
    col1      = col1.strip()
    col1      = col1.title()
  
    record = id + "|" + col1
    
    print (record)