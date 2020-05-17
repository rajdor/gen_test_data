import sys
fname=sys.argv[1]

with open(fname) as fin: 
   lines = fin.readlines()

i = 0
for line in lines:
    if line.startswith('#') :
         continue

    i = i + 1
    line = line.rstrip()
    fields=line.split(',')

    id        = str(i)
    col1      = str(fields[0])
    col2      = str(fields[1])
  
    record = id + "|" + col1 + "|" + col2
    
    print (record)