#!/usr/bin/env python
import sys
import csv

fname=sys.argv[1]
print (fname)

i = 0
with open(fname, newline='') as csvfile:
    rows = csv.reader(csvfile, delimiter=',')
    for row in rows:
        i = i + 1
        if i > 1:
            drive                    = row[24]
            enging_id                = row[25]
            engine_description       = row[26]
            fuel_type                = row[30]
            fuel_type_1              = row[31]
            make                     = row[46]
            model                    = row[47]
            transmission             = row[57]
            vehicle_class            = row[62]
            year                     = row[63]
            
            print ( str(i)             + "|" 
                  + drive              + "|" \
                  + enging_id          + "|" \
                  + engine_description + "|" \
                  + fuel_type          + "|" \
                  + fuel_type_1        + "|" \
                  + make               + "|" \
                  + model              + "|" \
                  + transmission       + "|" \
                  + vehicle_class      + "|" \
                  + year               + "|" \
                  )