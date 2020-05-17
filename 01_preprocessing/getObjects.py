import sys
import re

inFile=sys.argv[1]

tableName=''
constraintName=''
ForeignKeyName=''
with open(inFile, 'r') as f:
  for line in f:
         
     if 'ALTER TABLE ' in line:
        start='ALTER TABLE '
        end=' ADD'
        tableName = re.search('%s(.*)%s' % (start, end), line).group(1)
     if ' CONSTRAINT ' in line and ' FOREIGN KEY' in line:
        start=' CONSTRAINT '
        end=' FOREIGN KEY'
        ForeignKeyName = re.search('%s(.*)%s' % (start, end), line).group(1)
        
       
     if tableName != '' and ForeignKeyName != '':
        print ("ALTER TABLE %s DROP FOREIGN KEY %s;" % (tableName, ForeignKeyName))
        ForeignKeyName=''

     if 'CREATE TABLE ' in line:
        start='CREATE TABLE '
        end=' \('
        tableName = re.search('%s(.*)%s' % (start, end), line).group(1)
        print ("DROP TABLE IF EXISTS %s CASCADE;" % (tableName))
        
     if 'CREATE OR REPLACE VIEW ' in line:
        start='CREATE OR REPLACE VIEW '
        end=''
        viewName = re.search('%s(.*)%s' % (start, end), line).group(1)
        print ("DROP VIEW IF EXISTS %s;" % (viewName))        

     if 'CREATE VIEW ' in line:
        start='CREATE VIEW '
        end=''
        viewName = re.search('%s(.*)%s' % (start, end), line).group(1)
        print ("DROP VIEW IF EXISTS %s;" % (viewName))  