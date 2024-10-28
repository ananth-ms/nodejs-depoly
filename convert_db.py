#!/usr/bin/python
import MySQLdb
import pyodbc

typesFile = open('sqlserver_datatypes.txt', 'r').readlines()
dataTypes = dict((row.split(',')[0].strip(),row.split(',')[1].strip()) for row in typesFile)

#connection for MSSQL. (Note: you must have FreeTDS installed and configured!)
mysql_conn = pyodbc.connect('driver = {MySQL ODBC 9.0 Unicode Driver}; DATABASE=Classicmodels; user=root; port=3306; PWD=Ananth@03')
mysql_cursor = mysql_conn.cursor()

#connection for MySQL
mssql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.0.94;DATABASE=priya1;UID=sa;PWD=spriya123;Pooling=false')
mssql_cursor = mssql_conn.cursor()

mssql_cursor.execute("SELECT * FROM payments WHERE type='U'") #sysobjects is a table in MSSQL db's containing meta data about the database. (Note: this may vary depending on your MSSQL version!)
dbTables = mysql_cursor.fetchall()
noLength = [56, 58, 61] #list of MSSQL data types that don't require a defined lenght ie. datetime
for tbl in dbTables:
    print ('migrating {0}'.format(tbl[0]))
    mysql_cursor.execute("SELECT * FROM payments WHERE customerNumber = OBJECT_ID('%s')" % tbl[0]) #syscolumns: see sysobjects above.
    columns = mysql_cursor.fetchall()
    attr = ""
    for col in columns:
        colType = dataTypes[str(col.xtype)]
        if col.xtype == 60:
            colType = "float"
            attr += f"{col.name} {colType}({str(col.length)}),"
        elif col.xtype in noLength:
            attr += f"{col.name} {colType},"
        else:
            attr += f"{col.name} {colType}({str(col.length)}),"

    attr = attr.rstrip(',')

   
    print('Fetch rows from table {0}'.format(tbl[0]))
    mysql_cursor.execute("CREATE TABLE " + tbl[0] + " (" + attr + ");") #create the new table and all columns
    mysql_cursor.execute("select * from %s" % tbl[0])
    tblData = mysql_cursor.fetchmany(1000)

   
    while len(tblData) > 0:
        cnt = 0
        for row in tblData:
            fieldList = ""
            for field in row:
                if field is None:
                    fieldList += "NULL,"
                else:
                    field = MySQLdb.escape_string(str(field)).decode('utf-8')
                    fieldList += "'" + field + "',"
            
            fieldList = fieldList[:-1]  # Remove the trailing comma
            mssql_cursor.execute("INSERT INTO " + tbl[0] + " VALUES (" + fieldList + ")")
            
            cnt += 1
            if cnt % 100 == 0:
                print('Inserted 100 rows into table {0}'.format(tbl[0]))
        
        mssql_conn.commit()  # Commit the transaction after each batch
        tblData = mssql_cursor.fetchmany(1000)  


