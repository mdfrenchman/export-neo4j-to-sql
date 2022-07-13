# export-neo4j-to-sql
 Stored procedure base to export from neo4j-graph to ms-sql-server

 ## TestTypes TABLE Structure
 `CREATE TABLE TestTypes (theString varchar(50), theInt int, theBit bit, theFloat float, theDateTime datetime)`

 ## Verification for datetime and boolean conversion
 `CALL osmis.export.advanced("RETURN 'bob' as theString, 1 as theInt, true as theBit, 1.111 as theFloat, datetime() as theDateTime","TestTypes",10, "jdbc:sqlserver://localhost:1433;databaseName=dbTest;user=demoApp;password=demoApp;encrypt=false");`
