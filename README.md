# export-neo4j-to-sql
 Stored procedure base to export from neo4j-graph to ms-sql-server

 ## TestTypes TABLE Structure
 `CREATE TABLE TestTypes (theString varchar(50), theInt int, theBit bit, theFloat float, theDateTime datetime)`

 ## Verification for datetime and boolean conversion
 `CALL osmis.export.advanced("RETURN 'bob' as theString, 1 as theInt, true as theBit, 1.111 as theFloat, datetime() as theDateTime","TestTypes",10, "jdbc:sqlserver://localhost:1433;databaseName=dbTest;user=demoApp;password=demoApp;encrypt=false");`

 ## BigExport TABLE Structure
`CREATE TABLE BigExport (A0 varchar(50), A1 varchar(50), A2 varchar(50), A3 varchar(50), A4 varchar(50), A5 varchar(50), A6 varchar(50), A7 varchar(50), A8 varchar(50), A9 varchar(50), B0 varchar(50), B1 varchar(50), B2 varchar(50), B3 varchar(50), B4 varchar(50), B5 varchar(50), B6 varchar(50), B7 varchar(50), B8 varchar(50), B9 varchar(50), C0 varchar(50), C1 varchar(50), C2 varchar(50), C3 varchar(50), C4 varchar(50), C5 varchar(50), C6 varchar(50), C7 varchar(50), C8 varchar(50), C9 varchar(50), D0 varchar(50), D1 varchar(50), D2 varchar(50), D3 varchar(50), D4 varchar(50), D5 varchar(50), D6 varchar(50), D7 varchar(50), D8 varchar(50), D9 varchar(50))`
 ## BigExport Cypher Query
 `UNWIND RANGE(1,2000000,1) as i RETURN 'A0 - ' + i as A0, 'A1 - ' + i as A1, 'A2 - ' + i as A2, 'A3 - ' + i as A3, 'A4 - ' + i as A4, 'A5 - ' + i as A5, 'A6 - ' + i as A6, 'A7 - ' + i as A7, 'A8 - ' + i as A8, 'A9 - ' + i as A9, 'B0 - ' + i as B0, 'B1 - ' + i as B1, 'B2 - ' + i as B2, 'B3 - ' + i as B3, 'B4 - ' + i as B4, 'B5 - ' + i as B5, 'B6 - ' + i as B6, 'B7 - ' + i as B7, 'B8 - ' + i as B8, 'B9 - ' + i as B9, 'C0 - ' + i as C0, 'C1 - ' + i as C1, 'C2 - ' + i as C2, 'C3 - ' + i as C3, 'C4 - ' + i as C4, 'C5 - ' + i as C5, 'C6 - ' + i as C6, 'C7 - ' + i as C7, 'C8 - ' + i as C8, 'C9 - ' + i as C9, 'D0 - ' + i as D0, 'D1 - ' + i as D1, 'D2 - ' + i as D2, 'D3 - ' + i as D3, 'D4 - ' + i as D4, 'D5 - ' + i as D5, 'D6 - ' + i as D6, 'D7 - ' + i as D7, 'D8 - ' + i as D8, 'D9 - ' + i as D9`
