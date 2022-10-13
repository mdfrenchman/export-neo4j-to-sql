package osmis.graphSqlConnector.toSql;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import org.neo4j.logging.Log;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;

import osmis.graphSqlConnector.ConnectorBase;
import osmis.graphSqlConnector.Output;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExportBase extends ConnectorBase {

    protected Stream<Output> exportToSql(String query, String tableName, Long batchSize, String connString) throws Exception {
        guard.check();
        // System.out.println("this.getClass().getDeclaringClass().toString() :" + this.getClass().getDeclaringClass().toString());
        // System.out.println("this.getClass().getEnclosingMethod().toString() :" + this.getClass().getEnclosingMethod().toString());
        // System.out.println("this.getClass().getEnclosingClass().toString() :" + this.getClass().getEnclosingClass().toString());
        log.info("Starting private exportToSql");
        ExportBase.connectionUrl = connString + ";useBulkCopyForBatchInsert=true";
        Output metrics = new Output(tableName, batchSize);

        log.info("Starting Export at %s", System.currentTimeMillis());

        long funcStart = System.currentTimeMillis();
        // initial start time, needed for total run duration on failure.
        long start = funcStart;

        try (Result data = tx.execute(query)) {
            final List<String> columns = data.columns();
            final String insertSql = String.format("insert into %s (%s) values (%s)", tableName, columns.stream().collect(Collectors.joining(",")), columns.stream().map(s -> {return "?";}).collect(Collectors.joining(", ")));
            
            try (
                SQLServerConnection conn = DriverManager.getConnection(connectionUrl).unwrap(SQLServerConnection.class);
                SQLServerPreparedStatement exportStatement = conn.prepareStatement(insertSql).unwrap(SQLServerPreparedStatement.class);
            ){
                int rowsInBatchCount = 0;
                
                    
                    while (data.hasNext()){
                        if (metrics.recordsProcessed % batchSize == 0 ) {
                            start = System.currentTimeMillis();
                            metrics.batchCount++;
                        }
                        final Map<String, Object> map = data.next();
                        metrics.recordsProcessed++;
                        rowsInBatchCount++;
                        
                        // Question: is there a performance penalty for using setObject since it's infering TYPE?
                        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                            // map types from neo4j -> java -> sqlserver
                            if (map.get(columns.get(columnIndex)) instanceof java.time.ZonedDateTime)
                            {
                                ZonedDateTime dt = ((ZonedDateTime)map.get(columns.get(columnIndex)));
                                exportStatement.setDateTime(columnIndex+1,  Timestamp.from(dt.toInstant()));
                            } else if (map.get(columns.get(columnIndex)) instanceof Boolean) {
                                exportStatement.setBoolean(columnIndex+1, ((Boolean)map.get(columns.get(columnIndex))).booleanValue());
                            } else {
                                exportStatement.setObject(columnIndex+1, map.get(columns.get(columnIndex)));
                            }
                        }
                        exportStatement.addBatch();
                        
                        if (metrics.recordsProcessed % batchSize == 0 ) {
                            exportStatement.executeBatch();
                            long end = System.currentTimeMillis();
                            metrics.runDuration = metrics.runDuration + (end-start);
                            log.info("Finished batch " + metrics.batchCount + ". Submitted " + rowsInBatchCount + " rows. Time taken : " + (end - start) + " milliseconds.");
                            rowsInBatchCount = 0;
                        }
                        
                    }
                    // process the remaining tail after the last full batch.
                    if (rowsInBatchCount > 0) {
                        exportStatement.executeBatch();
                        long end = System.currentTimeMillis();
                        metrics.runDuration = metrics.runDuration + (end-start);
                        log.info("Finished batch " + metrics.batchCount + ". Submitted " + rowsInBatchCount + " rows. Time taken : " + (end - start) + " milliseconds.");
                        rowsInBatchCount = 0;
                    }
                    metrics.success();
                
                exportStatement.closeOnCompletion();
                conn.close();
            }
            catch (SQLServerException ex) {
                metrics.completedSuccessfully = false;
                metrics.message = ex.getMessage();
                metrics.runDuration = System.currentTimeMillis()-start;  
            }
            catch (Exception ex) {
                metrics.completedSuccessfully = false;
                metrics.message = ex.getMessage();
                metrics.runDuration = System.currentTimeMillis()-start;  
                throw ex;
            }    
        }
        catch (QueryExecutionException ex){
            metrics.completedSuccessfully = false;
            metrics.message = ex.getMessage();
            metrics.runDuration = System.currentTimeMillis()-start;
        }    
        log.info("Export Duration: %s ms", System.currentTimeMillis()-funcStart); 
        return Stream.of(metrics);
    }
}
