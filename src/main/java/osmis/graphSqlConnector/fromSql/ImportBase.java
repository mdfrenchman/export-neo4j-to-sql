package osmis.graphSqlConnector.fromSql;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import org.neo4j.logging.Log;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;

import osmis.graphSqlConnector.BatchTransaction;
import osmis.graphSqlConnector.ConnectorBase;

import osmis.graphSqlConnector.Output;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ImportBase extends ConnectorBase {

    protected Stream<Map<String, Object>> getFromSql(String sqlQuery, List<Object> params, String connString, int batchSize) throws Exception{
        guard.check();
        log.info("Starting importFromSql");
        ImportBase.connectionUrl = connString;
        Output metrics = new Output("parseItFrom(sqlQuery)", batchSize);
        
        long start = System.currentTimeMillis();
        Map<String, Object> output = new HashMap<String, Object>();
        List<Map<String, Object>> outputList = new ArrayList<Map<String, Object>>();

        try (SQLServerConnection conn = DriverManager.getConnection(connectionUrl).unwrap(SQLServerConnection.class); 
            SQLServerPreparedStatement statement = conn.prepareStatement(sqlQuery).unwrap(SQLServerPreparedStatement.class);
        ) {
            for (int i = 0; i < params.size(); i++) {
                statement.setObject(i+1,params.get(i));    
            }

            ResultSet result = statement.executeQuery();
            ResultSetMetaData meta = result.getMetaData();
            
            while (result.next()) {
                for (int i = 0; i < meta.getColumnCount(); i++) {
                    // REF for type mappings as ints: https://docs.oracle.com/javase/8/docs/api/constant-values.html#java.sql.Types.TIMESTAMP
                    if (meta.getColumnType(i+1) == 93) {
                        LocalDateTime lt = LocalDateTime.ofInstant(result.getTimestamp(i+1).toInstant(), ZoneId.of(("UTC")));
                        //n.setProperty(meta.getColumnName(i+1), lt.atZone(ZoneId.of("UTC")));
                        output.put(meta.getColumnName(i+1), lt.atZone(ZoneId.of("UTC"))); 
                    } else if (meta.getColumnType(i+1) == -7) {
                        //n.setProperty(meta.getColumnName(i+1), Boolean.valueOf(result.getBoolean(i+1)));
                        output.put(meta.getColumnName(i+1), Boolean.valueOf(result.getBoolean(i+1)));
                    } else {
                        //n.setProperty(meta.getColumnName(i+1), result.getObject(i+1));
                        output.put(meta.getColumnName(i+1), result.getObject(i+1));
                        //log.info("%s : %s", meta.getColumnName(i+1), result.getObject(i+1));
                    }
                    
                }  
                outputList.add(output);
                // if (outputList.size() == batchSize.intValue()){
                //     // if the list size reaches batchSize, then write to graph.

                // }
            }
            statement.closeOnCompletion();
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
        
        
     
        
        return outputList.stream();
    }  
    
    private void writeToGraph(List<Map<String, Object>> data, String writeQuery, Map<String, Object> writeParams){
        
        writeParams.put("data", data);
        Transaction incrementalTx = db.beginTx();
        incrementalTx.execute(writeQuery, writeParams);
        incrementalTx.commit();
    }

    private Map<String, Object> mapSqlResult(ResultSet result, ResultSetMetaData meta) throws SQLException, Exception {
        Map<String, Object> output = new HashMap<String, Object>();
        for (int i = 0; i < meta.getColumnCount(); i++) {
            // REF for type mappings as ints: https://docs.oracle.com/javase/8/docs/api/constant-values.html#java.sql.Types.TIMESTAMP
            //log.debug("type: %s ; colIndex: %d", meta.getColumnType(i+1), i+1);
            if (meta.getColumnType(i+1) == java.sql.Types.TIMESTAMP) {
                Timestamp ts = result.getTimestamp(i+1);
                //log.debug("wasNull: %s; ts: %s", result.wasNull(), ts);
                if (!result.wasNull()) {
                    LocalDateTime lt = LocalDateTime.ofInstant(ts.toInstant(), ZoneId.of(("UTC")));
                    //n.setProperty(meta.getColumnName(i+1), lt.atZone(ZoneId.of("UTC")));
                    output.put(meta.getColumnName(i+1), lt.atZone(ZoneId.of("UTC"))); 
                    //log.debug("%s : %s (ts is not null)", meta.getColumnName(i+1), ts);
                } else {
                    output.put(meta.getColumnName(i+1), null); 
                    //log.debug("%s : %s (ts is null)", meta.getColumnName(i+1), ts);
                }
            } else if (meta.getColumnType(i+1) == java.sql.Types.BIT) {
                //n.setProperty(meta.getColumnName(i+1), Boolean.valueOf(result.getBoolean(i+1)));
                output.put(meta.getColumnName(i+1), Boolean.valueOf(result.getBoolean(i+1)));
            } else {
                //n.setProperty(meta.getColumnName(i+1), result.getObject(i+1));
                output.put(meta.getColumnName(i+1), result.getObject(i+1));
                //log.debug("%s : %s", meta.getColumnName(i+1), result.getObject(i+1));
            }
        }
        return output;
    }
    protected Stream<Output> fromSqlBatchToGraph(String sqlQuery, List<Object> sqlParams, String writeQuery, Map<String, Object> writeParams, int batchSize, String connString) {
        guard.check();
        log.info("Starting importFromSql");
        ImportBase.connectionUrl = connString;
        
        String[] arrQuery = sqlQuery.split(" ");
        int index = Arrays.asList(arrQuery).indexOf("FROM");
        String tableName = arrQuery[index + 1];

        Output metrics = new Output(tableName, batchSize);
        
        long start = System.currentTimeMillis();
        

        try (SQLServerConnection conn = DriverManager.getConnection(connectionUrl).unwrap(SQLServerConnection.class); 
            SQLServerPreparedStatement statement = conn.prepareStatement(sqlQuery).unwrap(SQLServerPreparedStatement.class);
        ) {
            // setParameters for sql prepared read statement
            for (int i = 0; i < sqlParams.size(); i++) {
                statement.setObject(i+1,sqlParams.get(i));    
            }
            Long count = 0L;
            ResultSet result = statement.executeQuery();
            ResultSetMetaData meta = result.getMetaData();
            BatchTransaction batchTx = new BatchTransaction(db, batchSize, log);
            // Process each record as `$data` in the provided writeQuery Cypher.
            while (result.next()) {
                Map<String, Object> output = mapSqlResult(result, meta);
                writeParams.put("data", output);
                batchTx.execute(writeQuery, writeParams);
                batchTx.increment();
                count++;
            }
            batchTx.commit();
            statement.closeOnCompletion();
            conn.close(); 
            batchTx.close();
            
            metrics.batchCount =  Long.valueOf(batchTx.getCountExecutedBatches());
            metrics.statistics = batchTx.getBatchStatistics().toString(); 
            metrics.completedSuccessfully = true;
            metrics.runDuration = System.currentTimeMillis()-start; 
            metrics.recordsProcessed = count;                       
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
        }
        
        
     
        
        return Stream.of(metrics);
    }
}
