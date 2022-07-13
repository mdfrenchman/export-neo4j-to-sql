package osmis.export;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import org.neo4j.procedure.Mode;

import org.neo4j.logging.Log;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;

import java.sql.DriverManager;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExportBase {

    @Context
    public Log log;

    @Context 
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    protected Stream<Output> exportToSql(Result data, String tableName, Long batchSize, String connString) throws Exception {
        String connectionUrl = connString + ";useBulkCopyForBatchInsert=true";
        Output metrics = new Output(tableName, batchSize);

        log.info("Starting Export");

        List<String> columns = data.columns();

        String insertSql = String.format("insert into %s (%s) values (%s)", tableName, columns.stream().collect(Collectors.joining(",")), columns.stream().map(s -> {return "?";}).collect(Collectors.joining(", ")));

        try (
            SQLServerConnection conn = DriverManager.getConnection(connectionUrl).unwrap(SQLServerConnection.class);
            SQLServerPreparedStatement exportStatement = conn.prepareStatement(insertSql).unwrap(SQLServerPreparedStatement.class);
        ){
            int rowsInBatchCount = 0;
            long start = 0L;

            while (data.hasNext()){
                if (metrics.recordsExported % batchSize == 0 ) {
                    start = System.currentTimeMillis();
                    metrics.batchCount++;
                }
                Map<String, Object> map = data.next();
                metrics.recordsExported++;
                rowsInBatchCount++;
                
                // Question: is there a performance penalty for using setObject since it's infering TYPE?
                for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                        exportStatement.setObject(columnIndex+1, map.get(columns.get(columnIndex)));
                }
                exportStatement.addBatch();
                
                if (metrics.recordsExported % batchSize == 0 ) {
                    exportStatement.executeBatch();
                    long end = System.currentTimeMillis();
                    log.info("Finished batch " + metrics.batchCount + ". Submitted " + rowsInBatchCount + " rows. Time taken : " + (end - start) + " milliseconds.");
                    rowsInBatchCount = 0;
                }
                
            }
            // process the remaining tail after the last full batch.
            if (rowsInBatchCount > 0) {
                exportStatement.executeBatch();
                long end = System.currentTimeMillis();
                metrics.runDuration += (end-start);
                log.info("Finished batch " + metrics.batchCount + ". Submitted " + rowsInBatchCount + " rows. Time taken : " + (end - start) + " milliseconds.");
                rowsInBatchCount = 0;
            }

            exportStatement.closeOnCompletion();
            conn.close();
        }

        return Stream.of(metrics);
    }
}
