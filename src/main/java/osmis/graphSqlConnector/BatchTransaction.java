package osmis.graphSqlConnector;

import java.util.Map;

import org.neo4j.fabric.stream.summary.MergedQueryStatistics;
import org.neo4j.fabric.stream.summary.MergedSummary;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

public class BatchTransaction implements AutoCloseable {

    private final GraphDatabaseService db;
    private final Log log;
    private final int batchSize;
    Transaction tx;
    // total count of records for the entire function.
    int count = 0;
    // count of records in the current batch.
    int batchCount = 0;

    int processedBatches = 0;

    // combine the querystatistics for return
    MergedQueryStatistics statistics;
    

    public BatchTransaction (GraphDatabaseService db, int batchSize, Log log) {
        this.db = db;
        this.log = log;
        this.batchSize = batchSize;
        tx = beginTx();        
        statistics = new MergedQueryStatistics();
    }

    
    private Transaction beginTx(){
        log.info("begin new transaction total count of records: %d", count);
        return db.beginTx();
    }

    private void batchCommit() {
        tx.commit();
        tx.close();
        processedBatches++;
        // add logging
        tx = beginTx();
        batchCount = 0;
    }
    public void commit() {
        batchCommit();
    }
    public void rollback() {
        tx.rollback();
    }

    public void increment() {
        count++;
        batchCount++;
        if (batchCount >= batchSize) {
            batchCommit();
        }
    }

    public Result execute(String query, Map<String, Object> parameters) {
        Result r =  tx.execute(query, parameters);
        statistics.add(r.getQueryStatistics());
        return r;
    }

    public MergedQueryStatistics getBatchStatistics() {
        return statistics;
    }

    public int getCountExecutedBatches() {
        return processedBatches;
    }

    public int getCountTotal() {
        return count;
    }

    @Override
    public void close() throws Exception {
        if (tx != null) {
            tx.close();
        }
        
    }
}
