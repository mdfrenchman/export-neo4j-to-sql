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

public class ExportToSql extends ExportBase {
    @Context
    public Log log;

    @Context 
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Procedure(name = "osmis.export.advanced", mode = Mode.READ)
    @Description("Any query, any table, any connection string; GO WILD. Procedure is defined as READ-ONLY because it, well ... you know, exports things.")
    public Stream<Output> advanced(@Name("query") String query, @Name("tableName") String tableName, @Name("batchSize") Long batchSize, @Name("connString") String connString) throws Exception {
        Result result = tx.execute(query);
        return super.exportToSql(result, tableName, batchSize, connString);
    }    
}