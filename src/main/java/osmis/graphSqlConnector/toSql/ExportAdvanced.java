package osmis.graphSqlConnector.toSql;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import osmis.graphSqlConnector.Output;

import java.util.stream.Stream;

public class ExportAdvanced extends ExportBase {

    @Procedure(name = "osmis.export.advanced", mode = Mode.READ, eager = false)
    @Description("Any query, any table, any connection string; GO WILD. Procedure is defined as READ-ONLY because it, well ... you know, exports things.")
    public Stream<Output> advanced(@Name("query") String query, @Name("tableName") String tableName, @Name("batchSize") Long batchSize, @Name("connString") String connString) throws Exception {
        return super.exportToSql(query, tableName, batchSize, connString);
    }    
}