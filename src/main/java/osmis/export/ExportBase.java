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

    protected Stream<Output> exportToSql(Result data, String tableName, Long batchSize, String connString) throws Exception {
        Output metrics = new Output(tableName, batchSize);
        return Stream.of(metrics);
    }
}
