package osmis.graphSqlConnector;



import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import org.neo4j.logging.Log;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectorBase {
    
    @Context
    public Log log;

    @Context 
    public Transaction tx;

    @Context
    public GraphDatabaseService db;
    
    @Context
    public TerminationGuard guard;


    protected static String connectionUrl = "";
    protected static Long defaultBatchSize = 5000L;

}
    