package osmis.graphSqlConnector.fromSql;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import osmis.graphSqlConnector.Output;
//import osmis.graphSqlConnector.TestTypePojo;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ImportAdvanced extends ImportBase {
    
    @Procedure(name = "osmis.import.testType", mode = Mode.WRITE, eager = true)
    @Description("DEMO ONLY: Import from the TestType TABLE as an integration demo test. This is a WIP and does not batch yet.")
    public Stream<TestTypePojo> advanced(@Name("sqlQuery") String sqlQuery, @Name("orderedParams") List<Object> orderedParams, @Name("connString") String connString) throws Exception {
        
        return super.getFromSql(sqlQuery, orderedParams, connString, 0).map(m -> new TestTypePojo(m));
    }

    public class TestTypePojo {
        public Boolean theBit;
        public String theString;
        public Number theInt;
        public Number theFloat;
        public ZonedDateTime theDateTime;
    
        public TestTypePojo(Map<String, Object> obj) {
            theBit = (Boolean) obj.get("theBit");
            theString = (String) obj.get("theString");
            theInt =  (Number)obj.get("theInt");
            theFloat = (Number) obj.get("theFloat");
            theDateTime = (ZonedDateTime) obj.get("theDateTime");
        }
    }

    @Procedure(name = "osmis.import.sql2graph", mode = Mode.WRITE, eager = true)
    @Description("Import from a sql table to a graph pattern. $data is the cypher variable for each row that comes back from sql.")
    public Stream<Output> advanced(@Name("sqlQuery") String sqlQuery, @Name("orderedParams") List<Object> orderedParams, 
    @Name("writeQuery") String writeQuery, @Name("writeParams") Map<String, Object> writeParams, @Name("batchSize") Number batchSize,
    @Name("connString") String connString) throws Exception {
        return super.fromSqlBatchToGraph(sqlQuery, orderedParams, writeQuery, writeParams, batchSize.intValue(), connString);        
    }
    
}