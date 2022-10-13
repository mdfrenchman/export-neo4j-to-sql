package osmis.graphSqlConnector.toSql;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import osmis.graphSqlConnector.toSql.ExportAdvanced;

import static org.assertj.core.api.Assertions.assertThat;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExportAdvancedTest {
    
    private static final Config driverConfig = Config.builder().withoutEncryption().build();
    private Neo4j embeddedDatabaseServer;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder().withDisabledServer().withProcedure(ExportAdvanced.class).build();
    }

    // @Test
    // void procedure_fails_when_can_not_connect_to_sqlserver(){
        
    //     try(Driver driver =  GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig); Session session = driver.session()){
    //         String query = "CALL osmis.export.advanced(\"RETURN 'bob1' as theString, 1 as theInt, true as theBit, 1.111 as theFloat, datetime() as theDateTime\",\"TestTypes\",10, \"jdbc:sqlserver://localhost:1433;databaseName=dbTest;user=demoApp;password=OSMISdemo123$%^;encrypt=false\");";
    //         // When 
    //         Record record = session.run(query).single();
            
    //         // Then
    //         assertThat(record.get("completedSuccessfully").asBoolean()).isEqualTo(Boolean.FALSE.booleanValue());
            
    //     }
    // }
}
