package org.apache.flink.api.java.io.neo4j;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class Neo4jOutputTest {

  @Rule
  public Neo4jRule neo4j = new Neo4jRule()
    .withConfig("dbms.auth.enabled","false")
    .withFixture("CREATE" +
      "(alice:User { name : 'Alice', born : 1984, height : 1.72, trust : true  })," +
      "(bob:User   { name : 'Bob',   born : 1983, height : 1.81, trust : true  })," +
      "(eve:User   { name : 'Eve',   born : 1984, height : 1.62, trust : false })," +
      "(alice)-[:KNOWS {since : 2001}]->(bob)," +
      "(bob)-[:KNOWS   {since : 2002}]->(alice)");

  @SuppressWarnings("unchecked")
  @Test
  public void updateTest() throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    String restURI = neo4j.httpURI().resolve("/db/data/").toString();

    String updateQuery = "" +
      "UNWIND {updates} AS update " +
      "MATCH (p) " +
      "WHERE p.name = update.name " +
      "SET p.weight = update.weight, p.height = update.height, p.trust = update.trust";

    Neo4jOutputFormat<Tuple4<String, Long, Double, Boolean>> outputFormat = Neo4jOutputFormat.buildNeo4jOutputFormat()
      .setRestURI(restURI)
      .setConnectTimeout(10_000)
      .setReadTimeout(10_000)
      .setCypherQuery(updateQuery)
      .addParameterKey("name")
      .addParameterKey("weight")
      .addParameterKey("height")
      .addParameterKey("trust")
      .finish();

    env.fromElements(new Tuple4<>("Alice", 42L, 1.74d, false), new Tuple4<>("Bob", 75L, 1.82d, true))
      .output(outputFormat);

    env.execute();

    // test it

    GraphDatabaseService graphDB = neo4j.getGraphDatabaseService();
    try (Transaction tx = graphDB.beginTx()) {
      Result result = graphDB.execute("" +
        "MATCH (n:User) " +
        "WHERE n.name = 'Alice' OR n.name = 'Bob' " +
        "RETURN n.name AS name, n.weight AS weight, n.height AS height, n.trust AS trust");
      while(result.hasNext()) {
        Map<String, Object> row = result.next();
        if (row.get("name").equals("Alice")) {
          assertEquals("wrong weight attribute value", 42L, row.get("weight"));
          assertEquals("wrong height attribute value", 1.74d, row.get("height"));
          assertEquals("wrong trust attribute value", false, row.get("trust"));

        } else {
          assertEquals("wrong weight attribute value", 75L, row.get("weight"));
          assertEquals("wrong height attribute value", 1.82d, row.get("height"));
          assertEquals("wrong trust attribute value", true, row.get("trust"));
        }
      }
      tx.success();
    }
  }
}
