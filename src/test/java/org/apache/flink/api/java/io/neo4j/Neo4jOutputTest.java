package org.apache.flink.api.java.io.neo4j;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Neo4jOutputTest extends Neo4jFormatTest {

  @SuppressWarnings("unchecked")
  @Test
  public void createTest() throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

    String restURI = neo4j.httpURI().resolve("/db/data/").toString();

    String createQuery = "" +
      "UNWIND {inserts} AS i " +
      "CREATE (a:User {name:i.name, born:i.born, height:i.height, trust:i.trust})";

    Neo4jOutputFormat<Tuple4<String, Long, Double, Boolean>> outputFormat = Neo4jOutputFormat
      .buildNeo4jOutputFormat()
      .setRestURI(restURI)
      .setConnectTimeout(1_000)
      .setReadTimeout(1_000)
      .setCypherQuery(createQuery)
      .addParameterKey(0, "name")
      .addParameterKey(1, "born")
      .addParameterKey(2, "height")
      .addParameterKey(3, "trust")
      .finish();

    environment.fromElements(
      new Tuple4<>("Frank", 1982L, 1.84d, true),
      new Tuple4<>("Dave", 1976L, 1.82d, true))
      .output(outputFormat);

    environment.execute();

    // test it

    GraphDatabaseService graphDB = neo4j.getGraphDatabaseService();
    try (Transaction tx = graphDB.beginTx()) {
      Result result = graphDB.execute("" +
        "MATCH (n:User) " +
        "WHERE n.name = 'Frank' OR n.name = 'Dave' " +
        "RETURN n.name AS name, n.born AS born, n.height AS height, n.trust AS trust");
      int rows = 0;
      while(result.hasNext()) {
        rows++;
        Map<String, Object> row = result.next();
        if (row.get("name").equals("Frank")) {
          assertEquals("wrong born attribute value", 1982L, row.get("born"));
          assertEquals("wrong height attribute value", 1.84d, row.get("height"));
          assertEquals("wrong trust attribute value", true, row.get("trust"));
        } else if (row.get("name").equals("Dave")){
          assertEquals("wrong born attribute value", 1976L, row.get("born"));
          assertEquals("wrong height attribute value", 1.82d, row.get("height"));
          assertEquals("wrong trust attribute value", true, row.get("trust"));
        } else {
          assertTrue("Unexpected result", false);
        }
      }
      assertEquals("Unexpected row count", 2, rows);
      tx.success();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void updateTest() throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

    String restURI = neo4j.httpURI().resolve("/db/data/").toString();

    String updateQuery = "" +
      "UNWIND {updates} AS u " +
      "MATCH (p) " +
      "WHERE p.name = u.name " +
      "SET p.weight = u.weight, p.height = u.height, p.trust = u.trust";

    Neo4jOutputFormat<Tuple4<String, Long, Double, Boolean>> outputFormat = Neo4jOutputFormat
      .buildNeo4jOutputFormat()
      .setRestURI(restURI)
      .setConnectTimeout(10_000)
      .setReadTimeout(10_000)
      .setCypherQuery(updateQuery)
      .addParameterKey("name")
      .addParameterKey("weight")
      .addParameterKey("height")
      .addParameterKey("trust")
      .finish();

    environment.fromElements(
      new Tuple4<>("Alice", 42L, 1.74d, false),
      new Tuple4<>("Bob", 75L, 1.82d, true))
      .output(outputFormat);

    environment.execute();

    // test it

    GraphDatabaseService graphDB = neo4j.getGraphDatabaseService();
    try (Transaction tx = graphDB.beginTx()) {
      Result result = graphDB.execute("" +
        "MATCH (n:User) " +
        "WHERE n.name = 'Alice' OR n.name = 'Bob' " +
        "RETURN n.name AS name, n.weight AS weight, n.height AS height, n.trust AS trust");

      int rows = 0;
      while(result.hasNext()) {
        rows++;
        Map<String, Object> row = result.next();
        if (row.get("name").equals("Alice")) {
          assertEquals("wrong weight attribute value", 42L, row.get("weight"));
          assertEquals("wrong height attribute value", 1.74d, row.get("height"));
          assertEquals("wrong trust attribute value", false, row.get("trust"));
        } else if (row.get("name").equals("Bob")) {
          assertEquals("wrong weight attribute value", 75L, row.get("weight"));
          assertEquals("wrong height attribute value", 1.82d, row.get("height"));
          assertEquals("wrong trust attribute value", true, row.get("trust"));
        } else {
          assertTrue("Unexpected result", false);
        }
      }
      assertEquals("Unexpected row count", 2, rows);

      tx.success();
    }
  }
}
