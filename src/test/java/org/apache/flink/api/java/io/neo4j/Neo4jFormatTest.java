package org.apache.flink.api.java.io.neo4j;

import org.junit.Rule;
import org.neo4j.harness.junit.Neo4jRule;

public abstract class Neo4jFormatTest {

  /**
   * Test database
   */
  @Rule
  public Neo4jRule neo4j = new Neo4jRule()
    .withConfig("dbms.auth.enabled","false")
    .withFixture("CREATE" +
      "(alice:User { name : 'Alice', born : 1984, height : 1.72, trust : true  })," +
      "(bob:User   { name : 'Bob',   born : 1983, height : 1.81, trust : true  })," +
      "(eve:User   { name : 'Eve',   born : 1984, height : 1.62, trust : false })," +
      "(alice)-[:KNOWS {since : 2001}]->(bob)," +
      "(bob)-[:KNOWS   {since : 2002}]->(alice)");
}
