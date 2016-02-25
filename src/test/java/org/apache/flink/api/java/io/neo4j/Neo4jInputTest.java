package org.apache.flink.api.java.io.neo4j;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class Neo4jInputTest {

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
  public void readTest() throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    String restURI = neo4j.httpURI().resolve("/db/data/").toString();

    String vertexQuery = "MATCH (n:User) RETURN id(n), n.name, n.born, n.height, n.trust";

    Neo4jInputFormat<Tuple5<Integer, String, Integer, Double, Boolean>> vertexInput =
      Neo4jInputFormat.buildNeo4jInputFormat()
        .setRestURI(restURI)
        .setCypherQuery(vertexQuery)
        .setConnectTimeout(10000)
        .setReadTimeout(10000)
        .finish();

    DataSet<Tuple5<Integer, String, Integer, Double, Boolean>> vertexRows = env.createInput(vertexInput,
      new TupleTypeInfo<Tuple5<Integer, String, Integer, Double, Boolean>>(
        BasicTypeInfo.INT_TYPE_INFO,    // id
        BasicTypeInfo.STRING_TYPE_INFO, // name
        BasicTypeInfo.INT_TYPE_INFO,    // born
        BasicTypeInfo.DOUBLE_TYPE_INFO, // height
        BasicTypeInfo.BOOLEAN_TYPE_INFO // trust
        ));

    String edgeQuery = "MATCH (a:User)-[e]->(b:User) RETURN id(e), id(a), id(b), e.since";

    Neo4jInputFormat<Tuple4<Integer, Integer, Integer, Integer>> edgeInput =
      Neo4jInputFormat.buildNeo4jInputFormat()
        .setRestURI(restURI)
        .setCypherQuery(edgeQuery)
        .setConnectTimeout(10000)
        .setReadTimeout(10000)
        .finish();

    DataSet<Tuple4<Integer, Integer, Integer, Integer>> edgeRows = env.createInput(edgeInput,
      new TupleTypeInfo<Tuple4<Integer, Integer, Integer, Integer>>(
        BasicTypeInfo.INT_TYPE_INFO, // edge id
        BasicTypeInfo.INT_TYPE_INFO, // source id
        BasicTypeInfo.INT_TYPE_INFO, // target id
        BasicTypeInfo.INT_TYPE_INFO  // since
      ));

    List<Tuple5<Integer, String, Integer, Double, Boolean>> vertexList = Lists.newArrayList();
    List<Tuple4<Integer, Integer, Integer, Integer>> edgeList = Lists.newArrayList();

    vertexRows.output(new LocalCollectionOutputFormat<>(vertexList));
    edgeRows.output(new LocalCollectionOutputFormat<>(edgeList));

    env.execute();

    assertEquals("wrong number of vertices", 3, vertexList.size());
    assertEquals("wrong number of edges", 2, edgeList.size());

    Integer idAlice = 0, idBob = 0;

    for (Tuple5<Integer, String, Integer, Double, Boolean> vertex : vertexList) {
      switch (vertex.f1) {
        case "Alice":
          idAlice = vertex.f0;
          validateVertex(vertex, 1984, 1.72, Boolean.TRUE);
          break;
        case "Bob":
          idBob = vertex.f0;
          validateVertex(vertex, 1983, 1.81, Boolean.TRUE);
          break;
        case "Eve":
          validateVertex(vertex, 1984, 1.62, Boolean.FALSE);
          break;
      }
    }

    for (Tuple4<Integer, Integer, Integer, Integer> edge : edgeList) {
      if (edge.f1.equals(idAlice)) {
        validateEdge(edge, idBob, 2001);
      } else if (edge.f1.equals(idBob)) {
        validateEdge(edge, idAlice, 2002);
      }
    }
  }

  private void validateEdge(Tuple4<Integer, Integer, Integer, Integer> edge, Integer targetId, int since) {
    assertEquals("wrong target vertex id", targetId, edge.f2);
    assertEquals("wrong property value (since)", Integer.valueOf(since), edge.f3);
  }

  private void validateVertex(Tuple5<Integer, String, Integer, Double, Boolean> vertex, int born, double weight, boolean trust) {
    assertEquals("wrong property value (since)",  Integer.valueOf(born), vertex.f2);
    assertEquals("wrong property value (born)",   Double.valueOf(weight), vertex.f3);
    assertEquals("wrong property value (weight)", trust, vertex.f4);
  }
}
