package org.apache.flink.api.java.io.neo4j;

import org.apache.commons.lang.time.StopWatch;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.PageRank;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.types.NullValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Note: atm, the tests need a running Neo4j instance available at http://localhost:7474.
 *
 */
public class Neo4jInputTest {

  @SuppressWarnings("unchecked")
  @Test
  public void countTest() throws Exception {
    String restURI = "http://localhost:7474/db/data/";

    String cypherQuery = "MATCH (p1:Page)-[:Link]->(p2) RETURN id(p1), id(p2)";

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    Neo4jInputFormat<Tuple2<Integer, Integer>> neoInput = Neo4jInputFormat.buildNeo4jInputFormat()
      .setRestURI(restURI)
      .setCypherQuery(cypherQuery)
      .setUsername("neo4j")
      .setPassword("test")
      .setConnectTimeout(1000)
      .setReadTimeout(1000)
      .finish();

    DataSet<Tuple2<Integer, Integer>> edges = env.createInput(neoInput,
      new TupleTypeInfo(Tuple2.class, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO));

    Graph<Integer, NullValue, NullValue> graph = Graph.fromTuple2DataSet(edges, env);

    List<Vertex<Integer, NullValue>> vertexList = Lists.newArrayList();
    List<Edge<Integer, NullValue>> edgeList = Lists.newArrayList();

    graph.getVertices().output(new LocalCollectionOutputFormat<>(vertexList));
    graph.getEdges().output(new LocalCollectionOutputFormat<>(edgeList));

    env.execute();

    Assert.assertEquals("wrong vertex count", 430602, vertexList.size());
    Assert.assertEquals("wrong edge count", 2727302, edgeList.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void pageRankTest() throws Exception {
    String restURI = "http://localhost:7474/db/data/";
    String cypherQuery = "MATCH (p1:Page)-[:Link]->(p2) RETURN id(p1), id(p2)";

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    Neo4jInputFormat<Tuple2<Integer, Integer>> neoInput = Neo4jInputFormat.buildNeo4jInputFormat()
      .setRestURI(restURI)
      .setCypherQuery(cypherQuery)
      .setUsername("neo4j")
      .setPassword("test")
      .setConnectTimeout(1000)
      .setReadTimeout(1000)
      .finish();

    DataSet<Tuple3<Integer, Integer, Double>> edges = env.createInput(neoInput,
      new TupleTypeInfo(Tuple2.class, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
      .map(new MapFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>() {
        @Override
        public Tuple3<Integer, Integer, Double> map(Tuple2<Integer, Integer> neoEdge) throws Exception {
          return new Edge<>(neoEdge.f0, neoEdge.f1, 1.0);
        }
      }).withForwardedFields("f0;f1");


    Graph<Integer, Double, Double> graph = Graph.fromTupleDataSet(edges, new VertexInitializer(), env);

    DataSet<Vertex<Integer, Double>> ranks = graph.run(new PageRank<Integer>(0.85, 5));

    ranks.collect();

    System.out.println(String.format("NetRuntime [s]: %d",
      env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)));
  }

  private static class VertexInitializer implements MapFunction<Integer, Double> {

    @Override
    public Double map(Integer integer) throws Exception {
      return 1.0;
    }
  }
}
