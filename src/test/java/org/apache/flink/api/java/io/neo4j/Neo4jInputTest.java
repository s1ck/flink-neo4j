package org.apache.flink.api.java.io.neo4j;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.function.Function;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.helpers.collection.IteratorUtil;

import java.util.List;

public class Neo4jInputTest {

  /**
   * Starts a Neo4j server instance, creates a node and queries it.
   *
   * @throws Exception
   */
  @Test
  public void neo4jOnlyTest() throws Exception
  {
    // Given
    try ( ServerControls server = TestServerBuilders.newInProcessBuilder()
      .withFixture( new Function<GraphDatabaseService, Void>()
      {
        @Override
        public Void apply( GraphDatabaseService graphDatabaseService ) throws RuntimeException
        {
          try ( Transaction tx = graphDatabaseService.beginTx() )
          {
            graphDatabaseService.createNode( DynamicLabel.label("User") );
            tx.success();
          }
          return null;
        }
      } )
      .newServer() )
    {
      // When
      Result result = server.graph().execute( "MATCH (n:User) return n" );

      // Then
      Assert.assertEquals(1, IteratorUtil.count(result));
    }
  }

  /**
   * Starts a Neo4j server instance, creates a node and queries it using Flink.
   *
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  @Test
  public void neo4jAndFlinkTest() throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    try ( ServerControls server = TestServerBuilders.newInProcessBuilder()
      .withFixture( new Function<GraphDatabaseService, Void>()
      {
        @Override
        public Void apply( GraphDatabaseService graphDatabaseService ) throws RuntimeException
        {
          try ( Transaction tx = graphDatabaseService.beginTx() )
          {
            graphDatabaseService.createNode( DynamicLabel.label("User") );
            tx.success();
          }
          return null;
        }
      } )
      .newServer() )
    {
      String restURI = server.httpURI() + "db/data/";
      System.out.println(restURI);
      String cypherQuery = "MATCH (n:User) return id(n)";
      // When
      Neo4jInputFormat<Tuple2<Integer, Integer>> neoInput = Neo4jInputFormat.buildNeo4jInputFormat()
        .setRestURI(restURI)
        .setCypherQuery(cypherQuery)
        .setConnectTimeout(1000)
        .setReadTimeout(1000)
        .finish();

      DataSet<Tuple1<Integer>> users = env.createInput(neoInput,
        new TupleTypeInfo(Tuple1.class, BasicTypeInfo.INT_TYPE_INFO));

      List<Tuple1<Integer>> userList = Lists.newArrayList();

      users.output(new LocalCollectionOutputFormat<>(userList));

      env.execute();

      Assert.assertEquals(1, userList.size());
    }
  }

//  @SuppressWarnings("unchecked")
//  @Test
//  public void miniCountTest() throws Exception {
//    String restURI = "http://localhost:7474/db/data/";
//
//    String cypherQuery = "MATCH (p1:Page)-[:Link]->(p2) RETURN id(p1), id(p2)";
//
//    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//    Neo4jInputFormat<Tuple2<Integer, Integer>> neoInput = Neo4jInputFormat.buildNeo4jInputFormat()
//      .setRestURI(restURI)
//      .setCypherQuery(cypherQuery)
//      .setUsername("neo4j")
//      .setPassword("test")
//      .setConnectTimeout(1000)
//      .setReadTimeout(1000)
//      .finish();
//
//    DataSet<Tuple2<Integer, Integer>> edges = env.createInput(neoInput,
//      new TupleTypeInfo(Tuple2.class, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO));
//
//    Graph<Integer, NullValue, NullValue> graph = Graph.fromTuple2DataSet(edges, env);
//
//    List<Vertex<Integer, NullValue>> vertexList = Lists.newArrayList();
//    List<Edge<Integer, NullValue>> edgeList = Lists.newArrayList();
//
//    graph.getVertices().output(new LocalCollectionOutputFormat<>(vertexList));
//    graph.getEdges().output(new LocalCollectionOutputFormat<>(edgeList));
//
//    env.execute();
//
//    Assert.assertEquals("wrong vertex count", 4, vertexList.size());
//    Assert.assertEquals("wrong edge count", 5, edgeList.size());
//  }
//
//  @SuppressWarnings("unchecked")
//  @Test
//  public void countTest() throws Exception {
//    String restURI = "http://localhost:7474/db/data/";
//
//    String cypherQuery = "MATCH (p1:Page)-[:Link]->(p2) RETURN id(p1), id(p2)";
//
//    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//    Neo4jInputFormat<Tuple2<Integer, Integer>> neoInput = Neo4jInputFormat.buildNeo4jInputFormat()
//      .setRestURI(restURI)
//      .setCypherQuery(cypherQuery)
//      .setUsername("neo4j")
//      .setPassword("test")
//      .setConnectTimeout(1000)
//      .setReadTimeout(1000)
//      .finish();
//
//    DataSet<Tuple2<Integer, Integer>> edges = env.createInput(neoInput,
//      new TupleTypeInfo(Tuple2.class, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO));
//
//    Graph<Integer, NullValue, NullValue> graph = Graph.fromTuple2DataSet(edges, env);
//
//    List<Vertex<Integer, NullValue>> vertexList = Lists.newArrayList();
//    List<Edge<Integer, NullValue>> edgeList = Lists.newArrayList();
//
//    graph.getVertices().output(new LocalCollectionOutputFormat<>(vertexList));
//    graph.getEdges().output(new LocalCollectionOutputFormat<>(edgeList));
//
//    env.execute();
//
//    Assert.assertEquals("wrong vertex count", 430602, vertexList.size());
//    Assert.assertEquals("wrong edge count", 2727302, edgeList.size());
//  }
//
//  @SuppressWarnings("unchecked")
//  @Test
//  public void pageRankTest() throws Exception {
//    String restURI = "http://localhost:7474/db/data/";
//    String cypherQuery = "MATCH (p1:Page)-[:Link]->(p2) RETURN id(p1), id(p2)";
//
//    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//    Neo4jInputFormat<Tuple2<Integer, Integer>> neoInput = Neo4jInputFormat.buildNeo4jInputFormat()
//      .setRestURI(restURI)
//      .setCypherQuery(cypherQuery)
//      .setUsername("neo4j")
//      .setPassword("test")
//      .setConnectTimeout(1000)
//      .setReadTimeout(1000)
//      .finish();
//
//    DataSet<Tuple3<Integer, Integer, Double>> edges = env.createInput(neoInput,
//      new TupleTypeInfo(Tuple2.class, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
//      .map(new MapFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>() {
//        @Override
//        public Tuple3<Integer, Integer, Double> map(Tuple2<Integer, Integer> neoEdge) throws Exception {
//          return new Edge<>(neoEdge.f0, neoEdge.f1, 1.0);
//        }
//      }).withForwardedFields("f0;f1");
//
//
//    Graph<Integer, Double, Double> graph = Graph.fromTupleDataSet(edges, new VertexInitializer(), env);
//
//    DataSet<Vertex<Integer, Double>> ranks = graph.run(new PageRank<Integer>(0.85, 5));
//
//    ranks.collect();
//
//    System.out.println(String.format("NetRuntime [s]: %d",
//      env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)));
//  }
//
//  private static class VertexInitializer implements MapFunction<Integer, Double> {
//
//    @Override
//    public Double map(Integer integer) throws Exception {
//      return 1.0;
//    }
//  }
}
