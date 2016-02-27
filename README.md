[![Build Status](https://travis-ci.org/s1ck/flink-neo4j.svg?branch=master)](https://travis-ci.org/s1ck/flink-neo4j)


## flink-neo4j

Contains [Apache Flink](https://flink.apache.org/) specific input and output formats to read [Cypher](http://neo4j.com/docs/stable/cypher-query-lang.html) results from [Neo4j](http://neo4j.com/) and write data back in parallel using Cypher batches.

## Examples

* Read data from Neo4j into Flink datasets

```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

Neo4jInputFormat<Tuple3<Integer, String, Integer>> input = Neo4jInputFormat
    .buildNeo4jInputFormat()
    .setRestURI("http://localhost:7475/db/data/")
    .setUsername("neo4j")
    .setPassword("password")
    .setCypherQuery("MATCH (n:User) RETURN id(n), n.name, n.born")
    .setConnectTimeout(10000)
    .setReadTimeout(10000)
    .finish();

DataSet<Tuple3<Integer, String, Integer>> vertices = env.createInput(input,
    new TupleTypeInfo<Tuple3<Integer, String, Integer>>(
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO
    ));

// do something
```

* Write data to Neo4j using CREATE statements

```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

Neo4jOutputFormat<Tuple2<String, Integer>> outputFormat = Neo4jOutputFormat
    .buildNeo4jOutputFormat()
    .setRestURI("http://localhost:7475/db/data/")
    .setConnectTimeout(1_000)
    .setReadTimeout(1_000)
    .setCypherQuery("UNWIND {inserts} AS i CREATE (a:User {name:i.name, born:i.born})")
    .addParameterKey(0, "name")
    .addParameterKey(1, "born")
    .setTaskBatchSize(1000)
    .finish();

env.fromElements(new Tuple2<>("Alice", 1984),new Tuple2<>("Bob", 1976)).output(outputFormat);

env.execute();
```

## Setup

* Add repository and dependency to your maven project

```
<repositories>
  <repository>
    <id>dbleipzig</id>
    <name>Database Group Leipzig University</name>
    <url>https://wdiserv1.informatik.uni-leipzig.de:443/archiva/repository/dbleipzig/</url>
    <releases>
      <enabled>true</enabled>
    </releases>
    <snapshots>
      <enabled>true</enabled>
    </snapshots>
   </repository>
</repositories>

<dependency>
  <groupId>org.s1ck</groupId>
  <artifactId>flink-neo4j</artifactId>
  <version>0.1-SNAPSHOT</version>
</dependency>
```

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
