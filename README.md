## flink-neo4j

Contains input and output formats to read Cypher results from Neo4j and write data back in parallel using Cypher batches.

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
