/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.io.neo4j;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoop.shaded.com.google.common.base.Strings;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.shaded.com.google.common.base.Preconditions.checkArgument;

public class Neo4jOutputFormat<OUT extends Tuple> extends Neo4jFormatBase implements OutputFormat<OUT> {

  private static final long serialVersionUID = 1L;

  /**
   * The parameter used in the Cypher UNWIND statement.
   */
  private String parameterName;

  /**
   * The keys to access the values in each map (e.g. {name:'Alice',age:42} has the keys [name, age])
   */
  private String[] elementKeys = new String[0];

  /**
   * Data types of the values associated to the element keys.
   */
  private Class[] elementTypes;

  /**
   * Payload is continuously build as long as tuples come in. If the batch is full, or there are no more tuples, the
   * payload replaces the parameter in the query and get send to Neo4j.
   */
  private StringBuilder payload;

  /**
   * batchSize = -1: Batch is send once when {@link #close()} is called
   * batchSize > 0: Batch is send, when currentBatchSize == batchSize
   */
  private int batchSize = -1;

  /**
   * The current number of elements in the batch.
   */
  private int currentBatchSize = 0;

  @Override
  public void configure(Configuration configuration) {
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    parameterName = getParameterName();
    initBatch();
  }

  /**
   * Adds the tuple to the current batch. If the batch is full, it is send to the Neo4j server.
   *
   * @param tuple Flink tuple
   * @throws IOException
   */
  @Override
  public void writeRecord(OUT tuple) throws IOException {
    addToBatch(tuple);
    if (isBatchFull()) {
      finalizeBatch();
      sendBatch();
      initBatch();
    }
  }

  @Override
  public void close() throws IOException {
    if (!isBatchEmpty()) {
      finalizeBatch();
      sendBatch();
    }
  }

  /**
   * Returns the parameter name contained in the query, e.g. "UNWIND {params} MATCH ..." returns "{UNWIND}"
   *
   * @return parameter name
   */
  private String getParameterName() {
    Pattern pattern = Pattern.compile("[uU][nN][wW][iI][nN][dD] (\\{.+\\}) .*");
    Matcher matcher = pattern.matcher(getQuery());
    if (matcher.matches()) {
      return matcher.group(1);
    }
    throw new IllegalArgumentException("Query does not contain a parameter statement.");
  }

  private void initBatch() {
    payload = new StringBuilder();
    currentBatchSize = 0;
    payload.append("[");
  }

  private void addToBatch(OUT tuple) throws IOException {
    if (elementTypes == null) {
      initValueTypes(tuple);
    }

    if (!isBatchEmpty()) {
      payload.append(",");
    }

    payload.append("{");
    for (int i = 0; i < elementKeys.length; i++) {
      payload.append(elementKeys[i]).append(":");
      if (elementTypes[i].equals(String.class)) {
        payload.append("\\\"").append(tuple.getField(i)).append("\\\"");
      } else {
        payload.append(tuple.getField(i));
      }
      if (i < elementKeys.length - 1) {
        payload.append(",");
      }
    }
    payload.append("}");
    currentBatchSize++;
  }

  private void finalizeBatch() {
    payload.append("]");
  }

  private String buildBatchQuery() {
    return getQuery().replace(parameterName, payload.toString());
  }

  private void initValueTypes(OUT tuple) throws IOException {
    elementTypes = new Class[tuple.getArity()];
    for (int i = 0; i < tuple.getArity(); i++) {
      Object field = tuple.getField(i);
      if (field instanceof Boolean) {
        elementTypes[i] = Boolean.class;
      } else if (field instanceof Integer) {
        elementTypes[i] = Integer.class;
      } else if (field instanceof Long) {
        elementTypes[i] = Long.class;
      } else if (field instanceof Float) {
        elementTypes[i] = Float.class;
      } else if (field instanceof Double) {
        elementTypes[i] = Double.class;
      } else if (field instanceof String) {
        elementTypes[i] = String.class;
      } else {
        throw new IOException("Unsupported field type for value: " + field);
      }
    }
  }

  /**
   * Opens a connection to the Neo4j endpoint and sends the transactional batch query.
   *
   * @throws IOException
   */
  private void sendBatch() throws IOException {
    Client client = createClient();

    String batchQuery = buildBatchQuery();
    String payload = String.format(PAYLOAD_TEMPLATE, batchQuery);

    ClientResponse response = client
      .resource(restURI + TRANSACTION_URI)
      .accept(MediaType.APPLICATION_JSON)
      .header("Content-Type", "application/json;charset=UTF-8")
      .entity(payload)
      .post(ClientResponse.class);

    if (response.getStatus() != Response.Status.OK.getStatusCode()) {
      throw new IOException(String.format("Server returned status [%d]", response.getStatus()));
    }
    response.close();
  }


  private boolean isBatchEmpty() {
    return currentBatchSize == 0;
  }

  private boolean isBatchFull() {
    return currentBatchSize == batchSize;
  }

  public static Neo4jOutputFormatBuilder buildNeo4jOutputFormat() {
    return new Neo4jOutputFormatBuilder();
  }

  public static class Neo4jOutputFormatBuilder {
    private final Neo4jOutputFormat format = new Neo4jOutputFormat<>();

    private List<String> parameterKeys = Lists.newArrayList();

    public Neo4jOutputFormatBuilder setRestURI(String restURI) {
      format.restURI = restURI;
      return this;
    }

    public Neo4jOutputFormatBuilder setCypherQuery(String cypherQuery) {
      format.query = cypherQuery;
      return this;
    }

    public Neo4jOutputFormatBuilder setUsername(String username) {
      format.username = username;
      return this;
    }

    public Neo4jOutputFormatBuilder setPassword(String password) {
      format.password = password;
      return this;
    }

    public Neo4jOutputFormatBuilder setConnectTimeout(int connectTimeout) {
      format.connectTimeout = connectTimeout;
      return this;
    }

    public Neo4jOutputFormatBuilder setReadTimeout(int readTimeout) {
      format.readTimeout = readTimeout;
      return this;
    }

    public Neo4jOutputFormatBuilder addParameterKey(String key) {
      return addParameterKey(parameterKeys.size(), key);
    }

    public Neo4jOutputFormatBuilder addParameterKey(int position, String key) {
      checkArgument(!Strings.isNullOrEmpty(key), "Key must not be null or empty.");
      parameterKeys.add(position, key);
      return this;
    }

    public Neo4jOutputFormatBuilder setTaskBatchSize(int batchSize) {
      checkArgument(batchSize >= 0, "Batch size must be greater or equal than zero.");
      format.batchSize = batchSize;
      return this;
    }

    public Neo4jOutputFormat finish() {
      if (Strings.isNullOrEmpty(format.restURI)) {
        throw new IllegalArgumentException("No Rest URI was supplied.");
      }
      if (Strings.isNullOrEmpty(format.query)) {
        throw new IllegalArgumentException("No Cypher statement was supplied.");
      }
      if (parameterKeys.size() == 0) {
        throw new IllegalArgumentException("No parameter keys were supplied.");
      }
      format.elementKeys = parameterKeys.toArray(format.elementKeys);

      return format;
    }
  }
}
