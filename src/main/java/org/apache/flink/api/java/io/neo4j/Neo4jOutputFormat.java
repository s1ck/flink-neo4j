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
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;

public class Neo4jOutputFormat<OUT extends Tuple>
  extends Neo4jFormatBase implements OutputFormat<OUT> {

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
   * Payload is continuously build as long as tuples come in. If the batch is full, or there are no
   * more tuples, the payload replaces the parameter in the query and get send to Neo4j.
   */
  private StringBuilder payload;

  /**
   * Used to build parameter maps.
   */
  private JsonNodeFactory nodeFactory;

  /**
   * batchSize = -1: Batch is send once when {@link #close()} is called
   * batchSize > 0: Batch is send, when currentBatchSize == batchSize
   */
  private int batchSize = -1;

  /**
   * The current number of elements in the batch.
   */
  private int currentBatchSize = 0;

  public Neo4jOutputFormat(Builder builder) {
    super(builder);
    this.elementKeys = builder.elementKeys.toArray(this.elementKeys);
    this.batchSize = builder.batchSize;
  }

  @Override
  public void configure(Configuration configuration) {
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    parameterName = getParameterName();
    nodeFactory = JsonNodeFactory.instance;
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
   * Returns the parameter name contained in the query,
   * e.g. "UNWIND {params} MATCH ..." returns "params"
   *
   * @return parameter name
   */
  private String getParameterName() {
    Pattern pattern = Pattern.compile("^[uU][nN][wW][iI][nN][dD] \\{(.+)\\} .*");
    Matcher matcher = pattern.matcher(getCypherQuery());
    if (matcher.matches()) {
      return matcher.group(1);
    }
    throw new IllegalArgumentException("Query does not contain a parameter statement.");
  }

  private void initBatch() {
    payload = new StringBuilder();
    currentBatchSize = 0;
    payload.append(String.format("\"%s\" : [", parameterName));
  }

  private void addToBatch(OUT tuple) throws IOException {
    if (elementTypes == null) {
      initValueTypes(tuple);
    }

    if (!isBatchEmpty()) {
      payload.append(",");
    }

    ObjectNode node = nodeFactory.objectNode();

    for (int i = 0; i < elementKeys.length; i++) {
      if (elementTypes[i].equals(Boolean.class)) {
        node.put(elementKeys[i], (Boolean) tuple.getField(i));
      } else if (elementTypes[i].equals(Integer.class)) {
        node.put(elementKeys[i], (Integer) tuple.getField(i));
      } else if (elementTypes[i].equals(Long.class)) {
        node.put(elementKeys[i], (Long) tuple.getField(i));
      } else if (elementTypes[i].equals(Float.class)) {
        node.put(elementKeys[i], (Float) tuple.getField(i));
      } else if (elementTypes[i].equals(Double.class)) {
        node.put(elementKeys[i], (Double) tuple.getField(i));
      } else if (elementTypes[i].equals(String.class)) {
        node.put(elementKeys[i], (String) tuple.getField(i));
      } else {
        throw new IOException("Unsupported field type for value: " + tuple.getField(i));
      }
    }

    payload.append(node.toString());

    currentBatchSize++;
  }

  private void finalizeBatch() {
    payload.append("]");
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

    String requestPayload = String.format(PAYLOAD_TEMPLATE, getCypherQuery(), payload.toString());

    ClientResponse response = client
      .resource(restURI + TRANSACTION_URI)
      .accept(MediaType.APPLICATION_JSON)
      .header("Content-Type", "application/json;charset=UTF-8")
      .entity(requestPayload)
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

  public static Builder buildNeo4jOutputFormat() {
    return new Builder();
  }

  /**
   * Used to build instances of {@link Neo4jOutputFormat}.
   */
  public static class Builder extends Neo4jFormatBase.Builder<Builder> {

    private List<String> elementKeys = new ArrayList<>();

    private int batchSize;

    /**
     * Used to tell the output format which parameter keys are used.
     *
     * Consider the following cypher query:
     *
     * UNWIND {inserts} AS i CREATE (a:User {name:i.name, born:i.born})
     *
     * The parameter keys in that example are "name" and "born".
     *
     * @param key parameter key used to set a literal
     * @return builder
     */
    public Builder addParameterKey(String key) {
      return addParameterKey(elementKeys.size(), key);
    }

    /**
     * Used to tell the output format which parameter keys are used.
     *
     * Consider the following cypher query:
     *
     * UNWIND {inserts} AS i CREATE (a:User {name:i.name, born:i.born})
     *
     * The parameter keys in that example are "name" and "born".
     *
     * @param position index of the key in the query
     * @param key parameter key used to set a literal
     * @return builder
     */
    public Builder addParameterKey(int position, String key) {
      checkArgument(!(key.isEmpty() || key == null), "Key must not be null or empty.");
      elementKeys.add(position, key);
      return getThis();
    }

    /**
     * Sets the batch size per Flink task. Output are written in parallel batches where each
     * Flink task is responsible for a part of the dataset. Using this parameter, one can set
     * how many elements are contained in a single batch sent by a Flink task.
     *
     * @param batchSize batch size per task
     * @return builder
     */
    public Builder setTaskBatchSize(int batchSize) {
      checkArgument(batchSize >= 0, "Batch size must be greater or equal than zero.");
      this.batchSize = batchSize;
      return getThis();
    }

    @Override
    public Builder getThis() {
      return this;
    }

    /**
     * Creates the output format.
     *
     * @return output format
     */
    public Neo4jOutputFormat finish() {
      validate();
      if (elementKeys.size() == 0) {
        throw new IllegalArgumentException("No parameter keys were supplied.");
      }
      return new Neo4jOutputFormat(this);
    }
  }
}
