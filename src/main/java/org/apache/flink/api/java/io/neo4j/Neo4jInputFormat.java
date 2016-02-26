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
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.hadoop.shaded.com.google.common.base.Strings;
import org.apache.flink.types.NullValue;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.MappingJsonFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

public class Neo4jInputFormat<OUT extends Tuple> extends Neo4jFormatBase
  implements InputFormat<OUT, InputSplit>, NonParallelInput {

  private static final long serialVersionUID = 1L;

  private transient ClientResponse response;

  private transient JsonParser jsonParser;

  @Override
  public void open(InputSplit ignored) throws IOException {
    Client client = createClient();

    String payload = String.format(PAYLOAD_TEMPLATE, query, "");

    response = client
      .resource(restURI + TRANSACTION_URI)
      .accept(MediaType.APPLICATION_JSON)
      .header("Content-Type", "application/json;charset=UTF-8")
      .header("X-Stream", "true")
      .entity(payload)
      .post(ClientResponse.class);

    if (response.getStatus() == Response.Status.OK.getStatusCode()) {
      jsonParser = new MappingJsonFactory().createJsonParser(response.getEntityInputStream());
    } else {
      close();
      throw new IOException(String.format("Server returned status [%d]", response.getStatus()));
    }
  }

  /**
   * Moves JsonParser through the document until a "row" element is found or end of input.
   *
   * @return true, if there is another row element, false otherwise
   * @throws IOException
   */
  @Override
  public boolean reachedEnd() throws IOException {
    boolean foundTuple = false;
    while(!foundTuple && jsonParser.nextToken() != null) {
      foundTuple = ROW_FIELD.equals(jsonParser.getCurrentName());
    }
    return !foundTuple;
  }

  /**
   * Read "row" json node into tuple.
   *
   * @param reuse tuple for writing
   * @return tuple with row content
   * @throws IOException
   */
  @Override
  public OUT nextRecord(OUT reuse) throws IOException {
    JsonNode node = jsonParser.readValueAsTree().get(ROW_FIELD);
    readFields(reuse, node);
    return reuse;
  }

  /**
   * Fills the given tuple fields with values according to their JSON type.
   *
   * @param reuse tuple for writing
   * @param fieldValues row values form query result
   * @throws IOException
   */
  private void readFields(OUT reuse, JsonNode fieldValues) throws IOException {
    for (int i = 0; i < fieldValues.size(); i++) {
      JsonNode fieldValue = fieldValues.get(i);
      if (fieldValue.isNull()) {
        reuse.setField(NullValue.getInstance(), i);
      } else if (fieldValue.isBoolean()) {
        reuse.setField(fieldValue.getBooleanValue(), i);
      } else if (fieldValue.isInt()) {
        reuse.setField(fieldValue.getIntValue(), i);
      } else if (fieldValue.isLong()) {
        reuse.setField(fieldValue.getLongValue(), i);
      } else if (fieldValue.isDouble()) {
        reuse.setField(fieldValue.getDoubleValue(), i);
      } else if (fieldValue.isTextual()) {
        reuse.setField(fieldValue.getTextValue(), i);
      } else {
        close();
        throw new IOException("Unsupported field type for value: " + fieldValue.getTextValue());
      }
    }
  }

  @Override
  public void configure(Configuration configuration) {
  }

  /**
   * Close all used resources.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (jsonParser != null) {
      jsonParser.close();
    }
    if (response != null) {
      response.close();
    }
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
    return baseStatistics;
  }

  @Override
  public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
    return new GenericInputSplit[]{
      new GenericInputSplit(0, 1)
    };
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
    return new DefaultInputSplitAssigner(inputSplits);
  }

  public static Neo4jInputFormatBuilder buildNeo4jInputFormat() {
    return new Neo4jInputFormatBuilder();
  }

  public static class Neo4jInputFormatBuilder {
    private final Neo4jInputFormat format;

    public Neo4jInputFormatBuilder() {
      this.format = new Neo4jInputFormat();
    }

    public Neo4jInputFormatBuilder setRestURI(String restURL) {
      format.restURI = restURL;
      return this;
    }

    public Neo4jInputFormatBuilder setCypherQuery(String cypherQuery) {
      format.query = cypherQuery;
      return this;
    }

    public Neo4jInputFormatBuilder setUsername(String username) {
      format.username = username;
      return this;
    }

    public Neo4jInputFormatBuilder setPassword(String password) {
      format.password = password;
      return this;
    }

    public Neo4jInputFormatBuilder setConnectTimeout(int connectTimeout) {
      format.connectTimeout = connectTimeout;
      return this;
    }

    public Neo4jInputFormatBuilder setReadTimeout(int readTimeout) {
      format.readTimeout = readTimeout;
      return this;
    }

    public Neo4jInputFormat finish() {
      if (Strings.isNullOrEmpty(format.restURI)) {
        throw new IllegalArgumentException("No Rest URI was supplied.");
      }
      if (Strings.isNullOrEmpty(format.query)) {
        throw new IllegalArgumentException("No Cypher statement was supplied.");
      }
      return format;
    }
  }
}
