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
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import org.apache.flink.hadoop.shaded.com.google.common.base.Strings;

import java.io.Serializable;

/**
 * Base class for {@link Neo4jInputFormat} and {@link Neo4jOutputFormat} that handles
 * connection related information.
 */
public abstract class Neo4jFormatBase implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Gets attached to the rest URI for transactional queries
   */
  protected static final String TRANSACTION_URI = "transaction/commit";

  /**
   * Payload template which is used to send cypher statements and payload to the endpoint.
   */
  protected static final String PAYLOAD_TEMPLATE =
    "{\"statements\" : [ {\"statement\" : \"%s\", \"parameters\" : {%s} }]}";

  protected String restURI;

  protected String query;

  protected String username;

  protected String password;

  protected int connectTimeout;

  protected int readTimeout;

  public Neo4jFormatBase(Builder builder) {
    this.restURI        = builder.restURI;
    this.query          = builder.query.replaceAll("\"", "\\\\\"");
    this.username       = builder.username;
    this.password       = builder.password;
    this.connectTimeout = builder.connectTimeout;
    this.readTimeout    = builder.readTimeout;
  }

  public String getRestURI() {
    return restURI;
  }

  public String getCypherQuery() {
    return query;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public int getReadTimeout() {
    return readTimeout;
  }

  /**
   * Create and configure the client for the REST call.
   *
   * @return Client
   */
  protected Client createClient() {
    Client client = Client.create();
    client.setConnectTimeout(getConnectTimeout());
    client.setReadTimeout(getReadTimeout());

    if (getUsername() != null && getPassword() != null) {
      client.addFilter(new HTTPBasicAuthFilter(getUsername(), getPassword()));
    }
    return client;
  }

  /**
   * Base builder class for building Neo4j in- and output formats.
   *
   * @param <T> child builder instance
   */
  public static abstract class Builder<T extends Builder> {

    protected String restURI;

    protected String query;

    protected String username;

    protected String password;

    /**
     * connect timeout in ms (default 1000)
     */
    protected int connectTimeout = 1000;

    /**
     * read timeout in ms (default 1000)
     */
    protected int readTimeout = 1000;

    /**
     * Set the Neo4j REST endpoint (e.g. "http://localhost:7475/db/data/")
     *
     * @param restURI Neo4j REST endpoint
     * @return builder
     */
    public T setRestURI(String restURI) {
      this.restURI = restURI;
      return getThis();
    }

    /**
     * Set Cypher query to execute against the endpoint.
     *
     * @param query cypher query
     * @return builder
     */
    public T setCypherQuery(String query) {
      this.query = query;
      return getThis();
    }

    /**
     * Set username if http authentication is enabled.
     *
     * @param userName Neo4j http auth username
     * @return builder
     */
    public T setUsername(String userName) {
      this.username = userName;
      return getThis();
    }

    /**
     * Set password if http authentication is enabled.
     *
     * @param password Neo4j http auth password
     * @return builder
     */
    public T setPassword(String password) {
      this.password = password;
      return getThis();
    }

    /**
     * Set connect timeout in milliseconds.
     *
     * @param connectTimeout connect timeout
     * @return builder
     */
    public T setConnectTimeout(int connectTimeout) {
      this.connectTimeout = connectTimeout;
      return getThis();
    }

    /**
     * Set read timeout in milliseconds.
     *
     * @param readTimeout read timeout
     * @return builder
     */
    public T setReadTimeout(int readTimeout) {
      this.readTimeout = readTimeout;
      return getThis();
    }

    /**
     * Validates mandatory arguments.
     */
    protected void validate() {
      if (Strings.isNullOrEmpty(restURI)) {
        throw new IllegalArgumentException("No Rest URI was supplied.");
      }
      if (Strings.isNullOrEmpty(query)) {
        throw new IllegalArgumentException("No Cypher statement was supplied.");
      }
    }

    /**
     * Returns the concrete builder instance.
     * @return builder instance
     */
    public abstract T getThis();
  }
}
