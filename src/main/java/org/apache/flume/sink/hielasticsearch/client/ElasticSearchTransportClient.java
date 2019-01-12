/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.hielasticsearch.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;

import org.apache.flume.sink.hielasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.hielasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.apache.flume.sink.hielasticsearch.IndexNameBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.flume.sink.hielasticsearch.ElasticSearchSinkConstants.DEFAULT_PORT;

public class ElasticSearchTransportClient implements ElasticSearchClient {

  public static final Logger logger = LoggerFactory
      .getLogger(ElasticSearchTransportClient.class);

  private TransportAddress[] serverAddresses;
  private ElasticSearchEventSerializer serializer;
  private ElasticSearchIndexRequestBuilderFactory indexRequestBuilderFactory;
  private BulkRequestBuilder bulkRequestBuilder;

  private Client client;

  @VisibleForTesting
  TransportAddress[] getServerAddresses() {
    return serverAddresses;
  }

  @VisibleForTesting
  void setBulkRequestBuilder(BulkRequestBuilder bulkRequestBuilder) {
    this.bulkRequestBuilder = bulkRequestBuilder;
  }

  /**
   * Transport client for external cluster
   * 
   * @param hostNames
   * @param clusterName
   * @param serializer
   */
  public ElasticSearchTransportClient(String[] hostNames, String clusterName,
      ElasticSearchEventSerializer serializer) {
    configureHostnames(hostNames);
    this.serializer = serializer;
    openClient(clusterName);
  }

  public ElasticSearchTransportClient(String[] hostNames, String clusterName,
      ElasticSearchIndexRequestBuilderFactory indexBuilder) {
    configureHostnames(hostNames);
    this.indexRequestBuilderFactory = indexBuilder;
    openClient(clusterName);
  }
  
  /**
   * Local transport client only for testing
   * 
   * @param indexBuilderFactory
   */
  public ElasticSearchTransportClient(ElasticSearchIndexRequestBuilderFactory indexBuilderFactory) {
    this.indexRequestBuilderFactory = indexBuilderFactory;
    openLocalDiscoveryClient();
  }
  
  /**
   * Local transport client only for testing
   *
   * @param serializer
   */
  public ElasticSearchTransportClient(ElasticSearchEventSerializer serializer) {
    this.serializer = serializer;
    openLocalDiscoveryClient();
  }

  /**
   * Used for testing
   *
   * @param client
   *    ElasticSearch Client
   * @param serializer
   *    Event Serializer
   */
  public ElasticSearchTransportClient(Client client,
      ElasticSearchEventSerializer serializer) {
    this.client = client;
    this.serializer = serializer;
  }

  /**
   * Used for testing
   */
  public ElasticSearchTransportClient(Client client,
                                      ElasticSearchIndexRequestBuilderFactory requestBuilderFactory)
      throws IOException {
    this.client = client;
    requestBuilderFactory.createIndexRequest(client, null, null, null);
  }

  private void configureHostnames(String[] hostNames) {
    logger.warn(Arrays.toString(hostNames));
    serverAddresses = new TransportAddress[hostNames.length];
    for (int i = 0; i < hostNames.length; i++) {
      String[] hostPort = hostNames[i].trim().split(":");
      String host = hostPort[0].trim();
      int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim())
              : DEFAULT_PORT;
      serverAddresses[i] = new TransportAddress(new InetSocketAddress(host, port));
    }
  }
  

  public void close() {
    if (client != null) {
      client.close();
    }
    client = null;
  }

  public void addEvent(Event event, IndexNameBuilder indexNameBuilder,
      String indexType, long ttlMs) throws Exception {
    if (bulkRequestBuilder == null) {
      bulkRequestBuilder = client.prepareBulk();
    }
    IndexRequestBuilder indexRequestBuilder = null;
    if (indexRequestBuilderFactory == null) {
      //由于日志格式已处理，所以不需要使用flume提供的序列化方式，直接使用即可
      indexRequestBuilder = client
          .prepareIndex(indexNameBuilder.getIndexName(event), indexType)
          .setSource(event.getBody(), XContentType.JSON);
    } else {
      indexRequestBuilder = indexRequestBuilderFactory.createIndexRequest(
          client, indexNameBuilder.getIndexPrefix(event), indexType, event);
    }

    if (ttlMs > 0) {
      //es 已移除ttl
//      indexRequestBuilder.setTTL(ttlMs);
    }
    bulkRequestBuilder.add(indexRequestBuilder);
  }

  public void execute() throws Exception {
    try {
      BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
      if (bulkResponse.hasFailures()) {
        throw new EventDeliveryException(bulkResponse.buildFailureMessage());
      }
    } finally {
      bulkRequestBuilder = client.prepareBulk();
    }
  }

  /**
   * Open client to elaticsearch cluster
   * 
   * @param clusterName
   */
  private void openClient(String clusterName) {
    logger.info("Using ElasticSearch hostnames: {} ",
        Arrays.toString(serverAddresses));
    Settings settings = Settings.builder().put("cluster.name", clusterName).build();

    Collection<Class<? extends Plugin>> plugins  = Lists.newArrayList();
    TransportClient transportClient = new PreBuiltTransportClient(settings, plugins);
    for (TransportAddress host : serverAddresses) {
      transportClient.addTransportAddress(host);
    }
    if (client != null) {
      client.close();
    }
    client = transportClient;
  }

  /*
   * FOR TESTING ONLY...
   * 
   * Opens a local discovery node for talking to an elasticsearch server running
   * in the same JVM
   */
  private void openLocalDiscoveryClient() {
    logger.info("Using ElasticSearch AutoDiscovery mode");
    Node node = new Node(Settings.EMPTY){

      @Override
      protected void registerDerivedNodeNameWithLogger(String s) {

      }
    };

    if (client != null) {
      client.close();
    }

    client = node.client();
  }

  public void configure(Context context) {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
