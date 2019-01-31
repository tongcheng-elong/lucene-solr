/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cloud.api.collections;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ReindexCollectionCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ABORT = "abort";
  public static final String COL_PREFIX = ".reindex_";
  public static final String REINDEX_PROP = CollectionAdminRequest.PROPERTY_PREFIX + "reindex";
  public static final String REINDEX_PHASE_PROP = CollectionAdminRequest.PROPERTY_PREFIX + "reindex_phase";

  private final OverseerCollectionMessageHandler ocmh;

  public enum State {
    IDLE,
    RUNNING,
    ABORTED,
    FINISHED;

    public String toLower() {
      return toString().toLowerCase(Locale.ROOT);
    }

    public static State get(String p) {
      if (p == null) {
        return null;
      }
      return states.get(p.toLowerCase(Locale.ROOT));
    }
    static Map<String, State> states = Collections.unmodifiableMap(
        Stream.of(State.values()).collect(Collectors.toMap(State::toLower, Function.identity())));
  }

  public ReindexCollectionCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {

    log.info("*** called: {}", message);

    String collection = message.getStr(CommonParams.NAME);
    boolean abort = message.getBool(ABORT, false);
    if (collection == null || clusterState.getCollectionOrNull(collection) == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection name must be specified and must exist");
    }
    DocCollection coll = clusterState.getCollection(collection);
    if (abort) {
      ZkNodeProps props = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
          ZkStateReader.COLLECTION_PROP, collection,
          REINDEX_PROP, State.ABORTED.toLower());
      ocmh.overseer.offerStateUpdate(Utils.toJSON(props));
      results.add(State.ABORTED.toLower(), collection);
      return;
    }
    // check it's not already running
    State state = State.get(coll.getStr(REINDEX_PROP, State.IDLE.toLower()));
    if (state == State.RUNNING) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Reindex is already running for collection " + collection);
    }
    // set the flag
    ZkNodeProps props = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
        ZkStateReader.COLLECTION_PROP, collection,
        REINDEX_PROP, State.RUNNING.toLower());
    ocmh.overseer.offerStateUpdate(Utils.toJSON(props));
    boolean aborted = false;
    Integer rf = coll.getReplicationFactor();
    Integer numNrt = coll.getNumNrtReplicas();
    Integer numTlog = coll.getNumTlogReplicas();
    Integer numPull = coll.getNumPullReplicas();
    int numShards = coll.getActiveSlices().size();

    String configName = message.getStr(ZkStateReader.CONFIGNAME_PROP, ocmh.zkStateReader.readConfigName(collection));
    String tmpCollection = COL_PREFIX + collection;

    try {
      // 0. set up temp collection - delete first if necessary
      NamedList<Object> cmdResults = new NamedList<>();
      ZkNodeProps cmd;
      if (clusterState.getCollectionOrNull(tmpCollection) != null) {
        // delete any aliases and the collection
        ocmh.zkStateReader.aliasesManager.update();
        String alias = DeleteCollectionCmd.referencedByAlias(tmpCollection, ocmh.zkStateReader.getAliases());
        if (alias != null) {
          // delete the alias
          cmd = new ZkNodeProps(CommonParams.NAME, alias);
          ocmh.commandMap.get(CollectionParams.CollectionAction.DELETEALIAS).call(clusterState, cmd, cmdResults);
          // nocommit error checking
        }
        cmd = new ZkNodeProps(
            CommonParams.NAME, tmpCollection,
            CoreAdminParams.DELETE_METRICS_HISTORY, "true"
        );
        ocmh.commandMap.get(CollectionParams.CollectionAction.DELETE).call(clusterState, cmd, cmdResults);
        // nocommit error checking
      }
      // create the tmp collection - use RF=1
      cmd = new ZkNodeProps(
          CommonParams.NAME, tmpCollection,
          ZkStateReader.NUM_SHARDS_PROP, String.valueOf(numShards),
          ZkStateReader.REPLICATION_FACTOR, "1",
          CollectionAdminParams.COLL_CONF, configName,
          CommonAdminParams.WAIT_FOR_FINAL_STATE, "true"
      );
      ocmh.commandMap.get(CollectionParams.CollectionAction.CREATE).call(clusterState, cmd, cmdResults);
      // nocommit error checking
      // wait for a while until we see the collection
      TimeOut waitUntil = new TimeOut(30, TimeUnit.SECONDS, ocmh.timeSource);
      boolean created = false;
      while (! waitUntil.hasTimedOut()) {
        waitUntil.sleep(100);
        // this also refreshes our local var clusterState
        clusterState = ocmh.cloudManager.getClusterStateProvider().getClusterState();
        created = clusterState.hasCollection(tmpCollection);
        if(created) break;
      }
      if (!created) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not fully create collection: " + tmpCollection);
      }
      if (maybeAbort(collection)) {
        aborted = true;
        return;
      }

      // 1. copy existing docs


      // ?. set up alias - new docs will go to the tmpCollection
      cmd = new ZkNodeProps(
          CommonParams.NAME, collection,
          "collections", tmpCollection
      );
      ocmh.commandMap.get(CollectionParams.CollectionAction.CREATEALIAS).call(clusterState, cmd, cmdResults);
      // nocommit error checking
    } finally {
      if (aborted) {
        // nocommit - cleanup
        results.add(State.ABORTED.toLower(), collection);
      }
    }
  }

  private boolean maybeAbort(String collection) throws Exception {
    DocCollection coll = ocmh.cloudManager.getClusterStateProvider().getClusterState().getCollectionOrNull(collection);
    if (coll == null) {
      // collection no longer present - abort
      return true;
    }
    State state = State.get(coll.getStr(REINDEX_PROP, State.RUNNING.toLower()));
    if (state != State.ABORTED) {
      return false;
    }
    return true;
  }
}
