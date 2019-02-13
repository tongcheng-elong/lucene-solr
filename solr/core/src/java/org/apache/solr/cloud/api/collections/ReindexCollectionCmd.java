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

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
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
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reindex a collection, usually in order to change the index schema.
 * <p>WARNING: Reindexing is a potentially lossy operation - some indexed data that is not available as
 * stored fields may be irretrievably lost, so users should use this command with caution, evaluating
 * the potential impact by using different source and target collection names first, and preserving
 * the source collection until the evaluation is complete.</p>
 * <p>Reindexing follows these steps:</p>
 * <ol>
 *    <li>create a temporary collection using the most recent schema of the source collection
 *    (or the one specified in the parameters, which must already exist).</li>
 *    <li>copy the source documents to the temporary collection, reconstructing them from their stored
 *    fields and reindexing them using the specified schema. NOTE: some data
 *    loss may occur if the original stored field data is not available!</li>
 *    <li>if the target collection name is not specified
 *    then the same name as the source is assumed and at this step the source collection is permanently removed.</li>
 *    <li>create the target collection from scratch with the specified name (or the same as source if not
 *    specified), but using the new specified schema. NOTE: if the target name was not specified or is the same
 *    as the source collection then the original collection has been deleted in the previous step and it's
 *    not possible to roll-back the changes if the process is interrupted. The (possibly incomplete) data
 *    is still available in the temporary collection.</li>
 *    <li>copy the documents from the temporary collection to the target collection, using the specified schema.</li>
 *    <li>delete temporary collection(s) and optionally delete the source collection if it still exists.</li>
 * </ol>
 */
public class ReindexCollectionCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ABORT = "abort";
  public static final String KEEP_SOURCE = "keepSource";
  public static final String TARGET = "target";
  public static final String TMP_COL_PREFIX = ".reindex_";
  public static final String CHK_COL_PREFIX = ".reindex_ck_";
  public static final String REINDEX_PROP = CollectionAdminRequest.PROPERTY_PREFIX + "reindex";
  public static final String REINDEX_PHASE_PROP = CollectionAdminRequest.PROPERTY_PREFIX + "reindex_phase";
  public static final String READONLY_PROP = CollectionAdminRequest.PROPERTY_PREFIX + ZkStateReader.READ_ONLY_PROP;

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
      p = p.toLowerCase(Locale.ROOT);
      if (p.startsWith(CollectionAdminRequest.PROPERTY_PREFIX)) {
        p = p.substring(CollectionAdminRequest.PROPERTY_PREFIX.length());
      }
      return states.get(p);
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
    if (collection == null || clusterState.getCollectionOrNull(collection) == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection name must be specified and must exist");
    }
    String target = message.getStr(TARGET);
    if (target == null) {
      target = collection;
    }
    boolean keepSource = message.getBool(KEEP_SOURCE, false);
    if (keepSource && target.equals(collection)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can't specify keepSource=true when target is the same as source");
    }
    boolean abort = message.getBool(ABORT, false);
    DocCollection coll = clusterState.getCollection(collection);
    if (abort) {
      ZkNodeProps props = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
          ZkStateReader.COLLECTION_PROP, collection,
          REINDEX_PROP, State.ABORTED.toLower());
      ocmh.overseer.offerStateUpdate(Utils.toJSON(props));
      results.add(State.ABORTED.toLower(), collection);
      // if needed the cleanup will be performed by the running instance of the command
      return;
    }
    // check it's not already running
    State state = State.get(coll.getStr(REINDEX_PROP, State.IDLE.toLower()));
    if (state == State.RUNNING) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Reindex is already running for collection " + collection);
    }
    // set the running flag
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
    String tmpCollection = TMP_COL_PREFIX + collection;
    String chkCollection = CHK_COL_PREFIX + collection;

    try {
      // 0. set up temp and checkpoint collections - delete first if necessary
      NamedList<Object> cmdResults = new NamedList<>();
      ZkNodeProps cmd;
      if (clusterState.getCollectionOrNull(tmpCollection) != null) {
        // delete the tmp collection
        cmd = new ZkNodeProps(
            CommonParams.NAME, tmpCollection,
            CoreAdminParams.DELETE_METRICS_HISTORY, "true"
        );
        ocmh.commandMap.get(CollectionParams.CollectionAction.DELETE).call(clusterState, cmd, cmdResults);
        // nocommit error checking
      }
      if (clusterState.getCollectionOrNull(chkCollection) != null) {
        // delete the checkpoint collection
        cmd = new ZkNodeProps(
            CommonParams.NAME, chkCollection,
            CoreAdminParams.DELETE_METRICS_HISTORY, "true"
        );
        ocmh.commandMap.get(CollectionParams.CollectionAction.DELETE).call(clusterState, cmd, cmdResults);
        // nocommit error checking
      }

      if (maybeAbort(collection)) {
        aborted = true;
        return;
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

      // create the checkpoint collection - use RF=1 and 1 shard
      cmd = new ZkNodeProps(
          CommonParams.NAME, chkCollection,
          ZkStateReader.NUM_SHARDS_PROP, "1",
          ZkStateReader.REPLICATION_FACTOR, "1",
          CollectionAdminParams.COLL_CONF, configName,
          CommonAdminParams.WAIT_FOR_FINAL_STATE, "true"
      );
      ocmh.commandMap.get(CollectionParams.CollectionAction.CREATE).call(clusterState, cmd, cmdResults);
      // nocommit error checking
      // wait for a while until we see both collections
      TimeOut waitUntil = new TimeOut(30, TimeUnit.SECONDS, ocmh.timeSource);
      boolean created = false;
      while (! waitUntil.hasTimedOut()) {
        waitUntil.sleep(100);
        // this also refreshes our local var clusterState
        clusterState = ocmh.cloudManager.getClusterStateProvider().getClusterState();
        created = clusterState.hasCollection(tmpCollection) && clusterState.hasCollection(chkCollection);
        if(created) break;
      }
      if (!created) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not fully create temporary collection(s)");
      }
      if (maybeAbort(collection)) {
        aborted = true;
        return;
      }

      // 1. put the collection in read-only mode
      cmd = new ZkNodeProps(Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
          ZkStateReader.COLLECTION_PROP, collection,
          READONLY_PROP, "true");
      ocmh.overseer.offerStateUpdate(Utils.toJSON(props));

      // 2. copy the documents to tmp
      // Recipe taken from: http://joelsolr.blogspot.com/2016/10/solr-63-batch-jobs-parallel-etl-and.html
      ModifiableSolrParams q = new ModifiableSolrParams();
      q.set(CommonParams.QT, "/stream");
      q.set("expr",
          "daemon(id=\"" + tmpCollection + "\"," +
          "terminate=\"true\"," +
          "commit(" + tmpCollection + "," +
            "update(" + tmpCollection + "," +
              "batchSize=100," +
              "topic(" + chkCollection + "," +
                  collection + "," +
                  "q=\"*:*\"," +
                  "fl=\"*\"," +
                  "id=\"topic_" + tmpCollection + "\"," +
              // some of the documents eg. in .system contain large blobs
                  "rows=\"100\"," +
                  "initialCheckpoint=\"0\"))))");
      SolrResponse rsp = ocmh.cloudManager.request(new QueryRequest(q));

      // wait for the daemon to finish

      // 3. if target is the same then now delete source collection

      // 4. copy from tmp to source collection

      // wait for the daemon to finish

      // 5. optionally delete the source collection

      // nocommit error checking
    } finally {
      if (aborted) {
        // nocommit - cleanup

        // 1. kill the daemons
        // 2. cleanup tmp / chk collections IFF the source collection still exists and is not empty
        // 3. cleanup collection state
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
