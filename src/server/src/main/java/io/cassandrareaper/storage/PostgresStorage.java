/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cassandrareaper.storage;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.NodeMetrics;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.service.RepairParameters;
import io.cassandrareaper.service.RingRange;
import io.cassandrareaper.storage.postgresql.BigIntegerArgumentFactory;
import io.cassandrareaper.storage.postgresql.IStoragePostgreSql;
import io.cassandrareaper.storage.postgresql.InstantArgumentFactory;
import io.cassandrareaper.storage.postgresql.JdbiExceptionUtil;
import io.cassandrareaper.storage.postgresql.LongCollectionSqlTypeArgumentFactory;
import io.cassandrareaper.storage.postgresql.PostgresArrayArgumentFactory;
import io.cassandrareaper.storage.postgresql.PostgresRepairSegment;
import io.cassandrareaper.storage.postgresql.RepairParallelismArgumentFactory;
import io.cassandrareaper.storage.postgresql.RunStateArgumentFactory;
import io.cassandrareaper.storage.postgresql.ScheduleStateArgumentFactory;
import io.cassandrareaper.storage.postgresql.StateArgumentFactory;
import io.cassandrareaper.storage.postgresql.UuidArgumentFactory;
import io.cassandrareaper.storage.postgresql.UuidUtil;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.DBIException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the StorageAPI using PostgreSQL database.
 */
public class PostgresStorage implements IStorage, IDistributedStorage {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresStorage.class);
  private static final int DEFAULT_LEADER_TIMEOUT_MIN = 10;  // pulled value from Cassandra DDL
  private static final int DEFAULT_REAPER_TIMEOUT_MIN = 3;   // pulled value from Cassandra DDL

  protected final DBI jdbi;
  private final Duration leaderTimeout;
  private final Duration reaperTimeout;


  public PostgresStorage(DBI jdbi) {
    this.jdbi = jdbi;
    leaderTimeout = Duration.ofMinutes(DEFAULT_LEADER_TIMEOUT_MIN);
    reaperTimeout = Duration.ofMinutes(DEFAULT_REAPER_TIMEOUT_MIN);
  }

  public PostgresStorage(DBI jdbi, int leaderTimeoutInMinutes, int reaperTimeoutInMinutes) {
    this.jdbi = jdbi;
    leaderTimeout = Duration.ofMinutes(leaderTimeoutInMinutes);
    reaperTimeout = Duration.ofMinutes(reaperTimeoutInMinutes);
  }

  protected static IStoragePostgreSql getPostgresStorage(Handle handle) {
    handle.registerArgumentFactory(new LongCollectionSqlTypeArgumentFactory());
    handle.registerArgumentFactory(new PostgresArrayArgumentFactory());
    handle.registerArgumentFactory(new RunStateArgumentFactory());
    handle.registerArgumentFactory(new RepairParallelismArgumentFactory());
    handle.registerArgumentFactory(new StateArgumentFactory());
    handle.registerArgumentFactory(new BigIntegerArgumentFactory());
    handle.registerArgumentFactory(new ScheduleStateArgumentFactory());
    handle.registerArgumentFactory(new UuidArgumentFactory());
    handle.registerArgumentFactory(new InstantArgumentFactory());
    return handle.attach(IStoragePostgreSql.class);
  }

  @Override
  public Optional<Cluster> getCluster(String clusterName) {
    Cluster result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getCluster(clusterName);
    }
    return Optional.ofNullable(result);
  }

  @Override
  public Optional<Cluster> deleteCluster(String clusterName) {
    assert getRepairSchedulesForCluster(clusterName).isEmpty()
        : StringUtils.join(getRepairSchedulesForCluster(clusterName));

    assert getRepairRunsForCluster(clusterName, Optional.of(Integer.MAX_VALUE)).isEmpty()
        : StringUtils.join(getRepairRunsForCluster(clusterName, Optional.of(Integer.MAX_VALUE)));

    Cluster result = null;
    try (Handle h = jdbi.open()) {
      IStoragePostgreSql pg = getPostgresStorage(h);
      pg.deleteRepairUnits(clusterName);
      Cluster clusterToDel = pg.getCluster(clusterName);
      if (clusterToDel != null) {
        int rowsDeleted = pg.deleteCluster(clusterName);
        if (rowsDeleted > 0) {
          result = clusterToDel;
        }
      }
    }
    return Optional.ofNullable(result);
  }

  @Override
  public boolean isStorageConnected() {
    String currentDate = null;
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        currentDate = getPostgresStorage(h).getCurrentDate();
      }
    }
    return null != currentDate && !currentDate.trim().isEmpty();
  }

  @Override
  public Collection<Cluster> getClusters() {
    Collection<Cluster> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getClusters();
    }
    return result != null ? result : Lists.<Cluster>newArrayList();
  }

  @Override
  public boolean addCluster(Cluster newCluster) throws ReaperException {
    Cluster result = null;
    try (Handle h = jdbi.open()) {
      String properties = new ObjectMapper().writeValueAsString(newCluster.getProperties());
      Preconditions.checkState(newCluster.getPartitioner().isPresent(),
          "Cannot insert cluster with no partitioner.");
      try {
        int rowsAdded
            = getPostgresStorage(h)
            .insertCluster(
                newCluster.getName(),
                newCluster.getPartitioner().get(),
                newCluster.getSeedHosts(),
                properties);
        if (rowsAdded < 1) {
          LOG.warn("failed inserting cluster with name: {}", newCluster.getName());
        } else {
          result = newCluster; // no created id, as cluster name used for primary key
        }

      } catch (UnableToExecuteStatementException e) {
        if (JdbiExceptionUtil.isDuplicateKeyError(e)) {
          LOG.warn("cluster with name: {} already exists; updating instead", newCluster.getName());
          return updateCluster(newCluster);
        }
        LOG.warn("failed inserting cluster with name: {}", newCluster.getName());
        return false;
      }
    } catch (JsonProcessingException e) {
      throw new ReaperException(e);
    }
    return result != null;
  }

  @Override
  public boolean updateCluster(Cluster cluster) {
    boolean result = false;
    try (Handle h = jdbi.open()) {
      int rowsAdded = getPostgresStorage(h).updateCluster(cluster);
      if (rowsAdded < 1) {
        LOG.warn("failed updating cluster with name: {}", cluster.getName());
      } else {
        result = true;
      }
    }
    return result;
  }

  @Override
  public Optional<RepairRun> getRepairRun(UUID id) {
    RepairRun result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairRun(UuidUtil.toSequenceId(id));
    }
    return Optional.ofNullable(result);
  }

  @Override
  public Collection<RepairRun> getRepairRunsForCluster(String clusterName, Optional<Integer> limit) {
    Collection<RepairRun> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairRunsForCluster(clusterName, limit.orElse(1000));
    }
    return result == null ? Lists.<RepairRun>newArrayList() : result;
  }

  @Override
  public Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId) {
    Collection<RepairRun> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairRunsForUnit(UuidUtil.toSequenceId(repairUnitId));
    }
    return result == null ? Lists.<RepairRun>newArrayList() : result;
  }

  @Override
  public Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState) {
    Collection<RepairRun> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairRunsWithState(runState);
    }
    return result == null ? Lists.<RepairRun>newArrayList() : result;
  }

  @Override
  public Optional<RepairRun> deleteRepairRun(UUID id) {
    RepairRun result = null;
    Handle handle = null;
    try {
      handle = jdbi.open();
      handle.begin();
      IStoragePostgreSql pg = getPostgresStorage(handle);
      RepairRun runToDelete = pg.getRepairRun(UuidUtil.toSequenceId(id));
      if (runToDelete != null) {
        int segmentsRunning
            = pg.getSegmentAmountForRepairRunWithState(UuidUtil.toSequenceId(id), RepairSegment.State.RUNNING);
        if (segmentsRunning == 0) {
          pg.deleteRepairSegmentsForRun(UuidUtil.toSequenceId(runToDelete.getId()));
          pg.deleteRepairRun(UuidUtil.toSequenceId(id));
          result = runToDelete.with()
              .runState(RepairRun.RunState.DELETED)
              .endTime(DateTime.now())
              .build(id);
        } else {
          LOG.warn("not deleting RepairRun \"{}\" as it has segments running: {}", id, segmentsRunning);
        }
      }
      handle.commit();
    } catch (DBIException ex) {
      LOG.warn("DELETE failed", ex);
      if (handle != null) {
        handle.rollback();
      }
    } finally {
      if (handle != null) {
        handle.close();
      }
    }
    return Optional.ofNullable(result);
  }

  @Override
  public RepairRun addRepairRun(RepairRun.Builder newRepairRun, Collection<RepairSegment.Builder> newSegments) {

    RepairRun result;
    try (Handle h = jdbi.open()) {
      long insertedId = getPostgresStorage(h).insertRepairRun(newRepairRun.build(null));
      result = newRepairRun.build(UuidUtil.fromSequenceId(insertedId));
    }
    addRepairSegments(newSegments, result.getId());
    return result;
  }

  @Override
  public boolean updateRepairRun(RepairRun repairRun) {
    boolean result = false;
    try (Handle h = jdbi.open()) {
      int rowsAdded = getPostgresStorage(h).updateRepairRun(repairRun);
      if (rowsAdded < 1) {
        LOG.warn("failed updating repair run with id: {}", repairRun.getId());
      } else {
        result = true;
      }
    }
    return result;
  }

  @Override
  public RepairUnit addRepairUnit(RepairUnit.Builder newRepairUnit) {
    long insertedId;
    try (Handle h = jdbi.open()) {
      insertedId = getPostgresStorage(h).insertRepairUnit(newRepairUnit.build(null));
    }
    return newRepairUnit.build(UuidUtil.fromSequenceId(insertedId));
  }

  @Override
  public RepairUnit getRepairUnit(UUID id) {
    RepairUnit result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairUnit(UuidUtil.toSequenceId(id));
    }
    Preconditions.checkArgument(null != result);
    return result;
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(RepairUnit.Builder params) {
    RepairUnit result;
    try (Handle h = jdbi.open()) {

      result = getPostgresStorage(h).getRepairUnitByClusterAndTables(
              params.clusterName,
              params.keyspaceName,
              params.columnFamilies,
              params.incrementalRepair,
              params.nodes,
              params.datacenters,
              params.blacklistedTables,
              params.repairThreadCount);
    }
    return Optional.ofNullable(result);
  }

  private void addRepairSegments(Collection<RepairSegment.Builder> newSegments, UUID runId) {
    List<PostgresRepairSegment> insertableSegments = new ArrayList<>();
    for (RepairSegment.Builder segment : newSegments) {
      insertableSegments.add(
          new PostgresRepairSegment(segment.withRunId(runId).withId(null).build()));
    }
    try (Handle h = jdbi.open()) {
      getPostgresStorage(h).insertRepairSegments(insertableSegments.iterator());
    }
  }

  @Override
  public boolean updateRepairSegment(RepairSegment repairSegment) {
    boolean result = false;
    try (Handle h = jdbi.open()) {
      int rowsAdded = getPostgresStorage(h).updateRepairSegment(repairSegment);
      if (rowsAdded < 1) {
        LOG.warn("failed updating repair segment with id: {}", repairSegment.getId());
      } else {
        result = true;
      }
    }
    return result;
  }

  @Override
  public Optional<RepairSegment> getRepairSegment(UUID runId, UUID segmentId) {
    RepairSegment result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSegment(UuidUtil.toSequenceId(segmentId));
    }
    return Optional.ofNullable(result);
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRun(UUID runId) {
    try (Handle h = jdbi.open()) {
      return getPostgresStorage(h).getRepairSegmentsForRun(UuidUtil.toSequenceId(runId));
    }
  }

  private Optional<RepairSegment> getNextFreeSegment(UUID runId) {
    RepairSegment result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getNextFreeRepairSegment(UuidUtil.toSequenceId(runId));
    }
    return Optional.ofNullable(result);
  }

  @Override
  public Optional<RepairSegment> getNextFreeSegmentInRange(UUID runId, Optional<RingRange> range) {
    if (range.isPresent()) {
      RepairSegment result;
      try (Handle h = jdbi.open()) {
        IStoragePostgreSql storage = getPostgresStorage(h);
        if (!range.get().isWrapping()) {
          result = storage.getNextFreeRepairSegmentInNonWrappingRange(
              UuidUtil.toSequenceId(runId), range.get().getStart(), range.get().getEnd());
        } else {
          result = storage.getNextFreeRepairSegmentInWrappingRange(
              UuidUtil.toSequenceId(runId), range.get().getStart(), range.get().getEnd());
        }
      }
      return Optional.ofNullable(result);
    } else {
      return getNextFreeSegment(runId);
    }
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(UUID runId, RepairSegment.State segmentState) {
    Collection<RepairSegment> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSegmentsForRunWithState(UuidUtil.toSequenceId(runId), segmentState);
    }
    return result;
  }

  @Override
  public Collection<RepairParameters> getOngoingRepairsInCluster(String clusterName) {
    try (Handle h = jdbi.open()) {
      return getPostgresStorage(h).getRunningRepairsForCluster(clusterName);
    }
  }

  @Override
  public SortedSet<UUID> getRepairRunIdsForCluster(String clusterName) {
    SortedSet<UUID> result = Sets.newTreeSet(Collections.reverseOrder());
    try (Handle h = jdbi.open()) {
      for (Long l : getPostgresStorage(h).getRepairRunIdsForCluster(clusterName)) {
        result.add(UuidUtil.fromSequenceId(l));
      }
    }
    return result;
  }

  @Override
  public int getSegmentAmountForRepairRun(UUID runId) {
    try (Handle h = jdbi.open()) {
      return getPostgresStorage(h).getSegmentAmountForRepairRun(UuidUtil.toSequenceId(runId));
    }
  }

  @Override
  public int getSegmentAmountForRepairRunWithState(UUID runId, RepairSegment.State state) {
    int result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getSegmentAmountForRepairRunWithState(UuidUtil.toSequenceId(runId), state);
    }
    return result;
  }

  @Override
  public RepairSchedule addRepairSchedule(RepairSchedule.Builder repairSchedule) {
    long insertedId;
    try (Handle h = jdbi.open()) {
      insertedId = getPostgresStorage(h).insertRepairSchedule(repairSchedule.build(null));
    }
    return repairSchedule.build(UuidUtil.fromSequenceId(insertedId));
  }

  @Override
  public Optional<RepairSchedule> getRepairSchedule(UUID repairScheduleId) {
    RepairSchedule result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSchedule(UuidUtil.toSequenceId(repairScheduleId));
    }
    return Optional.ofNullable(result);
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName) {
    Collection<RepairSchedule> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSchedulesForCluster(clusterName);
    }
    return result;
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForKeyspace(String keyspaceName) {
    Collection<RepairSchedule> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSchedulesForKeyspace(keyspaceName);
    }
    return result;
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForClusterAndKeyspace(String clusterName, String keyspaceName) {
    Collection<RepairSchedule> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSchedulesForClusterAndKeySpace(clusterName, keyspaceName);
    }
    return result;
  }

  @Override
  public Collection<RepairSchedule> getAllRepairSchedules() {
    Collection<RepairSchedule> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getAllRepairSchedules();
    }
    return result;
  }

  @Override
  public boolean updateRepairSchedule(RepairSchedule newRepairSchedule) {
    boolean result = false;
    try (Handle h = jdbi.open()) {
      int rowsAdded = getPostgresStorage(h).updateRepairSchedule(newRepairSchedule);
      if (rowsAdded < 1) {
        LOG.warn("failed updating repair schedule with id: {}", newRepairSchedule.getId());
      } else {
        result = true;
      }
    }
    return result;
  }

  @Override
  public Optional<RepairSchedule> deleteRepairSchedule(UUID id) {
    RepairSchedule result = null;
    try (Handle h = jdbi.open()) {
      IStoragePostgreSql pg = getPostgresStorage(h);
      RepairSchedule scheduleToDel = pg.getRepairSchedule(UuidUtil.toSequenceId(id));
      if (scheduleToDel != null) {
        int rowsDeleted = pg.deleteRepairSchedule(UuidUtil.toSequenceId(scheduleToDel.getId()));
        if (rowsDeleted > 0) {
          result = scheduleToDel.with().state(RepairSchedule.State.DELETED).build(id);
        }
      }
    }
    return Optional.ofNullable(result);
  }

  @Override
  public Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit) {
    try (Handle h = jdbi.open()) {
      return getPostgresStorage(h).getClusterRunOverview(clusterName, limit);
    }
  }

  @Override
  public Collection<RepairScheduleStatus> getClusterScheduleStatuses(String clusterName) {
    try (Handle h = jdbi.open()) {
      return getPostgresStorage(h).getClusterScheduleOverview(clusterName);
    }
  }

  @Override
  public boolean saveSnapshot(Snapshot snapshot) {
    boolean result = false;
    try (Handle h = jdbi.open()) {
      int rowsAdded = getPostgresStorage(h).saveSnapshot(snapshot);
      if (rowsAdded < 1) {
        LOG.warn(
            "failed saving snapshot with name {} for cluster {}",
            snapshot.getName(),
            snapshot.getClusterName());
      } else {
        result = true;
      }
    }

    return result;
  }

  @Override
  public boolean deleteSnapshot(Snapshot snapshot) {
    boolean result = false;
    try (Handle h = jdbi.open()) {
      IStoragePostgreSql pg = getPostgresStorage(h);
      int rowsDeleted = pg.deleteSnapshot(snapshot.getClusterName(), snapshot.getName());
      if (rowsDeleted > 0) {
        result = true;
      }
    }
    return result;
  }

  @Override
  public Snapshot getSnapshot(String clusterName, String snapshotName) {
    try (Handle h = jdbi.open()) {
      return getPostgresStorage(h).getSnapshot(clusterName, snapshotName);
    }
  }

  @Override
  public boolean takeLead(UUID leaderId) {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        try {
          int rowsInserted = getPostgresStorage(h).insertLeaderEntry(
              leaderId,
              AppContext.REAPER_INSTANCE_ID,
              AppContext.REAPER_INSTANCE_ADDRESS
          );
          if (rowsInserted == 1) {  // insert should modify exactly 1 row
            return true;
          }
        } catch (UnableToExecuteStatementException e) {
          if (JdbiExceptionUtil.isDuplicateKeyError(e)) {
            // if it's a duplicate key error, then try to update it
            int rowsUpdated = getPostgresStorage(h).updateLeaderEntry(
                leaderId,
                AppContext.REAPER_INSTANCE_ID,
                AppContext.REAPER_INSTANCE_ADDRESS,
                getExpirationTime(leaderTimeout)
            );
            if (rowsUpdated == 1) {  // if row updated, took ownership from an expired leader
              LOG.debug("Took lead from expired entry for segment {}", leaderId);
              return true;
            }
          }
          return false;
        }
      }
    }
    LOG.warn("Unknown error occurred while taking lead on segment {}", leaderId);
    return false;
  }

  @Override
  public boolean renewLead(UUID leaderId) {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        int rowsUpdated = getPostgresStorage(h).renewLead(
            leaderId,
            AppContext.REAPER_INSTANCE_ID,
            AppContext.REAPER_INSTANCE_ADDRESS
        );

        if (rowsUpdated == 1) {
          LOG.debug("Renewed lead on segment {}", leaderId);
          return true;
        }
        LOG.error("Failed to renew lead on segment {}", leaderId);
      }
    }
    return false;
  }

  @Override
  public List<UUID> getLeaders() {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        List<Long> leaderSequenceIds = getPostgresStorage(h).getLeaders(getExpirationTime(leaderTimeout));
        return leaderSequenceIds
            .stream()
            .map(UuidUtil::fromSequenceId)
            .collect(Collectors.toList());
      }
    }
    return new ArrayList<>();
  }

  @Override
  public void releaseLead(UUID leaderId) {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        int rowsDeleted = getPostgresStorage(h).releaseLead(
            leaderId,
            AppContext.REAPER_INSTANCE_ID
        );
        if (rowsDeleted == 1) {
          LOG.debug("Released lead on segment {}", leaderId);
        } else {
          LOG.error("Could not release lead on segment {}", leaderId);
        }
      }
    }
  }

  @Override
  public void forceReleaseLead(UUID leaderId) {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        getPostgresStorage(h).forceReleaseLead(leaderId);
        LOG.debug("Force released lead on segment {}", leaderId);
      }
    }
  }

  @Override
  public void saveHeartbeat() {
    beat();
    deleteOldReapers();
    deleteOldMetrics();
  }

  @Override
  public int countRunningReapers() {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        return getPostgresStorage(h).countRunningReapers(getExpirationTime(reaperTimeout));
      }
    }
    LOG.warn("Failed to get running reaper count from storage");
    return 1;
  }

  @Override
  public void storeNodeMetrics(UUID runId, NodeMetrics nodeMetrics) {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
        for (int offset = 0; offset < reaperTimeout.toMinutes(); ++offset) {
          getPostgresStorage(h).storeNodeMetrics(
              minute + offset,
              UuidUtil.toSequenceId(runId),
              nodeMetrics.getNode(),
              nodeMetrics.getCluster(),
              nodeMetrics.getDatacenter(),
              nodeMetrics.isRequested(),
              nodeMetrics.getPendingCompactions(),
              nodeMetrics.hasRepairRunning(),
              nodeMetrics.getActiveAnticompactions()
          );
        }
      }
    }
  }

  @Override
  public Collection<NodeMetrics> getNodeMetrics(UUID runId) {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
        return getPostgresStorage(h).getNodeMetrics(
            minute,
            UuidUtil.toSequenceId(runId)
        );
      }
    }
    return new ArrayList<>();
  }

  @Override
  public Optional<NodeMetrics> getNodeMetrics(UUID runId, String node) {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
        NodeMetrics nm = getPostgresStorage(h).getNodeMetricsByNode(
            minute,
            UuidUtil.toSequenceId(runId),
            node
        );
        if (nm != null) {
          return Optional.of(nm);
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public void deleteNodeMetrics(UUID runId, String node) {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
        getPostgresStorage(h).deleteNodeMetricsByNode(
            minute,
            UuidUtil.toSequenceId(runId),
            node
        );
      }
    }
  }

  private void beat() {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        int rowsUpdated = getPostgresStorage(h).updateHeartbeat(
            AppContext.REAPER_INSTANCE_ID,
            AppContext.REAPER_INSTANCE_ADDRESS
        );
        if (rowsUpdated == 0) {
          LOG.debug("Creating new entry for reaper {}", AppContext.REAPER_INSTANCE_ID);
          int rowsInserted = getPostgresStorage(h).insertHeartbeat(
              AppContext.REAPER_INSTANCE_ID,
              AppContext.REAPER_INSTANCE_ADDRESS
          );
          if (rowsInserted != 1) {
            LOG.error("Failed to create entry for reaper {}", AppContext.REAPER_INSTANCE_ID);
          }
        }
      }
    }
  }

  private void deleteOldReapers() {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        getPostgresStorage(h).deleteOldReapers(getExpirationTime(reaperTimeout));
      }
    }
  }

  public void deleteOldMetrics() {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        long expirationMinute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis())
            - reaperTimeout.toMinutes();
        getPostgresStorage(h).deleteOldMetrics(expirationMinute);
      }
    }
  }

  private Instant getExpirationTime(Duration timeout) {
    return Instant.now().minus(timeout);
  }

}