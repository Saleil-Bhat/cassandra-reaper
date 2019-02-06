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
import io.cassandrareaper.core.NodeMetrics;
import io.cassandrareaper.storage.postgresql.JdbiExceptionUtil;
import io.cassandrareaper.storage.postgresql.UuidUtil;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiReaperPostgresStorage extends PostgresStorage implements IStorage, IDistributedStorage {

  private static final Logger LOG = LoggerFactory.getLogger(MultiReaperPostgresStorage.class);
  private static final int DEFAULT_LEADER_TIMEOUT_MIN = 10;  // pulled value from Cassandra DDL
  private static final int DEFAULT_REAPER_TIMEOUT_MIN = 3;   // pulled value from Cassandra DDL

  private final Duration leaderTimeout;
  private final Duration reaperTimeout;

  public MultiReaperPostgresStorage(DBI jdbi) {
    super(jdbi);
    leaderTimeout = Duration.ofMinutes(DEFAULT_LEADER_TIMEOUT_MIN);
    reaperTimeout = Duration.ofMinutes(DEFAULT_REAPER_TIMEOUT_MIN);
  }

  public MultiReaperPostgresStorage(DBI jdbi, int leaderTimeoutInMinutes, int reaperTimeoutInMinutes) {
    super(jdbi);
    leaderTimeout = Duration.ofMinutes(leaderTimeoutInMinutes);
    reaperTimeout = Duration.ofMinutes(reaperTimeoutInMinutes);
  }

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
    return false;
  }

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

  public void forceReleaseLead(UUID leaderId) {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        getPostgresStorage(h).forceReleaseLead(leaderId);
        LOG.debug("Force released lead on segment {}", leaderId);
      }
    }
  }

  public void saveHeartbeat() {
    beat();
    deleteOldReapers();
    deleteOldMetrics();
  }

  public int countRunningReapers() {
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        return getPostgresStorage(h).countRunningReapers(getExpirationTime(reaperTimeout));
      }
    }
    return 0;
  }

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
