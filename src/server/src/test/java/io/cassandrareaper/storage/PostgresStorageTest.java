/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
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
import io.cassandrareaper.storage.postgresql.IStoragePostgreSql;
import io.cassandrareaper.storage.postgresql.UuidUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.ibatis.common.jdbc.ScriptRunner;
import org.fest.assertions.api.Assertions;
import org.h2.tools.Server;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

public class PostgresStorageTest {

  private static final String DB_URL = "jdbc:h2:mem:test_mem;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";

  @Before
  public void setUp() throws SQLException, IOException {
    Server.createTcpServer().start();

    DBI dbi = new DBI(DB_URL);
    Handle handle = dbi.open();
    Connection conn = handle.getConnection();

    // to suppress output of ScriptRunner
    PrintStream tmp = new PrintStream(new OutputStream() {
      @Override
      public void write(int buff) throws IOException {
        // do nothing
      }
    });
    PrintStream console = System.out;
    System.setOut(tmp);

    String cwd = Paths.get("").toAbsolutePath().toString();
    String path = cwd + "/../src/test/resources/db/postgres/V12_0_0__multi_instance.sql";
    ScriptRunner scriptExecutor = new ScriptRunner(conn, false, true);
    Reader reader = new BufferedReader(new FileReader(path));
    scriptExecutor.runScript(reader);

    System.setOut(console);
  }

  @Test
  public void testTakeLead() {
    DBI dbi = new DBI(DB_URL);
    PostgresStorage storage = new PostgresStorage(dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from leader");

    int numEntries = 5;
    Set<UUID> leaderIds = new HashSet<>();
    for (int i = 0; i < numEntries; i++) {
      UUID msbLeaderId = UuidUtil.fromSequenceId(UuidUtil.toSequenceId(UUID.randomUUID()));
      leaderIds.add(msbLeaderId);
    }

    // insert all five leader entries
    for (UUID leaderId : leaderIds) {
      boolean result = storage.takeLead(leaderId);
      Assertions.assertThat(result).isEqualTo(true);
    }

    // make sure fetched leaders has all the inserted leaders
    List<UUID> fetchedLeaderIds = storage.getLeaders();
    for (UUID fetchedLeaderId : fetchedLeaderIds) {
      Assertions.assertThat(leaderIds.contains(fetchedLeaderId)).isTrue();
    }
  }

  @Test
  public void testNoLeaders() {
    DBI dbi = new DBI(DB_URL);
    PostgresStorage storage = new PostgresStorage(dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from leader");

    List<UUID> fetchedLeaderIds = storage.getLeaders();
    Assertions.assertThat(fetchedLeaderIds.size()).isEqualTo(0);
  }

  @Test
  public void testRenewLead() throws InterruptedException {
    DBI dbi = new DBI(DB_URL);
    PostgresStorage storage = new PostgresStorage(dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from leader");

    UUID leaderId = UUID.randomUUID();
    int sleepTime = 3;

    final Instant initialTime = Instant.now();
    storage.takeLead(leaderId);

    // sleep 3 seconds, then renew lead
    TimeUnit.SECONDS.sleep(sleepTime);
    Assertions.assertThat(storage.renewLead(leaderId)).isTrue();

    Instant hbTime = handle.createQuery("SELECT last_heartbeat FROM leader")
        .mapTo(Timestamp.class)
        .first()
        .toInstant();

    Duration between = Duration.between(initialTime, hbTime);
    Assertions.assertThat(between.getSeconds()).isGreaterThanOrEqualTo(sleepTime);
  }

  @Test
  public void testReleaseLead() {
    DBI dbi = new DBI(DB_URL);
    PostgresStorage storage = new PostgresStorage(dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from leader");

    UUID leaderIdForSelf = UUID.randomUUID();
    UUID leaderIdForOther = UUID.randomUUID();

    storage.takeLead(leaderIdForSelf);
    storage.takeLead(leaderIdForOther);

    List<UUID> fetchedLeaderIds = storage.getLeaders();
    Assertions.assertThat(fetchedLeaderIds.size()).isEqualTo(2);

    handle.createStatement("UPDATE leader SET reaper_instance_id = 0 WHERE leader_id = :id")
        .bind("id", UuidUtil.toSequenceId(leaderIdForOther))
        .execute();

    // test that releaseLead succeeds for entry where instance_id = self
    storage.releaseLead(leaderIdForSelf);
    fetchedLeaderIds = storage.getLeaders();
    Assertions.assertThat(fetchedLeaderIds.size()).isEqualTo(1);

    // test that releaseLead fails for entry where instance_id != self
    storage.releaseLead(leaderIdForOther);
    fetchedLeaderIds = storage.getLeaders();
    Assertions.assertThat(fetchedLeaderIds.size()).isEqualTo(1);
  }

  @Test
  public void testForceReleaseLead() {
    DBI dbi = new DBI(DB_URL);
    PostgresStorage storage = new PostgresStorage(dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from leader");

    UUID leaderIdForSelf = UUID.randomUUID();
    UUID leaderIdForOther = UUID.randomUUID();

    storage.takeLead(leaderIdForSelf);
    storage.takeLead(leaderIdForOther);

    List<UUID> fetchedLeaderIds = storage.getLeaders();
    Assertions.assertThat(fetchedLeaderIds.size()).isEqualTo(2);

    handle.createStatement("UPDATE leader SET reaper_instance_id = 0 WHERE leader_id = :id")
        .bind("id", UuidUtil.toSequenceId(leaderIdForOther))
        .execute();

    // test that forceReleaseLead succeeds for entry where instance_id = self
    storage.forceReleaseLead(leaderIdForSelf);
    fetchedLeaderIds = storage.getLeaders();
    Assertions.assertThat(fetchedLeaderIds.size()).isEqualTo(1);

    // test that forceReleaseLead succeeds for entry where instance_id != self
    storage.forceReleaseLead(leaderIdForOther);
    fetchedLeaderIds = storage.getLeaders();
    Assertions.assertThat(fetchedLeaderIds.size()).isEqualTo(0);
  }

  @Test
  public void testSaveHeartbeat() {
    DBI dbi = new DBI(DB_URL);
    PostgresStorage storage = new PostgresStorage(dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from running_reapers");

    storage.saveHeartbeat();
    int numReapers = storage.countRunningReapers();
    Assertions.assertThat(numReapers).isEqualTo(1);
  }

  @Test
  public void testNodeMetrics() {
    DBI dbi = new DBI(DB_URL);
    PostgresStorage storage = new PostgresStorage(dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from node_metrics_v1");

    UUID runId = UUID.randomUUID();

    // test empty result set
    ArrayList<NodeMetrics> emptyNmList = (ArrayList<NodeMetrics>) storage.getNodeMetrics(runId);
    Assertions.assertThat(emptyNmList.size()).isEqualTo(0);

    NodeMetrics originalNm = NodeMetrics.builder()
        .withNode("fake_node")
        .withCluster("fake_cluster")
        .withDatacenter("NYDC")
        .withHasRepairRunning(true)
        .withPendingCompactions(4)
        .withActiveAnticompactions(1)
        .build();

    storage.storeNodeMetrics(runId, originalNm);
    ArrayList<NodeMetrics> nodeMetricsList = (ArrayList<NodeMetrics>) storage.getNodeMetrics(runId);
    Assertions.assertThat(nodeMetricsList.size()).isEqualTo(1);

    NodeMetrics fetchedNm = nodeMetricsList.get(0);
    Assertions.assertThat(fetchedNm.getNode()).isEqualTo(originalNm.getNode());
    Assertions.assertThat(fetchedNm.getCluster()).isEqualTo(originalNm.getCluster());
    Assertions.assertThat(fetchedNm.getDatacenter()).isEqualTo(originalNm.getDatacenter());
    Assertions.assertThat(fetchedNm.hasRepairRunning()).isEqualTo(originalNm.hasRepairRunning());
    Assertions.assertThat(fetchedNm.getPendingCompactions()).isEqualTo(originalNm.getPendingCompactions());
    Assertions.assertThat(fetchedNm.getActiveAnticompactions()).isEqualTo(originalNm.getActiveAnticompactions());
  }

  @Test
  public void testNodeMetricsByNode() {
    DBI dbi = new DBI(DB_URL);
    PostgresStorage storage = new PostgresStorage(dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from node_metrics_v1");

    UUID runId = UUID.randomUUID();

    NodeMetrics nm1 = NodeMetrics.builder()
        .withNode("fake_node1")
        .withCluster("fake_cluster")
        .withDatacenter("NYDC")
        .withHasRepairRunning(true)
        .withPendingCompactions(4)
        .withActiveAnticompactions(1)
        .build();

    storage.storeNodeMetrics(runId, nm1);

    Optional<NodeMetrics> fetchedNm1Opt = storage.getNodeMetrics(runId, "fake_node1");
    Assertions.assertThat(fetchedNm1Opt.isPresent()).isTrue();
    NodeMetrics fetchedNm1 = fetchedNm1Opt.get();
    Assertions.assertThat(fetchedNm1.getNode()).isEqualTo(nm1.getNode());
    Assertions.assertThat(fetchedNm1.getCluster()).isEqualTo(nm1.getCluster());
    Assertions.assertThat(fetchedNm1.getDatacenter()).isEqualTo(nm1.getDatacenter());
    Assertions.assertThat(fetchedNm1.hasRepairRunning()).isEqualTo(nm1.hasRepairRunning());
    Assertions.assertThat(fetchedNm1.getPendingCompactions()).isEqualTo(nm1.getPendingCompactions());
    Assertions.assertThat(fetchedNm1.getActiveAnticompactions()).isEqualTo(nm1.getActiveAnticompactions());

    Optional<NodeMetrics> fetchedNm2Opt = storage.getNodeMetrics(runId, "fake_node2");
    Assertions.assertThat(fetchedNm2Opt.isPresent()).isFalse();
  }

  /*
  The following tests rely on timeouts; will take a few minutes to complete
   */

  @Test
  public void testUpdateLeaderEntry() throws InterruptedException {
    System.out.println("Testing leader timeout (this will take a minute)...");
    DBI dbi = new DBI(DB_URL);
    PostgresStorage storage = new PostgresStorage(dbi, 1, 1);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from leader");

    UUID leaderId = UUID.randomUUID();

    storage.takeLead(leaderId);
    List<UUID> fetchedLeaderIds = storage.getLeaders();
    Assertions.assertThat(fetchedLeaderIds.size()).isEqualTo(1);

    boolean result = storage.takeLead(leaderId); // should not work bc entry already exist
    Assertions.assertThat(result).isFalse();

    int rowsUpdated = handle.createStatement(IStoragePostgreSql.SQL_UPDATE_LEAD)
        .bind("reaperInstanceId", UuidUtil.toSequenceId(AppContext.REAPER_INSTANCE_ID))
        .bind("reaperInstanceHost", AppContext.REAPER_INSTANCE_ADDRESS)
        .bind("leaderId", UuidUtil.toSequenceId(leaderId))
        .bind("expirationTime", Instant.now().minus(Duration.ofSeconds(60)))
        .execute();

    Assertions.assertThat(rowsUpdated).isEqualTo(0);  // should not b/c original entry hasn't expired yet

    TimeUnit.SECONDS.sleep(60);

    rowsUpdated = handle.createStatement(IStoragePostgreSql.SQL_UPDATE_LEAD)
        .bind("reaperInstanceId", UuidUtil.toSequenceId(AppContext.REAPER_INSTANCE_ID))
        .bind("reaperInstanceHost", AppContext.REAPER_INSTANCE_ADDRESS)
        .bind("leaderId", UuidUtil.toSequenceId(leaderId))
        .bind("expirationTime", Instant.now().minus(Duration.ofSeconds(60)))
        .execute();

    Assertions.assertThat(rowsUpdated).isEqualTo(1);  // should update b/c original entry has expired
  }

  @Test
  public void testDeleteOldNodeMetrics() throws InterruptedException {
    System.out.println("Testing metrics timeout (this will take a few minutes)...");
    DBI dbi = new DBI(DB_URL);
    PostgresStorage storage = new PostgresStorage(dbi, 1, 1);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from node_metrics_v1");

    UUID runId = UUID.randomUUID();
    NodeMetrics originalNm = NodeMetrics.builder()
        .withNode("fake_node")
        .withCluster("fake_cluster")
        .withDatacenter("NYDC")
        .withHasRepairRunning(true)
        .withPendingCompactions(4)
        .withActiveAnticompactions(1)
        .build();
    storage.storeNodeMetrics(runId, originalNm);

    // first delete attempt shouldn't do anything; expirationTime < timePartition
    storage.deleteOldMetrics();
    int numMetrics = handle.createQuery("SELECT COUNT(*) FROM node_metrics_v1")
        .mapTo(Integer.class)
        .first();
    Assertions.assertThat(numMetrics).isEqualTo(1);

    TimeUnit.SECONDS.sleep(60);

    // second delete attempt shouldn't do anything; expirationTime == timePartition and 'tie goes to the runner'
    storage.deleteOldMetrics();
    numMetrics = handle.createQuery("SELECT COUNT(*) FROM node_metrics_v1")
        .mapTo(Integer.class)
        .first();
    Assertions.assertThat(numMetrics).isEqualTo(1);

    TimeUnit.SECONDS.sleep(60);

    // third delete attempt should work; expirationTime > timePartition so the entry is expired
    storage.deleteOldMetrics();
    numMetrics = handle.createQuery("SELECT COUNT(*) FROM node_metrics_v1")
        .mapTo(Integer.class)
        .first();
    Assertions.assertThat(numMetrics).isEqualTo(0);
  }

  @Test
  public void testManualDeleteNodeMetrics() {
    DBI dbi = new DBI(DB_URL);
    PostgresStorage storage = new PostgresStorage(dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from node_metrics_v1");

    UUID runId = UUID.randomUUID();
    NodeMetrics originalNm = NodeMetrics.builder()
        .withNode("fake_node")
        .withCluster("fake_cluster")
        .withDatacenter("NYDC")
        .withHasRepairRunning(true)
        .withPendingCompactions(4)
        .withActiveAnticompactions(1)
        .build();
    storage.storeNodeMetrics(runId, originalNm);

    int numMetrics = handle.createQuery("SELECT COUNT(*) FROM node_metrics_v1")
        .mapTo(Integer.class)
        .first();
    Assertions.assertThat(numMetrics).isEqualTo(3);  // bc default reaper timeout is 3 minutes

    // delete metric from table and verify delete succeeds
    storage.deleteNodeMetrics(runId, "fake_node");
    numMetrics = handle.createQuery("SELECT COUNT(*) FROM node_metrics_v1")
        .mapTo(Integer.class)
        .first();
    Assertions.assertThat(numMetrics).isEqualTo(2);
  }
}
