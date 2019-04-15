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

package io.cassandrareaper.acceptance;


import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Cucumber.class)
@CucumberOptions(
    features = "classpath:io.cassandrareaper.acceptance/integration_reaper_functionality.feature",
    plugin = {"pretty"}
    )
public class ReaperPostgresIT {

  private static final Logger LOG = LoggerFactory.getLogger(ReaperPostgresIT.class);
  private static final List<ReaperTestJettyRunner> RUNNER_INSTANCES = new CopyOnWriteArrayList<>();
  private static final String POSTGRES_CONFIG_FILE = "cassandra-reaper-postgres-at.yaml";
  private static final Random RAND = new Random(System.nanoTime());
  private static Thread GRIM_REAPER;


  protected ReaperPostgresIT() {}

  @BeforeClass
  public static void setUp() throws Exception {
    LOG.info("setting up testing Reaper runner with {} seed hosts defined and Postgres storage",
        TestContext.TEST_CLUSTER_SEED_HOSTS.size());

    int minReaperInstances = Integer.getInteger("grim.reaper.min", 1);
    int maxReaperInstances = Integer.getInteger("grim.reaper.max", minReaperInstances);

    for (int i = 0; i < minReaperInstances; ++i) {
      createReaperTestJettyRunner(Optional.empty());
    }

    GRIM_REAPER = new Thread(() -> {
      Thread.currentThread().setName("GRIM REAPER");
      while (!Thread.currentThread().isInterrupted()) { //keep adding/removing reaper instances while test is running
        try {
          if (maxReaperInstances > RUNNER_INSTANCES.size()) {
            createReaperTestJettyRunner(Optional.empty());
          } else {
            int remove = minReaperInstances + RAND.nextInt(maxReaperInstances - minReaperInstances);
            removeReaperTestJettyRunner(RUNNER_INSTANCES.get(remove));
          }
        } catch (RuntimeException | InterruptedException ex) {
          LOG.error("failed adding/removing reaper instance", ex);
        }
      }
    });
    if (minReaperInstances < maxReaperInstances) {
      GRIM_REAPER.start();
    }
  }

  @AfterClass
  public static void tearDown() {
    LOG.info("Stopping reaper service...");
    GRIM_REAPER.interrupt();
    RUNNER_INSTANCES.forEach(r -> r.runnerInstance.after());
  }

  private static void createReaperTestJettyRunner(Optional<String> version) throws InterruptedException {
    ReaperTestJettyRunner runner = new ReaperTestJettyRunner(POSTGRES_CONFIG_FILE, version);
    RUNNER_INSTANCES.add(runner);
    Thread.sleep(100);
    BasicSteps.addReaperRunner(runner);
  }

  private static void removeReaperTestJettyRunner(ReaperTestJettyRunner runner) throws InterruptedException {
    BasicSteps.removeReaperRunner(runner);
    Thread.sleep(200);
    runner.runnerInstance.after();
    RUNNER_INSTANCES.remove(runner);
  }
}
