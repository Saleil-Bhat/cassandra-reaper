/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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


import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

@RunWith(Cucumber.class)
@CucumberOptions(
    features = {
      "classpath:io.cassandrareaper.acceptance/integration_reaper_functionality.feature",
      "classpath:io.cassandrareaper.acceptance/event_subscriptions.feature"
    },
    plugin = {"pretty"}
    )
public class ReaperPostgresSidecarIT {

  private static final Logger LOG = LoggerFactory.getLogger(ReaperPostgresSidecarIT.class);
  private static final List<ReaperTestJettyRunner> RUNNER_INSTANCES = new CopyOnWriteArrayList<>();
  private static final String[] POSTGRES_CONFIG_FILE
      = {
      "reaper-postgres-sidecar1-at.yaml",
      "reaper-postgres-sidecar2-at.yaml",
  };


  protected ReaperPostgresSidecarIT() {}

  @BeforeClass
  public static void setUp() throws Exception {
    LOG.info("setting up testing Reaper runner with {} seed hosts defined and Postgres storage",
        TestContext.TEST_CLUSTER_SEED_HOSTS.size());

    int reaperInstances = Integer.getInteger("grim.reaper.min", 2);
    for (int i = 0;i < reaperInstances;i++) {
      createReaperTestJettyRunner(Optional.empty());
    }
  }

  @AfterClass
  public static void tearDown() {
    LOG.info("Stopping reaper service...");
    RUNNER_INSTANCES.forEach(r -> r.runnerInstance.after());
  }

  private static void createReaperTestJettyRunner(Optional<String> version) throws InterruptedException {
    ReaperTestJettyRunner runner = new ReaperTestJettyRunner(POSTGRES_CONFIG_FILE[RUNNER_INSTANCES.size()], version);
    RUNNER_INSTANCES.add(runner);
    Thread.sleep(100);
    BasicSteps.addReaperRunner(runner);
  }
}
