// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.server;

import java.util.List;
import javax.annotation.PostConstruct;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.aspectj.weaver.Lint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@EnableAutoConfiguration
@RestController
public class ServerApplication {
  private static final Logger logger = LogManager.getLogger(Lint.class);

  @Autowired private ObservationRepository obsRepo;

  public static void main(String[] args) {
    SpringApplication.run(ServerApplication.class, args);
  }

  @PostConstruct
  private void initDb() throws Exception {
    logger.info("==========  post construct");

    Observation o = new Observation();
    o.setObservationAbout("geoId/06");
    o.setObservationDate("2019");
    o.setValue("20");
    obsRepo.save(o);

    // logger.info("======== before");
    // FileGroup fg = FileGroup.Build(files, spec, logger);
    // logger.info("======== after");
    // LogWrapper logCtx = new LogWrapper(Debug.Log.newBuilder(), new File(".").toPath());
    // Processor processor = new Processor(logCtx);
    // processor.processTables(fg.GetTmcf(), fg.GetCsv(), ',', obsRepo);
  }

  @GetMapping("/hello")
  public List<Observation> hello(
      @RequestParam(value = "name", defaultValue = "World") String name) {
    return obsRepo.findAll();
  }
}
