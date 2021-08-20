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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@EnableAutoConfiguration
@RestController
public class ServerApplication {
  private static final Logger LOGGER = Logger.getLogger(Processor.class.getName());

  @Autowired
  private ObservationRepository obsRepo;

  public static void main(String[] args) {
    try {
      InputStream is = new ClassPathResource("OurWorldInData_Covid19.tmcf").getInputStream();
      if (is == null) {
        LOGGER.info("======= inputstream is null");
      } else {
        String text = new BufferedReader(
          new InputStreamReader(is, StandardCharsets.UTF_8))
            .lines()
                .collect(Collectors.joining("\n"));
        LOGGER.info(text);
      }
    } catch (IOException ex) {
      LOGGER.info("IO exception");
    }

    SpringApplication.run(ServerApplication.class, args);
  }

  // @PostConstruct
  // private void initDb() throws Exception {
  //   LOGGER.info("==========  post construct");

  //   List<File> csvFiles = new ArrayList<>();
  //   File csvFile = new File(getClass().getResource("OurWorldInData_Covid19.csv").getFile());
  //   LOGGER.info(Boolean.toString(csvFile.exists()));
  //   csvFiles.add(csvFile);
  //   File tmcfFile = new File(getClass().getResource("OurWorldInData_Covid19.tmcf").getFile());
  //   LOGGER.info(Boolean.toString(tmcfFile.exists()));

  //   Observation o = new Observation();
  //   o.setObservationAbout("geoId/06");
  //   o.setObservationDate("2019");
  //   o.setValue("20");
  //   obsRepo.save(o);

  //   LogWrapper logCtx = new LogWrapper(Debug.Log.newBuilder(), new File(".").toPath());
  //   Processor processor = new Processor(logCtx);
  //   try {
  //     processor.processTables(tmcfFile, csvFiles, ',', obsRepo);
  //   } catch (IOException ex) {
  //     LOGGER.log(Level.SEVERE, "process table error", ex);
  //     return;
  //   }
  // }

  @GetMapping("/hello")
  public List<Observation> hello(@RequestParam(value = "name", defaultValue = "World") String name) {
		return obsRepo.findAll();
  }
}
