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


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;

import org.datacommons.proto.Debug;
import org.datacommons.util.LogWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

public class DataLoader {
  private static final Logger LOGGER = Logger.getLogger(Processor.class.getName());

  @Autowired
  private ObservationRepository obsRepo;

  // @PostConstruct
  private void initDb() {
    List<File> csvFiles = new ArrayList<>();
    File csvFile = new File(getClass().getResource("OurWorldInData_Covid19.csv").getFile());
    csvFiles.add(csvFile);
    File tmcfFile = new File(getClass().getResource("OurWorldInData_Covid19.tmcf").getFile());

    Observation o = new Observation();
    o.setObservationAbout("geoId/06");
    o.setObservationDate("2019");
    o.setValue("20");
    obsRepo.save(o);

    LogWrapper logCtx = new LogWrapper(Debug.Log.newBuilder(), new File(".").toPath());
    Processor processor = new Processor(logCtx);
    try {
      processor.processTables(tmcfFile, csvFiles, ',', obsRepo);
    } catch (IOException ex) {
      LOGGER.log(Level.SEVERE, "process table error", ex);
      return;
    }
  }
}
