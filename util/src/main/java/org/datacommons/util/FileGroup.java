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

package org.datacommons.util;

import java.io.File;
import java.util.List;

public class FileGroup {
  private File tmcfFile;
  private List<File> csvFiles;
  private List<File> mcfFiles;
  int nTsv;

  public FileGroup(File tmcfFile, List<File> csvFiles, List<File> mcfFiles, int nTsv) {
    this.tmcfFile = tmcfFile;
    this.csvFiles = csvFiles;
    this.mcfFiles = mcfFiles;
    this.nTsv = nTsv;
  }

  public File GetTmcf() {
    return this.tmcfFile;
  }

  public List<File> GetCsv() {
    return this.csvFiles;
  }

  public List<File> GetMcf() {
    return this.mcfFiles;
  }

  public int GetNumTsv() {
    return this.nTsv;
  }
}
