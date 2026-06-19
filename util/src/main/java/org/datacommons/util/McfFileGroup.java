// Copyright 2026 Google LLC
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

/** FileGroup implementation for MCF processed files. */
public class McfFileGroup extends FileGroup {
  private final List<File> mcfFiles;
  private final List<File> tmcfFiles;

  public McfFileGroup(
      List<File> csvFiles, List<File> mcfFiles, List<File> tmcfFiles, char delimiter) {
    super(csvFiles, delimiter);
    this.mcfFiles = mcfFiles;
    this.tmcfFiles = tmcfFiles;
  }

  public List<File> getMcfs() {
    return mcfFiles;
  }

  public List<File> getTmcfs() {
    return tmcfFiles;
  }

  public File getTmcf() {
    if (tmcfFiles != null && !tmcfFiles.isEmpty()) return tmcfFiles.get(0);
    return null;
  }
}
