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

package org.datacommons.tool;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ArgsTest {

  @Test
  public void resolveNumThreads_explicitValueIsUsedAsIs() {
    assertEquals(4, Args.resolveNumThreads(4, 42, 32));
    assertEquals(64, Args.resolveNumThreads(64, 1, 8));
  }

  @Test
  public void resolveNumThreads_explicitValueIsNotCapped() {
    assertEquals(32, Args.resolveNumThreads(32, 42, 64));
  }

  @Test
  public void resolveNumThreads_defaultUsesOneThreadPerCsv() {
    assertEquals(12, Args.resolveNumThreads(0, 12, 32));
  }

  @Test
  public void resolveNumThreads_defaultIsCappedByAvailableProcessors() {
    assertEquals(8, Args.resolveNumThreads(0, 42, 8));
  }

  @Test
  public void resolveNumThreads_defaultIsCappedByMaxDefaultNumThreads() {
    assertEquals(16, Args.resolveNumThreads(0, 42, 64));
    assertEquals(16, Args.resolveNumThreads(0, 16, 32));
  }

  @Test
  public void resolveNumThreads_defaultIsAtLeastOne() {
    assertEquals(1, Args.resolveNumThreads(0, 0, 32));
    assertEquals(1, Args.resolveNumThreads(0, 1, 32));
  }

  @Test
  public void resolveNumThreads_negativeValueIsTreatedAsDefault() {
    assertEquals(8, Args.resolveNumThreads(-1, 42, 8));
  }
}
