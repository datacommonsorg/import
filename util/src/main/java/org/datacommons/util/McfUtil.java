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

import org.datacommons.proto.Mcf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// A container class of MCF related utilities.
public class McfUtil {
  // Serializes an MCFGraph proto into a string.  Sorts output if |sort| is true.
  public static String serializeMcfGraph(Mcf.McfGraph graph, boolean sort) {
    StringBuilder result = new StringBuilder();
    String sentinel = graph.getType() == Mcf.McfType.TEMPLATE_MCF ? "Template" : "Node";
    List<String> keys = new ArrayList<>(graph.getNodesMap().keySet());
    if (sort) Collections.sort(keys);
    for (String key : keys) {
      Mcf.McfGraph.PropertyValues node = graph.getNodesMap().get(key);
      result.append(sentinel + ": " + key + "\n");
      List<String> lines = new ArrayList<>();
      for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getPvsMap().entrySet()) {
        if (pv.getValue().getTypedValuesCount() == 0) continue;
        StringBuilder propStr = new StringBuilder();
        propStr.append(pv.getKey() + ": " + getValue(pv.getValue().getTypedValues(0)));
        for (int i = 1; i < pv.getValue().getTypedValuesCount(); i++) {
          propStr.append(", " + getValue(pv.getValue().getTypedValues(i)));
        }
        propStr.append("\n");
        lines.add(propStr.toString());
      }
      if (sort) Collections.sort(lines);
      result.append(String.join("", lines));
      result.append("\n");
    }
    return result.toString();
  }

  private static String getValue(Mcf.McfGraph.TypedValue typedValue) {
    if (typedValue.getType() == Mcf.ValueType.TEXT) {
      return "\"" + typedValue.getValue() + "\"";
    } else if (typedValue.getType() == Mcf.ValueType.RESOLVED_REF) {
      return Vocabulary.DCID_PREFIX + typedValue.getValue();
    } else {
      return typedValue.getValue();
    }
  }
}
