/**
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { CsvData, MappedThing, Mapping, MappingType } from "../types";

const FIXED_CSV_TABLE = 'CSVTable';
const DCID_PROP = 'dcid';
const SVOBS_TYPE = 'StatVarObservation';
const PLACE_TYPE = 'Place';

function initNode(idx: number, type: string): Array<string> {
  var pvs = Array<string>();
  pvs.push('Node: E:' + FIXED_CSV_TABLE + '->' + idx.toString());
  pvs.push('typeOf: dcs:' + type);  
  return pvs;
}

function getColPV(prop: string, col: string): string {
  return prop + ': C:' + FIXED_CSV_TABLE + '->' + col;
}

function getEntPV(prop: string, idx: number): string {
  return prop + ': E:' + FIXED_CSV_TABLE + '->' + idx.toString();
}

/**
 * Generates the tmcf file given the correct mappings.
 * REQUIRES: |mappings| passes checkMappings().
 * 
 * @param mappings
 * @returns
 */
export function generateTMCF(mappings: Mapping): string {
  var tmcfNodes = Array<Array<string>>();
  var idx = 0;

  // Do one pass over the mappings to compute the common PVs in every TMCF node. Everything
  // other than COLUMN_HEADER mappings get repeated in every node.
  var commonPVs = Array<string>();
  var colHdrProp:string = null;
  for (const [mthing, mval] of Object.entries(mappings)) {
    if (mval.type == MappingType.CONSTANT) {
      // Constants are references.
      commonPVs.push(mthing + ': dcid:' + mval.constant);
    } else if (mval.type == MappingType.COLUMN) {
      if (mthing == MappedThing.PLACE) {
        if (mval.placeProperty == DCID_PROP) {
          // Place with DCID property can be a column ref.
          commonPVs.push(getColPV(mthing, mval.column.id));
        } else {
          // For place with non-DCID property, we should introduce a place node,
          // and use entity reference.
          var node = initNode(idx, PLACE_TYPE);
          node.push(getColPV(mval.placeProperty, mval.column.id));
          tmcfNodes.push(node);
          commonPVs.push(getEntPV(mthing, idx));

          idx++;
        }
      } else {
        // For non-place types, column directly contains the corresponding values.
        commonPVs.push(getColPV(mthing, mval.column.id));
      }
    } else if (mval.type == MappingType.COLUMN_HEADER) {
      // Remember which thing has the column header for next pass.
      colHdrProp = mthing;
    }
  }

  if (colHdrProp != null) {
    // Compute one node per COLUMN_HEADER entry.
    for (const hdr of mappings[colHdrProp].headers) {
      var node = initNode(idx, SVOBS_TYPE);
      // Each column contains numerical values of SVObs.
      node.push(getColPV(MappedThing.VALUE, hdr.id));
      // TODO: double check the use of id vs. header here.
      node.push(colHdrProp + ': dcid:' + hdr.id);
      tmcfNodes.push(node);
      idx++;
    }
  } else {
    // There is only one node in this case.
    tmcfNodes.push(initNode(idx, SVOBS_TYPE));
    idx++;
  }

  // Add the common PVs.
  for (var node of tmcfNodes) {
    node.concat(commonPVs);
  }

  var nodeStrings = Array<string>();
  for (const pvs of tmcfNodes) {
    nodeStrings.push(pvs.join('\n'));
  }
  return nodeStrings.join('\n\n');
}