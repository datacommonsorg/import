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

/**
 * Component for showing the header of the table preview in the mapping section.
 */

import React from "react";

import { Column, MappedThing, MappingType } from "../types";
import { ColumnInfo } from "./mapping_section";

interface MappingTableHeaderProps {
  orderedColumns: Column[];
  columnInfo: Record<string, ColumnInfo>;
  onColumnSelected: (colIdx: number) => void;
  highlightedColumnRef: React.RefObject<HTMLTableHeaderCellElement>;
  selectedColumn: number;
}

export function MappingTableHeader(
  props: MappingTableHeaderProps
): JSX.Element {
  return (
    <>
      <thead className="mapping-info-row">
        <tr>
          <th className="row-num"></th>
          {props.orderedColumns.map((column, idx) => {
            const info = props.columnInfo[column.id];
            return (
              <th
                key={`mapping-info-${idx}`}
                onClick={() => props.onColumnSelected(idx)}
              >
                {getColumnMappingString(info)}
              </th>
            );
          })}
        </tr>
      </thead>
      <thead>
        <tr>
          <th className="row-num">0</th>
          {props.orderedColumns.map((column, idx) => {
            return (
              <th
                key={`heading-${idx}`}
                className={
                  props.selectedColumn === idx ? "highlighted-col" : ""
                }
                ref={
                  props.selectedColumn === idx
                    ? props.highlightedColumnRef
                    : null
                }
                onClick={() => props.onColumnSelected(idx)}
              >
                <div>{column.header}</div>
              </th>
            );
          })}
        </tr>
      </thead>
    </>
  );
}

function getColumnMappingString(column: ColumnInfo): string {
  if (column.type === MappingType.COLUMN) {
    let mString = `${column.mappedThing}s`;
    if (column.mappedThing === MappedThing.PLACE) {
      mString += ` of type ${column.columnPlaceType.displayName} and of format ${column.columnPlaceProperty.displayName}`;
    }
    return mString;
  }
  if (column.type === MappingType.COLUMN_HEADER) {
    return `header is a ${column.mappedThing}`;
  }
  return "Not mapped";
}
