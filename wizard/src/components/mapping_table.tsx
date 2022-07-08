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
 * Component for showing the table preview in the mapping section.
 */

import _ from "lodash";
import React, { useEffect, useRef } from "react";

import { CsvData } from "../types";
import { ColumnInfo } from "./mapping_section";
import { MappingTableBody } from "./mapping_table_body";
import { MappingTableHeader } from "./mapping_table_header";

const TABLE_ID = "mapping-section-table";

interface MappingTableProps {
  csvData: CsvData;
  selectedColumn: number;
  onColumnSelected: (colIdx: number) => void;
  // key is column id
  columnInfo: Record<string, ColumnInfo>;
}

export function MappingTable(props: MappingTableProps): JSX.Element {
  const highlightedColumn = useRef(null);

  useEffect(() => {
    // if there is a highlightedColumn, scroll it into view
    if (highlightedColumn.current) {
      const tableElement = document.getElementById(TABLE_ID);
      const tableRight = tableElement.offsetLeft + tableElement.offsetWidth;
      const highlightedElement = highlightedColumn.current as HTMLElement;
      const leftDiff =
        highlightedElement.offsetLeft +
        highlightedElement.offsetWidth -
        tableRight;
      if (leftDiff > 0) {
        tableElement.scrollLeft = leftDiff + highlightedElement.offsetWidth;
      } else {
        tableElement.scrollLeft = 0;
      }
    }
  });

  return (
    <table id={TABLE_ID}>
      <MappingTableHeader
        orderedColumns={props.csvData.orderedColumns}
        columnInfo={props.columnInfo}
        onColumnSelected={props.onColumnSelected}
        highlightedColumnRef={highlightedColumn}
        selectedColumn={props.selectedColumn}
      />
      <MappingTableBody
        csvData={props.csvData}
        selectedColumn={props.selectedColumn}
        onColumnSelected={props.onColumnSelected}
      />
    </table>
  );
}
