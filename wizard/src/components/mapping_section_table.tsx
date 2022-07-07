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

import { CsvData, MappedThing, MappingType } from "../types";
import { ColumnInfo } from "./mapping_section";

const TABLE_ID = "mapping-section-table";

interface MappingSectionTableProps {
  csvData: CsvData;
  selectedColumn: number;
  onColumnSelected: (colIdx: number) => void;
  // key is column id
  columnInfo: Record<string, ColumnInfo>;
}

export function MappingSectionTable(
  props: MappingSectionTableProps
): JSX.Element {
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
      }
    }
  });

  const orderedDataRows = Array.from(props.csvData.rowsForDisplay.keys()).sort(
    (a, b) => {
      if (a > b) {
        return 1;
      } else if (a < b) {
        return -1;
      } else {
        return 0;
      }
    }
  );
  // The row index of the first of the bottom n rows and where we want to add an
  // empty row before.
  const emptyRowIdx = orderedDataRows.find(
    (rowIdx, idx) => BigInt(idx + 1) < rowIdx
  );
  // Sometimes csv files will have an empty last row, so hideLastRow will be
  // true if the last row is empty.
  const lastRow = props.csvData.rowsForDisplay.get(
    orderedDataRows[orderedDataRows.length - 1]
  );
  const hideLastRow =
    _.isEmpty(lastRow) || (lastRow.length === 1 && _.isEmpty(lastRow[0]));
  return (
    <table id={TABLE_ID}>
      <thead className="mapping-info-row">
        <tr>
          <th className="row-num"></th>
          {props.csvData.orderedColumns.map((column, idx) => {
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
          {props.csvData.orderedColumns.map((column, idx) => {
            const info = props.columnInfo[column.id];
            return (
              <th
                key={`heading-${idx}`}
                className={
                  props.selectedColumn === idx ? "highlighted-col" : ""
                }
                ref={props.selectedColumn === idx ? highlightedColumn : null}
                onClick={() => props.onColumnSelected(idx)}
              >
                <div>{column.header}</div>
              </th>
            );
          })}
        </tr>
      </thead>
      <tbody>
        {orderedDataRows.map((csvRowNum, rowIdx) => {
          if (rowIdx === orderedDataRows.length - 1 && hideLastRow) {
            return null;
          }
          const csvRow = props.csvData.rowsForDisplay.get(csvRowNum);
          return (
            <>
              {csvRowNum === emptyRowIdx && (
                <tr key={`empty-row-${rowIdx}`}>
                  <td className="row-num">...</td>
                  {props.csvData.orderedColumns.map((_, idx) => {
                    return (
                      <td
                        key={`empty-cell-${idx}`}
                        className={
                          props.selectedColumn === idx ? "highlighted-col" : ""
                        }
                        onClick={() => props.onColumnSelected(idx)}
                      >
                        ...
                      </td>
                    );
                  })}
                </tr>
              )}
              <tr key={`row-${rowIdx}`}>
                <td className="row-num">{csvRowNum.toString()}</td>
                {csvRow.map((cell, cellIdx) => {
                  return (
                    <td
                      key={`row-${rowIdx}-cell-${cellIdx}`}
                      className={
                        props.selectedColumn === cellIdx
                          ? "highlighted-col"
                          : ""
                      }
                      onClick={() => props.onColumnSelected(cellIdx)}
                    >
                      {cell}
                    </td>
                  );
                })}
              </tr>
            </>
          );
        })}
      </tbody>
    </table>
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
