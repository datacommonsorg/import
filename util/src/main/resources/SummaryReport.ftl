<html>
  <head>
    <title>Summary Report</title>
    <!-- required by DataTables -->
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.6.0/jquery.min.js" type="text/javascript"></script>

    <!-- DataTables documentation: https://datatables.net/ -->
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/v/dt/dt-1.12.1/datatables.min.css"/>
    <script type="text/javascript" src="https://cdn.datatables.net/v/dt/dt-1.12.1/datatables.min.js"></script>
  </head>
  <body>
    <style>
      table,
      td,
      th {
        border: 1px solid black;
        border-collapse: collapse;
        padding: 5px;
      }
      td, th {
        max-width: 25rem;
        word-wrap: break-word;
        vertical-align: top;
      }
      tbody tr:hover {
        background-color: #ccc;
      }
      .datatables-table{
        border: 0;
      }
      summary {
        cursor: pointer;
        font-size: 1.2rem;
        font-weight: bold;
        padding-bottom: 1rem;
      }
      details {
        padding-bottom: 1rem;
      }
    </style>
    <h1>Summary Report</h1>
    <div>
      <h2>Import Run Details</h2>
      <table>
        <tr>
          <td>Existence Checks Enabled</td>
          <td>${commandArgs.getExistenceChecks()?string('yes', 'no')}</td>
        </tr>
        <tr>
          <td>Resolution Mode</td>
          <td>${commandArgs.getResolution()}</td>
        </tr>
        <tr>
          <td>Num Threads</td>
          <td>${commandArgs.getNumThreads()}</td>
        </tr>
        <tr>
          <td>Stat Checks Enabled</td>
          <td>${commandArgs.getStatChecks()?string('yes', 'no')}</td>
        </tr>
        <tr>
          <td>Sample Places Entered</td>
          <td>${commandArgs.getSamplePlacesList()?join(", ")}</td>
        </tr>
      </table>
    </div>
    <div>
      <h2>Counters</h2>
        <table>
          <thead>
            <tr>
              <th>Counter Name</th>
              <th>Num Occurences</th>
            </tr>
          </thead>
            <#list levelSummary as severity, counterSet>
              <tbody>
                <tr>
                  <th colspan="2" align="left">${severity}</th>
                </tr>
                <#list counterSet.getCounters() as counterKey, numOccurences>
                  <tr>
                    <td>${counterKey}</td>
                    <td>${numOccurences}</td>
                  </tr>
                </#list>
              </tbody>
            </#list>
        </table>
    </div>
    <#if svSummaryMap?has_content>
      <div>
        <h2>StatVarObservations by StatVar</h2>
        <!-- 
          classes here provide styling through DataTables.
          documentation:
          - hover: https://datatables.net/examples/styling/hover.html
          - order-column: https://datatables.net/examples/styling/order-column.html
        -->
        <table id="statvars-table" class="datatables-table hover order-column" width="95%">
          <thead>
            <tr>
              <th>Stat Var</th>
              <th>Num Places</th>
              <th>Num Observations</th>
              <th>Num Observation Dates</th>
              <th>Min Date</th>
              <th>Max Date</th>
              <th>Measurement Methods</th>
              <th>Units</th>
              <th>Scaling Factors</th>
              <th>Observation Periods</th>
            </tr>
          </thead>
          <tbody>
          <#list svSummaryMap as sv, svSummary>
            <tr>
              <td>${sv}</td>
              <td>${svSummary.getPlaces()?size}</td>
              <td>${svSummary.getNumObservations()}</td>
              <td>${svSummary.getUniqueDates()?size}</td>
              <td>${svSummary.getUniqueDates()?first!""}</td>
              <td>${svSummary.getUniqueDates()?last!""}</td>
              <td>
                <#list svSummary.getMMethods() as method>
                <div>${method}</div>
                </#list>
              </td>
              <td>
                <#list svSummary.getUnits() as unit>
                <div>${unit}</div>
                </#list>
              </td>
              <td>
                <#list svSummary.getSFactors() as sFactor>
                <div>${sFactor}</div>
                </#list>
              </td>
              <td>
                <#list svSummary.getObservationPeriods() as obsPeriod>
                <div>${obsPeriod}</div>
                </#list>
              </td>
            </tr>
          </#list>
          </tbody>
        </table>
      </div>
    </#if>
    <#if placeSeriesSummaryMap?has_content>
      <div>
        <h2>Series Summaries for Sample Places</h2>
          <#list placeSeriesSummaryMap as place, placeSeriesSummary>
            <details open>
              <summary>${place}</summary>
              <table id="sampleplaces-table--${place?counter}" class="datatables-table hover order-column" width="95%">
                <thead>
                  <tr>
                    <th>Stat Var</th>
                    <th>Num Observations</th>
                    <th>Dates</th>
                    <th>Corresponding Values</th>
                    <th>Measurement Methods</th>
                    <th>Units</th>
                    <th>Scaling Factors</th>
                    <th>Observation Periods</th>
                    <th>Time Series Chart</th>
                  </tr>
                </thead>
                <#list placeSeriesSummary.getStatVarSummaryMap() as sv, svSummary>
                <tbody>
                  <tr>
                    <td>${sv}</td>
                    <td>${svSummary.getNumObservations()}</td>
                    <td>${svSummary.getSeriesDates()?join(" | ")}</td>
                    <td>${svSummary.getSeriesValues()?join(" | ")}</td>
                    <td>
                      <#list svSummary.getMMethods() as method>
                      <div>${method}</div>
                      </#list>
                    </td>
                    <td>
                      <#list svSummary.getUnits() as unit>
                      <div>${unit}</div>
                      </#list>
                    </td>
                    <td>
                      <#list svSummary.getSFactors() as sFactor>
                      <div>${sFactor}</div>
                      </#list>
                    </td>
                    <td>
                      <#list svSummary.getObservationPeriods() as obsPeriod>
                      <div>${obsPeriod}</div>
                      </#list>
                    </td>
                    <td style="max-width:none;text-align: -webkit-center;">${svSummary.getTimeSeriesChartSVG()}</td>
                  </tr>
                  </tbody>
                  </#list>
              </table>
            </details>
          </#list>
      </div>
    </#if>
  </body>
  <script>
    function make_table_DataTable(id){
      // given a CSS selector for a <table> element, adds DataTable to it
      // with only sorting enabled, and with no default order column.
      
      $(id).DataTable({
        paging: false,
        searching: false,
        info: false,
        order: [] // don't apply initial ordering (which is turned on by default)
      });

      // DataTables seem to add a class "no-footer" to the tables managed by it,
      // which has no behavior effects but adds a weird 1px gray bottom-border,
      // so we remove that class after initializing the table as a DataTable.
      // reference for another post mentioning this issue:
      // https://datatables.net/forums/discussion/53837/class-no-footer-applied-to-table-despite-a-footer-is-present
      $(id).removeClass("no-footer");
    }

    $(document).ready(function () {
      const sampleplace_table_ids = [
        <#if placeSeriesSummaryMap?has_content>
            <#list placeSeriesSummaryMap as place, placeSeriesSummary>
            "#sampleplaces-table--${place?counter}",
            </#list>
          </#if>
      ]

      make_table_DataTable("#statvars-table");
      sampleplace_table_ids.forEach(( id ) => {
        make_table_DataTable(id)
      })
    });

  </script>
</html>