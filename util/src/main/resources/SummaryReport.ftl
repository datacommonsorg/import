<html>
  <head>
    <title>Summary Report</title>
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
        <table width="95%">
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
            <table width="95%">
              <thead>
                <tr>
                  <th>StatVar</th>
                  <th>Num Observations</th>
                  <th>Dates</th>
                  <th>Corresponding Values</th>
                  <th>Measurement Method</th>
                  <th>Unit</th>
                  <th>Scaling Factor</th>
                  <th>Observation Period</th>
                  <th>Time Series Chart</th>
                </tr>
              </thead>
              <#list placeSeriesSummary.getSvSeriesSummaryMap() as sv, timeSeries>
                <#list timeSeries as hash, seriesSummary>
                  <tbody>
                    <tr>
                      <td>${sv}</td>
                      <td>${seriesSummary.getTimeSeries()?size}</td>
                      <td>${seriesSummary.getDatesString()}</td>
                      <td>${seriesSummary.getValueString()}</td>
                      <td>
                        <div>${seriesSummary.getValidationResult().getMeasurementMethod()}</div>
                      </td>
                      <td>
                        <div>${seriesSummary.getValidationResult().getUnit()}</div>
                      </td>
                      <td>
                        <div>${seriesSummary.getValidationResult().getScalingFactor()}</div>
                      </td>
                      <td>
                        <div>${seriesSummary.getValidationResult().getObservationPeriod()}</div>
                      </td>
                      <td style="max-width:none;text-align: -webkit-center;">${seriesSummary.getTimeSeriesSVGChart()}</td>
                    </tr>
                  </tbody>
                </#list>
              </#list>
            </table>
          </details>
        </#list>
      </div>
    </#if>
  </body>
</html>