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
      .summary-em {
        cursor: pointer;
        font-size: 1.2rem;
        font-weight: bold;
        padding-bottom: 1rem;
      }
      .details-pad {
        padding-bottom: 1rem;
      }
      .toc-details-ul {
        list-style: none;
        padding-left: 0;
      }
    </style>
    <h1>Summary Report</h1>
    <h3>Table of Contents</h3>
    <ul>
      <li><a href="#import-run-details">Import Run Details</a></li>
      <li><a href="#counters">Counters</a></li>
      <ul class="toc-details-ul">
        <#list levelSummary as severity, counterSet>
          <li>
            <details>
              <summary>
                <a href="#counters--${severity}">
                  ${severity}
                </a>
              </summary>
              <ul>
                <#list counterSet.getCounters() as counterKey, numOccurences>
                <li>
                  <a href="#counters--${severity}--${counterKey}">${counterKey}</a>
                </li>
                </#list>
              </ul>
            </details>
          </li>
        </#list>
      </ul>
      
      <#if svSummaryMap?has_content>
        <li><a href="#statvars"">StatVarObservations by StatVar</a></li>
          <details>
            <summary>StatVars</summary>
              <ul>
                <#list svSummaryMap as sv, svSummary>
                  <li>
                    <a href="#statvars--${sv}">${sv}</a>
                  </li>
                </#list>
              </ul>
          </details>
      </#if>

      <#if placeSeriesSummaryMap?has_content>
        <li><a href="#places"">Series Summaries for Sample Places</a></li>
        <ul class="toc-details-ul">
          <#list placeSeriesSummaryMap as place, placeSeriesSummary>
            <li>
              <details>
                <summary>
                  <a href="#places--${place}">${place}</a>
                </summary>
                <ul>
                  <#list placeSeriesSummary.getStatVarSummaryMap() as sv, svSummary>
                  <li>
                    <a href="#places--${place}--${sv}">${sv}</a>
                  </li>
                  </#list>
                </ul>
              </details>
            </li>
          </#list>
        </ul>
      </#if>
    </ul>

    <div>
      <h2>
        Import Run Details
        <a name="import-run-details" href="#import-run-details">#</a>
      </h2>  
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
      <h2>
        Counters
        <a name="counters" href="#counters">#</a>
      </h2>
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
                <th colspan="2" align="left">${severity}  <a href="#counters--${severity}" name="counters--${severity}">#</a></th>
              </tr>
              <#list counterSet.getCounters() as counterKey, numOccurences>
                <tr>
                  <td>${counterKey} <a href="#counters--${severity}--${counterKey}" name="counters--${severity}--${counterKey}">#</a></td>
                  <td>${numOccurences}</td>
                </tr>
              </#list>
            </tbody>
          </#list>
      </table>
    </div>
    <#if svSummaryMap?has_content>
      
      <div>
        <h2>
          StatVarObservations by StatVar
          <a name="statvars" hrew="#statvars">#</a>
        </h2>
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
            </tr>
          </thead>
          <tbody>
          <#list svSummaryMap as sv, svSummary>
            <tr>
              <td>${sv} <a name="statvars--${sv}" href="#statvars--${sv}">#</a></td>
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
            </tr>
          </#list>
          </tbody>
        </table>
      </div>
    </#if>
    <#if placeSeriesSummaryMap?has_content>
      <div>
        <h2>
          Series Summaries for Sample Places
          <a name="places" href="#places">#</a>
        </h2>
        <#list placeSeriesSummaryMap as place, placeSeriesSummary>
          
          <details open class="details-pad">
            <summary class="summary-em">${place} <a name="places--${place}" href="#places--${place}">#</a></summary>
            <table width="95%">
              <thead>
                <tr>
                  <th>Stat Var</th>
                  <th>Num Observations</th>
                  <th>Dates</th>
                  <th>Corresponding Values</th>
                  <th>Measurement Methods</th>
                  <th>Units</th>
                  <th>Scaling Factors</th>
                  <th>Time Series Chart</th>
                </tr>
              </thead>
              <#list placeSeriesSummary.getStatVarSummaryMap() as sv, svSummary>
              <tbody>
                <tr>
                  <td>${sv} <a href="#places--${place}--${sv}" name="places--${place}--${sv}">#</a></td>
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
</html>