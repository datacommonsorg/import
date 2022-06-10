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
      .place-series-summary {
        cursor: pointer;
        font-size: 1.2rem;
        font-weight: bold;
        padding-bottom: 1rem;
      }
      .place-series-details {
        padding-bottom: 1rem;
      }
      .toc-details-ul {
        list-style: none;
        padding-left: 0;
      }
      #go-to-top{
        position: fixed;
        bottom: 10px;
        right: 10px;
      }
    </style>
    <a name="top"></a>
    <div id="go-to-top">
      <a href="#top">Go to Top</a>
    </div>
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
        <a name="import-run-details" href="#import-run-details">Import Run Details</a>
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
        <a name="counters" href="#counters">Counters</a>
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
                <th colspan="2" align="left"><a href="#counters--${severity}" name="counters--${severity}">${severity}</a></th>
              </tr>
              <#list counterSet.getCounters() as counterKey, numOccurences>
                <tr>
                  <td><a href="#counters--${severity}--${counterKey}" name="counters--${severity}--${counterKey}">${counterKey}</a></td>
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
          <a name="statvars" href="#statvars">StatVarObservations by StatVar</a>
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
              <th>Observation Periods</th>
            </tr>
          </thead>
          <tbody>
          <#list svSummaryMap as sv, svSummary>
            <tr>
              <td><a name="statvars--${sv}" href="#statvars--${sv}">${sv}</a></td>
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
        <h2>
          <a name="places" href="#places">Series Summaries for Sample Places</a>
        </h2>
        <#list placeSeriesSummaryMap as place, placeSeriesSummary>
          <details class="place-series-details">
            <summary class="place-series-summary"><a name="places--${place}" href="#places--${place}">${place}</a></summary>
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
                  <th>Observation Periods</th>
                  <th>Time Series Chart</th>
                </tr>
              </thead>
              <#list placeSeriesSummary.getStatVarSummaryMap() as sv, svSummary>
              <tbody>
                <tr>
                  <td><a href="#places--${place}--${sv}" name="places--${place}--${sv}">${sv}</a></td>
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
    <script>
      function handle_hash_change(){
        /*
        When the hash part of the location has changed, open the <detail> tag
        closest to the anchor that is being linked to.
        */

        const new_hash = CSS.escape( // we will use the location hash in a CSS selector, so we need to escape it
          location.hash // get the hash
          .substring(1) // drop the initial `#` character
          );

        const anchor_selector = "a[name=" + new_hash +"]";
        const anchor_element = document.querySelectorAll(anchor_selector)[0];
        const parent_details_tag = anchor_element.closest("details")

        if (parent_details_tag !== null){ // if tag exists
          parent_details_tag.open = true; // open it.
        }
      }

      window.onhashchange = handle_hash_change; // dynamically react to hash changes
      document.addEventListener('DOMContentLoaded', handle_hash_change, false); // if page loaded with a location hash, also react to that.
    </script>
  </body>
</html>