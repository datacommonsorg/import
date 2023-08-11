package org.datacommons.util;

import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;
import static org.datacommons.util.ReconClient.NUM_API_CALLS_COUNTER;
import static org.datacommons.util.TestUtil.getCounter;
import static org.datacommons.util.TestUtil.newLogCtx;

import java.net.http.HttpClient;
import org.datacommons.proto.Recon.ResolveCoordinatesRequest;
import org.datacommons.proto.Recon.ResolveCoordinatesRequest.Coordinate;
import org.datacommons.proto.Recon.ResolveCoordinatesResponse;
import org.junit.Test;

public class ReconClientTest {
  private static final String USA_DCID = "country/USA";
  private static final String GBR_DCID = "country/GBR";
  private static final Coordinate SF_COORDINATES =
      Coordinate.newBuilder().setLatitude(37.77493).setLongitude(-122.41942).build();
  private static final Coordinate BIG_BEN_COORDINATES =
      Coordinate.newBuilder().setLatitude(51.510357).setLongitude(-0.116773).build();

  @Test
  public void resolveCoordinates() {
    LogWrapper logWrapper = newLogCtx();
    ReconClient client = new ReconClient(HttpClient.newHttpClient(), logWrapper);

    ResolveCoordinatesRequest request =
        ResolveCoordinatesRequest.newBuilder()
            .addCoordinates(SF_COORDINATES)
            .addCoordinates(BIG_BEN_COORDINATES)
            .build();

    ResolveCoordinatesResponse result = client.resolveCoordinates(request);

    assertThat(result.getPlaceCoordinatesCount()).isEqualTo(2);
    assertThat(
            result.getPlaceCoordinates(0).getPlacesList().stream()
                .map(ResolveCoordinatesResponse.Place::getDcid)
                .collect(toList()))
        .contains(USA_DCID);
    assertThat(
            result.getPlaceCoordinates(1).getPlacesList().stream()
                .map(ResolveCoordinatesResponse.Place::getDcid)
                .collect(toList()))
        .contains(GBR_DCID);
    assertThat(getCounter(logWrapper.getLog(), NUM_API_CALLS_COUNTER)).isEqualTo(1);
  }

  @Test
  public void resolveCoordinates_chunked() {
    LogWrapper logWrapper = newLogCtx();
    ReconClient client = new ReconClient(HttpClient.newHttpClient(), logWrapper, 1);

    ResolveCoordinatesRequest request =
        ResolveCoordinatesRequest.newBuilder()
            .addCoordinates(SF_COORDINATES)
            .addCoordinates(BIG_BEN_COORDINATES)
            .build();

    ResolveCoordinatesResponse result = client.resolveCoordinates(request);

    assertThat(result.getPlaceCoordinatesCount()).isEqualTo(2);
    assertThat(
            result.getPlaceCoordinates(0).getPlacesList().stream()
                .map(ResolveCoordinatesResponse.Place::getDcid)
                .collect(toList()))
        .contains(USA_DCID);
    assertThat(
            result.getPlaceCoordinates(1).getPlacesList().stream()
                .map(ResolveCoordinatesResponse.Place::getDcid)
                .collect(toList()))
        .contains(GBR_DCID);
    assertThat(getCounter(logWrapper.getLog(), NUM_API_CALLS_COUNTER)).isEqualTo(2);
  }
}
