package org.datacommons.util;

import static com.google.common.truth.Truth.assertThat;

import java.net.http.HttpClient;
import org.datacommons.proto.Recon;
import org.junit.Test;

public class ReconClientTest {
  @Test
  public void resolveCoordinates_endToEndApiCall() throws Exception {
    var client = new ReconClient(HttpClient.newHttpClient());

    // San Francisco coordinates
    var request =
        Recon.ResolveCoordinatesRequest.newBuilder()
            .addCoordinates(
                Recon.ResolveCoordinatesRequest.Coordinate.newBuilder()
                    .setLatitude(37.77493)
                    .setLongitude(-122.41942)
                    .build())
            .build();

    var result = client.resolveCoordinates(request).get();

    assertThat(result.getPlaceCoordinatesCount()).isEqualTo(1);
    assertThat(result.getPlaceCoordinates(0).getPlaceDcidsList()).contains("country/USA");
  }
}
