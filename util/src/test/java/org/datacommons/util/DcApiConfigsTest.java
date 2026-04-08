package org.datacommons.util;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.junit.Test;

public class DcApiConfigsTest {
  @Test
  public void createDefaultsToProdRoot() {
    DcApiConfig config = DcApiConfigs.create(Map.of("DC_API_KEY", "prod-key"));

    assertEquals("https://api.datacommons.org", config.apiRoot());
    assertEquals("prod-key", config.apiKey());
  }

  @Test
  public void createNormalizesExplicitRoot() {
    DcApiConfig config =
        DcApiConfigs.create(
            Map.of("DC_API_ROOT", "https://custom.api.datacommons.org/", "DC_API_KEY", "key"));

    assertEquals("https://custom.api.datacommons.org", config.apiRoot());
    assertEquals("key", config.apiKey());
  }

  @Test
  public void createUsesEmptyKeyWhenMissing() {
    DcApiConfig config = DcApiConfigs.create(Map.of());

    assertEquals("https://api.datacommons.org", config.apiRoot());
    assertEquals("", config.apiKey());
  }
}
