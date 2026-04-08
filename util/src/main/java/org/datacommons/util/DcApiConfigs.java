package org.datacommons.util;

import java.util.Map;

final class DcApiConfigs {
  static final String API_ROOT_ENV = "DC_API_ROOT";
  static final String API_KEY_ENV = "DC_API_KEY";

  private static final String DEFAULT_API_ROOT = "https://api.datacommons.org";

  private DcApiConfigs() {}

  static DcApiConfig getConfig() {
    return create(System.getenv());
  }

  static DcApiConfig create(Map<String, String> values) {
    return new DcApiConfig(
        values.getOrDefault(API_ROOT_ENV, DEFAULT_API_ROOT), values.get(API_KEY_ENV));
  }
}

final class DcApiConfig {
  private final String apiRoot;
  private final String apiKey;

  DcApiConfig(String apiRoot, String apiKey) {
    this.apiRoot = normalizeApiRoot(apiRoot);
    this.apiKey = apiKey == null ? "" : apiKey;
  }

  String apiRoot() {
    return apiRoot;
  }

  String apiKey() {
    return apiKey;
  }

  private static String normalizeApiRoot(String apiRoot) {
    if (apiRoot.endsWith("/")) {
      return apiRoot.substring(0, apiRoot.length() - 1);
    }
    return apiRoot;
  }
}
