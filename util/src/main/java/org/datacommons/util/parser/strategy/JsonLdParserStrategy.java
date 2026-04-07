package org.datacommons.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class JsonLdParserStrategy implements ParserStrategy {
  @Override
  public boolean supports(List<File> files) {
    return files.stream()
        .anyMatch(f -> f.getName().endsWith(".jsonld") || f.getName().endsWith(".tmpl.jsonld"));
  }

  @Override
  public FileGroup createGroup(List<File> files, Character overrideDelimiter) {
    List<File> jsonLdFiles = new ArrayList<>();
    List<File> tmplJsonLdFiles = new ArrayList<>();
    List<File> csvFiles = new ArrayList<>();

    for (File f : files) {
      String path = f.getPath().toLowerCase();
      if (path.endsWith(".tmpl.jsonld")) {
        tmplJsonLdFiles.add(f);
      } else if (path.endsWith(".jsonld")) {
        jsonLdFiles.add(f);
      } else if (path.endsWith(".csv") || path.endsWith(".tsv")) {
        csvFiles.add(f);
      }
    }

    char delim = overrideDelimiter == null ? ',' : overrideDelimiter;
    return new JsonLdFileGroup(csvFiles, jsonLdFiles, tmplJsonLdFiles, delim);
  }
}
