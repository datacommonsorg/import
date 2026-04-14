package org.datacommons.util.parser.strategy;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.datacommons.util.FileGroup;
import org.datacommons.util.JsonLdFileGroup;

public class JsonLdParserStrategy implements ParserStrategy {
  @Override
  public boolean supports(List<File> files) {
    return files.stream().anyMatch(f -> f.getName().endsWith(".jsonld"));
  }

  @Override
  public FileGroup createGroup(List<File> files, Character overrideDelimiter) {
    List<File> jsonLdFiles = new ArrayList<>();
    List<File> csvFiles = new ArrayList<>();

    for (File f : files) {
      String path = f.getPath().toLowerCase();
      if (path.endsWith(".jsonld")) {
        jsonLdFiles.add(f);
      } else if (path.endsWith(".csv") || path.endsWith(".tsv")) {
        csvFiles.add(f);
      }
    }

    boolean hasTsv = csvFiles.stream().anyMatch(f -> f.getName().endsWith(".tsv"));
    boolean hasCsv = csvFiles.stream().anyMatch(f -> f.getName().endsWith(".csv"));
    if (hasTsv && hasCsv) {
      throw new IllegalArgumentException("Please do not mix .tsv and .csv files");
    }
    char delim = overrideDelimiter != null ? overrideDelimiter : (hasTsv ? '\t' : ',');
    return new JsonLdFileGroup(csvFiles, jsonLdFiles, delim);
  }
}
