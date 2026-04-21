package org.datacommons.util.parser.strategy;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.datacommons.util.FileGroup;
import org.datacommons.util.McfFileGroup;

public class McfParserStrategy implements ParserStrategy {
  @Override
  public boolean supports(List<File> files) {
    return files.stream()
        .anyMatch(f -> f.getName().contains(".mcf") || f.getName().endsWith(".tmcf"));
  }

  @Override
  public FileGroup createGroup(List<File> files, Character overrideDelimiter) {
    List<File> mcfFiles = new ArrayList<>();
    List<File> tmcfFiles = new ArrayList<>();
    List<File> csvFiles = new ArrayList<>();

    for (File f : files) {
      String path = f.getPath().toLowerCase();
      if (path.contains(".mcf")) {
        mcfFiles.add(f);
      } else if (path.endsWith(".tmcf")) {
        tmcfFiles.add(f);
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
    return new McfFileGroup(csvFiles, mcfFiles, tmcfFiles, delim);
  }
}
