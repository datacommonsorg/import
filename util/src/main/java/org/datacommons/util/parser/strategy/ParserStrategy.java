package org.datacommons.util;

import java.io.File;
import java.util.List;

/**
 * Strategy interface that allows decoupled registration of different input schemas (e.g., MCF vs.
 * JSON-LD) without hard-coding specific conditions in the core FileGroup builder.
 */
public interface ParserStrategy {
  boolean supports(List<File> files);

  FileGroup createGroup(List<File> files, Character overrideDelimiter);
}
