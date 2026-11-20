package org.datacommons.ingestion.util;

import com.google.cloud.ByteArray;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.Node;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.ValueType;

/** Shared test builders for {@link McfGraph}, {@link Node}, and {@link Edge}. */
public final class GraphTestUtils {
  public static final String PROVENANCE = "dc/base/Test";

  private GraphTestUtils() {}

  public static McfGraph.Values values(TypedValue... vals) {
    return McfGraph.Values.newBuilder().addAllTypedValues(Arrays.asList(vals)).build();
  }

  public static TypedValue text(String value) {
    return TypedValue.newBuilder().setType(ValueType.TEXT).setValue(value).build();
  }

  public static TypedValue ref(String value) {
    return TypedValue.newBuilder().setType(ValueType.RESOLVED_REF).setValue(value).build();
  }

  public static Node subjectNode(String subjectId, String name, String... types) {
    return Node.builder()
        .subjectId(subjectId)
        .value(subjectId)
        .name(name)
        .types(ImmutableList.copyOf(types))
        .build();
  }

  public static Node leaf(String value) {
    return Node.builder()
        .subjectId(PipelineUtils.generateObjectValueKey(value))
        .value(value)
        .build();
  }

  public static Node bytesLeaf(String rawValue) {
    return Node.builder()
        .subjectId(PipelineUtils.generateObjectValueKey(rawValue))
        .bytes(ByteArray.copyFrom(PipelineUtils.compressString(rawValue)))
        .build();
  }

  public static Edge edge(String subjectId, String predicate, String objectId) {
    return Edge.builder()
        .subjectId(subjectId)
        .predicate(predicate)
        .objectId(objectId)
        .provenance(PROVENANCE)
        .build();
  }
}
