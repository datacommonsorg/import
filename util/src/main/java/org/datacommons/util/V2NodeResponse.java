package org.datacommons.util;

import java.util.List;
import java.util.Map;

public class V2NodeResponse {
    public Map<String, NodeData> data;

    public static class NodeData {
        public Map<String, ArcData> arcs;
    }

    public static class ArcData {
        public List<NodeInfo> nodes;
    }

    public static class NodeInfo {
        public String dcid;
        public String value;
    }
}
