SELECT
    subject_id,
    node_types,
    embedding_content
FROM GRAPH_TABLE(DCGraph
    MATCH
    (n:Node WHERE "Topic" IN UNNEST(n.types) AND TRUE)
    OPTIONAL MATCH
    (n)-[e: Edge
        WHERE e.predicate IN UNNEST(['description'])]->
    (o:Node
        WHERE o.value IS NOT NULL
        AND o.value <> "")
    WITH
        n,
        e.predicate AS pred,
        STRING_AGG(o.value, ". ") AS values,
        LOGICAL_OR(o.last_update_timestamp > TIMESTAMP('2026-04-25 00:00:00')) AS update_property_data
    GROUP BY n, pred
    RETURN
    n.subject_id AS subject_id,
    n.types AS node_types,
    n.last_update_timestamp > TIMESTAMP('2026-04-25 00:00:00') AS update_node_data,
    CASE 
        WHEN COUNT(pred) > 0 THEN
        JSON_OBJECT(
            "subject_id", n.subject_id,
            "name", n.name,
            "properties", JSON_OBJECT(
            ARRAY_AGG(pred IGNORE NULLS),
            ARRAY_AGG(TO_JSON(values) IGNORE NULLS)
            )
        )
        ELSE
        JSON_OBJECT(
            "subject_id", n.subject_id,
            "name", n.name
        )
    END AS embedding_content,
    CASE 
        WHEN COUNT(pred) > 0 THEN
            LOGICAL_OR(update_property_data)
        ELSE
            FALSE
    END AS update_property_data
    GROUP BY n
)
WHERE update_node_data OR update_property_data
