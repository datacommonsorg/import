Graph DCGraph
MATCH
(n:Node WHERE "Topic" IN UNNEST(n.types) AND n.subject_id IN UNNEST(@nl_stat_vars) AND IF(@timestamp IS NOT NULL, n.last_update_timestamp > @timestamp, TRUE))
OPTIONAL MATCH
(n)-[e: Edge
    WHERE e.predicate IN UNNEST(['description'])]->
(o:Node
    WHERE o.value IS NOT NULL
    AND o.value <> "")
WITH
    n,
    e.predicate AS pred,
    STRING_AGG(o.value, ". ") AS values
GROUP BY n, pred
RETURN
n.subject_id AS subject_id,
n.types AS node_types,
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
END AS embedding_content
GROUP BY n
