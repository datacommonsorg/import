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
JSON_OBJECT(
  ARRAY_CONCAT(
    ['title', 'name'],
    IF(COUNT(pred) > 0, ARRAY_AGG(pred), ARRAY<STRING>[])
  ),
  ARRAY_CONCAT(
    [TO_JSON(n.subject_id), TO_JSON(n.name)],
    IF(COUNT(pred) > 0, ARRAY_AGG(TO_JSON(values)), ARRAY<JSON>[])
  )
) AS embedding_content
GROUP BY n
