# Entities (importType: entities)

Each row of an entities CSV becomes a node of `rowEntityType`. Defaults: a
provenance with a source.

```mcf file=provenance.mcf
Node: dcid:TestSource
typeOf: dcs:Source
name: "Test Source"
url: "https://source.example.org"

Node: dcid:TestProvenance
typeOf: dcs:Provenance
name: "Test Provenance"
source: dcid:TestSource
```

```config
{
  "inputFiles": [
    {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
    {"pattern": "case.csv", "importType": "entities", "rowEntityType": "Author",
     "idColumn": "author_id", "provenance": "dcid:TestProvenance"}
  ],
  "importProxyEntities": false
}
```

## rows become typed nodes keyed by the id column

```csv
author_id,author_name,author_country
author1,Jane Doe,country/USA
author2,Joe Smith,country/CAN
```

```expect
[
  "$class_Author",
  "$property_author_name",
  {
    "@id": "dcid:author1",
    "@type": "dcid:Author",
    "dcid:author_country": "country/USA",
    "dcid:author_id": "author1",
    "dcid:author_name": "Jane Doe",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"}
  },
  {
    "@id": "dcid:author2",
    "@type": "dcid:Author",
    "dcid:author_country": "country/CAN",
    "dcid:author_id": "author2",
    "dcid:author_name": "Joe Smith",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"}
  },
  {"@id": "dcid:author_country", "@type": "dcid:Property", "dcid:name": "author_country"},
  {"@id": "dcid:author_id", "@type": "dcid:Property", "dcid:name": "author_id"}
]
```

## without an id column, ids are generated

```csv
author_name,author_country
Jane Doe,country/USA
Joe Smith,country/CAN
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "case.csv", "importType": "entities", "rowEntityType": "Author",
   "provenance": "dcid:TestProvenance"}]}
```

```expect
[
  "$class_Author",
  "$property_author_name",
  {
    "@id": "dcid:Author_0",
    "@type": "dcid:Author",
    "dcid:author_country": "country/USA",
    "dcid:author_name": "Jane Doe",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"}
  },
  {
    "@id": "dcid:Author_1",
    "@type": "dcid:Author",
    "dcid:author_country": "country/CAN",
    "dcid:author_name": "Joe Smith",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"}
  },
  {"@id": "dcid:author_country", "@type": "dcid:Property", "dcid:name": "author_country"}
]
```

## columns to resolve become references, one per listed value

```csv
article_id,article_title,article_author
article1,Article 1,author1
article2,Article 2,"author1,author2"
article3,Article 3,"author2, author3"
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "case.csv", "importType": "entities", "rowEntityType": "Article",
   "idColumn": "article_id", "columnsToResolve": ["article_author"],
   "provenance": "dcid:TestProvenance"}]}
```

```dc_api
{"author1": {"typeOf": "Author"}, "author2": {"typeOf": "Author"}, "author3": {"typeOf": "Author"}}
```

```expect
[
  {
    "@id": "dcid:Article",
    "@type": "dcid:Class",
    "dcid:includedIn": [{"@id": "dcid:TestProvenance"}, {"@id": "dcid:TestSource"}],
    "dcid:name": "Article"
  },
  {
    "@id": "dcid:article1",
    "@type": "dcid:Article",
    "dcid:article_author": {"@id": "dcid:author1"},
    "dcid:article_id": "article1",
    "dcid:article_title": "Article 1",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"}
  },
  {
    "@id": "dcid:article2",
    "@type": "dcid:Article",
    "dcid:article_author": [{"@id": "dcid:author1"}, {"@id": "dcid:author2"}],
    "dcid:article_id": "article2",
    "dcid:article_title": "Article 2",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"}
  },
  {
    "@id": "dcid:article3",
    "@type": "dcid:Article",
    "dcid:article_author": [{"@id": "dcid:author2"}, {"@id": "dcid:author3"}],
    "dcid:article_id": "article3",
    "dcid:article_title": "Article 3",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"}
  },
  {"@id": "dcid:article_author", "@type": "dcid:Property", "dcid:name": "article_author"},
  {"@id": "dcid:article_id", "@type": "dcid:Property", "dcid:name": "article_id"},
  {"@id": "dcid:article_title", "@type": "dcid:Property", "dcid:name": "article_title"}
]
```

## values that look like numbers

```csv
author_id,author_name,papers,h_index
author1,Jane Doe,12,3.5
```

```expect
[
  "$class_Author",
  "$property_author_name",
  {
    "@id": "dcid:author1",
    "@type": "dcid:Author",
    "dcid:author_id": "author1",
    "dcid:author_name": "Jane Doe",
    "dcid:h_index": 3.5,
    "dcid:includedIn": {"@id": "dcid:TestProvenance"},
    "dcid:papers": 12
  },
  {"@id": "dcid:author_id", "@type": "dcid:Property", "dcid:name": "author_id"},
  {"@id": "dcid:h_index", "@type": "dcid:Property", "dcid:name": "h_index"},
  {"@id": "dcid:papers", "@type": "dcid:Property", "dcid:name": "papers"}
]
```
