# MCF nodes

Each case below is one MCF snippet and the exact JSON-LD records it produces.
The blocks before the first case are the defaults that every case runs with:
a provenance (with its source) and a config that imports `*.mcf` files under
it. Records that the defaults alone produce are not listed in the cases.

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
  "inputFiles": [{"pattern": "*.mcf", "provenance": "dcid:TestProvenance"}]
}
```

## basic node

```mcf
Node: dcid:Thing1
typeOf: dcs:Thing
name: "Thing One"
```

```expect
[
  {"@id": "dcid:Thing1", "@type": "dcid:Thing", "dcid:name": "Thing One"}
]
```

## dcid property names a local node

```mcf
Node: LocalName
dcid: "Thing2"
typeOf: dcs:Thing
name: "Thing Two"
```

```expect
[
  {"@id": "dcid:Thing2", "@type": "dcid:Thing", "dcid:name": "Thing Two"}
]
```

## standard namespaces in references are normalized

```mcf
Node: dcid:Thing3
typeOf: schema:Thing
containedInPlace: dcs:country/USA
relatedTo: dcid:Thing1
seeAlso: schema:Place
```

```expect
[
  {
    "@id": "dcid:Thing3",
    "@type": "dcid:Thing",
    "dcid:containedInPlace": {"@id": "dcid:country/USA"},
    "dcid:relatedTo": {"@id": "dcid:Thing1"},
    "dcid:seeAlso": {"@id": "dcid:Place"}
  }
]
```

## quoted typeOf is still a reference

```mcf
Node: dcid:Thing4
typeOf: "Thing"
```

```expect
[
  {"@id": "dcid:Thing4", "@type": "dcid:Thing"}
]
```

## quoted string containing a comma stays one value

```mcf
Node: dcid:Thing5
typeOf: dcs:Thing
name: "Paris, France"
```

```expect
[
  {"@id": "dcid:Thing5", "@type": "dcid:Thing", "dcid:name": "Paris, France"}
]
```

## comma-separated references become several values

```mcf
Node: dcid:Thing6
typeOf: dcs:Thing
relatedTo: dcid:A, dcid:B, dcs:C
```

```expect
[
  {
    "@id": "dcid:Thing6",
    "@type": "dcid:Thing",
    "dcid:relatedTo": [{"@id": "dcid:A"}, {"@id": "dcid:B"}, {"@id": "dcid:C"}]
  }
]
```

## local references resolve to dcids

> xfail: object values written as l:<local id> are not resolved to the referenced node's dcid (the output points at dcid:L2 instead of dcid:Parent)

```mcf
Node: L1
dcid: "Child"
typeOf: dcs:Thing
parent: l:L2

Node: L2
dcid: "Parent"
typeOf: dcs:Thing
```

```expect
[
  {
    "@id": "dcid:Child",
    "@type": "dcid:Thing",
    "dcid:parent": {
      "@id": "dcid:Parent"
    }
  },
  {
    "@id": "dcid:Parent",
    "@type": "dcid:Thing"
  }
]
```

## unquoted numbers and booleans

```mcf
Node: dcid:Thing7
typeOf: dcs:Thing
count: 42
ratio: 0.25
delta: -3
flag: true
```

```expect
[
  {
    "@id": "dcid:Thing7",
    "@type": "dcid:Thing",
    "dcid:count": 42,
    "dcid:delta": -3,
    "dcid:flag": "true",
    "dcid:ratio": 0.25
  }
]
```

## quoted number stays a string

> xfail: quoted values that look numeric are emitted as JSON numbers, so "00123" loses its leading zeros

```mcf
Node: dcid:Thing8
typeOf: dcs:Thing
code: "00123"
```

```expect
[{"@id": "dcid:Thing8", "@type": "dcid:Thing", "dcid:code": "00123"}]
```

## numeric spellings

```mcf
Node: dcid:Thing9
typeOf: dcs:Thing
leadingZeros: 007
exponent: 1e5
infinity: inf
trailingZero: 2.0
```

```expect
[
  {
    "@id": "dcid:Thing9",
    "@type": "dcid:Thing",
    "dcid:exponent": "1e5",
    "dcid:infinity": "inf",
    "dcid:leadingZeros": 7,
    "dcid:trailingZero": 2.0
  }
]
```

## unicode values are preserved

```mcf
Node: dcid:Thing10
typeOf: dcs:Thing
name: "Zürich – 東京 🚀"
```

```expect
[
  {"@id": "dcid:Thing10", "@type": "dcid:Thing", "dcid:name": "Zürich – 東京 🚀"}
]
```

## several types

```mcf
Node: dcid:Thing11
typeOf: dcs:Thing, dcs:Place
```

```expect
[
  {"@id": "dcid:Thing11", "@type": ["dcid:Place", "dcid:Thing"]}
]
```

## repeated values are deduplicated

```mcf
Node: dcid:Thing12
typeOf: dcs:Thing
relatedTo: dcid:A, dcid:A
```

```expect
[
  {"@id": "dcid:Thing12", "@type": "dcid:Thing", "dcid:relatedTo": {"@id": "dcid:A"}}
]
```

## one node defined in two blocks is merged

```mcf
Node: dcid:Thing13
typeOf: dcs:Thing
name: "First"

Node: dcid:Thing13
description: "Second"
```

```expect
[
  {"@id": "dcid:Thing13", "@type": "dcid:Thing", "dcid:description": "Second", "dcid:name": "First"}
]
```

## Context namespaces expand to full URIs

```mcf
Context:
namespace: "ex=http://example.org/"

Node: dcid:Thing14
typeOf: dcs:Thing
sameAs: ex:item/1
```

```expect
[
  {"@id": "dcid:Thing14", "@type": "dcid:Thing", "dcid:sameAs": {"@id": "http://example.org/item/1"}}
]
```

## complex values are kept verbatim

```mcf
Node: dcid:Thing15
typeOf: dcs:Thing
age: [10 20 dcs:Years]
```

```expect
[
  {"@id": "dcid:Thing15", "@type": "dcid:Thing", "dcid:age": "[10 20 dcs:Years]"}
]
```

## custom namespace subjects and references become dcid

The subject is rewritten to `dcid:`. The custom-namespace value stays the
literal string "undata:Other" for now; it will become a reference once the
loader supports custom namespaces.

```mcf
Node: undata:Thing16
typeOf: dcs:Thing
relatedTo: undata:Other
```

```expect
[
  {"@id": "dcid:Thing16", "@type": "dcid:Thing", "dcid:relatedTo": "undata:Other"}
]
```

## comments and blank lines are ignored

```mcf
// A comment
# Another comment

Node: dcid:Thing17

typeOf: dcs:Thing
```

```expect
[
  {"@id": "dcid:Thing17", "@type": "dcid:Thing"}
]
```

## statistical variable node passes through

```mcf
Node: dcid:Count_Person_Female
typeOf: dcs:StatisticalVariable
populationType: dcs:Person
measuredProperty: dcs:count
statType: dcs:measuredValue
gender: dcs:Female
name: "Female population"
```

```expect
[
  {
    "@id": "dcid:Count_Person_Female",
    "@type": "dcid:StatisticalVariable",
    "dcid:gender": {"@id": "dcid:Female"},
    "dcid:measuredProperty": {"@id": "dcid:count"},
    "dcid:name": "Female population",
    "dcid:populationType": {"@id": "dcid:Person"},
    "dcid:statType": {"@id": "dcid:measuredValue"}
  }
]
```

## source url adds a domain

Source and Provenance nodes are written twice into the same folder: once from
the MCF triples and once from the preprocessor's node registry (which adds
`domain`, and a default `name` when there is none).

```mcf
Node: dcid:OtherSource
typeOf: dcs:Source
name: "Other Source"
url: "https://data.example.com/path"
```

```expect
[
  {
    "@id": "dcid:OtherSource",
    "@type": "dcid:Source",
    "dcid:domain": "data.example.com",
    "dcid:name": "Other Source",
    "dcid:url": "https://data.example.com/path"
  },
  {
    "@id": "dcid:OtherSource",
    "@type": "dcid:Source",
    "dcid:name": "Other Source",
    "dcid:url": "https://data.example.com/path"
  }
]
```

## provenance without a name

```mcf
Node: dcid:OtherSource2
typeOf: dcs:Source
url: "https://other.example.com"

Node: dcid:OtherProvenance
typeOf: dcs:Provenance
source: dcid:OtherSource2
```

```expect
[
  {
    "@id": "dcid:OtherProvenance",
    "@type": "dcid:Provenance",
    "dcid:name": "OtherProvenance",
    "dcid:source": {"@id": "dcid:OtherSource2"}
  },
  {"@id": "dcid:OtherProvenance", "@type": "dcid:Provenance", "dcid:source": {"@id": "dcid:OtherSource2"}},
  {
    "@id": "dcid:OtherSource2",
    "@type": "dcid:Source",
    "dcid:domain": "other.example.com",
    "dcid:name": "OtherSource2",
    "dcid:url": "https://other.example.com"
  },
  {"@id": "dcid:OtherSource2", "@type": "dcid:Source", "dcid:url": "https://other.example.com"}
]
```

## node without a dcid fails

```mcf
Node: NoDcid
typeOf: dcs:Thing
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "dcid not specified for node: NoDcid"
  ],
  "handshake_error_types": [
    "GENERIC_ERROR"
  ]
}
```

## line without a colon fails

```mcf
Node: dcid:Thing18
this line has no colon
```

```expect-error
{
  "type": "AssertionError",
  "contains": [
    "Line 2: Malformed line without a colon delimiter"
  ],
  "handshake_error_types": [
    "GENERIC_ERROR"
  ]
}
```

## property before any Node fails

```mcf
typeOf: dcs:Thing
Node: dcid:Thing19
```

```expect-error
{
  "type": "AssertionError",
  "contains": [
    "Line 1: Prop-Values before Node or Context block"
  ],
  "handshake_error_types": [
    "GENERIC_ERROR"
  ]
}
```
