# Complex Values

Some values in MCF need refer to values that can not be represented by single
numeric or text values. We represent these using "complex values".

A complex value may either be:
1. a [Quantity](https://datacommons.org/browser/Quantity), coded as:
 - `[<unit> <val>]`

2. a [QuantityRange](https://datacommons.org/browser/QuantityRange), coded as one of:
 - `[<unit> <startval> <endval>]`
 - `[<unit> - <endval>]`
 - `[<unit> <startval> -]`
 
3. a [GeoCoordinates](https://datacommons.org/browser/GeoCoordinates) type, coded as one of:
 - `[LatLong <lat_value> <long_value>]`
 - `[<lat_value> <long_value> LatLong]`