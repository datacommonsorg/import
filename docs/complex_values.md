# Complex Values

Some values in MCF need refer to values that can not be represented by single
numeric or text values. We represent these using "complex values".

Examples of complex values include:
1. a [Quantity](https://datacommons.org/browser/Quantity), coded as:
 - `[<unit> <val>]`, for example: [`[% 5]`](https://datacommons.org/browser/%5), or [`[Acre 128]`](https://datacommons.org/browser/Acre128)

2. a [QuantityRange](https://datacommons.org/browser/QuantityRange), coded as one of:
 - `[<unit> <startval> <endval>]` represents `[X, Y]`, for example: [`[Acre 1 9.9]`](https://datacommons.org/browser/Acre1To9.9) represents Acre quantities between `1` and `9.9` (endpoints inclusive)
 - `[<unit> - <endval>]` represents `(-INF, Y]`, for example: [`[Celsius - -5]`](https://datacommons.org/browser/CelsiusUpto-5) represents Celsius quantities lesser than or equal to `-5` (inclusive)
 - `[<unit> <startval> -]` represents `[X, INF)`, for example: [`[Acre 1000 -]`](https://datacommons.org/browser/Acre1000Onwards) represents Acre quantities greater than or equal to `1000`.

3. a [GeoCoordinate](https://datacommons.org/browser/GeoCoordinates), coded as one of:
 - `[LatLong <lat_value> <long_value>]`, for example: [`[LatLong -10.136 161.173]`](https://datacommons.org/browser/latLong/-1013600_16117300)
 - `[<lat_value> <long_value> LatLong]`, for example: [`[10.136S 161.173E LatLong]`](https://datacommons.org/browser/latLong/-1013600_16117300)