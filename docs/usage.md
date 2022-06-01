# Usage

Use the import tool from the command line, like so:
```
java -jar <path-to-jar> <mode> <list of mcf/tmcf/csv files>
```

Hint: it can be useful to create an alias for the jar file, such as:
```
alias dc-import='java -jar <path-to-jar>'
```

This is the form that will be used in the rest of the documentation.

Hint: to access a concise explanation of usage modes and flags, run
`dc-import --help`

## Usage Modes

The import tool can either lint StatVars and other schema files, or
it can generate instance MCFs from a TMCF and CSV.

### Lint Mode (`lint`)
To run the tool in lint mode, use:
```
dc-import lint <list of mcf files>
```

### Instance MCF Generation Mode (`genmcf`)
To run the tool in genmcf mode, use:
```
dc-import genmcf <list of csv/tmcf files>
```

Note that in this mode, schema file(s) are optional to pass in.

## Flags
To see a list of flags that can be used, run `dc-import --help`.
