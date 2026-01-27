# Data Commons: Import Tools and Pipelines

This is a repository for tools and pipelines for importing data into [Data
Commons](https://datacommons.org).

## About Data Commons

[Data Commons](https://datacommons.org/) is an Open Knowledge Graph that
provides a unified view across multiple public data sets and statistics. It
includes [APIs](https://docs.datacommons.org/api/) and visual tools to easily
explore and analyze data across different datasets without data cleaning or
joining.

## Using Import Tool

Detailed documentation on the Import Tool is available [here](docs/usage.md).

- Make sure Java 11+ is installed([download link](https://www.oracle.com/java/technologies/javase-downloads.html#javasejdk)).

- [Download the tool](https://github.com/datacommonsorg/import/releases) and run it with:

  ```bash
  java -jar <path-to-jar> lint <list of mcf/tmcf/csv files>
  ```

  It's useful to create an alias like

  ```bash
  alias dc-import='java -jar <path-to-jar>'
  ```

  so you can invoke the tool as `dc-import lint`

- If there are warnings or errors, the tool will produce a JSON report with a
  table of exemplar errors.

  - It's useful to install an extension like
    [Json-As-Table](https://chrome.google.com/webstore/detail/json-as-table-viewer/khclkgjdjddedohnomokbhinlmpclick?hl=en-US)
    to view the JSON report (but be sure to allow the extension access to
    file URLs like
    [this](https://user-images.githubusercontent.com/4375037/129290496-ed8eb0a3-b5e2-4de6-bdf2-449814df8fcf.png)).

    Another option is to copy/paste the JSON content in [jsongrid](https://jsongrid.com).

- To see the list of flags that can be used and what the default values are: `dc-import --help`.

## Development

NOTE: The instructions below are relevant for developing the tool. To use the
tool, you just need the Java (11+) installed (see instructions above).

### Prerequisites

1. The tools are built using Apache Maven version 3.8.0.
   - For MacOS: `brew install maven`
2. The tools use protobuf and require that `protoc` be installed.
   - For MacOS: `brew install protobuf`
3. Make sure Java 11+ is installed
   - You can install it from [here](https://www.oracle.com/java/technologies/javase-downloads.html#javasejdk)
4. Check what version of Java Maven is using: `mvn --version`

### Build and Test Import Tool

You can build and test the Java code from a Unix shell.

To build: `mvn compile`

To run tests: `mvn test`

To build binary: `mvn package`

- which will produce `tool/target/datacommons-import-tool-<version>-jar-with-dependencies.jar`
- and you can run it with

  ```bash
  java -jar tool/target/datacommons-import-tool-<version>-jar-with-dependencies.jar
  ```

> To run the above maven commands on M1 macs ([details][m1]), use the `-Dos.arch=x86_64` option.
> 
> e.g. `mvn compile -Dos.arch=x86_64`

[m1]: https://github.com/os72/protoc-jar/pull/94#issuecomment-1271505497

### Run Server

The repo also hosts an experimental server for private DC.

To build: `mvn compile`

To run tests: `mvn test`

To build binary: `mvn package`

- which will produce `server/target/datacommons-server-<version>.jar`
- and you can run it with

  ```bash
  java -jar server/target/datacommons-server-<version>.jar <file1.tmcf> <file2.csv>
  ```

Send a request:

```bash
curl http://localhost:8080/stat/series?place=country/USA&statVar=<statVar>
```

Then should see "Hello World!" in the console output.

### Coding Guidelines

The code is formatted using
[`google-java-format`](https://github.com/google/google-java-format). Please
follow instructions in the
[README](https://github.com/google/google-java-format/blob/master/README.md)
to integrate with IntelliJ/Eclipse IDEs.

The formatting is done as part of the build. It can be checked by running:
`mvn com.spotify.fmt:fmt-maven-plugin:check`

### Contributing Changes

From the [repo page](https://github.com/datacommonsorg/import), click on "Fork" button to fork the
repo.

Clone your forked repo to your desktop.

Add datacommonsorg/import repo as a remote:

```shell
git remote add dc https://github.com/datacommonsorg/import.git
```

Every time when you want to send a Pull Request, do the following steps:

```shell
git checkout master
git pull dc master
git checkout -b new_branch_name
# Make some code change
git add .
git commit -m "commit message"
git push -u origin new_branch_name
```

Then in your forked repo, you can send a Pull Request. If this is your first
time contributing to a Google Open Source project, you may need to follow the
steps in [contributing.md](contributing.md).

Wait for approval of the Pull Request and merge the change.

### Creating a Release

1.  Update the version in `pom.xml` to the release version (remove `-SNAPSHOT`).
2.  Build the project: `mvn clean package`.
3.  Create a new Release on GitHub.
4.  Upload the built artifacts (e.g., `tool/target/datacommons-import-tool-<version>-jar-with-dependencies.jar`) to the GitHub release.
5.  Update the version in `pom.xml` to the next snapshot version (e.g., `0.3.2-SNAPSHOT`) and raise a Pull Request.

## License

Apache 2.0

## Support

For general questions or issues, please open an issue on our
[issues](https://github.com/datacommonsorg/import/issues) page. For all other
questions, please send an email to `support@datacommons.org`.

**Note** - This is not an officially supported Google product.
