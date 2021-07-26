# Data Commons: Import Tools and Pipelines

This is a repository for tools and pipelines for importing data into [Data
Commons](https://datacommons.org).

## About Data Commons

[Data Commons](https://datacommons.org/) is an Open Knowledge Graph that
provides a unified view across multiple public data sets and statistics. It
includes [APIs](https://docs.datacommons.org/api/) and visual tools to easily
explore and analyze data across different datasets without data cleaning or
joining.

## Dependencies

* The tools are built using Apache Maven version 3.8.0.

  For MacOS: ```brew install maven```

* The tools use protobuf and require that `protoc` be installed.

  For MacOS: ```brew install protoc```

## Development

### Build and Test

You can build and test the Java code from a Unix shell.

To build: ```mvn compile```

To run tests: ```mvn test```

### Coding Guidelines

The code is formatted using
[`google-java-format`](https://github.com/google/google-java-format). Please
follow instructions in the
[README](https://github.com/google/google-java-format/blob/master/README.md)
to integrate with IntelliJ/Eclipse IDEs.

The formatting is done as part of the build. It can be checked by running:
```mvn com.coveo:fmt-maven-plugin:check```

### Contributing Changes

In https://github.com/datacommonsorg/import, click on "Fork" button to fork the
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

## License

Apache 2.0

## Support

For general questions or issues, please open an issue on our
[issues](https://github.com/datacommonsorg/import/issues) page. For all other
questions, please send an email to `support@datacommons.org`.

**Note** - This is not an officially supported Google product.

