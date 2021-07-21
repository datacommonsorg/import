# Data Commons Import

This is a repository for data import related code of the [Data
Commons](https://datacommons.org) project.

## About Data Commons

[Data Commons](https://datacommons.org/) is an Open Knowledge Graph that
provides a unified view across multiple public data sets and statistics. It
includes [APIs](https://docs.datacommons.org/api/) and visual tools to easily
explore and analyze data across different datasets without data cleaning or
joining.

## License

Apache 2.0

### GitHub Development Process

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

## Support

For general questions or issues about tool development, please open an issue
on our [issues](https://github.com/datacommonsorg/import/issues) page. For all
other questions, please send an email to `support@datacommons.org`.

**Note** - This is not an officially supported Google product.

