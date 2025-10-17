# pvduck

[![Lint, Test, Build, Deploy](https://github.com/vegardege/pvduck/actions/workflows/lint-test-build-deploy.yml/badge.svg)](https://github.com/vegardege/pvduck/actions/workflows/lint-test-build-deploy.yml)

`pvduck` is a cli tool allowing you to sample, download, filter, parse, and
aggregate Wikimedia pageviews dumps, creating an overview of the most visited
pages on a sampled subset of Wikimedia pages in a `duckdb` database.

This is useful if you want a rough overview of pageviews for a specific
subset of pages, but lack access to the analytics servers and don't want to
host a full HDFS cluster yourself. Instead of storing several GB per day,
you can aggregate years worth of data in a small database holding just the
data you need.

The tool was developed for my own weekend projects, where gauging popularity
without perfect precision or history is very helpful.

## Setup

The easiest way to use the tool is via docker:

```bash
docker run --rm -it \
    -v config_volume:$HOME/.config \
    -v data_volume:$HOME/.local/share \
    vegardege/pvduck:latest \
    create project_name
```

Note that you need `-it` for actions opening an editor.

`config_volume` and `data_volume` is the location of your preferred host
directories for config and data storage respectively. All files will be
stored in a subdirectory named `pvduck`.

## Usage

Call `pvduck --help` for instructions and `pvduck --install-completion` to
install auto-completion in your shell, both of which will help.

The tool has six commands:

| Command                  | Description                                                    |
| ------------------------ | -------------------------------------------------------------- |
| `create <project_name>`  | Create a new project                                           |
| `edit <project_name>`    | Edit project configuration                                     |
| `rm <project_name>`      | Delete config and database                                     |
| `sync <project_name>`    | Download missing data (if any) and aggregate into the database |
| `open <project_name>`    | Open the project's database in `duckdb`                        |
| `status <project_name>`  | See progress status for the project                            |
| `compact <project_name>` | Reclaim disk space according to `duckdb` best practice         |
| `ls`                     | List all existing projects                                     |

By default, `sync` will run until it exhausted the date range given with the
given sample rate. If you want to run it for a limited time only, apply the
`max_files` option to stop after a specific number of files.

Note that syncing can be memory intensive. It operates in chunks of 1 000 000
rows by default, which can be modified with the `PVDUCK_CHUNK_SIZE` environment
variable. Increase the value for faster syncs, decrease it for more memory
efficient (but slower) execution.

When you create a new project, you can define the configuration:

| Param         | Description                                                                  |
| ------------- | ---------------------------------------------------------------------------- |
| `base_url`    | Which Wikimedia mirror to use (recommended: use mirror close to you)         |
| `sleep_time`  | How many seconds to wait between each file download                          |
| `start_date`  | Date of the first dump file to download                                      |
| `end_date`    | Date of the last dump file to download (or blank for current date expanding) |
| `sample_rate` |  Probability of downloading each hourly file in the interval                 |

In addition, the config file contains filters to reduce the size of the
dataset. All filters can be set to blank values, which means no rows are
excluded.

| Filter         | Type        | Description                                                 |
| -------------- | ----------- | ----------------------------------------------------------- |
| `line_regex`   | `regex`     | Regular expression used to filter lines before parsing      |
| `page_title`   | `regex`     | Regular expression used to filter page titles after parsing |
| `domain_codes` | `list[str]` | List of domain codes to accept                              |
| `min_views`    | `int`       | Minimum amount of views needed to be accepted               |
| `max_views`    | `int`       | Maximum amount of views allowed                             |
| `languages`    | `list[str]` | List of languages to accept                                 |
| `domains`      | `list[str]` | List of domains to accept                                   |
| `mobile`       | `bool`      | If set, filter on whether the row belongs to a mobile site  |

> [!IMPORTANT]  
> The `sync` operation is destructive. It keeps track of which files you have
> downloaded, but can not revert any aggregation operations. As a result, only
> some parameters and filters can be changed without putting the database in an
> inconsistent state. Notably, you _can_ expand the date range and increase the
> sample rate without issues.

## Test

```
uv run pytest tests/ --cov=src

Name                       Stmts   Miss  Cover
----------------------------------------------
src/pvduck/__init__.py         0      0   100%
src/pvduck/cli.py            102      0   100%
src/pvduck/config.py          61      0   100%
src/pvduck/db.py              75      0   100%
src/pvduck/project.py         21      0   100%
src/pvduck/stream.py          20      0   100%
src/pvduck/timeseries.py      32      0   100%
src/pvduck/validators.py      10      0   100%
src/pvduck/wikimedia.py        8      0   100%
----------------------------------------------
TOTAL                        329      0   100%
```

> [!CAUTION]
> Tests are separated in unit tests and integration tests. The integration
> tests take several minutes to run and downloads files from the configured
> server. Don't run the full test suite frivolously.
