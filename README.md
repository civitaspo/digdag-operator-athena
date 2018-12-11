# digdag-operator-athena
[![Jitpack](https://jitpack.io/v/pro.civitaspo/digdag-operator-athena.svg)](https://jitpack.io/#pro.civitaspo/digdag-operator-athena) [![CircleCI](https://circleci.com/gh/civitaspo/digdag-operator-athena.svg?style=shield)](https://circleci.com/gh/civitaspo/digdag-operator-athena) [![Digdag](https://img.shields.io/badge/digdag-v0.9.27-brightgreen.svg)](https://github.com/treasure-data/digdag/releases/tag/v0.9.27)

digdag plugin for operating a query on athena.

# Overview

- Plugin type: operator

# Usage

```yaml
_export:
  plugin:
    repositories:
      - https://jitpack.io
    dependencies:
      - pro.civitaspo:digdag-operator-athena:0.1.5
  athena:
    auth_method: profile

+step1:
  athena.query>: template.sql

+step2:
  echo>: ${athena.last_query}

+stap3:
  athena.ctas>:
  select_query: template.sql
  table: hoge
  output: s3://mybucket/prefix/
```

# Configuration

## Remarks

- type `DurationParam` is strings matched `\s*(?:(?<days>\d+)\s*d)?\s*(?:(?<hours>\d+)\s*h)?\s*(?:(?<minutes>\d+)\s*m)?\s*(?:(?<seconds>\d+)\s*s)?\s*`.
  - The strings is used as `java.time.Duration`.

## Common Configuration

### System Options

Define the below options on properties (which is indicated by `-c`, `--config`).

- **athena.allow_auth_method_env**: Indicates whether users can use **auth_method** `"env"` (boolean, default: `false`)
- **athena.allow_auth_method_instance**: Indicates whether users can use **auth_method** `"instance"` (boolean, default: `false`)
- **athena.allow_auth_method_profile**: Indicates whether users can use **auth_method** `"profile"` (boolean, default: `false`)
- **athena.allow_auth_method_properties**: Indicates whether users can use **auth_method** `"properties"` (boolean, default: `false`)
- **athena.assume_role_timeout_duration**: Maximum duration which server administer allows when users assume **role_arn**. (`DurationParam`, default: `1h`)

### Secrets

- **athena.access_key_id**: The AWS Access Key ID to use when submitting Athena queries. (optional)
- **athena.secret_access_key**: The AWS Secret Access Key to use when submitting Athena queries. (optional)
- **athena.session_token**: The AWS session token to use when submitting Athena queries. This is used only **auth_method** is `"session"` (optional)
- **athena.role_arn**: The AWS Role to assume when submitting Athena queries. (optional)
- **athena.role_session_name**: The AWS Role Session Name when assuming the role. (default: `digdag-athena-${session_uuid}`)
- **athena.http_proxy.host**: proxy host (required if **use_http_proxy** is `true`)
- **athena.http_proxy.port** proxy port (optional)
- **athena.http_proxy.scheme** `"https"` or `"http"` (default: `"https"`)
- **athena.http_proxy.user** proxy user (optional)
- **athena.http_proxy.password**: http proxy password (optional)

### Options

- **auth_method**: name of mechanism to authenticate requests (`"basic"`, `"env"`, `"instance"`, `"profile"`, `"properties"`, `"anonymous"`, or `"session"`. default: `"basic"`)
  - `"basic"`: uses access_key_id and secret_access_key to authenticate.
  - `"env"`: uses AWS_ACCESS_KEY_ID (or AWS_ACCESS_KEY) and AWS_SECRET_KEY (or AWS_SECRET_ACCESS_KEY) environment variables.
  - `"instance"`: uses EC2 instance profile.
  - `"profile"`: uses credentials written in a file. Format of the file is as following, where `[...]` is a name of profile.
    - **profile_file**: path to a profiles file. (string, default: given by AWS_CREDENTIAL_PROFILES_FILE environment varialbe, or ~/.aws/credentials).
    - **profile_name**: name of a profile. (string, default: `"default"`)
  - `"properties"`: uses aws.accessKeyId and aws.secretKey Java system properties.
  - `"anonymous"`: uses anonymous access. This auth method can access only public files.
  - `"session"`: uses temporary-generated access_key_id, secret_access_key and session_token.
- **use_http_proxy**: Indicate whether using when accessing AWS via http proxy. (boolean, default: `false`)
- **region**: The AWS region to use for Athena service. (string, optional)
- **endpoint**: The Amazon Athena endpoint address to use. (string, optional)

## Configuration for `athena.query>` operator

### Options

- **athena.query>**: The SQL query statements or file location (in local or Amazon S3) to be executed. You can use digdag's template engine like `${...}` in the SQL query. (string, required)
- **token_prefix**: Prefix for `ClientRequestToken` that a unique case-sensitive string used to ensure the request to create the query is idempotent (executes only once). On this plugin, the token is composed like `${token_prefix}-${session_uuid}-${hash value of query}-${random string}`. (string, default: `"digdag-athena"`)
- **database**: The name of the database. (string, optional)
- **output**: The location in Amazon S3 where your query results are stored, such as `"s3://path/to/query/"`. For more information, see [Queries and Query Result Files](https://docs.aws.amazon.com/athena/latest/ug/querying.html). (string, default: `"s3://aws-athena-query-results-${AWS_ACCOUNT_ID}-<AWS_REGION>"`)
- **timeout**: Specify timeout period. (`DurationParam`, default: `"10m"`)
- **preview**: Call `athena.preview>` operator after run `athena.query>`. (boolean, default: `true`)

### Output Parameters

- **athena.last_query.id**: The unique identifier for each query execution. (string)
- **athena.last_query.database**: The name of the database. (string)
- **athena.last_query.query**: The SQL query statements which the query execution ran. (string)
- **athena.last_query.output**: The location in Amazon S3 where your query results are stored. (string)
- **athena.last_query.scan_bytes**: The number of bytes in the data that was queried. (long)
- **athena.last_query.exec_millis**: The number of milliseconds that the query took to execute. (long)
- **athena.last_query.state**: The state of query execution. SUBMITTED indicates that the query is queued for execution. RUNNING indicates that the query is scanning data and returning results. SUCCEEDED indicates that the query completed without error. FAILED indicates that the query experienced an error and did not complete processing. CANCELLED indicates that user input interrupted query execution. (string)
- **athena.last_query.state_change_reason**: Further detail about the status of the query. (string)
- **athena.last_query.submitted_at**: The unix timestamp that the query was submitted. (integer)
- **athena.last_query.completed_at**: The unix timestamp that the query completed. (integer)

## Configuration for `athena.preview>` operator

### Options

- **athena.preview>**: The identifier for the query execution that is succeeded. (string, required)
- **max_rows**: The maximum number of rows to preview. 0 ~ 100 is valid. (integer, default: `10`)

### Output Parameters

- **athena.last_preview.id**: The identifier for the query execution. (string)
- **athena.last_preview.columns**: The information that describes the column structure and data types of a table of query results. (map of array)
  - **case_sensitive**: Indicates whether values in the column are case-sensitive. (boolean)
  - **catalog**: The catalog to which the query results belong. (string)
  - **label**: A column label. (string)
  - **name**: The name of the column. (string)
  - **nullable**: Indicates the column's nullable status. (one of `NOT_NULL`, `NULLABLE`, `UNKNOWN`)
  - **precision**: For `DECIMAL` data types, specifies the total number of digits, up to 38. For performance reasons, we recommend up to 18 digits. (integer)
  - **scale**: For `DECIMAL` data types, specifies the total number of digits in the fractional part of the value. Defaults to 0. (integer)
  - **database**: The database name to which the query results belong. (string)
  - **table**: The table name for the query results. (string)
  - **type**: The data type of the column. (string)
- **athena.last_preview.rows**: The rows in the preview results. (array of array)

## Configuration for `athena.ctas>` operator

### Options

- **select_query**: The select SQL statements or file location (in local or Amazon S3) to be executed for a new table by [`Create Table As Select`]((https://aws.amazon.com/jp/about-aws/whats-new/2018/10/athena_ctas_support/)). You can use digdag's template engine like `${...}` in the SQL query. (string, required)
- **database**: The database name for query execution context. (string, optional)
- **table**: The table name for the new table (string, default: `digdag_athena_ctas_${session_uuid.replaceAll("-", "")}_${random}`)
- **output**: Output location for data created by CTAS (string, default: `"s3://aws-athena-query-results-${AWS_ACCOUNT_ID}-<AWS_REGION>/Unsaved/${YEAR}/${MONTH}/${DAY}/${athena_query_id}/"`)
- **format**: The data format for the CTAS query results, such as `"orc"`, `"parquet"`, `"avro"`, `"json"`, or `"textfile"`. (string, default: `"parquet"`)
- **compression**: The compression type to use for `"orc"` or `"parquet"`. (string, default: `"snappy"`)
- **field_delimiter**: The field delimiter for files in CSV, TSV, and text files. This option is applied only when **format** is specific to text-based data storage formats. (string, optional)
- **partitioned_by**: An array list of columns by which the CTAS table will be partitioned. Verify that the names of partitioned columns are listed last in the list of columns in the SELECT statement. (array of string, optional)
- **bucketed_by**: An array list of buckets to bucket data. If omitted, Athena does not bucket your data in this query. (array of string, optional)
- **bucket_count**: The number of buckets for bucketing your data. If omitted, Athena does not bucket your data. (integer, optional)
- **additional_properties**: Additional properties for CTAS. These are used for CTAS WITH clause without escaping. (string to string map, optional)
- **table_mode**: Specify the expected behavior of CTAS results. Available values are `"default"`, `"empty"`, `"data_only"`. See the below explanation of the behaviour. (string, default: `"default"`)
  - `"default"`: Do not do any care. This option require the least IAM privileges for digdag, but the behaviour depends on Athena.
  - `"empty_table"`: Create a new empty table with the same schema as the select query results.
  - `"data_only"`: Create a new table with data by CTAS, but drop this after CTAS execution. The table created by CTAS is an external table, so the data is left even if the table is dropped.
- **save_mode**: Specify the expected behavior of CTAS. Available values are `"none"`, `"error_if_exists"`, `"ignore"`, `"overwrite"`. See the below explanation of the behaviour. (string, default: `"overwrite"`)
  - `"none"`: Do not do any care. This option require the least IAM privileges for digdag, but the behaviour depends on Athena.
  - `"error_if_exists"`: Raise error if the distination table or location exists.
  - `"ignore"`: Skip CTAS query if the distination table or location exists.
  - `"overwrite"`: Drop the distination table and remove objects before executing CTAS. This operation is not atomic.
- **token_prefix**: Prefix for `ClientRequestToken` that a unique case-sensitive string used to ensure the request to create the query is idempotent (executes only once). On this plugin, the token is composed like `${token_prefix}-${session_uuid}-${hash value of query}-${radom string}`. (string, default: `"digdag-athena-ctas"`)
- **timeout**: Specify timeout period. (`DurationParam`, default: `"10m"`)

### Output Parameters

Nothing

# Development

## Run an Example

### 1) build

```sh
./gradlew publish
```

Artifacts are build on local repos: `./build/repo`.

### 2) get your aws profile

```sh
aws configure
```

### 3) run an example

```sh
./example/run.sh
```

## (TODO) Run Tests

```sh
./gradlew test
```

# ChangeLog

[CHANGELOG.md](./CHANGELOG.md)

# License

[Apache License 2.0](./LICENSE.txt)

# Author

@civitaspo

# Contributor

- @s-wool
- @daichi-hirose
