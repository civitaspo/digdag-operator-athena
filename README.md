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
      - pro.civitaspo:digdag-operator-athena:0.0.1
  athena:
    auth_method: profile

+step1:
  athena.query>: template.sql
  output: s3://mybucket/prefix/

+step2:
  echo>: ${athena.last_query}

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

- **athena.access_key_id**: The AWS Access Key ID to use when submitting Athena Query. (optional)
- **athena.secret_access_key**: The AWS Secret Access Key to use when submitting Athena Query. (optional)
- **athena.session_token**: The AWS session token to use when submitting Athena Query. This is used only **auth_method** is `"session"` (optional)
- **athena.role_arn**: The AWS Role to assume when submitting Athena Query. (optional)
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

- **athena.query>**: The SQL query statements or file to be executed. You can use digdag's template engine like `${...}` in the SQL query. (string, required)
- **token_prefix**: Prefix for `ClientRequestToken` that a unique case-sensitive string used to ensure the request to create the query is idempotent (executes only once). On this plugin, the token is composed like `${token_prefix}-${session_uuid}-${hash value of query}`. (string, default: `"digdag-athena"`)
- **database**: The name of the database. (string, optional)
- **output**: The location in Amazon S3 where your query results are stored, such as s3://path/to/query/bucket/. For more information, see [Queries and Query Result Files](https://docs.aws.amazon.com/athena/latest/ug/querying.html). (string, required)
- **timeout**: Specify timeout period. (`DurationParam`, default: `"10m"`)

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
