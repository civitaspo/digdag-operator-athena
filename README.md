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
  athena.query>:


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

- **athena.access_key_id**: The AWS Access Key ID to use when submitting EMR jobs. (optional)
- **athena.secret_access_key**: The AWS Secret Access Key to use when submitting EMR jobs. (optional)
- **athena.session_token**: The AWS session token to use when submitting EMR jobs. This is used only **auth_method** is `"session"` (optional)
- **athena.role_arn**: The AWS Role to assume when submitting EMR jobs. (optional)
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
- **region**: The AWS region to use for EMR service. (string, optional)
- **endpoint**: The AWS EMR endpoint address to use. (string, optional)


You can optionally create Eclipse/Idea project files as follows:
```sh
gradle eclipse
gradle idea
```

*Note:* _It's better to change the dependencies from `provided` to `compile` in [build.gradle](https://github.com/myui/digdag-plugin-example/blob/master/build.gradle) for creating idea/eclipse project config._

# Plugin Loading

Digdag loads pluigins from Maven repositories by configuring [plugin options](https://github.com/myui/digdag-plugin-example/blob/master/sample/plugin.dig).

You can use a local Maven repository (local FS, Amazon S3) or any public Maven repository ([Maven Central](http://search.maven.org/), [Sonatype](https://www.sonatype.com/), [Bintary](https://bintray.com/), [Jitpack](https://jitpack.io/)) for the plugin artifact repository.

# Publishing your plugin using Github and Jitpack

[Jitpack](https://jitpack.io/) is useful for publishing your github repository as a maven repository.

```sh
git tag v0.1.3
git push origin v0.1.3
```

https://jitpack.io/#myui/digdag-plugin-example/v0.1.3

Now, you can load the artifact from a github repository in [a dig file](https://github.com/myui/digdag-plugin-example/blob/master/sample/plugin.dig) as follows:

```
_export:
  plugin:
    repositories:
      # - file://${repos}
      - https://jitpack.io
    dependencies:
      # - io.digdag.plugin:digdag-plugin-example:0.1.3
      - com.github.myui:digdag-plugin-example:v0.1.3
```

# Further reading

- [Operators](http://docs.digdag.io/operators.html) and [their implementations](https://github.com/treasure-data/digdag/tree/master/digdag-standards/src/main/java/io/digdag/standards/operator)
