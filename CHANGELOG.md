0.3.0 (2019-07-30)
==================

* [Breaking change -- `athena.ctas>`] Remove output option which has been deprecated since v0.2.2.
* [Breaking change -- `athena.ctas>`] Remove select_query option which has been deprecated since v0.2.0.
* [Note -- `athena.ctas>`] Replace com.google.common.base.Optional -> scala.Option.
* [Enhancement -- `athena.ctas>`] Add `/` as suffix if location option does not have.
* [Change  -- `athena.ctas>`] Use `athena.drop_table>` operator instead of `athena.query>` operator when deleting the table.
    * This change is to reduce the number of query executions.
    * `athena.ctas>` operator depends on Glue privileges by this change.
* [Note -- `athena.ctas>`] Use `default` database if not database option specified.

0.2.5 (2019-07-29)
==================

* [Fix] Catch any initialization exception and re-throw as `ConfigException`

0.2.4 (2019-07-29)
==================

* [New Feature] Add `athena.table_exists?>` operator
* [New Feature] Add `athena.partition_exists?>` operator

0.2.3 (2019-07-19)
==================
* [New Feature] Add `athena.drop_table_multi>` operator
* [Enhancement] Expose the real error message and state when the query execution is failed.

0.2.2 (2019-07-19)
==================

* [Enhancement] Use scala-logging for logging instead of using slf4j directly
* [Enhancement] Use workgroup default output location for athena query result output location.
* [Change - `athena.ctas>`] Introduce `location` option and `output` option become deprecated.


0.2.1 (2019-07-16)
==================

* [Enhancement] Add `format`, `compression` and `field_delimiter` options to `athena.apas>` operator. If not set, these are detected automatically.

0.2.0 (2019-07-16)
==================

* [New Feature] Add `athena.apas>` operator.
* [Enhancement] Use scala 2.13.0
* [New Feature] `athena.add_partition>` operator
* [New Feature] `athena.drop_partition>` operator
* [New Feature] `athena.drop_table>` operator
* [Enhancement] Suppress aws-java-sdk log
* [Note] Use aws-java-sdk-glue for catalog only operations.
* [Breaking Change - `athena.query>`] `preview` option is `false` by default.
* [Enhancement - `athena.ctas>`] Remove `;` from the query.
* [Enhancement] Create wrappers for aws-java-sdk for the readability and the separation of responsibilities.
    * The change of STS has a possibility to break the backward compatibility of `assume role` behavior.
* [Enhancement] Introduce region variable for `Aws` to resolve region according to `auth_method` option.
* [New Feature] Add `workgroup` option.
* [Breaking Change - `athena.query`] Remove the `output` option as the deprecation is notified from before.
* [Deprecated - `athena.ctas>`] Make `select_query` deprecated.
* [Note] Introduce `pro.civitaspo.digdag.plugin.athena.aws` package to divide dependencies about aws.
* [Note] Use the Intellij formatter instead of spotless, so remove spotless from CI.


0.1.5 (2018-12-11)
==================

* [Enhancement] Expose more logs when loading queries.
* [Enhancement] * Use an unique table name as default in `athena.ctas>`

0.1.4 (2018-12-07)
==================

* [New Feature] Add supporting to use Amazon S3 file for query execution

0.1.3 (2018-12-07)
==================

* [Fix] All queries are executed as a new query, even if the same query is executed in the same digdag session.

0.1.2 (2018-12-07)
==================

* [Fix] `athena.ctas>` Use normalized table name by default.

0.1.1 (2018-12-07)
==================

* [Fix] `athena.ctas>` Skip removing objects if removable targets do not exist.

0.1.0 (2018-10-20)
==================

* [New Feature] Add `athena.ctas>` operator
* [Breaking Change] Remove **keep_metadata** and **save_mode** option from athena.query operator.
* [Change] Change **output** option in `athena.query` to not required
* [Deprecated] Change **output** option in `athena.query` to deprecated

0.0.6 (2018-10-14)
==================

* [Deprecated] Add warning messages for **keep_metadata** and **save_mode** for `athena.query` operator.

0.0.5 (2018-09-23)
==================

* [New Feature] Add `athena.preview` operator
* [New Feature] Add `preview` option for `athena.query` operator. This option is true, then run `athena.preview` operator after `athena.query` is executed.
* [Enhancement] Add `athena.remove_metadata` operator for internal use. `athena.query` execute this when `keep_metadata` is false after `athena.preview` operator because `athena.preview` requires matadata.

0.0.4 (2018-08-13)
==================

* [Fix] Make `output` not uri encoded

0.0.3 (2018-08-13)
==================

* [New Feature / Destructive Change] Add `keep_metadata` option to indicate whether metadata file is expected to be kept on S3. This opiton is `false` by default, so the behaviour is changed when you use the same configuration as the prior version than this version. Please be careful.
* [New Feature / Destructive Change] Add `save_mode` option to specify the expected behavior of saving the query results. This option is `"overwrite"` by default, so the behaviour is changed when you use the same configuration as the prior version than this version. Please be careful.

0.0.2 (2018-08-09)
==================

* [Enhancement] Fail correctly if not retryable

0.0.1 (2018-08-09)
==================

* First Release
