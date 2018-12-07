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
