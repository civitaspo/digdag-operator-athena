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
