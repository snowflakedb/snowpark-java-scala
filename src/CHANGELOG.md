Adding the following methods:

| API                                   | Description                                                                                     |
| ------------------------------------- | ----------------------------------------------------------------------------------------------- |
| DataFrame.selectExpr                  | Selects a set of SQL expressions. This is a variant of `select` that accepts SQL expressions. |
| DataFrame.filter                      | Filters rows using the given SQL expression.                                                    |
| DataFrame.dropDuplicates              | Returns a new DataFrame with duplicate rows removed, considering only the subset of columns.    |
| DataFrame.transform                   | Chaining custom transformations.                                                                |
| DataFrame.head                        | Returns the first row / Returns the first `n` rows.                                          |
| DataFrame.take                        | Returns the first `n` rows                                                                    |
| DataFrame.cache                       | Alias for cacheResult                                                                           |
| DataFrame.orderBy                     | Alias for Sort                                                                                  |
| DataFrame.orderBy                     | Alias for Sort                                                                                  |
| DataFrame.printSchema                 | Shortcut for schema.printTreeString                                                             |
| DataFrame.toJSON                      | Returns data as JSON                                                                            |
| DataFrame.collectAsList               | Collects results as a list of Row                                                               |
| DataFrame.withColumnRenamed           | Returns a new dataframe with the renamed columns                                                |
| Session.getOrCreate                   | Gets the active session or creates new one                                                      |
| Column.isin                           | Overload that accepts an array of strings                                                       |
| Column.isNotNull                      | alias for is_not_null                                                                           |
| Column.isNull                         | alias for is_null                                                                               |
| Column.startsWith                     |                                                                                                 |
| Column.contains                       |                                                                                                 |
| Column.regexp_replace                 |                                                                                                 |
| Column.as                             | overload for Symbol                                                                             |
| Column.isNaN                          |                                                                                                 |
| Column.substr                         | overload for column arguments                                                                   |
| Column.substr                         | overload for int arguments                                                                      |
| Column.notEqual                       | overload for not_equal                                                                          |
| Column.like                           | overload for string argument                                                                    |
| Column.rlike                          | alias for regexp                                                                                |
| Column.bitwiseAND                     | alias for bitand                                                                                |
| Column.bitwiseOR                      | alias for bitor                                                                                |
| Column.bitwiseXOR                     | alias for bitxor                                                                               |
| Column.getItem                        | calls builtin get                                                                               |
| Column.getField                       | class builtin get                                                                               |
| Column.cast                           | with string expression                                                                          |
| Column.eqNullSafe                     | alias for equal_null                                                                            |
| CaseExpr.when                         | support for when with<br />* int<br />* String<br />* Float<br />* double<br />* booelan        |
| CaseExpr.otherwise                    | support for otherwise with:<br />* int<br />* String<br />* float<br />* double<br />* boolean  |
| CaseExpr.else                         | support for otherwise with:<br />* int<br />* String<br />* float<br />* double<br />* boolean  |
| functions.expr                        | alias for sqlExpr                                                                               |
| functions.desc                        | equivalent to Column.desc                                                                       |
| functions.asc                         | equivalent to Columns.asc                                                                       |
| functions.size                        | equivalent to array_size                                                                        |
| functions.arrray                      | alias for array_construct                                                                       |
| functions.date_format                 | alias for to_varchar                                                                            |
| functions.last                        | LAST_VALUE                                                                                      |
| functions.format_string               | FORMAT_STRING                                                                                   |
| functions.locate                      | POSITION                                                                                        |
| function.log10                        | call builtin LOG                                                                                |
| functions.log1p                       | ln c + 1                                                                                        |
| functions.nanvl                       | check if NaN                                                                                    |
| functions.base64                      | base64_ENCODE                                                                                   |
| functions.unbase64                    | BASE64_DECODE_STRING                                                                            |
| functions.ntile                       | NTILE                                                                                           |
| functions.shiftleft                   | alias for bitshiftleft                                                                         |
| functions.shiftright                  | alias for bitshiftright                                                                        |
| functions.hex                         | HEX_ENCODE                                                                                      |
| functions.unhex                       | HEX_DECODE_STRING                                                                               |
| functions.randn                       | RANDOM                                                                                          |
| functions.json_tuple                  | JSON_EXTRACT_PATH_TEXT                                                                          |
| functions.cbrt                        | CBRT                                                                                            |
| functions.from_json                   | TRY_PARSE_JSO                                                                                   |
| functions.date_sub                    | implements equivalent to spark                                                                  |
| functions.regexp_extract              | implements equivalent to regexp_extract                                                         |
| functions.signum                      | SIGN                                                                                            |
| functions.substring_index             | implements equivalent to spark                                                                  |
| functions.collect_list                | alias for array_agg                                                                             |
| functions.reverse                     | REVERSE                                                                                         |
| functions.isnull                      | alias for is_null                                                                              |
| functions.conv                        | CONV                                                                                            |
| functions.unix_timestamp              | datetime to epoch                                                                               |
| functions.regexp_replace              | REGEXP_REPLACE                                                                                  |
| functions.date_add                    | adds days                                                                                       |
| functions.collect_set                 | ARRAY_AGG(DISTINCT)                                                                             |
| functions.from_unixtime               | epoch to datetime                                                                               |
| functions.monotonically_increasing_id | alias for seq8                                                                                  |
| functions.months_between              | MONTHS_BETWEEN                                                                                  |
| functions.instr                       | REGEXP_INSTR                                                                                    |
| functions.from_utc_timestamp          | TO_TIMESTAMP_T                                                                                  |
| functions.format_number               | TO_VARCHAR                                                                                      |
| functions.log2                        | LOG                                                                                             |
| functions.element_at                  | alias for get_path                                                                              |
