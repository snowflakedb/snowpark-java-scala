/**
 * Provides interfaces for creating Java User-Defined Aggregate Functions (UDAFs) in Snowpark.
 *
 * <p>This package contains the base {@link com.snowflake.snowpark_java.udaf.JavaUDAF} interface and
 * specialized interfaces ({@link com.snowflake.snowpark_java.udaf.JavaUDAF0}, {@link
 * com.snowflake.snowpark_java.udaf.JavaUDAF1}, etc.) for UDAFs with different numbers of arguments.
 *
 * <h2>UDAF vs UDF vs UDTF</h2>
 *
 * <table border="1">
 *   <caption>Function Type Comparison</caption>
 *   <tr>
 *     <th>Type</th>
 *     <th>Input</th>
 *     <th>Output</th>
 *     <th>State</th>
 *     <th>Example</th>
 *   </tr>
 *   <tr>
 *     <td>UDF</td>
 *     <td>Single row</td>
 *     <td>Single value</td>
 *     <td>Stateless</td>
 *     <td>UPPER(name)</td>
 *   </tr>
 *   <tr>
 *     <td>UDTF</td>
 *     <td>Single row</td>
 *     <td>Multiple rows</td>
 *     <td>Optional</td>
 *     <td>SPLIT_TO_TABLE(text, ',')</td>
 *   </tr>
 *   <tr>
 *     <td>UDAF</td>
 *     <td>Multiple rows</td>
 *     <td>Single value</td>
 *     <td>Required</td>
 *     <td>SUM(amount), AVG(price)</td>
 *   </tr>
 * </table>
 *
 * @see com.snowflake.snowpark_java.UDAFRegistration
 * @since 1.16.0
 */
package com.snowflake.snowpark_java.udaf;
