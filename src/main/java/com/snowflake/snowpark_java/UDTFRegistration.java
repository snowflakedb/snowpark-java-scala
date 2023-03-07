package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import com.snowflake.snowpark_java.udtf.*;

/**
 * Provides methods to register a UDTF (user-defined table function) in the Snowflake database.
 * {@code Session.udtf()} returns an object of this class.
 *
 * <p>To register an UDTF, you must:
 *
 * <ol>
 *   <li>Define a UDTF class.
 *   <li>Create an instance of that class, and register that instance as a UDTF.
 * </ol>
 *
 * <p>The next sections describe these steps in more detail.
 *
 * <h2>Defining the UDTF Class</h2>
 *
 * Define a class that implements one of the JavaUDTF[N] interfaces (e.g. {@link
 * com.snowflake.snowpark_java.udtf.JavaUDTF0}, {@link com.snowflake.snowpark_java.udtf.JavaUDTF1},
 * etc.), where "n" specifies the number of input arguments for your UDTF. For example, if your UDTF
 * passes in 3 input arguments, implements the {@link com.snowflake.snowpark_java.udtf.JavaUDTF3}
 * interface.
 *
 * <p>In your class, implement the following four methods:
 *
 * <ul>
 *   <li>process() , which is called once for each row in the input partition.
 *   <li>endPartition(), which is called once for each partition after all rows have been passed to
 *       process().
 *   <li>outputSchema(), which returns a {@link com.snowflake.snowpark_java.types.StructType} object
 *       that describes the schema for the returned rows.
 *   <li>inputSchema(), which returns a {@link com.snowflake.snowpark_java.types.StructType} object
 *       that describes the types of the input parameters. If your {@code process()} method passes
 *       in {@code Map} arguments, you must implement the {@code inputSchema()} method. If the
 *       method does not pass in {@code Map} arguments, implementing {@code inputSchema()} is
 *       optional.
 * </ul>
 *
 * <p>When a UDTF is called, the rows are grouped into partitions before they are passed to the
 * UDTF:
 *
 * <ul>
 *   <li>If the statement that calls the UDTF specifies the PARTITION clause (explicit partitions),
 *       that clause determines how the rows are partitioned.
 *   <li>If the statement does not specify the PARTITION clause (implicit partitions), Snowflake
 *       determines how best to partition the rows.
 * </ul>
 *
 * <p>For an explanation of partitions, see <a
 * href="https://docs.snowflake.com/en/developer-guide/udf/java/udf-java-tabular-functions.html#label-udf-java-partitions">Table
 * Functions and Partitions</a>
 *
 * <h3>Defining the process() Method</h3>
 *
 * This method is invoked once for each row in the input partition.
 *
 * <p>The arguments passed to the registered UDTF are passed to process(). For each argument passed
 * to the UDTF, you must have a corresponding argument in the signature of the process() method.
 * Make sure that the type of the argument in the process() method matches the Snowflake data type
 * of the corresponding argument in the UDTF.
 *
 * <p>Snowflake supports the following data types for the parameters for a UDTF:
 *
 * <table border="1">
 *   <caption>Supported Snowflake Data Types and Corresponding Java Types</caption>
 *   <tr> <th>SQL Type</th> <th>Java Type</th> <th>Notes</th> </tr>
 *   <tr> <td>NUMBER</td> <td>java.lang.Short</td> <td>Supported</td> </tr>
 *   <tr> <td>NUMBER</td> <td>java.lang.Integer</td> <td>Supported</td> </tr>
 *   <tr> <td>NUMBER</td> <td> java.lang.Long</td> <td>Supported</td> </tr>
 *   <tr> <td>FLOAT</td> <td>java.lang.Float</td> <td>Supported</td> </tr>
 *   <tr> <td>DOUBLE</td> <td>java.lang.Double</td> <td>Supported</td> </tr>
 *   <tr> <td>NUMBER</td> <td>java.math.BigDecimal</td> <td>Supported</td> </tr>
 *   <tr> <td>VARCHAR</td> <td>String</td> <td>Supported</td> </tr>
 *   <tr> <td>BOOL</td> <td>java.lang.Boolean</td> <td>Supported</td> </tr>
 *   <tr> <td>DATE</td> <td>java.sql.Date</td> <td>Supported</td> </tr>
 *   <tr> <td>TIMESTAMP</td> <td>java.sql.Timestamp</td> <td>Supported</td> </tr>
 *   <tr> <td>BINARY</td> <td>byte[]</td> <td>Supported</td> </tr>
 *   <tr> <td>ARRAY</td> <td>String[] or Variant[]</td> <td>Supported String[] or Variant[]</td> </tr>
 *   <tr> <td>OBJECT</td> <td>Map&lt;String, String&gt; or Map&lt;String, Variant&gt;</td> <td>Supported Map&lt;String, String&gt; or Map&lt;String, Variant&gt; </td> </tr>
 *   <tr> <td>VARIANT</td> <td>{@link com.snowflake.snowpark_java.types.Variant}</td> <td>Supported</td> </tr>
 * </table>
 *
 * <h3>Defining the endPartition() Method</h3>
 *
 * This method is invoked once for each partition, after all rows in that partition have been passed
 * to the process() method.
 *
 * <p>You can use this method to generate output rows, based on any state information that you
 * aggregate in the process() method.
 *
 * <h3>Defining the outputSchema() Method</h3>
 *
 * In this method, define the output schema for the rows returned by the process() and
 * endPartition() methods.
 *
 * <p>Construct and return a {@link com.snowflake.snowpark_java.types.StructType} object that uses
 * an Array of {@link com.snowflake.snowpark_java.types.StructField} objects to specify the
 * Snowflake data type of each field in a returned row.
 *
 * <p>Snowflake supports the following {@link com.snowflake.snowpark_java.types.DataTypes} for the
 * input types:
 *
 * <table border="1">
 *   <caption>Supported Snowflake Data Types and Corresponding Java Types</caption>
 *   <tr> <th>Snowpark {@link com.snowflake.snowpark_java.types.DataTypes}</th> <th>Java Type</th> <th>Notes</th> </tr>
 *   <tr> <td>ShortType</td> <td>java.lang.Short</td> <td>Supported</td> </tr>
 *   <tr> <td>IntegerType</td> <td>java.lang.Integer</td> <td>Supported</td> </tr>
 *   <tr> <td>LongType</td> <td> java.lang.Long</td> <td>Supported</td> </tr>
 *   <tr> <td>FloatType</td> <td>java.lang.Float</td> <td>Supported</td> </tr>
 *   <tr> <td>DoubleType</td> <td>java.lang.Double</td> <td>Supported</td> </tr>
 *   <tr> <td>DecimalType</td> <td>java.math.BigDecimal</td> <td>Supported</td> </tr>
 *   <tr> <td>StringType</td> <td>String</td> <td>Supported</td> </tr>
 *   <tr> <td>BooleanType</td> <td>java.lang.Boolean</td> <td>Supported</td> </tr>
 *   <tr> <td>DateType</td> <td>java.sql.Date</td> <td>Supported</td> </tr>
 *   <tr> <td>TimestampType</td> <td>java.sql.Timestamp</td> <td>Supported</td> </tr>
 *   <tr> <td>BinaryType</td> <td>byte[]</td> <td>Supported</td> </tr>
 *   <tr> <td>ArrayType(StringType) or ArrayType(Variant)</td> <td>String[] or Variant[]</td> <td>Supported String[] or Variant[]</td> </tr>
 *   <tr> <td>MapType(StringTyoe, StringType) or MapType(StringTyoe, VariantType)</td> <td>Map&lt;String, String&gt; or Map&lt;String, Variant&gt;</td> <td>Supported Map&lt;String, String&gt; or Map&lt;String, Variant&gt; </td> </tr>
 *   <tr> <td>VariantType</td> <td>{@link com.snowflake.snowpark_java.types.Variant}</td> <td>Supported</td> </tr>
 * </table>
 *
 * <h3>Defining the inputSchema() Method</h3>
 *
 * In this method, define the input schema for the input arguments for process(). Snowpark can infer
 * the input schema if Map is not used as process().
 *
 * <p>Construct and return a {@link com.snowflake.snowpark_java.types.StructType} object that uses
 * an Array of {@link com.snowflake.snowpark_java.types.StructField} objects to specify the data
 * type of each parameter for process().
 *
 * <p>Snowflake supports the following {@link com.snowflake.snowpark_java.types.DataTypes} for input
 * types
 *
 * <table border="1">
 *   <caption>Supported Java Types and Corresponding Snowflake Data Types</caption>
 *   <tr> <th>Java Type</th> <th>{@link com.snowflake.snowpark_java.types.DataTypes}</th> <th>Notes</th> </tr>
 *   <tr> <td>java.lang.Boolean</td> <td>BooleanType</td> <td>Supported</td> </tr>
 *   <tr> <td>java.lang.Short</td> <td>ShortType</td> <td>Supported</td> </tr>
 *   <tr> <td>java.lang.Integer</td> <td>IntegerType</td> <td>Supported</td> </tr>
 *   <tr> <td>java.lang.Long</td> <td>LongType</td> <td>Supported</td> </tr>
 *   <tr> <td>java.lang.Float</td> <td>FloatType</td> <td>Supported</td> </tr>
 *   <tr> <td>java.lang.Double</td> <td>DoubleType</td> <td>Supported</td> </tr>
 *   <tr> <td>java.math.BigDecimal</td> <td>DecimalType</td> <td>Supported</td> </tr>
 *   <tr> <td>String</td> <td>StringType</td> <td>Supported</td> </tr>
 *   <tr> <td>java.sql.Date</td> <td>DateType</td> <td>Supported</td> </tr>
 *   <tr> <td>java.sql.Timestamp</td> <td>TimestampType</td> <td>Supported</td> </tr>
 *   <tr> <td>byte[]</td> <td>BinaryType</td> <td>Supported</td> </tr>
 *   <tr> <td>String[]</td> <td>ArrayType(StringType)</td> <td>Supported</td> </tr>
 *   <tr> <td>Variant[]</td> <td>ArrayType(Variant)</td> <td>Supported</td> </tr>
 *   <tr> <td>Map&lt;String, String&gt;</td> <td>MapType(StringType, StringType)</td> <td>Supported</td> </tr>
 *   <tr> <td>Map&lt;String, Variant&gt;</td> <td>MapType(StringType, VariantType)</td> <td>Supported</td> </tr>
 *   <tr> <td>com.snowflake.snowpark_java.types.Variant</td> <td>VariantType</td> <td>Supported</td> </tr>
 * </table>
 *
 * <h3>Example of a UDTF Class</h3>
 *
 * The following is an example of a UDTF class that generates a range of rows.
 *
 * <p>The UDTF passes in 2 arguments, so the class implements JavaUDTF2.
 *
 * <p>The arguments {@code start} and {@code count} specify the starting number for the row and the
 * number of rows to generate.
 *
 * <pre>{@code
 * import java.util.stream.Stream;
 * import com.snowflake.snowpark_java.types.*;
 * import com.snowflake.snowpark_java.udtf.*;
 *
 * class MyRangeUdtf implements JavaUDTF2<Integer, Integer> {
 *   public StructType outputSchema() {
 *     return StructType.create(new StructField("C1", DataTypes.IntegerType));
 *   }
 *
 *   // Because the process() method in this example does not pass in Map arguments,
 *   // implementing the inputSchema() method is optional.
 *   public StructType inputSchema() {
 *     return StructType.create(
 *             new StructField("start_value", DataTypes.IntegerType),
 *             new StructField("value_count", DataTypes.IntegerType));
 *   }
 *
 *   public Stream<Row> endPartition() {
 *     return Stream.empty();
 *   }
 *
 *   public Stream<Row> process(Integer start, Integer count) {
 *     Stream.Builder<Row> builder = Stream.builder();
 *     for (int i = start; i < start + count ; i++) {
 *       builder.add(Row.create(i));
 *     }
 *     return builder.build();
 *   }
 * }
 * }</pre>
 *
 * <h2>Registering and Calling the UDTF</h2>
 *
 * Next, create an instance of the new class, and register the class by calling one of the
 * UDTFRegistration methods. You can register a temporary or permanent UDTF by name. If you don't
 * need to call the UDTF by name, you can register an anonymous UDTF.
 *
 * <h3>Registering a Temporary UDTF By Name</h3>
 *
 * To register a temporary UDTF by name, call registerTemporary, passing in a name for the UDTF and
 * an instance of the UDTF class. For example:
 *
 * <pre>{@code
 * // Use the MyRangeUdtf defined in previous example.
 * TableFunction tableFunction = session.udtf().registerTemporary("myUdtf", new MyRangeUdtf());
 * session.tableFunction(tableFunction, Functions.lit(10), Functions.lit(5)).show();
 * }</pre>
 *
 * <h3>Registering a Permanent UDTF By Name</h3>
 *
 * If you need to use the UDTF in subsequent sessions, register a permanent UDTF by calling {@code
 * registerPermanent()}.
 *
 * <p>When registering a permanent UDTF, you must specify a stage where the registration method will
 * upload the JAR files for the UDTF and its dependencies. For example:
 *
 * <pre>{@code
 * // Use the MyRangeUdtf defined in previous example.
 * TableFunction tableFunction = session.udtf().registerPermanent("myUdtf", new MyRangeUdtf(), "@myStage");
 * session.tableFunction(tableFunction, Functions.lit(10), Functions.lit(5)).show();
 * }</pre>
 *
 * <h3>Registering an Anonymous Temporary UDTF</h3>
 *
 * If you do not need to refer to a UDTF by name, use {@code registerTemporary(JavaUDTF udtf)} to
 * create an anonymous UDTF instead. For example:
 *
 * <pre>{@code
 * // Use the MyRangeUdtf defined in previous example.
 * TableFunction tableFunction = session.udtf().registerTemporary(new MyRangeUdtf());
 * session.tableFunction(tableFunction, Functions.lit(10), Functions.lit(5)).show();
 * }</pre>
 *
 * @since 1.4.0
 */
public class UDTFRegistration {
  private final com.snowflake.snowpark.UDTFRegistration udtfRegistration;

  UDTFRegistration(com.snowflake.snowpark.UDTFRegistration udtfRegistration) {
    this.udtfRegistration = udtfRegistration;
  }

  /**
   * Registers an UDTF instance as a temporary anonymous UDTF that is scoped to this session.
   *
   * @param udtf The UDTF instance to be registered
   * @since 1.4.0
   * @return A TableFunction that represents the corresponding FUNCTION created in Snowflake
   */
  public TableFunction registerTemporary(JavaUDTF udtf) {
    return new TableFunction(JavaUtils.registerJavaUDTF(this.udtfRegistration, null, udtf, null));
  }

  /**
   * Registers an UDTF instance as a temporary Snowflake UDTF that you plan to use in the session.
   *
   * @param funcName The name that you want to use to refer to the UDTF.
   * @param udtf The UDTF instance to be registered
   * @since 1.4.0
   * @return A TableFunction that represents the corresponding FUNCTION created in Snowflake
   */
  public TableFunction registerTemporary(String funcName, JavaUDTF udtf) {
    return new TableFunction(
        JavaUtils.registerJavaUDTF(this.udtfRegistration, funcName, udtf, null));
  }

  /**
   * Registers an UDTF instance as a Snowflake UDTF.
   *
   * <p>The function uploads the JAR files that the UDTF depends upon to the specified stage. Each
   * JAR file is uploaded to a subdirectory named after the MD5 checksum for the file.
   *
   * <p>If you register multiple UDTFs and specify the same stage location, any dependent JAR files
   * used by those functions will only be uploaded once. The JAR file for the UDTF code itself will
   * be uploaded to a subdirectory named after the UDTF.
   *
   * @param funcName The name that you want to use to refer to the UDTF.
   * @param udtf The UDTF instance to be registered.
   * @param stageLocation Stage location where the JAR files for the UDTF and its dependencies
   *     should be uploaded
   * @since 1.4.0
   * @return A TableFunction that represents the corresponding FUNCTION created in Snowflake
   */
  public TableFunction registerPermanent(String funcName, JavaUDTF udtf, String stageLocation) {
    return new TableFunction(
        JavaUtils.registerJavaUDTF(this.udtfRegistration, funcName, udtf, stageLocation));
  }
}
