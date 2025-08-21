![Build Status](https://github.com/snowflakedb/snowpark-java-scala/workflows/precommit%20test/badge.svg)

# Snowflake Snowpark Java & Scala API

The Snowpark library provides intuitive APIs for querying and processing data in a data pipeline. Using this library, you can build applications that process data in Snowflake without having to move data to the system where your application code runs.

| Scala | Java |
|-----------|-----------|
| [Scala Developer Guide] | [Java Developer Guide] |
| [Scala Tutorial]| |
| [scaladoc (latest)]  | [javadoc (latest)] |

## Getting Started

### Have your Snowflake account ready

If you don't have a Snowflake account yet, you can [sign up for a 30-day free trial account][sign up trial].

### Prepare your local environment

Java 11 or Scala 2.12 is required. You can download Java 11 from [the Eclipse foundation](https://adoptium.net/temurin/releases/?version=11) (formerly AdoptOpenJDK), and you can download Scala 2.11 from [the Scala website](https://www.scala-lang.org/download/2.12.17.html).

Note that Java 17, Scala 2.13 and 3.x are currently not supported.

### Install the library

Add the Snowpark SDK to your build file:

- Maven

    ```xml
    <dependency>
        <groupId>com.snowflake</groupId>
        <artifactId>snowpark</artifactId>
        <version>1.16.0</version>
    </dependency>
    ```

- Gradle

    ```groovy
    implementation 'com.snowflake:snowpark:1.16.0'
    ```

- sbt

    ```scala
    libraryDependencies += "com.snowflake" % "snowpark" % "1.16.0"
    ```

### Create a session and start querying

For Java: 

```java
import com.snowflake.snowpark_java.*;
import java.util.HashMap;
import java.util.Map;

public class App 
{
    public static void main( String[] args )
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("URL", "https://<account_identifier>.snowflakecomputing.com:443");
        properties.put("USER", "<user name>");
        properties.put("ROLE", "<role name>");
        properties.put("WAREHOUSE", "<warehouse name>");
        properties.put("DB", "<database name>");
        properties.put("SCHEMA", "<schema name>");
        Session session = Session.builder().configs(properties).create();

        session.sql("show tables").show();
    }
}
```

For Scala:

```scala
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._

object Main {
  def main(args: Array[String]): Unit = {
    // Replace the <placeholders> below.
    val configs = Map (
      "URL" -> "https://<account_identifier>.snowflakecomputing.com:443",
      "USER" -> "<user name>",
      "PASSWORD" -> "<password>",
      "ROLE" -> "<role name>",
      "WAREHOUSE" -> "<warehouse name>",
      "DB" -> "<database name>",
      "SCHEMA" -> "<schema name>"
    )
    val session = Session.builder.configs(configs).create
    session.sql("show tables").show()
  }
}
```

## Contributing

To get involved, please see the [contributing guide](CONTRIBUTING.md) and check the [open issues](https://github.com/snowflakedb/snowpark-java-scala/issues).

## Test Report

[Test Coverage](https://sonarqube.int.snowflakecomputing.com/dashboard?id=snowpark)

[java developer guide]: https://docs.snowflake.com/en/developer-guide/snowpark/java/index.html
[javadoc (latest)]: https://docs.snowflake.com/en/developer-guide/snowpark/reference/java/index.html


[scala developer guide]: https://docs.snowflake.com/en/developer-guide/snowpark/scala/index.html
[scaladoc (latest)]: https://docs.snowflake.com/en/developer-guide/snowpark/reference/scala/com/snowflake/snowpark/index.html
[scala tutorial]: https://quickstarts.snowflake.com/guide/getting_started_with_snowpark_scala/index.html

[snowpark]: https://www.snowflake.com/snowpark
[sign up trial]: https://signup.snowflake.com
[source code]: https://github.com/snowflakedb/snowpark-java-scala
[contributing]: https://github.com/snowflakedb/snowpark-java-scala/blob/main/CONTRIBUTING.md
