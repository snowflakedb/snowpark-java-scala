#!/bin/bash -ex

# Since Java Doc Plugin doesn't work with Scala-Java mixed project,
# we generate java doc in following steps:
# 1, Compile and release Snowpark jar to local .m2 repo.
# 2, Copy Java codes to a sub-directory, and import Scala codes from the jar released before.
# 3, Generate Java doc for Java codes.

export JAVA_DIR="javaDoc"

# release scala code to local .m2 repo
mvn -Dgpg.skip -DskipTests clean install

# clean sub dir
rm -rf $JAVA_DIR

# copy java codes
mkdir -p $JAVA_DIR/src/main
cp -r src/main/java $JAVA_DIR/src/main

# remove one internal class that should be exclude from doc.
# javadoc plugin can only exclude a whole package but not single class.
rm $JAVA_DIR/src/main/java/com/snowflake/snowpark_java/types/InternalUtils.java
rm $JAVA_DIR/src/main/java/com/snowflake/snowpark_java/types/UsernamePassword.java
rm $JAVA_DIR/src/main/java/com/snowflake/snowpark_java/types/SnowflakeSecrets.java

# copy pom
cp java_doc.xml $JAVA_DIR/pom.xml

# generate doc
cd javaDoc
mvn javadoc:javadoc
