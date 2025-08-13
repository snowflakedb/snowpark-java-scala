#!/bin/bash -ex
#
# Push Snowpark Java/Scala to the public maven repository
# This script needs to be executed by snowflake jenkins job
# If the FIPS
#

if [ -z "$GPG_KEY_ID" ]; then
  echo "[ERROR] Key Id not specified!"
  exit 1
fi

if [ -z "$GPG_KEY_PASSPHRASE" ]; then
  echo "[ERROR] GPG passphrase is not specified for $GPG_KEY_ID!"
  exit 1
fi

if [ -z "$GPG_PRIVATE_KEY" ]; then
  echo "[ERROR] GPG private key file is not specified!"
  exit 1
fi

if [ -z "$SONATYPE_USER" ]; then
  echo "[ERROR] Jenkins sonatype user is not specified!"
  exit 1
fi

if [ -z "$SONATYPE_PASSWORD" ]; then
  echo "[ERROR] Jenkins sonatype pwd is not specified!"
  exit 1
fi

if [ -z "$PUBLISH" ]; then
  echo "[ERROR] 'PUBLISH' is not specified!"
  exit 1
fi

if [ -z "$github_version_tag" ]; then
  echo "[ERROR] 'github_version_tag' is not specified!"
  exit 1
fi

# SBT will build FIPS version of Snowpark automatically if the environment variable exists.
if [ -v SNOWPARK_FIPS ]: then
  echo "[INFO] Publishing Snowpark FIPS Build."
else
  echo "[INFO] Publishing Snowpark non-FIPS Build."
fi

mkdir -p ~/.ivy2

STR=$'realm=Sonatype Nexus Repository Manager
host=oss.sonatype.org
user='$SONATYPE_USER$'
password='$SONATYPE_PASSWORD$''

echo "$STR" > ~/.ivy2/.credentials

# import private key first
if [ ! -z "$GPG_PRIVATE_KEY" ] && [ -f "$GPG_PRIVATE_KEY" ]; then
  # First check if already imported private key
  if ! gpg --list-secret-key | grep "$GPG_KEY_ID"; then
    gpg --allow-secret-key-import --import "$GPG_PRIVATE_KEY"
  fi
fi

which sbt
if [ $? -ne 0 ]
then
   pushd ..
   echo "[INFO] sbt is not installed, downloading latest sbt for test and build."
   curl -L -o sbt-1.11.4.zip https://github.com/sbt/sbt/releases/download/v1.11.4/sbt-1.11.4.zip
   unzip sbt-1.11.4.zip
   PATH=$PWD/sbt/bin:$PATH
   popd
else
   echo "[INFO] Using system installed sbt."
fi
which sbt
sbt version


if [ "$PUBLISH" = true ]; then
  echo "[INFO] Publishing snowpark-java-scala @ $github_version_tag."
  sbt +publishSigned

else
  #release to s3
  echo "[INFO] Releasing to S3."
  rm -rf ~/.ivy2/local/
  sbt +publishLocalSigned

  aws s3 cp ~/.ivy2/local s3://sfc-eng-jenkins/repository/snowparkclient/$github_version_tag/ --recursive
  aws s3 cp ~/.ivy2/local s3://sfc-eng-data/client/snowparkclient/releases/$github_version_tag/ --recursive
fi
