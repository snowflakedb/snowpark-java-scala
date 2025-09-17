#!/bin/bash -ex
#
# Push Snowpark Java/Scala FIPS build to the public maven repository.
# This script needs to be executed by snowflake jenkins job.
#

if [ -z "$GPG_KEY_ID" ]; then
  export GPG_KEY_ID="Snowflake Computing"
  echo "[WARN] GPG key ID not specified, using default: $GPG_KEY_ID."
fi

if [ -z "$GPG_KEY_PASSPHRASE" ]; then
  echo "[ERROR] GPG passphrase is not specified for $GPG_KEY_ID!"
  exit 1
fi

if [ -z "$GPG_PRIVATE_KEY" ]; then
  echo "[ERROR] GPG private key file is not specified!"
  exit 1
fi

if [ -z "$sonatype_user" ]; then
  echo "[ERROR] Jenkins sonatype user is not specified!"
  exit 1
fi

if [ -z "$sonatype_password" ]; then
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

mkdir -p ~/.ivy2

STR=$'host=central.sonatype.com
user='$sonatype_user'
password='$sonatype_password''

echo "$STR" > ~/.ivy2/.credentials

# import private key first
echo "[INFO] Importing PGP key."
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

echo "[INFO] Checking out snowpark-java-scala @ tag: $github_version_tag."
git checkout $github_version_tag

export SNOWPARK_FIPS="true"

if [ "$PUBLISH" = true ]; then
  if [ "$SNOWPARK_FIPS" = true ]; then
    echo "[INFO] Packaging snowpark-fips @ tag: $github_version_tag."
  else
    echo "[INFO] Packaging snowpark @ tag: $github_version_tag."
  fi
  sbt +publishSigned
  echo "[INFO] Staged packaged artifacts locally with PGP signing."
  sbt sonaUpload
  echo "[SUCCESS] Uploaded artifacts to central portal."
  echo "[ACTION-REQUIRED] Please log in to Central Portal to publish artifacts: https://central.sonatype.com/"
  # TODO: alternatively automate publishing fully
#  sbt sonaRelease
#  echo "[SUCCESS] Released Snowpark Java-Scala v$github_version_tag to Maven."
else
  #release to s3
  echo "[INFO] Staging signed artifacts to local ivy2 repository."
  rm -rf ~/.ivy2/local/
  sbt +publishLocalSigned

  # SBT will build FIPS version of Snowpark automatically if the environment variable exists.
  if [ -v SNOWPARK_FIPS ]; then
    S3_JENKINS_URL="s3://sfc-eng-jenkins/repository/snowparkclient-fips"
    S3_DATA_URL="s3://sfc-eng-data/client/snowparkclient-fips/releases"
    echo "[INFO] Uploading snowpark-fips artifacts to:"
  else
    S3_JENKINS_URL="s3://sfc-eng-jenkins/repository/snowparkclient"
    S3_DATA_URL="s3://sfc-eng-data/client/snowparkclient/releases"
    echo "[INFO] Uploading snowpark artifacts to:"
  fi
  echo "[INFO]   - $S3_JENKINS_URL/$github_version_tag/"
  echo "[INFO]   - $S3_DATA_URL/$github_version_tag/"

  # Rename all produced artifacts to include version number (sbt doesn't by default when publishing to local ivy2 repository).
  find ~/.ivy2/local -type f -name "*snowpark*" | while read file; do newfile=$(echo "$file" | sed "s/\(2\.1[23]\)\([-\.]\)/\1-${github_version_tag#v}\2/"); mv "$file" "$newfile"; done

  # Copy all files, flattening the nested structure of the ivy2 repository into the expected structure on s3.
  find ~/.ivy2/local -type f -name "*snowpark*" -exec aws s3 cp \{\} $S3_JENKINS_URL/$github_version_tag/ \;
  find ~/.ivy2/local -type f -name "*snowpark*" -exec aws s3 cp \{\} $S3_DATA_URL/$github_version_tag/ \;

  echo "[SUCCESS] Published Snowpark Java-Scala v$github_version_tag artifacts to S3."
fi
