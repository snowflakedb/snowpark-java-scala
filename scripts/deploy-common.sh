#!/bin/bash -ex
#
# DO NOT RUN DIRECTLY.
# Script must be sourced by deploy.sh or deploy-fips.sh
# after setting or unsetting `SNOWPARK_FIPS` environment variable.
#

# disable xtrace so credentials are not echoed to logs
set +x

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

# Restrict the release ref to an immutable vMAJOR.MINOR.PATCH tag. This rejects
# branches, raw commit hashes, HEAD, and option-like values, and (together with
# quoting below) neutralizes word-splitting/globbing on $github_version_tag.
if ! [[ "$github_version_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "[ERROR] 'github_version_tag' must match vMAJOR.MINOR.PATCH (got: $github_version_tag)"
  exit 1
fi

# Fetch, verify, and check out the release tag BEFORE any release secrets are
# loaded, so a malicious/poisoned build.sbt can never be evaluated while the
# Sonatype credentials and GPG signing key are present in the environment.
echo "[INFO] Fetching and verifying tag: $github_version_tag."
git fetch --tags --force origin "refs/tags/${github_version_tag}:refs/tags/${github_version_tag}"
if ! git rev-parse --verify --quiet "refs/tags/${github_version_tag}^{commit}" >/dev/null; then
  echo "[ERROR] tag refs/tags/${github_version_tag} not found"
  exit 1
fi
echo "[INFO] Checking out snowpark-java-scala @ tag: $github_version_tag."
git -c advice.detachedHead=false checkout --detach "refs/tags/${github_version_tag}"

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

# re-enable xtrace now that credential handling is done
set -x

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
# Safe: the release tag was already checked out above, so this evaluates the
# verified tree (not the pre-checkout workspace HEAD).
sbt version

# clean locally staged artifacts
rm -rf ~/.ivy2/local/

if [ "$PUBLISH" = true ]; then
  if [ "$SNOWPARK_FIPS" = true ]; then
    echo "[INFO] Packaging snowpark-fips @ tag: $github_version_tag."
  else
    echo "[INFO] Packaging snowpark @ tag: $github_version_tag."
  fi
  sbt +publishSigned
  echo "[INFO] Staged packaged artifacts locally with PGP signing."
  sbt sonaUpload
  echo "[INFO] Uploaded artifacts to sonatype central portal."
  sbt sonaRelease
  if [ "$SNOWPARK_FIPS" = true ]; then
    echo "[SUCCESS] Released snowpark-fips_2.12-$github_version_tag and snowpark-fips_2.13-$github_version_tag to Maven Central"
  else
    echo "[SUCCESS] Released snowpark_2.12-$github_version_tag and snowpark_2.13-$github_version_tag to Maven Central."
  fi
else
  #release to s3
  echo "[INFO] Staging signed artifacts to local ivy2 repository."
  sbt +publishLocalSigned

  # SBT will build FIPS version of Snowpark automatically if the environment variable exists.
  if [ "$SNOWPARK_FIPS" = true ]; then
    S3_JENKINS_URL="s3://sfc-eng-jenkins/repository/snowparkclient-fips/$github_version_tag/"
    S3_DATA_URL="s3://sfc-eng-data/client/snowparkclient-fips/releases/$github_version_tag/"
    echo "[INFO] Uploading snowpark-fips artifacts to:"
  else
    S3_JENKINS_URL="s3://sfc-eng-jenkins/repository/snowparkclient/$github_version_tag/"
    S3_DATA_URL="s3://sfc-eng-data/client/snowparkclient/releases/$github_version_tag/"
    echo "[INFO] Uploading snowpark artifacts to:"
  fi
  echo "[INFO]   - $S3_JENKINS_URL"
  echo "[INFO]   - $S3_DATA_URL"

  # Remove release folders in s3 for current release version if they already exist due to previously failed release pipeline runs.
  echo "[INFO] Deleting $github_version_tag release folders in s3 if they already exist."
  aws s3 rm "$S3_JENKINS_URL" --recursive
  echo "[INFO] $S3_JENKINS_URL folder deleted if it exists."
  aws s3 rm "$S3_DATA_URL" --recursive
  echo "[INFO] $S3_DATA_URL folder deleted if it exists."

  # Rename all produced artifacts to include version number (sbt doesn't by default when publishing to local ivy2 repository).
  # TODO: BEFORE SNOWPARK v2.12.0, fix the regex in the sed command to not match the 2.12.x or 2.13.x named folder under ~/.ivy2/local/com.snowflake/snowpark_2.1[23]/
  find ~/.ivy2/local -type f -name '*snowpark*' | while read file; do newfile=$(echo "$file" | sed "s/\(2\.1[23]\)\([-\.]\)/\1-${github_version_tag#v}\2/"); mv "$file" "$newfile"; done

  # Generate sha256 checksums for all artifacts produced except .md5, .sha1, and existing .sha256 checksum files.
  find ~/.ivy2/local -type f -name '*snowpark*' ! -name '*.md5' ! -name '*.sha1' ! -name '*.sha256' -exec sh -c 'for f; do sha256sum "$f" | awk '"'"'{printf "%s", $1}'"'"' > "$f.sha256"; done' _ {} +

  # Copy all files, flattening the nested structure of the ivy2 repository into the expected structure on s3.
  find ~/.ivy2/local -type f -name '*snowpark*' ! -name '*.sha1' -exec aws s3 cp \{\} $S3_JENKINS_URL \;
  find ~/.ivy2/local -type f -name '*snowpark*' ! -name '*.sha1' -exec aws s3 cp \{\} $S3_DATA_URL \;

  echo "[SUCCESS] Published Snowpark Java-Scala $github_version_tag artifacts to S3."
fi
