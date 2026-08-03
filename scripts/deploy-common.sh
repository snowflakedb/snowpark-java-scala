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
# Fetch the tag from the canonical upstream by explicit URL rather than a named
# remote: the Jenkins workspace does not reliably configure an "origin" remote,
# and pinning the URL guarantees the tag comes from the known upstream repo
# regardless of local remote configuration. Overridable via GITHUB_REPO_URL.
GITHUB_REPO_URL="${GITHUB_REPO_URL:-https://github.com/snowflakedb/snowpark-java-scala.git}"
echo "[INFO] Fetching and verifying tag: $github_version_tag from $GITHUB_REPO_URL."
git fetch --tags --force "$GITHUB_REPO_URL" "refs/tags/${github_version_tag}:refs/tags/${github_version_tag}"
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

# Absolute path to the verified release checkout; used to anchor sbt staging
# paths so they cannot drift with the working directory.
REPO_ROOT="$(git rev-parse --show-toplevel)"

# clean locally staged artifacts
#   ~/.ivy2/local       - used by the PUBLISH=false (S3) path below
#   target/sona-staging - the Maven-layout tree that becomes the bundle
#   target/sona-bundle  - the zip built from that tree (sbt sonaBundle)
#
# sbt 1.11's Central Portal support publishes into target/sona-staging
# and `sonaRelease` zips that entire tree -- it never cleans it. deploy.sh
# and deploy-fips.sh run back-to-back as two steps of one Jenkins freestyle
# job (same workspace, no cleanup between them), so a dirty staging tree
# from the previous step silently adds already-published coordinates to the
# next bundle. Central rejects the deployment as a unit, discarding the FIPS
# artifacts along with it. The globs also cover any variant-scoped
# directories added by a future build.sbt stagingDirectory override.
rm -rf ~/.ivy2/local/
rm -rf "$REPO_ROOT"/target/sona-staging* "$REPO_ROOT"/target/sona-bundle*

# ---------------------------------------------------------------------------
# central_pom_status <artifact_id> <version>
#
# Echoes the HTTP status of the released .pom for one fully-qualified
# artifactId on Maven Central. "000" means the request itself failed.
# ---------------------------------------------------------------------------
central_pom_status() {
  local artifact_id="$1"  # e.g. snowpark-fips_2.12
  local version="$2"      # e.g. 1.21.0, no leading "v"
  local url="https://repo1.maven.org/maven2/com/snowflake/${artifact_id}/${version}/${artifact_id}-${version}.pom"
  local code
  code="$(curl -sS -I --max-time 30 --retry 3 --retry-delay 5 \
            -o /dev/null -w '%{http_code}' "$url" 2>/dev/null)" || code=000
  echo "${code:-000}"
}

# ---------------------------------------------------------------------------
# maven_central_has_release <artifact_base> <version>
#
#   returns 0 -> variant is already live on Maven Central; caller must skip
#   returns 1 -> coordinates are free; caller must publish
#   exits   1 -> indeterminate; a human has to look
#
# The two Scala cross-versions ship in one Central deployment and are accepted
# or rejected atomically, so _2.12 alone is decisive. _2.13 is probed only to
# detect a partial publish (which re-running will never fix).
# ---------------------------------------------------------------------------
maven_central_has_release() {
  local artifact_base="$1"  # "snowpark" or "snowpark-fips"
  local version="$2"        # e.g. 1.21.0, no leading "v"
  local code_212 code_213

  code_212="$(central_pom_status "${artifact_base}_2.12" "$version")"
  case "$code_212" in
    200|404) ;;
    *)
      echo "[ERROR] Could not determine whether ${artifact_base}_2.12:${version} is"
      echo "[ERROR] already on Maven Central (HTTP $code_212)."
      echo "[ERROR] Refusing to guess: publishing over a live coordinate is a hard"
      echo "[ERROR] failure, and skipping a real release would ship nothing."
      echo "[ERROR] Re-run the build once repo1.maven.org is reachable."
      exit 1
      ;;
  esac

  code_213="$(central_pom_status "${artifact_base}_2.13" "$version")"
  if [ "$code_212" != "$code_213" ]; then
    echo "[ERROR] ${artifact_base}_2.12:${version} -> HTTP $code_212 but"
    echo "[ERROR] ${artifact_base}_2.13:${version} -> HTTP $code_213."
    echo "[ERROR] The Scala cross-versions are published atomically, so this is a"
    echo "[ERROR] partial publish. Inspect the deployment history at"
    echo "[ERROR] https://central.sonatype.com/publishing/deployments before retrying."
    exit 1
  fi

  [ "$code_212" = 200 ]
}

if [ "$SNOWPARK_FIPS" = true ]; then
  ARTIFACT_BASE="snowpark-fips"
else
  ARTIFACT_BASE="snowpark"
fi
RELEASE_VERSION="${github_version_tag#v}"

if [ "$PUBLISH" = true ]; then
  # Idempotency guard. Maven Central coordinates are immutable, so a variant
  # that is already released must not be uploaded again. Skipping (rather than
  # failing) is what makes re-runs and the 1.19.0-1.21.0 FIPS backfill work
  # through the ordinary job: deploy.sh declines, deploy-fips.sh publishes.
  if maven_central_has_release "$ARTIFACT_BASE" "$RELEASE_VERSION"; then
    echo "[INFO] ${ARTIFACT_BASE}_2.12:${RELEASE_VERSION} and ${ARTIFACT_BASE}_2.13:${RELEASE_VERSION}"
    echo "[INFO] are already published on Maven Central. Published coordinates are"
    echo "[INFO] immutable, so there is nothing to do."
    echo "[SUCCESS] Skipped $ARTIFACT_BASE @ $github_version_tag (already released)."
    exit 0
  fi

  echo "[INFO] Packaging $ARTIFACT_BASE @ tag: $github_version_tag."
  sbt +publishSigned
  echo "[INFO] Staged packaged artifacts locally with PGP signing."
  sbt sonaRelease
  if [ "$SNOWPARK_FIPS" = true ]; then
    echo "[SUCCESS] Released snowpark-fips_2.12-${RELEASE_VERSION} and snowpark-fips_2.13-${RELEASE_VERSION} to Maven Central."
  else
    echo "[SUCCESS] Released snowpark_2.12-${RELEASE_VERSION} and snowpark_2.13-${RELEASE_VERSION} to Maven Central."
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
