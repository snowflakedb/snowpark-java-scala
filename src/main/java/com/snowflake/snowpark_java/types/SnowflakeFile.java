package com.snowflake.snowpark_java.types;

import java.io.InputStream;

/**
 * Custom Snowflake class that provides users with additional information on top of the core
 * capability of reading Snowflake files.
 *
 * @since 1.3.0
 */
public class SnowflakeFile {
  /**
   * Create a new instance of SnowflakeFile. Calls into our C++ layer to construct it.
   *
   * @param scopedUrl can be scoped URL. Stage file references should use
   *     newInstanceFromOwnerFileUrl.
   * @return A new instance of SnowflakeFile for the given URL.
   * @since 1.3.0
   */
  public static native SnowflakeFile newInstance(String scopedUrl);

  /**
   * Create a new instance of SnowflakeFile. Calls into our C++ layer to construct it.
   *
   * <p>This method should be used when the UDF author (owner) intends to access their own files via
   * a stage URL. If scoped URL is required, newInstance(scopedUrl) can be used. This method
   * requires a scoped URL and is the same as calling newInstance(scopedUrl,
   * /*requiredScopedUrl*\/=true).
   *
   * <p>All files are accessed in the context of the UDF owner (with the exception of caller's
   * rights stored procedures which use the caller's context). UDF callers should use scoped URLs to
   * allow the UDF to access their files. By accepting only scoped URLs the UDF owner can ensure the
   * UDF caller had access to the provided file. Removing the requirement that the URL is a scoped
   * URL (requireScopedUrl=false) allows the caller to provide URLs that may be only accessible by
   * the UDF owner.
   *
   * @param url scoped URL, file URL, or string path for files located in a stage
   * @param requireScopedUrl whether to fail if this URL is not a scoped URL
   * @return A new instance of SnowflakeFile for the given URL.
   * @since 1.8.0
   */
  public static native SnowflakeFile newInstance(String url, boolean requireScopedUrl);

  /**
   * Create a new instance of SnowflakeFile. Calls into our C++ layer to construct it.
   *
   * <p>This method should be used when the UDF author (owner) intends to access their own files
   * whereas the other method (newInstance) should be used when the UDF owner intends to access
   * files passed into the function by the caller.
   *
   * <p>All files are accessed in the context of the UDF owner and therefore implementation should
   * prefer using the newInstance for callers files as it only allows scoped URLs and therefore a
   * caller cannot inadvertently access the owner's files.
   *
   * @deprecated Use newInstance(String, bool) instead.
   * @param url can be scoped URL or a stage file reference.
   * @return A new instance of SnowflakeFile for the given URL.
   * @since 1.8.0
   */
  @Deprecated
  public static native SnowflakeFile newInstanceFromOwnerFileUrl(String url);

  /**
   * Obtain and return the input stream for the file. Successive calls do not create a new input
   * stream, but rather return the cached stream for the file.
   *
   * @return An InputStream for the SnowflakeFile.
   * @since 1.3.0
   */
  public synchronized InputStream getInputStream() {
    throw new UnsupportedOperationException();
  }

  /**
   * Total size of the file obtained from the cloud storage provider when available, returns null
   * otherwise.
   *
   * @return the size of the file, as provided by the cloud storage provider.
   * @since 1.3.0
   */
  public synchronized Long getSize() {
    throw new UnsupportedOperationException();
  }
}
