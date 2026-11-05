package com.snowflake.session.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

/** Locates the session factories through {@link ServiceLoader}. */
public final class SnowflakeSessionLoader {

  private SnowflakeSessionLoader() {}

  /**
   * Returns the client session factory visible to {@code classLoader}, or empty if there is none,
   * for example with a JDBC 3.x driver.
   *
   * @throws IllegalStateException if there is more than one
   */
  public static Optional<SnowflakeClientSessionFactory> loadClientSessionFactory(
      ClassLoader classLoader) {
    List<SnowflakeClientSessionFactory> providers =
        load(SnowflakeClientSessionFactory.class, classLoader);
    if (providers.size() > 1) {
      throw ambiguous(SnowflakeClientSessionFactory.class, providers);
    }
    return providers.isEmpty() ? Optional.empty() : Optional.of(providers.get(0));
  }

  /**
   * Returns the stored-procedure session factory visible to {@code classLoader}.
   *
   * @throws IllegalStateException if there is none or more than one
   */
  public static SnowflakeStoredProcSessionFactory loadStoredProcSessionFactory(
      ClassLoader classLoader) {
    List<SnowflakeStoredProcSessionFactory> providers =
        load(SnowflakeStoredProcSessionFactory.class, classLoader);
    if (providers.size() != 1) {
      throw ambiguous(SnowflakeStoredProcSessionFactory.class, providers);
    }
    return providers.get(0);
  }

  private static <T> List<T> load(Class<T> service, ClassLoader classLoader) {
    List<T> providers = new ArrayList<>();
    for (T provider : ServiceLoader.load(service, classLoader)) {
      providers.add(provider);
    }
    return providers;
  }

  private static IllegalStateException ambiguous(Class<?> service, List<?> providers) {
    List<String> names = new ArrayList<>();
    for (Object provider : providers) {
      names.add(provider.getClass().getName());
    }
    return new IllegalStateException(
        "Expected exactly one " + service.getName() + " provider, found " + names);
  }
}
