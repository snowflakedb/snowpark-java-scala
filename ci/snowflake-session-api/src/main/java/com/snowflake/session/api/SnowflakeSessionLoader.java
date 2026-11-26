package com.snowflake.session.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

/** Locates the session factories through {@link ServiceLoader}. */
public final class SnowflakeSessionLoader {

  private SnowflakeSessionLoader() {}

  /**
   * Returns the client session factory visible to {@code classLoader}, or empty if there is none,
   * for example with a JDBC 3.x driver.
   *
   * <p>Duplicate provider registrations fail explicitly instead of selecting one based on classpath
   * order.
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
    List<String> registrations = registrations(service, classLoader);
    if (registrations.size() > 1) {
      throw new IllegalStateException(
          "Expected exactly one " + service.getName() + " provider, found " + registrations);
    }
    List<T> providers = new ArrayList<>();
    for (T provider : ServiceLoader.load(service, classLoader)) {
      providers.add(provider);
    }
    return providers;
  }

  private static IllegalStateException ambiguous(Class<?> service, List<?> providers) {
    List<String> names = new ArrayList<>();
    for (Object provider : providers) {
      names.add(describe(provider));
    }
    return new IllegalStateException(
        "Expected exactly one " + service.getName() + " provider, found " + names);
  }

  private static List<String> registrations(Class<?> service, ClassLoader classLoader) {
    String resourceName = "META-INF/services/" + service.getName();
    Set<String> registrations = new LinkedHashSet<>();
    try {
      Enumeration<URL> resources =
          classLoader == null
              ? ClassLoader.getSystemResources(resourceName)
              : classLoader.getResources(resourceName);
      while (resources.hasMoreElements()) {
        URL resource = resources.nextElement();
        for (String provider : providerNames(resource)) {
          registrations.add(provider + " from " + resource);
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unable to inspect " + service.getName() + " providers", e);
    }
    return new ArrayList<>(registrations);
  }

  private static Set<String> providerNames(URL resource) throws IOException {
    Set<String> names = new LinkedHashSet<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(resource.openStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        int comment = line.indexOf('#');
        String name = (comment < 0 ? line : line.substring(0, comment)).trim();
        if (!name.isEmpty()) {
          names.add(name);
        }
      }
    }
    return names;
  }

  private static String describe(Object provider) {
    Class<?> type = provider.getClass();
    try {
      ProtectionDomain domain = type.getProtectionDomain();
      CodeSource source = domain == null ? null : domain.getCodeSource();
      if (source != null && source.getLocation() != null) {
        return type.getName() + " from " + source.getLocation();
      }
    } catch (SecurityException ignored) {
      // Some class loaders do not allow access to protection-domain metadata.
    }
    return type.getName();
  }
}
