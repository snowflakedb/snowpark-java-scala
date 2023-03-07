package com.snowflake.snowpark;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.scalatest.TagAnnotation;

/**
 * These tests only verify if the new pre-commit code meets the requirement of this code repository.
 * They are not functionality tests. Example usages: pom.xml, fips.xml, and java_doc.xml should be
 * updated together
 */
@TagAnnotation
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface CodeVerification {}
