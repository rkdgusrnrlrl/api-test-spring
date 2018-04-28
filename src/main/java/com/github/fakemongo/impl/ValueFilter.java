package com.github.fakemongo.impl;

/**
 * Filter for any value, not just DBObject as {@link Filter}
 * Needed by $pull command that can filter out both sub-documents and primitive values like string, etc
 */
public interface ValueFilter {
    boolean apply(Object object);
}
