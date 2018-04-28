package com.github.fakemongo.impl;

import com.mongodb.DBObject;

public interface Filter {
  boolean apply(DBObject o);
}