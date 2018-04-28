package com.github.fakemongo.impl.index;

import java.util.*;


public class IndexedList<Е> {
  private Map<Е, List<Integer>> indexes;

  private final List<Е> elements;

  private boolean isSingle = true;

  public IndexedList(List<Е> elements) {
    this.elements = elements;

    if (elements.size() > 1) {
      initIndex(elements);
    }
  }

  private void initIndex(List<Е> newElements) {
    indexes = new HashMap<Е, List<Integer>>();

    isSingle = false;

    int count = 0;
    for (Е el : newElements) {
      addIndex(el, count);
      count += 1;
    }
  }

  public List<Е> getElements() {
    return elements;
  }

  public int size() {
    return elements.size();
  }

  public boolean contains(Е element) {
    if (isSingle)
      return elements.contains(element);

    return indexes.containsKey(element);
  }

  public void add(Е element) {
    if (elements.size() == 1) {
      initIndex(elements);
    }

    elements.add(element);

    if (elements.size() > 1)
      addIndex(element, elements.size()-1);
  }

  private void addIndex(Е element, int position) {
    final List<Integer> list;
    if (!indexes.containsKey(element)) {
      list = new ArrayList<Integer>();
      indexes.put(element, list);
    }
    else
      list = indexes.get(element);

    list.add(position);
  }

  public void remove(Е element) {
    if (isSingle) {
      elements.remove(element);
      return;
    }

    List<Integer> index = indexes.get(element);

    if (index == null)
      return;

    if (index.size() != 0) {
      int pos = index.get(0);
      elements.remove(pos);
      index.remove(0);

      for(int i=pos; i<elements.size(); i++) {
        Е key = elements.get(i);
        decrementIndexes(indexes.get(key));
      }
    }

    if (index.size() == 0) {
      indexes.remove(element);
    }
  }

  private void decrementIndexes(List<Integer> index) {
    for (int i = 0; i < index.size(); i++) {
      index.set(i, index.get(i) - 1);
    }
  }
}
