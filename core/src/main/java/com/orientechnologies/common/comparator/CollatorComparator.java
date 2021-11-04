package com.orientechnologies.common.comparator;

import com.ibm.icu.text.Collator;
import com.orientechnologies.orient.core.index.OCompositeKey;
import java.util.Comparator;

public class CollatorComparator implements Comparator<Object> {
  private final Collator collator;

  public CollatorComparator(Collator collator) {
    this.collator = collator;
  }

  @Override
  public int compare(final Object objectOne, final Object objectTwo) {
    if (objectOne instanceof String) {
      final String stringOne = (String) objectOne;
      final String stringTwo = (String) objectTwo;

      return collator.compare(stringOne, stringTwo);
    } else if (objectOne instanceof OCompositeKey) {
      final OCompositeKey compositeKeyOne = (OCompositeKey) objectOne;
      final OCompositeKey compositeKeyTwo = (OCompositeKey) objectTwo;

      return compositeKeyOne.compare(this, compositeKeyTwo);
    } else if (objectOne instanceof Comparable) {
      @SuppressWarnings("rawtypes")
      final Comparable comparableOne = (Comparable) objectOne;
      @SuppressWarnings("rawtypes")
      final Comparable comparableTwo = (Comparable) objectTwo;

      //noinspection unchecked
      return comparableOne.compareTo(comparableTwo);
    } else {
      throw new IllegalArgumentException(
          "Not supported object type is passed into comparator "
              + (objectOne == null ? "null" : objectOne.getClass().getName()));
    }
  }
}
