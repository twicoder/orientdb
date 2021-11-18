package com.orientechnologies.orient.core.sql.executor;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import java.util.*;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * Special implementation of Java Set&lt;ORID&gt; to efficiently handle memory and performance. It
 * does not store actual RIDs, but it only keeps track that a RID was stored, so the iterator will
 * return new instances.
 *
 * @author Luigi Dell'Aquila
 */
public class ORidSet extends AbstractSet<ORID> {

  private final IntObjectHashMap<Roaring64Bitmap> rids = new IntObjectHashMap<>();

  private int size = 0;

  protected int maxArraySize;

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size == 0L;
  }

  @Override
  public boolean contains(Object o) {
    if (size == 0) {
      return false;
    }

    if (!(o instanceof ORID)) {
      throw new IllegalArgumentException();
    }

    final ORID identifiable = ((ORID) o);

    final int cluster = identifiable.getClusterId();
    final long position = identifiable.getClusterPosition();

    final Roaring64Bitmap bitmap = rids.get(cluster);
    if (bitmap == null) {
      return false;
    }

    return bitmap.contains(position);
  }

  @Override
  public Iterator<ORID> iterator() {
    return new Iterator<ORID>() {
      private Iterator<Long> currentBitmapIterator;
      private Iterator<IntObjectCursor<Roaring64Bitmap>> bitmapIterator;

      private ORID currentRid;
      private int currentCluster = -1;

      @Override
      public boolean hasNext() {
        if (currentRid != null) {
          return true;
        }

        if (bitmapIterator == null) {
          bitmapIterator = rids.iterator();
        }

        if (currentBitmapIterator == null) {
          if (bitmapIterator.hasNext()) {
            final IntObjectCursor<Roaring64Bitmap> cursor = bitmapIterator.next();
            currentBitmapIterator = cursor.value.iterator();
            currentCluster = cursor.key;
          } else {
            return false;
          }
        }

        if (currentBitmapIterator.hasNext()) {
          currentRid = new ORecordId(currentCluster, currentBitmapIterator.next());
          return true;
        } else if (bitmapIterator.hasNext()) {
          final IntObjectCursor<Roaring64Bitmap> cursor = bitmapIterator.next();
          currentBitmapIterator = cursor.value.iterator();
          currentCluster = cursor.key;
          currentRid = new ORecordId(currentCluster, currentBitmapIterator.next());

          return true;
        }

        return false;
      }

      @Override
      public ORID next() {
        if (hasNext()) {
          final ORID result = currentRid;
          currentRid = null;
          return result;
        }

        throw new NoSuchElementException();
      }
    };
  }

  @Override
  public boolean add(final ORID identifiable) {
    Objects.requireNonNull(identifiable);

    final int cluster = identifiable.getClusterId();
    final long position = identifiable.getClusterPosition();

    Roaring64Bitmap bitmap = rids.get(cluster);
    if (bitmap == null) {
      bitmap = new Roaring64Bitmap();
      rids.put(cluster, bitmap);
    }

    if (bitmap.contains(position)) {
      return false;
    }

    bitmap.add(position);
    size++;
    return true;
  }

  @Override
  public boolean remove(Object o) {
    if (!(o instanceof ORID)) {
      throw new IllegalArgumentException();
    }

    final ORID rid = (ORID) o;

    final int cluster = rid.getClusterId();
    final long position = rid.getClusterPosition();

    final Roaring64Bitmap bitmap = rids.get(cluster);
    if (bitmap == null) {
      return false;
    }

    if (bitmap.contains(position)) {
      bitmap.removeLong(position);
      size--;
      return true;
    }

    return false;
  }

  @Override
  public void clear() {
    rids.clear();
    size = 0;
  }
}
