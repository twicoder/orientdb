package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.mmap;

import com.orientechnologies.common.concur.lock.ScalableRWLock;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecordsFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.WriteableWALRecord;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.agrona.IoUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.mmap.ReadOnlyWALSegment.*;

public final class ModifiableWALSegment implements AutoCloseable {
  private static final XXHash64 xxHash = XXHashFactory.fastestInstance().hash64();
  private final Path segmentPath;

  private final AtomicInteger currentPosition = new AtomicInteger();
  private final ConcurrentSkipListMap<Integer, WriteableWALRecord> records =
      new ConcurrentSkipListMap<>();

  private final ScalableRWLock syncLock = new ScalableRWLock();

  private final MappedByteBuffer buffer;
  private final ByteBuffer dataBuffer;

  private final long segmentIndex;

  private boolean synced = false;
  private int lastRecord = -1;
  private int firstRecord = -1;

  private boolean closed;

  public ModifiableWALSegment(
      final Path segmentPath, final int segmentSize, final long segmentIndex) throws IOException {
    syncLock.exclusiveLock();
    try {
      this.segmentPath = segmentPath;
      this.segmentIndex = segmentIndex;

      try (final FileChannel fileChannel =
          FileChannel.open(
              segmentPath,
              StandardOpenOption.READ,
              StandardOpenOption.CREATE_NEW,
              StandardOpenOption.WRITE)) {
        buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, segmentSize);
      }

      buffer.position(METADATA_SIZE);

      dataBuffer = buffer.slice();
    } finally {
      syncLock.exclusiveUnlock();
    }
  }

  public Optional<OLogSequenceNumber> write(final WriteableWALRecord record) {
    syncLock.sharedLock();
    try {
      checkForClose();

      if (synced) {
        throw new IllegalStateException(
            "Segment " + segmentIndex + " is already synced and can not be used for writes.");
      }

      final int recordSize = record.serializedSize();
      if (recordSize + RECORD_SYSTEM_DATA_SIZE > dataBuffer.capacity()) {
        throw new IllegalArgumentException(
            "Record size exceeds maximum allowed size of the record "
                + recordSize
                + " vs "
                + (dataBuffer.capacity() - RECORD_SYSTEM_DATA_SIZE));
      }

      int position = this.currentPosition.get();
      while (true) {
        final long newPosition = position + recordSize + RECORD_SYSTEM_DATA_SIZE;
        if (newPosition < dataBuffer.capacity()) {
          if (currentPosition.compareAndSet(position, (int) newPosition)) {
            final OLogSequenceNumber lsn = new OLogSequenceNumber(segmentIndex, position);

            record.setOperationIdLsn(lsn, 0);
            records.put(position, record);

            final ByteBuffer copy = dataBuffer.duplicate();
            copy.position(position);

            copy.putShort((short) record.getId());
            copy.putInt(recordSize);
            record.toStream(copy);

            final long hash =
                xxHash.hash(
                    copy, position, recordSize + RECORD_SIZE_SIZE + RECORD_ID_SIZE, XX_HASH_SEED);

            copy.putLong(hash);

            return Optional.of(new OLogSequenceNumber(segmentIndex, position));
          }
        } else {
          return Optional.empty();
        }
      }
    } finally {
      syncLock.sharedUnlock();
    }
  }

  public Optional<WriteableWALRecord> read(final OLogSequenceNumber lsn) {
    syncLock.sharedLock();
    try {
      checkForClose();

      return doRead(lsn);
    } finally {
      syncLock.sharedUnlock();
    }
  }

  private Optional<WriteableWALRecord> doRead(OLogSequenceNumber lsn) {
    if (lsn.getSegment() != segmentIndex) {
      throw new IllegalArgumentException(
          "Segment index passed in LSN differs from current segment index. "
              + lsn.getSegment()
              + " vs. "
              + segmentIndex);
    }

    if (lsn.getPosition() >= dataBuffer.capacity()) {
      throw new IllegalArgumentException(
          "Requested record position "
              + lsn.getPosition()
              + " bigger than segment size "
              + dataBuffer.capacity());
    }

    if (lsn.getPosition() >= dataBuffer.capacity() - RECORD_SYSTEM_DATA_SIZE) {
      return Optional.empty();
    }

    if (!synced) {
      return Optional.ofNullable(records.get(lsn.getPosition()));
    }

    final ByteBuffer copy = dataBuffer.duplicate();
    copy.position(lsn.getPosition());

    final int recordId = copy.getShort();
    if (recordId < 0) {
      return Optional.empty();
    }

    final int recordSize = copy.getInt();
    if (recordSize < 0
        || lsn.getPosition() +  recordSize + RECORD_SYSTEM_DATA_SIZE
            > dataBuffer.capacity()) {
      return Optional.empty();
    }

    final long hash =
        copy.getLong(lsn.getPosition() + RECORD_ID_SIZE + RECORD_SIZE_SIZE + recordSize);
    if (xxHash.hash(
            copy, lsn.getPosition(), RECORD_ID_SIZE + RECORD_SIZE_SIZE + recordSize, XX_HASH_SEED)
        != hash) {
      return Optional.empty();
    }

    final WriteableWALRecord record = OWALRecordsFactory.INSTANCE.walRecordById(recordId);
    record.fromStream(copy);

    record.setOperationIdLsn(lsn, 0);

    return Optional.of(record);
  }

  public Optional<WriteableWALRecord> next(final WriteableWALRecord record) {
    syncLock.sharedLock();
    try {
      checkForClose();

      return doNext(record);
    } finally {
      syncLock.sharedUnlock();
    }
  }

  private Optional<WriteableWALRecord> doNext(WriteableWALRecord record) {
    final int serializedSize = record.serializedSize();
    final int nextPosition =
        record.getOperationIdLSN().lsn.getPosition()
            + serializedSize
            + RECORD_SYSTEM_DATA_SIZE;

    if (!synced) {
      return Optional.ofNullable(records.get(nextPosition));
    } else {
      return read(new OLogSequenceNumber(segmentIndex, nextPosition));
    }
  }

  public Optional<WriteableWALRecord> next(final OLogSequenceNumber lsn) {
    syncLock.sharedLock();
    try {
      checkForClose();

      final Optional<WriteableWALRecord> record = doRead(lsn);
      return record.flatMap(this::doNext);
    } finally {
      syncLock.sharedUnlock();
    }
  }

  public void sync() {
    syncLock.exclusiveLock();
    try {
      checkForClose();

      doSync();
    } finally {
      syncLock.exclusiveUnlock();
    }
  }

  private void doSync() {
    if (synced) {
      return;
    }

    if (!records.isEmpty()) {
      firstRecord = records.firstKey();
      lastRecord = records.lastKey();
      records.clear();
    }

    buffer.position(0);

    buffer.putInt(firstRecord);
    buffer.putInt(lastRecord);

    final long hash = xxHash.hash(buffer, 0, METADATA_SIZE, XX_HASH_SEED);
    buffer.putLong(hash);

    buffer.force();

    synced = true;
  }

  public Optional<OLogSequenceNumber> begin() {
    syncLock.sharedLock();
    try {
      checkForClose();

      if (!synced) {
        if (records.isEmpty()) {
          return Optional.empty();
        }

        return Optional.of(new OLogSequenceNumber(segmentIndex, records.firstKey()));
      } else {
        if (firstRecord < 0) {
          return Optional.empty();
        }
        return Optional.of(new OLogSequenceNumber(segmentIndex, firstRecord));
      }
    } finally {
      syncLock.sharedUnlock();
    }
  }

  public Optional<OLogSequenceNumber> end() {
    syncLock.sharedLock();
    try {
      checkForClose();

      if (!synced) {
        if (records.isEmpty()) {
          return Optional.empty();
        }

        return Optional.of(new OLogSequenceNumber(segmentIndex, records.lastKey()));
      } else {
        if (lastRecord < 0) {
          return Optional.empty();
        }

        return Optional.of(new OLogSequenceNumber(segmentIndex, lastRecord));
      }
    } finally {
      syncLock.sharedUnlock();
    }
  }

  @Override
  public void close() throws Exception {
    syncLock.exclusiveLock();
    try {
      doClose();
    } finally {
      syncLock.exclusiveUnlock();
    }
  }

  private void doClose() {
    if (closed) {
      return;
    }

    if (!synced) {
      doSync();
    }

    IoUtil.unmap(buffer);

    closed = true;
  }

  public void delete() throws IOException {
    syncLock.exclusiveUnlock();
    try {
      checkForClose();

      doClose();

      Files.delete(segmentPath);
    } finally {
      syncLock.exclusiveUnlock();
    }
  }

  private void checkForClose() {
    if (closed) {
      throw new IllegalStateException(
          "Segment with index " + segmentIndex + " is closed and can not be used");
    }
  }
}
