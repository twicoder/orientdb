package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.mmap;

import com.orientechnologies.common.concur.lock.ScalableRWLock;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
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

public final class ReadOnlyWALSegment {
  private static final XXHash64 xxHash = XXHashFactory.fastestInstance().hash64();


  static final int RECORD_ID_SIZE = OShortSerializer.SHORT_SIZE;
  static final int RECORD_SIZE_SIZE = OIntegerSerializer.INT_SIZE;

  static final int XX_HASH_SIZE = 8;

  static final int RECORD_SYSTEM_DATA_SIZE = XX_HASH_SIZE + RECORD_SIZE_SIZE + RECORD_SIZE_SIZE;

  static final long XX_HASH_SEED = 0xA36FE94F;

  static final int METADATA_SIZE = XX_HASH_SIZE + XX_HASH_SIZE;

  private final Path segmentPath;

  private final MappedByteBuffer buffer;
  private final ByteBuffer dataBuffer;

  private final long segmentIndex;

  private final int lastRecord;
  private final int firstRecord;

  private final ScalableRWLock closeLock = new ScalableRWLock();
  private boolean closed = false;

  public ReadOnlyWALSegment(final Path segmentPath, final int segmentSize, final long segmentIndex)
      throws IOException {
    closeLock.exclusiveLock();
    try {
      this.segmentPath = segmentPath;
      this.segmentIndex = segmentIndex;

      try (final FileChannel fileChannel = FileChannel.open(segmentPath, StandardOpenOption.READ)) {
        buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, segmentSize);
      }

      buffer.position(METADATA_SIZE + XX_HASH_SIZE);
      dataBuffer = buffer.slice();

      if (buffer.capacity() >= 2 * 4 + 8) {
        buffer.position(0);
        final int fr = buffer.getInt();
        final int lr = buffer.getInt();

        final long hash = buffer.getLong();

        if (xxHash.hash(buffer, 0, METADATA_SIZE, XX_HASH_SEED) == hash) {
          firstRecord = fr;
          lastRecord = lr;
        } else {
          final int[] recs = scanSegment();
          if (recs[0] != -1) {
            firstRecord = recs[0];
            lastRecord = recs[1];
          } else {
            firstRecord = -1;
            lastRecord = -1;
          }
        }
      } else {
        firstRecord = -1;
        lastRecord = -1;
      }
    } finally {
      closeLock.exclusiveUnlock();
    }
  }

  private int[] scanSegment() {
    int fr = -1;
    int lr = -1;

    OLogSequenceNumber startLSN = new OLogSequenceNumber(segmentIndex, 0);

    Optional<WriteableWALRecord> record = doRead(startLSN);
    while (record.isPresent()) {
      final WriteableWALRecord writeableWALRecord = record.get();
      final OLogSequenceNumber lsn = writeableWALRecord.getOperationIdLSN().lsn;

      if (fr == -1) {
        fr = lsn.getPosition();
      }

      lr = lsn.getPosition();

      record = doNext(writeableWALRecord);
    }

    if (fr == -1 || lr == -1) {
      return new int[] {-1, -1};
    }

    return new int[] {fr, lr};
  }

  public Optional<WriteableWALRecord> read(final OLogSequenceNumber lsn) {
    closeLock.sharedLock();
    try {
      checkForClose();

      return doRead(lsn);
    } finally {
      closeLock.sharedUnlock();
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

    if (lsn.getPosition() + RECORD_ID_SIZE + RECORD_SIZE_SIZE + XX_HASH_SIZE
        > dataBuffer.capacity()) {
      return Optional.empty();
    }

    final ByteBuffer copy = dataBuffer.duplicate();
    copy.position(lsn.getPosition());

    final int recordId = copy.getShort();
    if (recordId < 0) {
      return Optional.empty();
    }

    final int recordSize = copy.getInt();

    if (recordSize < 0
        || lsn.getPosition() + RECORD_SIZE_SIZE + RECORD_ID_SIZE + recordSize + XX_HASH_SIZE
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
    closeLock.sharedLock();
    try {
      checkForClose();

      return doNext(record);
    } finally {
      closeLock.sharedUnlock();
    }
  }

  private Optional<WriteableWALRecord> doNext(WriteableWALRecord record) {
    final int serializedSize = record.serializedSize();
    final int nextPosition =
        record.getOperationIdLSN().lsn.getPosition()
            + serializedSize
            + RECORD_ID_SIZE
            + RECORD_SIZE_SIZE
            + XX_HASH_SIZE;
    return doRead(new OLogSequenceNumber(segmentIndex, nextPosition));
  }

  public Optional<WriteableWALRecord> next(final OLogSequenceNumber lsn) {
    closeLock.readLock();
    try {
      checkForClose();

      return doNext(lsn);
    } finally {
      closeLock.sharedUnlock();
    }
  }

  private Optional<WriteableWALRecord> doNext(OLogSequenceNumber lsn) {
    final Optional<WriteableWALRecord> record = doRead(lsn);
    return record.flatMap(this::doNext);
  }

  public Optional<OLogSequenceNumber> begin() {
    if (firstRecord == -1) {
      return Optional.empty();
    }

    return Optional.of(new OLogSequenceNumber(segmentIndex, firstRecord));
  }

  public Optional<OLogSequenceNumber> end() {
    if (lastRecord == -1) {
      return Optional.empty();
    }

    return Optional.of(new OLogSequenceNumber(segmentIndex, lastRecord));
  }

  public void close() throws Exception {
    closeLock.exclusiveLock();
    try {
      doClose();
    } finally {
      closeLock.exclusiveUnlock();
    }
  }

  private void doClose() {
    if (closed) {
      return;
    }

    closed = true;

    IoUtil.unmap(buffer);
  }

  public void delete() throws IOException {
    closeLock.exclusiveLock();
    try{
      checkForClose();

      doClose();

      Files.delete(segmentPath);
    } finally{
      closeLock.exclusiveUnlock();
    }
  }

  private void checkForClose() {
    if (closed) {
      throw new IllegalStateException("Segment with index " + segmentIndex + " already closed");
    }
  }
}
