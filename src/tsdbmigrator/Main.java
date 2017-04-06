
package tsdbmigrator;

import java.io.File;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;

import net.opentsdb.core.Aggregators;
import net.opentsdb.core.AppendDataPoints;
import net.opentsdb.core.Const;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.Internal;
import net.opentsdb.core.Internal.Cell;
import net.opentsdb.core.Query;
import net.opentsdb.core.RateOptions;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.tools.ArgP;
import net.opentsdb.uid.NoSuchUniqueName;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

// import org.jboss.netty.logging.InternalLoggerFactory;
// import org.jboss.netty.logging.Slf4JLoggerFactory;

final class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static final String keyspace = "tsdb";
  public static final String cf = "t";

	public static final String SCHEMA = String.format("CREATE TABLE %s.%s (" +
																										  "key blob, " +
																										  "column1 blob, " +
																										  "value blob, " +
                                                      "PRIMARY KEY (key, column1))" +
                                                      " WITH COMPACT STORAGE" +
                                                      " AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}" +
                                                      " AND default_time_to_live = 78796800", keyspace, cf);

  public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (key, column1, value) VALUES (?, ?, ?) USING TIMESTAMP ?", keyspace, cf);
  // public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (key, column1, value) VALUES (?, ?, ?)", keyspace, cf);

  static CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
  static CQLSSTableWriter writer;

  public static void main(String[] args) throws Exception {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    argp.addOption("--import", "Prints the rows in a format suitable for"
                   + " the 'import' command.");
    argp.addOption("--delete", "Deletes rows as they are scanned.");
    argp.addOption("--start", "START", "Start time.");
    argp.addOption("--stop", "STOP", "Stop time.");
    argp.addOption("--metrics", "tmp/metrics", "File having a newline separated list of metrics.");
    argp.addOption("--data", "./data", "Where to dump the sstables.");
    args = CliOptions.parse(argp, args);
    if (args == null) {
      System.err.print("Invalid usage.");
      System.exit(-1);
    }

    // get a config object
    net.opentsdb.utils.Config config = CliOptions.getConfig(argp);

    // magic
    Config.setClientMode(true);

    final TSDB tsdb = new TSDB(config);

    final CassandraClient cass = new CassandraClient(config);

    int start = Integer.parseInt(argp.get("--start", "0"));
    final int stop = Integer.parseInt(argp.get("--stop", "2114413200"));
    final String metricsFileName = argp.get("--metrics", "");
    final String datadir = argp.get("--data", "./data");
    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
    argp = null;


    File metricsFile = new File(metricsFileName);
    java.util.Scanner sc = new java.util.Scanner(metricsFile);

    ArrayList<String> metrics = new ArrayList<String>();
    while (sc.hasNextLine()) {
      metrics.add(sc.nextLine());
    }
    sc.close();

    File outputDir = new File(datadir + "/" + keyspace + "/" + cf);
    if (!outputDir.exists() && !outputDir.mkdirs()) {
      throw new RuntimeException("Can't make output dir: " + outputDir);
    }

    builder.inDirectory(outputDir).forTable(SCHEMA).using(INSERT_STMT).withPartitioner(new Murmur3Partitioner());
    // builder.withBufferSizeInMB(256);

    try {
      // migrateIds(tsdb, tsdb.getClient(), cass);
      int interstart = start - (start % 86400);
      int interstop = Math.min((interstart + 86400) - 1, stop);
      for (; interstop <= stop && interstart < stop; interstop+=86400, interstart+=86400) {
        writer = builder.build();
        final int realstop = Math.min(interstop, stop);
        final int realstart = Math.max(interstart, start);
        LOG.warn("Starting time range " + realstart + "-" + realstop);
        for (String metric : metrics) {
          // LOG.warn("Starting metric " + metric + " on time range " + realstart + "-" + realstop);
          migrateData(tsdb, tsdb.getClient(), cass, realstart, realstop, metric);
        }
        index_cache = new HashMap<ByteBuffer, Boolean>(); // reset the cache
        writer.close();
      }
    } catch (Exception e) {
      LOG.error("Exception ", e);
    } finally {
      tsdb.shutdown().joinUninterruptibly();
    }
  }

  public static void migrateIds(TSDB tsdb, HBaseClient client, CassandraClient cass) throws Exception {
    final Scanner scanner = client.newScanner("tsdb-uid".getBytes());
    scanner.setMaxNumRows(1024);

    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          // found |= printResult(row, CliUtils.ID_FAMILY, true);
          final Iterator<KeyValue> i = row.iterator();
          while (i.hasNext()) {
            final KeyValue kv = i.next();
            if (Bytes.memcmp(kv.family(), "id".getBytes()) == 0 && Bytes.memcmp(kv.key(), CassandraClient.idKey) != 0) {
              cass.getOrCreateId(kv.key(), CassandraClient.fromBytes(kv.qualifier()));
            }

          }

        }
      }
    } catch (HBaseException e) {
      LOG.error("Error while scanning HBase, scanner=" + scanner, e);
      throw e;
    } catch (Exception e) {
      throw e;
    }

  }



  public static void migrateData(TSDB tsdb, HBaseClient client, CassandraClient cass, int start, int stop, String metric_name) throws Exception {
    Query query = tsdb.newQuery();

    RateOptions rate_options = new RateOptions(false, Long.MAX_VALUE,
        RateOptions.DEFAULT_RESET_VALUE);
    final HashMap<String, String> t = new HashMap<String, String>();
    query.setStartTime(start);
    query.setEndTime(stop);
    try {
    query.setTimeSeries(metric_name, t, Aggregators.get("sum"), false, rate_options);
    } catch (NoSuchUniqueName e) {
      LOG.warn("Can't find metric. Skipping " + metric_name);
      return;
    }

    final List<Scanner> scanners = Internal.getScanners(query);
      for (Scanner scanner : scanners) {
        scanner.setMaxNumRows(100);
        scanner.setServerBlockCache(false);
        ArrayList<ArrayList<KeyValue>> rows;
        while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
          for (final ArrayList<KeyValue> row : rows) {
            final byte[] key = row.get(0).key();
            final long base_time = Internal.baseTime(tsdb, key);
            final String metric = Internal.metricName(tsdb, key);

            for (final KeyValue kv : row) {
              sendDataPoint(tsdb, cass, kv, base_time, metric);
              if (cass.buffered_mutations.getRowCount() > 500 || indexMutationCount > 100) {
                cass.buffered_mutations.execute();
                indexMutationCount = 0;
              }
            }

          }
        }
      }

      if (cass.buffered_mutations.getRowCount() > 0) {
        cass.buffered_mutations.execute();
      }
    // LOG.warn("Done with metric " + metric_name);
  }

  static short METRICS_WIDTH = 3;
  static short TAG_NAME_WIDTH = 3;
  static short TAG_VALUE_WIDTH = 3;
  static final short TIMESTAMP_BYTES = 4;

  static int indexMutationCount = 0;

  static HashMap<ByteBuffer, Boolean> index_cache = new HashMap<ByteBuffer, Boolean>();

  private static void indexMutation(byte[] orig_key, byte[] orig_column, MutationBatch mutation) throws ConnectionException {
    // Take the first 8 bytes of the orig key and put them in the new key.
    // Take the last 6 bytes of the orig key and all of the orig_column and put
    // them in the new column.
    //
    // Take the metric of the orig key and put it in the new key
    // Take the timestamp of the orig key, normalize it to a month, and put it in the new key
    // Take the timestamp of the orig key and put it in the column name.
    // Take only the tags from the orig key, and put them the column name.

    synchronized (indexedKeys) {
      if (indexedKeys.put(ByteBuffer.wrap(orig_key), true) != null) {
        // We already indexed this key
        return;
      }
    }

    final byte[] ts = Arrays.copyOfRange(orig_key, SALT_WIDTH + METRICS_WIDTH, TIMESTAMP_BYTES + SALT_WIDTH + METRICS_WIDTH);
    final int tsInt = Bytes.getInt(ts);
    int month = tsInt - (tsInt % (86400 * 28));
    byte[] new_key = new byte[SALT_WIDTH + METRICS_WIDTH + TIMESTAMP_BYTES];
    byte[] new_col = new byte[orig_key.length - METRICS_WIDTH - SALT_WIDTH];
    System.arraycopy(orig_key, 0, new_key, 0, SALT_WIDTH + METRICS_WIDTH);
    System.arraycopy(Bytes.fromInt(month), 0, new_key, SALT_WIDTH + METRICS_WIDTH, TIMESTAMP_BYTES);
    System.arraycopy(ts, 0, new_col, 0, ts.length);
    System.arraycopy(orig_key, SALT_WIDTH + METRICS_WIDTH + TIMESTAMP_BYTES, new_col, TIMESTAMP_BYTES, new_col.length - TIMESTAMP_BYTES);


    indexMutationCount++;
    mutation.withRow(CassandraClient.TSDB_T_INDEX, new_key).putColumn(new_col, new byte[]{0});
  }

  /** Mask for the millisecond qualifier flag */
  public static final byte MS_BYTE_FLAG = (byte)0xF0;

  /** Flag to set on millisecond qualifier timestamps */
  public static final int MS_FLAG = 0xF0000000;

  /** Number of LSBs in time_deltas reserved for flags.  */
  public static final short FLAG_BITS = 4;

  /** Number of LSBs in time_deltas reserved for flags.  */
  public static final short MS_FLAG_BITS = 6;

  /**
   * When this bit is set, the value is a floating point value.
   * Otherwise it's an integer value.
   */
  public static final short FLAG_FLOAT = 0x8;

  /** Mask to select the size of a value from the qualifier.  */
  public static final short LENGTH_MASK = 0x7;

  /** Mask to select all the FLAG_BITS.  */
  public static final short FLAGS_MASK = FLAG_FLOAT | LENGTH_MASK;

  public static int getOffsetFromQualifier(final byte[] qualifier, 
      final int offset) {
    // validateQualifier(qualifier, offset);
    if ((qualifier[offset] & MS_BYTE_FLAG) == MS_BYTE_FLAG) {
      return (int)(Bytes.getUnsignedInt(qualifier, offset) & 0x0FFFFFC0) 
        >>> MS_FLAG_BITS;
    } else {
      final int seconds = (Bytes.getUnsignedShort(qualifier, offset) & 0xFFFF) 
        >>> FLAG_BITS;
      return seconds * 1000;
    }
  }

 public static short getFlagsFromQualifier(final byte[] qualifier, 
      final int offset) {
    // validateQualifier(qualifier, offset);
    if ((qualifier[offset] & MS_BYTE_FLAG) == MS_BYTE_FLAG) {
      return (short) (qualifier[offset + 3] & FLAGS_MASK); 
    } else {
      return (short) (qualifier[offset + 1] & FLAGS_MASK);
    }
  }

  private static void tMutation(byte[] orig_key, byte[] orig_column, byte[] value, long base_time, CQLSSTableWriter writer, MutationBatch mutation) throws ConnectionException, Exception {
    // Take the timestamp of the orig key, normalize it to the 28-day period, and put it in the new key.
    // Take the metric + tags of the orig key and put it in the new key.
    // Take the offset from the column, add it to the difference between the orig ts and the new base, and put it in the column.
    //
    // If the column is in seconds, we'll use 22 bits to store the offset.
    // If the column is in MS, we'll need 31 bits to store the offset. - DEPRECATING
    // We need 4 bits for the format flag.

    final byte[] ts = Arrays.copyOfRange(orig_key, SALT_WIDTH + METRICS_WIDTH, TIMESTAMP_BYTES + SALT_WIDTH + METRICS_WIDTH);
    final int tsInt = Bytes.getInt(ts);
    int month = tsInt - (tsInt % (86400 * 28));

    final int offset = (tsInt - month) + (getOffsetFromQualifier(orig_column, 0) / 1000);
    final int flags = getFlagsFromQualifier(orig_column, 0);
    final byte[] new_col = Bytes.fromInt(offset << 10 | flags);


    byte[] new_key = Arrays.copyOf(orig_key, orig_key.length);
    System.arraycopy(Bytes.fromInt(month), 0, new_key, SALT_WIDTH + METRICS_WIDTH, TIMESTAMP_BYTES);

    // byte[] new_col = new byte[new_offset.length + flags.length];
    // System.arraycopy(new_offset, 0, new_col, 0, new_offset.length);
    // System.arraycopy(flags, 0, new_col, new_offset.length, flags.length);
    // mutation.withRow(CassandraClient.TSDB_T, new_key)
    //   .putColumn(new_col, request.value());
    writer.addRow(ByteBuffer.wrap(new_key), ByteBuffer.wrap(new_col), ByteBuffer.wrap(value), base_time * 1000 * 1000);

    // TODO: We only need to check this once a month now.
    synchronized (indexedKeys) {
      if (indexedKeys.put(ByteBuffer.wrap(new_key), true) != null) {
        // We already indexed this key
        return;
      }
    }

    // Take the tags out of the orig key and place them in the column
    byte[] index_key = Arrays.copyOfRange(new_key, 0, SALT_WIDTH + METRICS_WIDTH + TIMESTAMP_BYTES);
    final byte[] index_col = Arrays.copyOfRange(orig_key, METRICS_WIDTH + SALT_WIDTH + TIMESTAMP_BYTES, orig_key.length);

    indexMutationCount++;
    mutation.withRow(CassandraClient.TSDB_T_INDEX, index_key).putColumn(index_col, new byte[]{0});
  }



  private static void sendDataPoint(
      final TSDB tsdb,
      final CassandraClient cass,
      final KeyValue kv,
      final long base_time,
      final String metric) throws ConnectionException, Exception {

    final MutationBatch mutation = cass.buffered_mutations;
    final byte[] qualifier = kv.qualifier();
    // final byte[] value = kv.value();
    final int q_len = qualifier.length;
    final ColumnFamily<byte[], byte[]> cf = cass.column_family_schemas.get("t".getBytes());

    final String metricName = Internal.metricName(tsdb, kv.key());

    byte[] key = kv.key();
    if (SALT_WIDTH > 0) {
      key = saltKey(kv.key());
    }
    final byte[] final_key = reIdKey(cass, key, Internal.getTags(tsdb, kv.key()), metricName);

    // System.out.print("orig:   " + Bytes.pretty(kv.key()) + " " + kv.key().length + "\n");
    // System.out.print("final:  " + Bytes.pretty(final_key) + " " + final_key.length + "\n");

    if (!AppendDataPoints.isAppendDataPoints(qualifier) && q_len % 2 != 0) {
      // custom data object, not a data point
    } else if (q_len == 2 || q_len == 4 && Internal.inMilliseconds(qualifier)) {
      // regular data point
      final Cell cell = Internal.parseSingleValue(kv);
      if (cell == null) {
        throw new IllegalDataException("Unable to parse row: " + kv);
      }
        // mutation.withTimestamp(base_time * 1000 * 1000) // microseconds
        //         .withRow(cf, final_key)
        //         .putColumn(cell.qualifier(), cell.value());
        // writer.addRow(final_key, cell.qualifier(), cell.value());
        // writer.addRow(ByteBuffer.wrap(final_key), ByteBuffer.wrap(cell.qualifier()), ByteBuffer.wrap(cell.value()), base_time * 1000 * 1000);
        tMutation(final_key, cell.qualifier(), cell.value(), base_time, writer, mutation);
    } else {
      final Collection<Cell> cells;
      if (q_len == 3) {
        // append data points
        final AppendDataPoints adps = new AppendDataPoints();
        cells = adps.parseKeyValue(tsdb, kv);
      } else {
        // compacted column
        cells = Internal.extractDataPoints(kv);
      }

      for (Cell cell : cells) {
        // mutation.withTimestamp(base_time * 1000 * 1000)
        //         .withRow(cf, final_key)
        //         .putColumn(cell.qualifier(), cell.value());
        // writer.addRow("foo", "bar", "baz");
        // writer.addRow(ByteBuffer.wrap(final_key), ByteBuffer.wrap(cell.qualifier()), ByteBuffer.wrap(cell.value()), base_time * 1000 * 1000);
        tMutation(final_key, cell.qualifier(), cell.value(), base_time, writer, mutation);
      }
    }
  }

  static int SALT_WIDTH = 1;
  static int SALT_BUCKETS = 5;

  // Cobbled together from RowKey.prefixKeyWithSalt
  private static byte[] saltKey(byte[] key) {
    final byte[] newKey = 
        new byte[SALT_WIDTH + key.length];
    System.arraycopy(key, 0, newKey, SALT_WIDTH, key.length);

    final int tags_start = SALT_WIDTH + TSDB.metrics_width() + 
        Const.TIMESTAMP_BYTES;

    // we want the metric and tags, not the timestamp
    final byte[] salt_base = 
        new byte[newKey.length - SALT_WIDTH - Const.TIMESTAMP_BYTES];
    System.arraycopy(newKey, SALT_WIDTH, salt_base, 0, TSDB.metrics_width());
    System.arraycopy(newKey, tags_start,salt_base, TSDB.metrics_width(), 
        newKey.length - tags_start);
    int modulo = Arrays.hashCode(salt_base) % SALT_BUCKETS;
    if (modulo < 0) {
      // make sure we return a positive salt.
      modulo = modulo * -1;
    }

    final byte[] salt = getSaltBytes(modulo);
    System.arraycopy(salt, 0, newKey, 0, SALT_WIDTH);
    return newKey;
  }

  // RowKey.getSaltBytes
  public static byte[] getSaltBytes(final int bucket) {
    final byte[] bytes = new byte[SALT_WIDTH];
    int shift = 0;
    for (int i = 1;i <= SALT_WIDTH; i++) {
      bytes[SALT_WIDTH - i] = (byte) (bucket >>> shift);
      shift += 8;
    }
    return bytes;
  }

  private static byte[] reIdKey(CassandraClient cass, byte[] key, Map<String, String> tags, String metricName) throws ConnectionException, Exception {
    final int tags_start = SALT_WIDTH + TSDB.metrics_width() +
        Const.TIMESTAMP_BYTES;

    final byte[] newKey = new byte[key.length];
    System.arraycopy(key, 0, newKey, 0, tags_start);

    int tagPos = tags_start;
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      String tagk = entry.getKey();
      String tagv = entry.getValue();

      final byte[] newTagk = cass.getOrCreateId(tagk.getBytes(), "tagk");
      final byte[] newTagv = cass.getOrCreateId(tagv.getBytes(), "tagv");

      System.arraycopy(newTagk, 0, newKey, tagPos, TSDB.tagk_width());
      tagPos += TSDB.tagk_width();
      System.arraycopy(newTagv, 0, newKey, tagPos, TSDB.tagv_width());
      tagPos += TSDB.tagv_width();
    }

    final byte[] newMetric = cass.getOrCreateId(metricName.getBytes(), "metrics");
    System.arraycopy(newMetric, 0, newKey, SALT_WIDTH, TSDB.metrics_width());

    return newKey;
  }

	public static String bytesToHex(byte[] in) {
			final StringBuilder sb = new StringBuilder();
			for(byte b : in) {
					sb.append(String.format("%02x", b));
			}
			return sb.toString();
	}

  private static final MaxSizeHashMap<ByteBuffer, Boolean> indexedKeys = new MaxSizeHashMap<ByteBuffer, Boolean>(30000);

  private static class MaxSizeHashMap<K, V> extends LinkedHashMap<K, V> {
    private static final long serialVersionUID = 1L;
    private final int maxSize;

    public MaxSizeHashMap(int maxSize) {
      this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() > maxSize;
    }
  }

}


