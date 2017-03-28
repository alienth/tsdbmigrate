
package tsdbmigrator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
import net.opentsdb.utils.Config;

// import org.jboss.netty.logging.InternalLoggerFactory;
// import org.jboss.netty.logging.Slf4JLoggerFactory;

final class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    argp.addOption("--import", "Prints the rows in a format suitable for"
                   + " the 'import' command.");
    argp.addOption("--delete", "Deletes rows as they are scanned.");
    argp.addOption("--start", "START", "Start time.");
    argp.addOption("--stop", "STOP", "Stop time.");
    argp.addOption("--metrics", "os.cpu", "Metrics");
    args = CliOptions.parse(argp, args);
    if (args == null) {
      System.err.print("Invalid usage.");
      System.exit(-1);
    }

    // get a config object
    Config config = CliOptions.getConfig(argp);

    final TSDB tsdb = new TSDB(config);

    final CassandraClient cass = new CassandraClient(config);

    final int start = Integer.parseInt(argp.get("--start", "0"));
    final int stop = Integer.parseInt(argp.get("--stop", "2114413200"));
    final String[] metrics = argp.get("--metrics", "os.cpu").split(",");
    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
    argp = null;


    try {
      // migrateIds(tsdb, tsdb.getClient(), cass);
      for (String metric : metrics) {
        migrateData(tsdb, tsdb.getClient(), cass, start, stop, metric);
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
      System.out.println("Can't find metric. Skipping " + metric_name);
      return;
    }

    final StringBuilder buf = new StringBuilder();
    final List<Scanner> scanners = Internal.getScanners(query);
      for (Scanner scanner : scanners) {
        ArrayList<ArrayList<KeyValue>> rows;
        while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
          for (final ArrayList<KeyValue> row : rows) {
            buf.setLength(0);
            final byte[] key = row.get(0).key();
            final long base_time = Internal.baseTime(tsdb, key);
            final String metric = Internal.metricName(tsdb, key);

            for (final KeyValue kv : row) {
              sendDataPoint(buf, tsdb, cass, kv, base_time, metric);
              if (cass.buffered_mutations.getRowCount() > 500) {
                cass.buffered_mutations.execute();
                cass.buffered_mutations.discardMutations();
                System.out.print(buf);
              }

            }

          }
        }
      }

      if (cass.buffered_mutations.getRowCount() > 0) {
        cass.buffered_mutations.execute();
      }
    System.out.println("Done with metric " + metric_name);
  }

  static short METRICS_WIDTH = 3;
  static short TAG_NAME_WIDTH = 3;
  static short TAG_VALUE_WIDTH = 3;
  static final short TIMESTAMP_BYTES = 4;

  private static void indexMutation(byte[] orig_key, byte[] orig_column, MutationBatch mutation) {
    // Take the first 8 bytes of the orig key and put them in the new key.
    // Take the last 6 bytes of the orig key and all of the orig_column and put
    // them in the new column.
    //
    // Take the metric of the orig key and put it in the new key
    // Take the timestamp of the orig key, normalize it to a month, and put it in the new key
    // Take the timestamp of the orig key and put it in the column name.
    // Take only the tags from the orig key, and put them the column name.

    final byte[] ts = Arrays.copyOfRange(orig_key, SALT_WIDTH + METRICS_WIDTH, TIMESTAMP_BYTES + SALT_WIDTH + METRICS_WIDTH);
    final int tsInt = Bytes.getInt(ts);
    int month = tsInt - (tsInt % (86400 * 28));
    byte[] new_key = new byte[SALT_WIDTH + METRICS_WIDTH + TIMESTAMP_BYTES];
    byte[] new_col = new byte[orig_key.length - METRICS_WIDTH - SALT_WIDTH];
    System.arraycopy(orig_key, 0, new_key, 0, SALT_WIDTH + METRICS_WIDTH);
    System.arraycopy(Bytes.fromInt(month), 0, new_key, SALT_WIDTH + METRICS_WIDTH, TIMESTAMP_BYTES);
    System.arraycopy(ts, 0, new_col, 0, ts.length);
    System.arraycopy(orig_key, SALT_WIDTH + METRICS_WIDTH + TIMESTAMP_BYTES, new_col, TIMESTAMP_BYTES, new_col.length - TIMESTAMP_BYTES);

    // TODO - prevent duplicate puts here.
    mutation.withRow(CassandraClient.TSDB_T_INDEX, new_key).putColumn(new_col, new byte[]{0});
  }


  private static void sendDataPoint(final StringBuilder buf,
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
        mutation.withTimestamp(base_time * 1000 * 1000) // microseconds
                .withRow(cf, final_key)
                .putColumn(cell.qualifier(), cell.value());
        indexMutation(final_key, cell.qualifier(), mutation);
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
        mutation.withRow(cf, final_key)
                .putColumn(cell.qualifier(), cell.value());
        indexMutation(final_key, cell.qualifier(), mutation);
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

}


