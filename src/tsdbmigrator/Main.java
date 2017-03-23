
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
    args = CliOptions.parse(argp, args);
    if (args == null) {
      System.err.print("Invalid usage.");
      System.exit(-1);
    }

    // get a config object
    Config config = CliOptions.getConfig(argp);

    final TSDB tsdb = new TSDB(config);

    final CassandraClient cass = new CassandraClient(config);

    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
    argp = null;

    try {
      // migrateIds(tsdb, tsdb.getClient(), cass);
      migrateData(tsdb, tsdb.getClient(), cass, "os.cpu");
    } catch (Exception e) {
      LOG.error("Exception ", e);
    } finally {
      tsdb.shutdown().joinUninterruptibly();
    }
  }

  public static void migrateIds(TSDB tsdb, HBaseClient client, CassandraClient cass) throws Exception {
    final Scanner scanner = client.newScanner("tsdb-uid".getBytes());
    scanner.setMaxNumRows(1024);

    final MutationBatch mutation = cass.buffered_mutations;
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          // found |= printResult(row, CliUtils.ID_FAMILY, true);
          final Iterator<KeyValue> i = row.iterator();
          while (i.hasNext()) {
            final KeyValue kv = i.next();
            final ColumnFamily<byte[], byte[]> cf = cass.column_family_schemas.get(kv.family());

            mutation.withRow(cf, kv.key())
                    .putColumn(kv.qualifier(), kv.value());
          }

          if (mutation.getRowCount() > 5000) {
            mutation.execute();
            System.out.println("sent");
          }
        }
      }
    } catch (HBaseException e) {
      LOG.error("Error while scanning HBase, scanner=" + scanner, e);
      throw e;
    } catch (Exception e) {
      throw e;
    }

    if (mutation.getRowCount() > 0) {
      mutation.execute();
    }
  }



  public static void migrateData(TSDB tsdb, HBaseClient client, CassandraClient cass, String metric_name) throws Exception {
    Query query = tsdb.newQuery();

    RateOptions rate_options = new RateOptions(false, Long.MAX_VALUE,
        RateOptions.DEFAULT_RESET_VALUE);
    final HashMap<String, String> t = new HashMap<String, String>();
    query.setStartTime(0);
    query.setTimeSeries(metric_name, t, Aggregators.get("sum"), false, rate_options);

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
              System.out.print("" + cass.buffered_mutations.getRowCount() + "\n");
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
  }

  private static void sendDataPoint(final StringBuilder buf,
      final TSDB tsdb,
      final CassandraClient cass,
      final KeyValue kv,
      final long base_time,
      final String metric) throws ConnectionException {

    final MutationBatch mutation = cass.buffered_mutations;
    final byte[] qualifier = kv.qualifier();
    // final byte[] value = kv.value();
    final int q_len = qualifier.length;
    final ColumnFamily<byte[], byte[]> cf = cass.column_family_schemas.get("t".getBytes());

    final String metricName = Internal.metricName(tsdb, kv.key());

    final byte[] salted_key = saltKey(kv.key());
    final byte[] final_key = reIdKey(cass, salted_key, Internal.getTags(tsdb, kv.key()), metricName);

    System.out.print("orig:   " + Bytes.pretty(kv.key()) + " " + kv.key().length + "\n");
    System.out.print("final:  " + Bytes.pretty(final_key) + " " + final_key.length + "\n");

    if (!AppendDataPoints.isAppendDataPoints(qualifier) && q_len % 2 != 0) {
      // custom data object, not a data point
    } else if (q_len == 2 || q_len == 4 && Internal.inMilliseconds(qualifier)) {
      // regular data point
      final Cell cell = Internal.parseSingleValue(kv);
      if (cell == null) {
        throw new IllegalDataException("Unable to parse row: " + kv);
      }
        mutation.withRow(cf, final_key)
                .putColumn(cell.qualifier(), cell.value());
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

      int i = 0;
      for (Cell cell : cells) {
          if (i < cells.size() - 1) {
            mutation.withRow(cf, final_key)
                    .putColumn(cell.qualifier(), cell.value());
          }
        i++;
      }
    }
  }

  static int SALT_WIDTH = 1;
  static int SALT_BUCKETS = 20;

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

  private static byte[] reIdKey(CassandraClient cass, byte[] key, Map<String, String> tags, String metricName) throws ConnectionException {
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


