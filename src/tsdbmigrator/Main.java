
package tsdbmigrator;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
  public static final String cfIndex = "tindex";

  private static final Charset CHARSET = Charset.forName("ISO-8859-1");

	public static final String SCHEMA = String.format("CREATE TABLE %s.%s (" +
																										  "key blob, " +
																										  "column1 blob, " +
																										  "value blob, " +
                                                      "PRIMARY KEY (key, column1))" +
                                                      " WITH COMPACT STORAGE" +
                                                      " AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.DeflateCompressor'}" +
                                                      " ", keyspace, cf);

  public static final String INDEX_SCHEMA = String.format("CREATE TABLE %s.%s (" +
																										  "key blob, " +
																										  "column1 blob, " +
																										  "value blob, " +
                                                      "PRIMARY KEY (key, column1))" +
                                                      " WITH COMPACT STORAGE" +
                                                      " AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}" +
                                                      " ", keyspace, cfIndex);

  public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (key, column1, value) VALUES (?, ?, ?) USING TIMESTAMP ?", keyspace, cf);
  public static final String INDEX_INSERT_STMT = String.format("INSERT INTO %s.%s (key, column1, value) VALUES (?, ?, ?) USING TIMESTAMP ?", keyspace, cfIndex);
  // public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (key, column1, value) VALUES (?, ?, ?)", keyspace, cf);

  static CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
  static CQLSSTableWriter.Builder indexBuilder = CQLSSTableWriter.builder();
  static CQLSSTableWriter writer;
  static CQLSSTableWriter indexWriter;

  static TSDB tsdb;

  public static void main(String[] args) throws Exception {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    argp.addOption("--import", "Prints the rows in a format suitable for"
                   + " the 'import' command.");
    argp.addOption("--delete", "Deletes rows as they are scanned.");
    argp.addOption("--start", "START", "Start time.");
    argp.addOption("--stop", "STOP", "Stop time.");
    argp.addOption("--interval", "INTERVAL", "interval.");
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

    tsdb = new TSDB(config);

    // final CassandraClient cass = new CassandraClient(config);

    int start = Integer.parseInt(argp.get("--start", "0"));
    final int stop = Integer.parseInt(argp.get("--stop", "2114413200"));
    final int interval = Integer.parseInt(argp.get("--interval", "86400"));
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

    File outputDirIndex = new File(datadir + "/" + keyspace + "/" + cfIndex);
    if (!outputDirIndex.exists() && !outputDirIndex.mkdirs()) {
      throw new RuntimeException("Can't make output dir: " + outputDirIndex);
    }

    builder.inDirectory(outputDir).forTable(SCHEMA).using(INSERT_STMT).withPartitioner(new Murmur3Partitioner());
    indexBuilder.inDirectory(outputDirIndex).forTable(INDEX_SCHEMA).using(INDEX_INSERT_STMT).withPartitioner(new Murmur3Partitioner());
    // builder.withBufferSizeInMB(256);

    try {
      // migrateIds(tsdb, tsdb.getClient(), cass);
      int interstart = start - (start % interval);
      int interstop = Math.min((interstart + interval) - 1, stop);
      for (; interstop <= stop && interstart < stop; interstop+=interval, interstart+=interval) {
        writer = builder.build();
        indexWriter = indexBuilder.build();
        final int realstop = Math.min(interstop, stop);
        final int realstart = Math.max(interstart, start);
        LOG.warn("Starting time range " + realstart + "-" + realstop);
        for (String metric : metrics) {
          // LOG.warn("Starting metric " + metric + " on time range " + realstart + "-" + realstop);
          migrateData(tsdb, tsdb.getClient(), realstart, realstop, metric);
        }
        LOG.warn(dpCount + " datapoints created");
        dpCount = 0;
        index_cache = new HashMap<ByteBuffer, Boolean>(); // reset the cache
        writer.close();
        indexWriter.close();
      }
    } catch (Exception e) {
      LOG.error("Exception ", e);
      writer.close();
      indexWriter.close();
    } finally {
      tsdb.shutdown().joinUninterruptibly();
    }
  }

  static long dpCount = 0;


  public static void migrateData(TSDB tsdb, HBaseClient client, int start, int stop, String metric_name) throws Exception {
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
              sendDataPoint(tsdb, kv, base_time, metric);
            }

          }
        }
      }
  }

  static HashMap<ByteBuffer, Boolean> index_cache = new HashMap<ByteBuffer, Boolean>();

  static final byte[] tag_equals = "=".getBytes(CHARSET);
  static final byte[] tag_delim = ":".getBytes(CHARSET);

  static final ByteBuffer buf = ByteBuffer.allocate(2000);

  private static void tMutation(KeyValue kv, byte[] value) throws IOException {
    final int base_time = (int) Internal.baseTime(tsdb, kv.key());
    final String metric_name = Internal.metricName(tsdb, kv.key());
    final Map<String, String> tags = Internal.getTags(tsdb, kv.key());
    final short flags = Internal.getFlagsFromQualifier(kv.qualifier());
    final long timestamp = Internal.getTimestampFromQualifier(kv.qualifier(), (long) base_time) / 1000;

    buf.clear();
    boolean first = true;
    final List<String> sorted_tagks = new ArrayList<String>(tags.keySet());
    Collections.sort(sorted_tagks);
    for (final String tagk : sorted_tagks) {
      if (!first) {
        buf.put(tag_delim);
      }
      buf.put(tagk.getBytes(CHARSET));
      buf.put(tag_equals);
      buf.put(tags.get(tagk).getBytes(CHARSET));
      first = false;
    }

    final byte[] tag_bytes = new byte[buf.position()];
    buf.rewind();
    buf.get(tag_bytes);

    final int offset = (int) (timestamp - (timestamp % 2419200));

    final byte[] metric_bytes = metric_name.getBytes(CHARSET);
    final byte[] base_bytes = Bytes.fromInt(base_time);
    final byte[] new_key = new byte[metric_bytes.length + 1 + base_bytes.length + tag_bytes.length];
    System.arraycopy(metric_bytes, 0, new_key, 0, metric_bytes.length);
    System.arraycopy(base_bytes, 0, new_key, metric_bytes.length + 1, base_bytes.length);
    System.arraycopy(tag_bytes, 0, new_key, metric_bytes.length + 1 + base_bytes.length, tag_bytes.length);

    final byte[] new_qual = Bytes.fromInt((offset << 10) | flags);

    writer.addRow(ByteBuffer.wrap(new_key), ByteBuffer.wrap(new_qual), ByteBuffer.wrap(value), timestamp * 1000 * 1000);
    dpCount++;

    // TODO: We only need to check this once a month now.
    synchronized (indexedKeys) {
      if (indexedKeys.put(ByteBuffer.wrap(new_key), true) != null) {
        // We already indexed this key
        return;
      }
    }

    final byte[] index_key = Arrays.copyOfRange(new_key, 0, metric_bytes.length + 1 + base_bytes.length);
    indexWriter.addRow(ByteBuffer.wrap(index_key), ByteBuffer.wrap(tag_bytes), ByteBuffer.wrap(new byte[]{ 0 }), timestamp * 1000 * 1000);
  }



  private static void sendDataPoint(
      final TSDB tsdb,
      final KeyValue kv,
      final long base_time,
      final String metric) throws ConnectionException, Exception {

    final byte[] qualifier = kv.qualifier();
    final int q_len = qualifier.length;

    if (!AppendDataPoints.isAppendDataPoints(qualifier) && q_len % 2 != 0) {
      // custom data object, not a data point
    } else if (q_len == 2 || q_len == 4 && Internal.inMilliseconds(qualifier)) {
      // regular data point
      final Cell cell = Internal.parseSingleValue(kv);
      if (cell == null) {
        throw new IllegalDataException("Unable to parse row: " + kv);
      }
        tMutation(kv, cell.value());
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
        tMutation(kv, cell.value());
      }
    }
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


