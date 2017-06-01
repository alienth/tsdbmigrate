
package tsdbmigrator;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashSet;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Aggregators;
import net.opentsdb.core.AppendDataPoints;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.Internal;
import net.opentsdb.core.Internal.Cell;
import net.opentsdb.core.Query;
import net.opentsdb.core.RateOptions;
import net.opentsdb.core.TSDB;
import net.opentsdb.tools.ArgP;
import net.opentsdb.uid.NoSuchUniqueName;

import com.microsoft.sqlserver.jdbc.*;

import java.sql.*;

import org.apache.commons.dbcp2.*;

// import org.jboss.netty.logging.InternalLoggerFactory;
// import org.jboss.netty.logging.Slf4JLoggerFactory;

final class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  // public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (key, column1, value) VALUES (?, ?, ?) USING TIMESTAMP ?", keyspace, cf);
  // public static final String INDEX_INSERT_STMT = String.format("INSERT INTO %s.%s (key, column1, value) VALUES (?, ?, ?) USING TIMESTAMP ?", keyspace, cfIndex);
  // public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (key, column1, value) VALUES (?, ?, ?)", keyspace, cf);

  static TSDB tsdb;

  static BasicDataSource connectionPool;

  static private Map<String, DatapointBatch> buffered_datapoint = new HashMap<String, DatapointBatch>();
  static private final AtomicLong num_buffered_pushes = new AtomicLong();

  static net.opentsdb.utils.Config config;


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
    config = CliOptions.getConfig(argp);

    tsdb = new TSDB(config);

    // final CassandraClient cass = new CassandraClient(config);

    int start = Integer.parseInt(argp.get("--start", "0"));
    final int stop = Integer.parseInt(argp.get("--stop", "2114413200"));
    final int interval = Integer.parseInt(argp.get("--interval", "86400"));
    final String metricsFileName = argp.get("--metrics", "");
    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
    argp = null;


    File metricsFile = new File(metricsFileName);
    java.util.Scanner sc = new java.util.Scanner(metricsFile);

    ArrayList<String> metrics = new ArrayList<String>();
    while (sc.hasNextLine()) {
      metrics.add(sc.nextLine());
    }
    sc.close();

    connectionPool = new BasicDataSource();
    connectionPool.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    connectionPool.setUrl(String.format("jdbc:sqlserver://%s;user=%s;password=%s", config.getString("sql.server"), config.getString("sql.user"), config.getString("sql.password")));
    connectionPool.setInitialSize(10);

    try {
      // migrateIds(tsdb, tsdb.getClient(), cass);
      int interstart = start - (start % interval);
      int interstop = Math.min((interstart + interval) - 1, stop);
      for (; interstop <= stop && interstart < stop; interstop+=interval, interstart+=interval) {
        final int realstop = Math.min(interstop, stop);
        final int realstart = Math.max(interstart, start);
        LOG.warn("Starting time range " + realstart + "-" + realstop);
        for (String metric : metrics) {
          // LOG.warn("Starting metric " + metric + " on time range " + realstart + "-" + realstop);
          migrateData(tsdb, tsdb.getClient(), realstart, realstop, metric);
        }
        LOG.warn(dpCount + " datapoints created");
        dpCount = 0;
      }
    } catch (Exception e) {
      LOG.error("Exception ", e);
    } finally {
      insertInternal(buffered_datapoint);
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

  static private HashMap<String, Set<String>> tables = new HashMap<String, Set<String>>();

  static final ByteBuffer buf = ByteBuffer.allocate(20000);

  private static void tMutation(KeyValue kv, byte[] qual, Number value) throws SQLException {
    int base_time = (int) Internal.baseTime(tsdb, kv.key());
    final String metric = Internal.metricName(tsdb, kv.key());
    final Map<String, String> tagm = Internal.getTags(tsdb, kv.key());
    // final short flags = Internal.getFlagsFromQualifier(qual);
    final long timestamp = Internal.getTimestampFromQualifier(qual, (long) base_time);

    final Set<String> tagSet = tagm.keySet();

    Set<String> tableTags;
    synchronized(tables) {
      tableTags = tables.get(metric);
    }
    if (tableTags == null || ! tableTags.containsAll(tagSet)) {
      try (Connection connection = connectionPool.getConnection()) {
        LOG.warn("Syncing schema for metric " + metric);
        syncSchema(connection, metric, tagSet);
      }
    }

    synchronized (buffered_datapoint) {
      DatapointBatch dps = buffered_datapoint.get(metric);
      if (dps == null) {
        dps = new DatapointBatch();
        buffered_datapoint.put(metric, dps);
      }
      Datapoint dp = new Datapoint(metric, tagm, timestamp, value.doubleValue());
      dps.add(dp);
      dpCount++;
      if (num_buffered_pushes.incrementAndGet() > config.getInt("sql.batchsize")) {
        num_buffered_pushes.set(0);
        insertInternal(buffered_datapoint);
        buffered_datapoint = new HashMap<String, DatapointBatch>();
      }
    }

  }

  public static void insertInternal(Map<String, DatapointBatch> batches) {
    try (Connection connection = DriverManager.getConnection(connectionPool.getUrl())) {
    // try (Connection connection = connectionPool.getConnection()) {

      SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection);
      for (Entry<String, DatapointBatch> entry : batches.entrySet()) {

				// for (Datapoint dp : entry.getValue().dps) {
					// final StringBuilder columns = new StringBuilder(100);
					// final StringBuilder values = new StringBuilder(20);
          // final String[] tagKeys = new String[dp.tagm.size()];
          // dp.tagm.keySet().toArray(tagKeys);
					// for (String key : tagKeys) {
					// 	columns.append("[tag.");
					// 	columns.append(key);
					// 	columns.append("], ");
					// 	values.append("?,");
					// }
					// columns.append("timestamp, value");
					// values.append(" ?, ?");

					// // TODO: prevent injection
					// String insert = String.format("INSERT INTO [dbo].[%s] (%s) VALUES (%s)", dp.metric, columns, values);
					// PreparedStatement prep = connection.prepareStatement(insert);
					// // stmt.setString(1, metric);

					// final int tagCount = dp.tagm.size();
					// for (int i = 0; i < tagCount; i++) {
					// 	prep.setString(i+1, dp.tagm.get(tagKeys[i]));
					// }
					// prep.setTimestamp(tagCount+1, new Timestamp(dp.timestamp));
					// prep.setDouble(tagCount+2, dp.value);
					// prep.executeUpdate();
					// // stmt.setDate(request.t
					// // Statement stmt = connection.createStatement();
				// }
			// }



        LOG.warn("Pushing datapoints for " + entry.getKey());
        bulkCopy.addColumnMapping("value", "value");
        bulkCopy.addColumnMapping("timestamp", "timestamp");
        for (String tag : entry.getValue().tags) {
          bulkCopy.addColumnMapping(tag, "tag." + tag);
        }
        bulkCopy.setDestinationTableName("[metrics]." + "[" + entry.getKey() + "]");
        bulkCopy.writeToServer(entry.getValue());
        bulkCopy.clearColumnMappings();
      }
      bulkCopy.close();
    } catch (Exception e) {
      LOG.warn("Exception pushing batch: " + e);
    }
  }

  public static void syncSchema(Connection connection, String metric, Set<String> tags) throws SQLException {
    Statement stmt;
    DatabaseMetaData md = connection.getMetaData();
    ResultSet rs = md.getTables(null, "metrics", metric, new String[] {"TABLE"});
    if (!rs.next()) {
      final StringBuilder columnDefs = new StringBuilder(100);
      for (String key : tags) {
        columnDefs.append("[tag.");
        columnDefs.append(key);
        columnDefs.append("] nvarchar(100) NULL,");
      }
      columnDefs.append("timestamp datetime NOT NULL, value float NOT NULL");

      String create = String.format("CREATE TABLE [metrics].[%s] (%s);", metric, columnDefs);
      String index = String.format("CREATE Clustered Columnstore Index [CCI_%s] ON [metrics].[%s];", metric, metric);

      LOG.warn("Creating table " + metric);
      stmt = connection.createStatement();
      stmt.executeUpdate(create);
      stmt = connection.createStatement();
      stmt.executeUpdate(index);

      Set<String> foo = new HashSet<String>(tags);
      synchronized (tables) {
        Set<String> existing = tables.get(metric);
        if (existing != null) {
          foo.addAll(existing);
        }
        tables.put(metric, foo);
      }
    } else {
      rs = md.getColumns(null, null, metric, "tag.%");

      final Set<String> columnSet = new HashSet<String>();
      while (rs.next()) {
        ResultSetMetaData rsMeta = rs.getMetaData();
        for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
          String name = rs.getString(i);
          if (name != null && name.startsWith("tag.")) {
            columnSet.add(rs.getString(i).replaceFirst("^tag\\.", ""));
          }
        }
      }

      Set<String> foo = new HashSet<String>(tags);
      foo.removeAll(columnSet);

      if (foo.size() > 0) {
        final StringBuilder alter = new StringBuilder(200);
        alter.append(String.format("ALTER TABLE [metrics].[%s] ADD ", metric));
        for (String column : foo) {
          alter.append(String.format(" [tag.%s] nvarchar(100) NULL,", column));
        }
        alter.setLength(alter.length() - 1);

        LOG.warn("Altering table " + metric);
        stmt = connection.createStatement();
        LOG.warn(alter.toString());
        stmt.executeUpdate(alter.toString());
      }
      Set<String> foo2 = new HashSet<String>(tags);
      synchronized (tables) {
        Set<String> existing = tables.get(metric);
        if (existing != null) {
          foo2.addAll(existing);
        }
        tables.put(metric, foo2);
      }
    }



  }



  private static void sendDataPoint(
      final TSDB tsdb,
      final KeyValue kv,
      final long base_time,
      final String metric) throws SQLException {

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
        tMutation(kv, kv.qualifier(), cell.parseValue());
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
        tMutation(kv, cell.qualifier(), cell.parseValue());
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


