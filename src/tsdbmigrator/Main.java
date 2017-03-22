
package tsdbmigrator;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;

import net.opentsdb.core.Aggregators;
import net.opentsdb.core.AppendDataPoints;
import net.opentsdb.core.Const;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.Internal;
import net.opentsdb.core.Internal.Cell;
import net.opentsdb.core.Query;
import net.opentsdb.core.RateOptions;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.tools.ArgP;
import net.opentsdb.utils.Config;

// import org.jboss.netty.logging.InternalLoggerFactory;
// import org.jboss.netty.logging.Slf4JLoggerFactory;

final class Main {
  /** Prints usage and exits with the given retval. */
  private static void usage(final ArgP argp, final String errmsg,
                            final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: scan"
        + " [--delete|--import] START-DATE [END-DATE] query [queries...]\n"
        + "To see the format in which queries should be written, see the help"
        + " of the 'query' command.\n"
        + "The --import flag changes the format in which the output is printed"
        + " to use a format suiteable for the 'import' command instead of the"
        + " default output format, which better represents how the data is"
        + " stored in HBase.\n"
        + "The --delete flag will delete every row matched by the query."
        + "  This flag implies --import.");
    System.err.print(argp.usage());
    System.exit(retval);
  }

  public static void main(String[] args) throws Exception {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    argp.addOption("--import", "Prints the rows in a format suitable for"
                   + " the 'import' command.");
    argp.addOption("--delete", "Deletes rows as they are scanned.");
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, "Invalid usage.", 1);
    }// else if (args.length < 3) {
      // usage(argp, "Not enough arguments.", 2);
    // }

    // get a config object
    Config config = CliOptions.getConfig(argp);

    final TSDB tsdb = new TSDB(config);

    final CassandraClient cass = new CassandraClient(config);

    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
    final byte[] table = config.getString("tsd.storage.hbase.data_table").getBytes();
    argp = null;
    try {
      migrateData(tsdb, tsdb.getClient(), cass, table);
    } finally {
      tsdb.shutdown().joinUninterruptibly();
    }
  }

  public static void migrateData(TSDB tsdb, HBaseClient client, CassandraClient cass, byte[] table) throws Exception {
    // Get all keys
    // Iterate through each KV
    // Build a cassandra-friendly key
    // Write out a mutation


    Query query = tsdb.newQuery();

    RateOptions rate_options = new RateOptions(false, Long.MAX_VALUE,
        RateOptions.DEFAULT_RESET_VALUE);
    final HashMap<String, String> tags = new HashMap<String, String>();
    query.setStartTime(0);
    query.setTimeSeries("os.cpu", tags, Aggregators.get("sum"), false, rate_options);

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
            // Print the row key.
              buf.append(Arrays.toString(key))
                      .append(' ')
                      .append(metric)
                      .append(' ')
                      .append(base_time)
                      .append(" (").append(base_time).append(") ");
              try {
                buf.append(Internal.getTags(tsdb, key));
              } catch (RuntimeException e) {
                buf.append(e.getClass().getName() + ": " + e.getMessage());
              }
              buf.append('\n');

            // Print individual cells.
            //buf.setLength(0);
            for (final KeyValue kv : row) {
              sendDataPoint(buf, tsdb, cass, kv, base_time, metric);
              System.out.print("" + cass.buffered_mutations.getRowCount() + "\n");
              if (cass.buffered_mutations.getRowCount() > 500) {
                cass.buffered_mutations.execute();
                cass.buffered_mutations.discardMutations();
                // System.out.print(buf);
              }

              // Discard everything or keep initial spaces.
              // formatKeyValue(buf, tsdb, false, kv, base_time, metric);
              // if (buf.length() > 0) {
              //     buf.append('\n');
              //   System.out.print(buf);
              // }
            }

          }
        }
      }
  }
  static void formatKeyValue(final StringBuilder buf,
                             final TSDB tsdb,
                             final KeyValue kv,
                             final long base_time) {
    formatKeyValue(buf, tsdb, true, kv, base_time,
                   Internal.metricName(tsdb, kv.key()));
  }

  private static void sendDataPoint(final StringBuilder buf,
      final TSDB tsdb,
      final CassandraClient cass,
      final KeyValue kv,
      final long base_time,
      final String metric) {

    MutationBatch mutation = cass.buffered_mutations;
    final byte[] qualifier = kv.qualifier();
    // final byte[] value = kv.value();
    final int q_len = qualifier.length;
    final ColumnFamily<byte[], byte[]> cf = cass.column_family_schemas.get("t");

    if (!AppendDataPoints.isAppendDataPoints(qualifier) && q_len % 2 != 0) {
      // custom data object, not a data point
    } else if (q_len == 2 || q_len == 4 && Internal.inMilliseconds(qualifier)) {
      // regular data point
      final Cell cell = Internal.parseSingleValue(kv);
      if (cell == null) {
        throw new IllegalDataException("Unable to parse row: " + kv);
      }
        mutation.withRow(cf, kv.key())
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
            buf.append("\n");
            mutation.withRow(cf, kv.key())
                    .putColumn(cell.qualifier(), cell.value());
          }
        i++;
      }


      }
  }

  private static void formatKeyValue(final StringBuilder buf,
      final TSDB tsdb,
      final boolean importformat,
      final KeyValue kv,
      final long base_time,
      final String metric) {

    final String tags;
    if (importformat) {
      final StringBuilder tagsbuf = new StringBuilder();
      for (final Map.Entry<String, String> tag
           : Internal.getTags(tsdb, kv.key()).entrySet()) {
        tagsbuf.append(' ').append(tag.getKey())
          .append('=').append(tag.getValue());
      }
      tags = tagsbuf.toString();
    } else {
      tags = null;
    }

    final byte[] qualifier = kv.qualifier();
    final byte[] value = kv.value();
    final int q_len = qualifier.length;

    if (!AppendDataPoints.isAppendDataPoints(qualifier) && q_len % 2 != 0) {
      if (!importformat) {
        // custom data object, not a data point
        if (kv.qualifier()[0] == Annotation.PREFIX()) {
          appendAnnotation(buf, kv, base_time);
        } else {
          buf.append(Arrays.toString(value))
            .append("\t[Not a data point]");
        }
      }
    } else if (q_len == 2 || q_len == 4 && Internal.inMilliseconds(qualifier)) {
      // regular data point
      final Cell cell = Internal.parseSingleValue(kv);
      if (cell == null) {
        throw new IllegalDataException("Unable to parse row: " + kv);
      }
      if (!importformat) {
        appendRawCell(buf, cell, base_time);
      } else {
        buf.append(metric).append(' ');
        appendImportCell(buf, cell, base_time, tags);
      }
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

      if (!importformat) {
        buf.append(Arrays.toString(kv.qualifier()))
           .append('\t')
           .append(Arrays.toString(kv.value()))
           .append(" = ")
           .append(cells.size())
           .append(" values:");
      }

      int i = 0;
      for (Cell cell : cells) {
        if (!importformat) {
          buf.append("\n    ");
          appendRawCell(buf, cell, base_time);
        } else {
          buf.append(metric).append(' ');
          appendImportCell(buf, cell, base_time, tags);
          if (i < cells.size() - 1) {
            buf.append("\n");
          }
        }
        i++;
      }
    }
  }

  static void appendAnnotation(final StringBuilder buf, final KeyValue kv,
      final long base_time) {
    final long timestamp =
        Internal.getTimestampFromQualifier(kv.qualifier(), base_time);
    buf.append(Arrays.toString(kv.qualifier()))
    .append("\t")
    .append(Arrays.toString(kv.value()))
    .append("\t")
    .append(Internal.getOffsetFromQualifier(kv.qualifier(), 1) / 1000)
    .append("\t")
    .append(new String(kv.value(), Charset.forName("ISO-8859-1")))
    .append("\t")
    .append(timestamp)
    .append("\t")
    .append("(")
    .append(timestamp)
    .append(")");
  }


  static void appendRawCell(final StringBuilder buf, final Cell cell,
      final long base_time) {
    final long timestamp = cell.absoluteTimestamp(base_time);
    buf.append(Arrays.toString(cell.qualifier()))
    .append("\t")
    .append(Arrays.toString(cell.value()))
    .append("\t");
    if ((timestamp & Const.SECOND_MASK) != 0) {
      buf.append(Internal.getOffsetFromQualifier(cell.qualifier()));
    } else {
      buf.append(Internal.getOffsetFromQualifier(cell.qualifier()) / 1000);
    }
    buf.append("\t")
    .append(cell.isInteger() ? "l" : "f")
    .append("\t")
    .append(timestamp)
    .append("\t")
    .append("(")
    .append(timestamp)
    .append(")");
  }

  static void appendImportCell(final StringBuilder buf, final Cell cell,
      final long base_time, final String tags) {
    buf.append(cell.absoluteTimestamp(base_time))
    .append(" ")
    .append(cell.parseValue())
    .append(tags);
  }

}


