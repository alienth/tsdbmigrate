
package tsdbmigrator;

import java.util.HashMap;

import org.hbase.async.HBaseClient;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.Query;
import net.opentsdb.core.RateOptions;
import net.opentsdb.tools.ArgP;
import net.opentsdb.utils.Config;

// import org.jboss.netty.logging.InternalLoggerFactory;
// import org.jboss.netty.logging.Slf4JLoggerFactory;

final class TSDBImporter {
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
    } else if (args.length < 3) {
      usage(argp, "Not enough arguments.", 2);
    }

    // get a config object
    Config config = CliOptions.getConfig(argp);

    final TSDB tsdb = new TSDB(config);

    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
    final byte[] table = config.getString("tsd.storage.hbase.data_table").getBytes();
    argp = null;
    try {
      migrateData(tsdb, tsdb.getClient(), table);
    } finally {
      tsdb.shutdown().joinUninterruptibly();
    }
  }

  public static void migrateData(TSDB tsdb, HBaseClient client, byte[] table) { 
    Query query = tsdb.newQuery();

    RateOptions rate_options = new RateOptions(false, Long.MAX_VALUE,
        RateOptions.DEFAULT_RESET_VALUE);
    final HashMap<String, String> tags = new HashMap<String, String>();
    query.setStartTime(0);
    query.setTimeSeries("os.cpu", tags, Aggregators.get("sum"), false, rate_options);
    // Get all keys
    // Iterate through each KV
    // Build a cassandra-friendly key
    // Write out a mutation

  }
}


