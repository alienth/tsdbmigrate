package tsdbmigrator;

import org.hbase.async.Bytes.ByteMap;

// import org.apache.cassandra.thrift.Cassandra;
import com.netflix.astyanax.AstyanaxContext;
// import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
// import com.netflix.astyanax.connectionpool.OperationResult;
// import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
// import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
// import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
// import com.netflix.astyanax.model.ColumnList;
// import com.netflix.astyanax.model.ColumnMap;
import com.netflix.astyanax.model.ConsistencyLevel;
// import com.netflix.astyanax.query.RowQuery;
// import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
// import com.netflix.astyanax.retry.BoundedExponentialBackoff;
import com.netflix.astyanax.serializers.BytesArraySerializer;
// import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import net.opentsdb.utils.Config;



final class CassandraClient {
  public static final ColumnFamily<byte[], byte[]> TSDB_T = new ColumnFamily<byte[], byte[]>(
      "t",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      BytesArraySerializer.get());  // Column Serializer

  public static final ColumnFamily<byte[], byte[]> TSDB_UID_NAME = new ColumnFamily<byte[], byte[]>(
      "name",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      BytesArraySerializer.get());  // Column Serializer

  public static final ColumnFamily<byte[], byte[]> TSDB_UID_ID = new ColumnFamily<byte[], byte[]>(
      "id",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      BytesArraySerializer.get());  // Column Serializer


  final AstyanaxConfigurationImpl ast_config;
  final ConnectionPoolConfigurationImpl pool;
  final AstyanaxContext<Keyspace> context;
  final Keyspace keyspace;
  public MutationBatch buffered_mutations;

  final byte[] tsdb_table;
  final byte[] tsdb_uid_table;

  public final ByteMap<ColumnFamily<byte[], byte[]>> column_family_schemas = 
      new ByteMap<ColumnFamily<byte[], byte[]>>();
 

  public CassandraClient(final Config config) {
    ast_config = new AstyanaxConfigurationImpl()
      .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
      .setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_ANY);
    pool = new ConnectionPoolConfigurationImpl("MyConnectionPool")
      .setPort(config.getInt("asynccassandra.port"))
      .setMaxConnsPerHost(config.getInt("asynccassandra.max_conns_per_host"))
      .setSocketTimeout(60000)
      .setSeeds(config.getString("asynccassandra.seeds"));
    if (config.hasProperty("asynccassandra.datacenter")) {
      pool.setLocalDatacenter(config.getString("asynccassandra.datacenter"));
    }
    context = new AstyanaxContext.Builder()
      .forCluster(config.getString("asynccassandra.cluster"))
      .forKeyspace(config.getString("asynccassandra.keyspace"))
      .withAstyanaxConfiguration(ast_config)
      .withConnectionPoolConfiguration(pool)
      .buildKeyspace(ThriftFamilyFactory.getInstance());

    keyspace = context.getClient();
    context.start();
    buffered_mutations = keyspace.prepareMutationBatch();

    tsdb_table = config.getString("tsdb").getBytes();
    tsdb_uid_table = config.getString("tsdbuid").getBytes();

    this.column_family_schemas.put("t".getBytes(), TSDB_T);
    this.column_family_schemas.put("name".getBytes(), TSDB_UID_NAME);
    this.column_family_schemas.put("id".getBytes(), TSDB_UID_ID);
  }

}
