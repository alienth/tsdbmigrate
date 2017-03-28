package tsdbmigrator;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.hbase.async.Bytes;
import org.hbase.async.Bytes.ByteMap;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnMap;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.retry.BoundedExponentialBackoff;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import net.opentsdb.core.IllegalDataException;
import net.opentsdb.uid.UniqueId;
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

  public static final ColumnFamily<byte[], String> TSDB_UID_NAME_CAS = 
      new ColumnFamily<byte[], String>(
      "name",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      StringSerializer.get());  // Column Serializer
  
  public static final ColumnFamily<byte[], String> TSDB_UID_ID_CAS = 
      new ColumnFamily<byte[], String>(
      "id",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      StringSerializer.get());  // Column Serializer

  public static final ColumnFamily<byte[], byte[]> TSDB_T_INDEX = new ColumnFamily<byte[], byte[]>(
      "tindex",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      BytesArraySerializer.get());  // Column Serializer

  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  final int lock_timeout = 5000;
  public static final byte[] EMPTY_ARRAY = new byte[0];

  /** Cache for forward mappings (name to ID). */
  private final HashMap<String, HashMap<String, byte[]>> name_cache =
    new HashMap<String, HashMap<String, byte[]>>();
  /** Cache for backward mappings (ID to name).
   * The ID in the key is a byte[] converted to a String to be Comparable. */
  private final HashMap<String, HashMap<String, String>> id_cache =
    new HashMap<String, HashMap<String, String>>();

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
      .setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_LOCAL_QUORUM);
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
    this.buffered_mutations = keyspace.prepareMutationBatch();

    tsdb_table = "tsdb".getBytes();
    tsdb_uid_table = "tsdbuid".getBytes();

    this.column_family_schemas.put("t".getBytes(), TSDB_T);
    this.column_family_schemas.put("name".getBytes(), TSDB_UID_NAME);
    this.column_family_schemas.put("id".getBytes(), TSDB_UID_ID);

    name_cache.put("tagk", new HashMap<String, byte[]>());
    name_cache.put("tagv", new HashMap<String, byte[]>());
    name_cache.put("metrics", new HashMap<String, byte[]>());

    id_cache.put("tagk", new HashMap<String, String>());
    id_cache.put("tagv", new HashMap<String, String>());
    id_cache.put("metrics", new HashMap<String, String>());

    ids.put("tagk", (long) 0);
    ids.put("tagv", (long) 0);
    ids.put("metrics", (long) 0);
  }


  final static byte[] idKey = { 0 };

  final static short id_width = 3;

  private HashMap<String, Long> ids = new HashMap<String, Long>();

  public byte[] getOrCreateId(byte[] name, String kind) throws ConnectionException, Exception {
    // Check if it is already in Cassandra, if so return it.
    //
    // If not, increment the 0x00 key with the proper qualifier to get the ID.
    // 
    // Write the resulting ID to both id and name cf.

    final byte[] id = getIdFromCache(kind, fromBytes(name));
    if (id != null) {
      return id;
    }

    byte[] idFromCass = getKeyValue(toBytes(kind), name, TSDB_UID_ID);
    if (idFromCass != null) {
      cacheMapping(kind, fromBytes(name), idFromCass);
      return idFromCass;
    }

    // System.out.println("Creating ID for " + fromBytes(name));


    long uid = atomicIncrement(kind);
    if (uid == 0) {
      final String message = "Unable to get ID for kind " + kind;
      throw new Exception(message);
    }

    byte[] rowKey = Bytes.fromLong(uid);

    // Verify that we're going to drop bytes that are 0.
    for (int i = 0; i < rowKey.length - id_width; i++) {
      if (rowKey[i] != 0) {
        final String message = "All Unique IDs for " + kind
          + " on " + id_width + " bytes are already assigned!";
        System.out.println("OMG " + message);
        throw new IllegalStateException(message);
      }
    }

    // Shrink the ID on the requested number of bytes.
    rowKey = Arrays.copyOfRange(rowKey, rowKey.length - id_width, rowKey.length);

    if (!compareAndSet(TSDB_UID_NAME_CAS, TSDB_UID_NAME, toBytes(kind), rowKey, name, EMPTY_ARRAY)) {
      final String message = "Unable to set name for " + fromBytes(name);
      throw new Exception(message);
    }

    if (!compareAndSet(TSDB_UID_ID_CAS, TSDB_UID_ID, toBytes(kind), name, rowKey, EMPTY_ARRAY)) {
      final String message = "Unable to set ID for " + fromBytes(name);
      throw new Exception(message);
    }

    cacheMapping(kind, fromBytes(name), rowKey);

    return rowKey;
  }

  public byte[] getKeyValue(byte[] key, byte[] column, ColumnFamily<byte[], byte[]> cf) throws ConnectionException {


    try {
      ColumnList<byte[]> result = keyspace.prepareQuery(cf)
        .getKey(key)
        .execute().getResult();

      for (Column<byte[]> c : result) {
        if (Bytes.memcmp(c.getName(), column) == 0) {
          return c.getByteArrayValue();
        }
      }
      return null;
    } catch (NotFoundException e) {
      return null;
    }

  }

  public static byte[] toBytes(final String s) {
    return s.getBytes(CHARSET);
  }

  public static String fromBytes(final byte[] b) {
    return new String(b, CHARSET);
  }

  /** Adds the bidirectional mapping in the cache. */
  private void cacheMapping(final String kind, final String name, final byte[] id) {
    addIdToCache(kind, name, id);
    addNameToCache(kind, id, name);
  } 
 
  private void addIdToCache(final String kind, final String name, final byte[] id) {
    byte[] found = name_cache.get(kind).get(name);
    if (found == null) {
      found = name_cache.get(kind).putIfAbsent(name,
                                    // Must make a defensive copy to be immune
                                    // to any changes the caller may do on the
                                    // array later on.
                                    Arrays.copyOf(id, id.length));
    }
    if (found != null && !Arrays.equals(found, id)) {
      throw new IllegalStateException("name=" + name + " => id="
          + Arrays.toString(id) + ", already mapped to "
          + Arrays.toString(found));
    }
  }

  private void addNameToCache(final String kind, final byte[] id, final String name) {
    final String key = fromBytes(id);
    String found = id_cache.get(kind).get(key);
    if (found == null) {
      found = id_cache.get(kind).putIfAbsent(key, name);
    }
    if (found != null && !found.equals(name)) {
      throw new IllegalStateException("id=" + Arrays.toString(id) + " => name="
          + name + ", already mapped to " + found);
    }
  }

  private byte[] getIdFromCache(final String kind, final String name) {
    return name_cache.get(kind).get(name);
  }

  private String getNameFromCache(final String kind, final byte[] id) {
    return id_cache.get(kind).get(fromBytes(id));
  }



  public long atomicIncrement(String kind) {

    ColumnPrefixDistributedRowLock<byte[]> lock = 
        new ColumnPrefixDistributedRowLock<byte[]>(keyspace, 
            TSDB_UID_ID_CAS, idKey)
            .withBackoff(new BoundedExponentialBackoff(250, 10000, 10))
            .withTtl(30)
            .expireLockAfter(lock_timeout, TimeUnit.MILLISECONDS);
    try {
      final ColumnMap<String> columns = lock.acquireLockAndReadRow();
              
      // Modify a value and add it to a batch mutation
      long value = 1;
      if (columns.get(kind) != null) {
        value = columns.get(kind).getLongValue() + 1;
      }
      final MutationBatch mutation = keyspace.prepareMutationBatch();
      mutation.setConsistencyLevel(ConsistencyLevel.CL_LOCAL_QUORUM);
      mutation.withRow(TSDB_UID_ID_CAS, idKey)
        .putColumn(kind, value, null);
      lock.releaseWithMutation(mutation);
      return value;
    } catch (Exception e) {
      try {
        lock.release();
      } catch (Exception e1) {
        System.out.println("Error releasing lock post exception for kind: " + kind + ". Error: " + e1);
      }
      
      return 0;
    }
  }


  public boolean compareAndSet(ColumnFamily<byte[], String> lockCf, ColumnFamily<byte[], byte[]> cf, byte[] key, byte[] column, byte[] v, byte[] expected) {
    
    ColumnPrefixDistributedRowLock<byte[]> lock = 
        new ColumnPrefixDistributedRowLock<byte[]>(keyspace, lockCf,
            column)
            .withBackoff(new BoundedExponentialBackoff(250, 10000, 10))
            .withConsistencyLevel(ConsistencyLevel.CL_LOCAL_QUORUM)
            .withTtl(30)
            .expireLockAfter(lock_timeout, TimeUnit.MILLISECONDS);
    try {
      lock.acquire();
      byte[] value = null;
      try {
        value = keyspace.prepareQuery(cf).getKey(key).getColumn(column).execute().getResult().getByteArrayValue();
      } catch (NotFoundException e) {
        // The common case - there is no value here.
      }
      final MutationBatch mutation = keyspace.prepareMutationBatch();
      mutation.setConsistencyLevel(ConsistencyLevel.CL_LOCAL_QUORUM);
      mutation.withRow(cf, key)
        .putColumn(column, v, null);
      
      if (value == null && (expected == null || expected.length < 1)) {
        // Have to separate the mutation and the release due to differing serialization types.
        mutation.execute();
        lock.release();
        return true;
      } else if (expected != null && value != null &&
          Bytes.memcmpMaybeNull(value, expected) == 0) {
        mutation.execute();
        lock.release();
        return true;
      }
      
      try {
        lock.release();
      } catch (Exception e) {
        System.out.println("Error releasing lock post exception for key: " + Bytes.pretty(key) + ". Err: " + e);
      }
      return false;
    } catch (Exception e) {
      try {
        lock.release();
      } catch (Exception e1) {
        System.out.println("Error releasing lock post exception for key: " + Bytes.pretty(key) + ". Err: " + e);
      }
      return false;
    }
  }

}
