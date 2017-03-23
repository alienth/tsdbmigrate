package tsdbmigrator;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.RowQuery;
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

  private static final Charset CHARSET = Charset.forName("ISO-8859-1");


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

  public byte[] getOrCreateId(byte[] name, String kind) throws ConnectionException {
    // Check if it is already in Cassandra, if so return it.
    //
    // If not, increment the 0x00 key with the proper qualifier to get the ID.
    // 
    // Write the resulting ID to both id and name cf.

    final byte[] id = getIdFromCache(kind, fromBytes(name));
    if (id != null) {
      return id;
    }

    // byte[] idFromCass = getKeyValue(name, kind, TSDB_UID_ID_CAS);
    // if (idFromCass != null) {
    //   return idFromCass;
    // }

    byte[] rowKey = Bytes.fromLong((long) 1);

    long uid = ids.get(kind);
    uid++;
    ids.put(kind, uid);
    rowKey = Bytes.fromLong(uid);

    // byte[] storedId = getKeyValue(idKey, kind, TSDB_UID_ID_CAS);
    // if (storedId != null) {
    //   long uidLong = UniqueId.uidToLong(storedId, id_width);
    //   uidLong++;
    //   rowKey = Bytes.fromLong(uidLong);
    // }

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

    this.buffered_mutations.withRow(TSDB_UID_ID_CAS, idKey)
      .putColumn(kind, rowKey);

    this.buffered_mutations.withRow(TSDB_UID_ID_CAS, name)
      .putColumn(kind, rowKey);

    this.buffered_mutations.withRow(TSDB_UID_NAME_CAS, rowKey)
      .putColumn(kind, name);

    cacheMapping(kind, fromBytes(name), rowKey);

    return rowKey;
  }

  public byte[] getKeyValue(byte[] key, String column, ColumnFamily<byte[], String> cf) throws ConnectionException {


    try {
      ColumnList<String> result = keyspace.prepareQuery(cf)
        .getKey(key)
        .execute().getResult();

      for (Column<String> c : result) {
        if (c.getName().equals(column)) {
          return c.getByteArrayValue();
        }
      }
      return null;
    } catch (NotFoundException e) {
      return null;
    }

  }

  private static byte[] toBytes(final String s) {
    return s.getBytes(CHARSET);
  }

  private static String fromBytes(final byte[] b) {
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




}
