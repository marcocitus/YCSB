/**
 * Copyright (c) 2010 - 2016 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying 
 * LICENSE file. 
 */
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.sql.*;
import java.util.*;

import org.apache.commons.lang.StringEscapeUtils;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced
 * with YCSB. This class extends {@link DB} and implements the database
 * interface used by YCSB client.
 * 
 * <br>
 * Each client will have its own instance of this class. This client is not
 * thread safe.
 * 
 * <br>
 * This interface expects a schema <key> <field1> <field2> <field3> ... All
 * attributes are of type VARCHAR. All accesses are through the primary key.
 * Therefore, only one index on the primary key is needed.
 */
public class JdbcDBClient extends DB {

  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "db.url";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "db.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "db.passwd";

  /** Whether CitusDB is used. */
  public static final String CITUS_ENABLED = "citus";

  /** The JDBC fetch size hinted to the driver. */
  public static final String JDBC_FETCH_SIZE = "jdbc.fetchsize";

  /** The JDBC connection auto-commit property for the driver. */
  public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";

  /** The name of the property for the number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /** Default number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  /** Representing a NULL value. */
  public static final String NULL_VALUE = "NULL";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_PREFIX = "FIELD";

  private ArrayList<Connection> conns;
  private boolean initialized = false;
  private Properties props;
  private Integer jdbcFetchSize;
  private static final String DEFAULT_PROP = "";
  private boolean isCitus = false;

  /**
   * The statement type for the prepared statements.
   */
  private static class StatementType {

    enum Type {
      INSERT(1), DELETE(2), READ(3), UPDATE(4), SCAN(5);

      private final int internalType;

      private Type(int type) {
        internalType = type;
      }

      int getHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + internalType;
        return result;
      }
    }

    private Type type;
    private int shardIndex;
    private int numFields;
    private String tableName;

    StatementType(Type type, String tableName, int numFields, int shardIndex) {
      this.type = type;
      this.tableName = tableName;
      this.numFields = numFields;
      this.shardIndex = shardIndex;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + numFields + 100 * shardIndex;
      result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
      result = prime * result + ((type == null) ? 0 : type.getHashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      StatementType other = (StatementType) obj;
      if (numFields != other.numFields) {
        return false;
      }
      if (shardIndex != other.shardIndex) {
        return false;
      }
      if (tableName == null) {
        if (other.tableName != null) {
          return false;
        }
      } else if (!tableName.equals(other.tableName)) {
        return false;
      }
      if (type != other.type) {
        return false;
      }
      return true;
    }
  }

  /**
   * For the given key, returns what shard contains data for this key.
   *
   * @param key Data key to do operation on
   * @return Shard index
   */
  private int getShardIndexByKey(String key) {
    int ret = Math.abs(key.hashCode()) % conns.size();
    return ret;
  }

  /**
   * For the given key, returns Connection object that holds connection to the
   * shard that contains this key.
   *
   * @param key Data key to get information for
   * @return Connection object
   */
  private Connection getShardConnectionByKey(String key) {
    return conns.get(getShardIndexByKey(key));
  }

  private void cleanupAllConnections() throws SQLException {
    for (Connection conn : conns) {
      conn.close();
    }
  }

  @Override
  public void init() throws DBException {
    if (initialized) {
      System.err.println("Client connection already initialized.");
      return;
    }
    props = getProperties();
    String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    String driver = props.getProperty(DRIVER_CLASS);

    String jdbcFetchSizeStr = props.getProperty(JDBC_FETCH_SIZE);
    if (jdbcFetchSizeStr != null) {
      try {
        this.jdbcFetchSize = Integer.parseInt(jdbcFetchSizeStr);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid JDBC fetch size specified: " + jdbcFetchSizeStr);
        throw new DBException(nfe);
      }
    }

    String autoCommitStr = props.getProperty(JDBC_AUTO_COMMIT, Boolean.TRUE.toString());
    Boolean autoCommit = Boolean.parseBoolean(autoCommitStr);

    String isCitusStr = props.getProperty(CITUS_ENABLED, Boolean.FALSE.toString());
    isCitus = Boolean.parseBoolean(isCitusStr);

    try {
      if (driver != null) {
        Class.forName(driver);
      }
      int shardCount = 0;
      conns = new ArrayList<Connection>(3);
      for (String url : urls.split(",")) {
        System.out.println("Adding shard node URL: " + url);
        Connection conn = DriverManager.getConnection(url, user, passwd);

        // Since there is no explicit commit method in the DB interface, all
        // operations should auto commit, except when explicitly told not to
        // (this is necessary in cases such as for PostgreSQL when running a
        // scan workload with fetchSize)
        conn.setAutoCommit(autoCommit);

        if (isCitus) {
          Statement stmt = conn.createStatement();
          stmt.execute("SET citusdb.task_executor_type TO 'router'");
        }

        shardCount++;
        conns.add(conn);
      }

      System.out.println("Using " + shardCount + " shards");

    } catch (ClassNotFoundException e) {
      System.err.println("Error in initializing the JDBS driver: " + e);
      throw new DBException(e);
    } catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      System.err.println("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    }
    initialized = true;
  }

  @Override
  public void cleanup() throws DBException {
    try {
      cleanupAllConnections();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String tableName, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try {
      StringBuilder read = new StringBuilder("SELECT * FROM ");
      read.append(tableName);
      read.append(" WHERE ");
      read.append(PRIMARY_KEY);
      read.append(" = ");
      read.append("'");
      read.append(StringEscapeUtils.escapeSql(key));
      read.append("'");
      Statement readStatement = getShardConnectionByKey(key).createStatement();
      ResultSet resultSet = readStatement.executeQuery(read.toString());
      if (!resultSet.next()) {
        resultSet.close();
        return Status.NOT_FOUND;
      }
      if (result != null && fields != null) {
        for (String field : fields) {
          String value = resultSet.getString(field);
          result.put(field, new StringByteIterator(value));
        }
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing read of table " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      Connection connection = getShardConnectionByKey(startKey);

      if (isCitus) {
        Statement stmt = connection.createStatement();
        stmt.execute("SET citusdb.task_executor_type TO 'real-time'");
      }
      StringBuilder select = new StringBuilder("SELECT * FROM ");
      select.append(tableName);
      select.append(" WHERE ");
      select.append(PRIMARY_KEY);
      select.append(" >= ");
      select.append("'");
      select.append(StringEscapeUtils.escapeSql(startKey));
      select.append("'");
      select.append(" ORDER BY ");
      select.append(PRIMARY_KEY);
      select.append(" LIMIT ");
      select.append(recordcount);
      Statement scanStatement = connection.createStatement();
      ResultSet resultSet = scanStatement.executeQuery(select.toString());
      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = resultSet.getString(field);
            values.put(field, new StringByteIterator(value));
          }
          result.add(values);
        }
      }
      resultSet.close();
      if (isCitus) {
        Statement stmt = connection.createStatement();
        stmt.execute("SET citusdb.task_executor_type TO 'router'");
      }
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing scan of table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key, HashMap<String, ByteIterator> values) {
    try {
      int i =0;
      StringBuilder update = new StringBuilder("UPDATE ");
      update.append(tableName);
      update.append(" SET ");
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        String field = entry.getValue().toString();
        if (i > 0) {
          update.append(", ");
        }
        update.append(COLUMN_PREFIX);
        update.append(i);
        update.append("=");
        update.append("'");
        update.append(StringEscapeUtils.escapeSql(field));
        update.append("'");
        i++;
      }
      update.append(" WHERE ");
      update.append(PRIMARY_KEY);
      update.append(" = ");
      update.append("'");
      update.append(StringEscapeUtils.escapeSql(key));
      update.append("'");

      Statement updateStatement = getShardConnectionByKey(key).createStatement();
      int result = updateStatement.executeUpdate(update.toString());
      if (result == 1) {
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing update to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String tableName, String key, HashMap<String, ByteIterator> values) {
    try {
      StringBuilder insert = new StringBuilder("INSERT INTO ");
      insert.append(tableName);
      insert.append(" VALUES(");
      insert.append("'");
      insert.append(StringEscapeUtils.escapeSql(key));
      insert.append("'");
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        String field = entry.getValue().toString();
        insert.append(",");
        insert.append("'");
        insert.append(StringEscapeUtils.escapeSql(field));
        insert.append("'");
      }
      insert.append(")");
      Statement insertStatement = getShardConnectionByKey(key).createStatement();
      int result = insertStatement.executeUpdate(insert.toString());
      if (result == 1) {
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing insert to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try {
      StringBuilder delete = new StringBuilder("DELETE FROM ");
      delete.append(tableName);
      delete.append(" WHERE ");
      delete.append(PRIMARY_KEY);
      delete.append(" = ");
      delete.append("'");
      delete.append(StringEscapeUtils.escapeSql(key));
      delete.append("'");
      Statement deleteStatement = getShardConnectionByKey(key).createStatement();
      int result = deleteStatement.executeUpdate(delete.toString());
      if (result == 1) {
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }
}
