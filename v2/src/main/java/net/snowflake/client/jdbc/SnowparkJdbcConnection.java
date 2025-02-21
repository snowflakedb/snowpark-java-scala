package net.snowflake.client.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SnowparkJdbcConnection {
    private final SnowflakeConnectionV1 connection;

    public SnowparkJdbcConnection(SnowflakeConnectionV1 connection) {
        this.connection = connection;
    }

    public ResultSet executeQuery(byte[] protobuf) throws SQLException {
        return null;
    }
}
