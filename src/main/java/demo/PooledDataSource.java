package demo;

import demo.exception.PooledDataSourceException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ConnectionBuilder;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.ShardingKeyBuilder;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class PooledDataSource implements DataSource {
    private DataSource dataSource;
    public static final int POOL_SIZE = 10;
    private final Queue<Connection> connectionPool = new ConcurrentLinkedQueue<>();
    public PooledDataSource(String url, String user, String password) {
        for (int i = 0; i < POOL_SIZE; i++) {
            Connection connection;
            try {
                Connection realConnection = DriverManager.getConnection(url, user, password);
                connection = new ProxyConnection(realConnection, connectionPool);
            } catch (SQLException e) {
                throw new PooledDataSourceException("Can't create connection to DB", e);
            }
            connectionPool.add(connection);
        }
    }


    @Override
    public Connection getConnection()  {
        return connectionPool.poll();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return dataSource.getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return dataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        dataSource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        dataSource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return dataSource.getLoginTimeout();
    }

    @Override
    public ConnectionBuilder createConnectionBuilder() throws SQLException {
        return dataSource.createConnectionBuilder();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return dataSource.getParentLogger();
    }

    @Override
    public ShardingKeyBuilder createShardingKeyBuilder() throws SQLException {
        return dataSource.createShardingKeyBuilder();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return dataSource.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return dataSource.isWrapperFor(iface);
    }
}
