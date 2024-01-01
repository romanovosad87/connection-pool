package demo;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PostgresDbTest {
    public static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/postgres?currentSchema=test";
    public static final String POSTGRES_USER = "postgres";
    public static final String PASSWORD = System.getenv("DB_PASSWORD");
    public static final int DEFAULT_POOL_SIZE = 10;
    public static final int ROWS_SIZE = 100;
    private static PooledDataSource pooledDataSource;

    @BeforeAll
    static void beforeAll() {
        pooledDataSource = new PooledDataSource(POSTGRES_URL, POSTGRES_USER, PASSWORD);
        createTable();
    }

    @Order(1)
    @Test
    @DisplayName("quantity of connections should be less or equals pool size that is 10")
    void shouldUseConnectionsFromPool() {
        Set<Connection> connections = new HashSet<>();
        String insertQuery = """
                INSERT INTO users (name, age) 
                values (?, ?);
                """;
        for (int i = 0; i < ROWS_SIZE; i++) {
            try (var connection = pooledDataSource.getConnection()) {
                try (var statement = connection.prepareStatement(insertQuery)) {
                    connections.add(connection);
                    statement.setString(1, "name" + i);
                    statement.setInt(2, i);
                    statement.executeUpdate();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        assertThat(connections.size()).isLessThanOrEqualTo(DEFAULT_POOL_SIZE);
    }

    @Order(2)
    @Test
    @DisplayName("should be 100 rows in the table")
    void shouldInsertRowsToTable() {
        String countQuery = "SELECT count(u.id) from users u;";
        long actualRows = 0;
        try (var connection = pooledDataSource.getConnection()) {
            try (var statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery(countQuery);
                if (resultSet.next()) {
                    actualRows = resultSet.getLong(1);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        assertThat(actualRows).isEqualTo(ROWS_SIZE);
    }

    @AfterAll
    static void afterAll() {
        dropTable();
    }

    private static void createTable() {
        String createQuery = """
                CREATE TABLE IF NOT EXISTS users (
                id bigserial PRIMARY KEY,
                name varchar (50) not null,
                age int not null);
                 """;
        try (var connection = pooledDataSource.getConnection()) {
            try (var statement = connection.createStatement()) {
                statement.execute(createQuery);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void dropTable() {
        String dropQuery = """
                DROP TABLE users;
                 """;
        try (var connection = pooledDataSource.getConnection()) {
            try (var statement = connection.createStatement()) {
                statement.execute(dropQuery);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
