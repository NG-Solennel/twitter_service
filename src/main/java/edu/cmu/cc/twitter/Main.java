package edu.cmu.cc.twitter;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.undertow.Undertow;

public class Main {

    public static void main(String[] args) throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(System.getenv("DB_URL") != null ?
            System.getenv("DB_URL") : "jdbc:mysql://localhost:3306/twitter");
        config.setUsername(System.getenv("DB_USER") != null ?
            System.getenv("DB_USER") : "root");
        config.setPassword(System.getenv("DB_PASSWORD") != null ?
            System.getenv("DB_PASSWORD") : "");
        config.setMaximumPoolSize(50);
        config.setMinimumIdle(10);
        config.setConnectionTimeout(3000);
        config.setIdleTimeout(60000);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");

        HikariDataSource ds = new HikariDataSource(config);

        String authUrl = System.getenv("AUTH_SERVICE_URL") != null ?
            System.getenv("AUTH_SERVICE_URL") : "http://auth-service:9000";

        TwitterHandler handler = new TwitterHandler(ds, authUrl);

        Undertow server = Undertow.builder()
            .addHttpListener(8080, "0.0.0.0")
            .setHandler(exchange -> exchange.dispatch(handler))
            .setIoThreads(4)
            .setWorkerThreads(200)
            .build();

        server.start();
        System.out.println("Twitter service started on port 8080");
    }
}
