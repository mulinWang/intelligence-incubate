package com.longyuan.intelligence.incubate.presto;

import java.sql.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by mulin on 2017/12/6.
 */
public class PrestoTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrestoTest.class);

    @Test
    public void testConnect() {
        String url = "jdbc:presto://52.80.113.55:8180/cassandra/miwu";
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "");
        try {
            Connection connection = DriverManager.getConnection(url, properties);

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("show tables");
            while (rs.next()) {
                LOGGER.info("table:{}", rs.getString(1));
            }
            rs.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
