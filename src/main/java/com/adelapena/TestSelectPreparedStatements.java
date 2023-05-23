package com.adelapena;

import java.time.Duration;
import java.util.Objects;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;

import static java.lang.String.format;

public class TestSelectPreparedStatements
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tab";
    private static final String UDT = "udt";

    public static void main(String[] args)
    {
        CqlSessionBuilder builder = new CqlSessionBuilder();

        try (CqlSession session = builder.build())
        {
            session.execute(SimpleStatement.newInstance(format("DROP KEYSPACE IF EXISTS %s", KEYSPACE)).setTimeout(Duration.ofMinutes(1)));
            session.execute(SimpleStatement.newInstance(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", KEYSPACE)).setTimeout(Duration.ofMinutes(1)));
            session.execute(SimpleStatement.newInstance(format("CREATE TYPE %s.%s (a int, b int)", KEYSPACE, UDT)).setTimeout(Duration.ofMinutes(1)));
            session.execute(SimpleStatement.newInstance(format("CREATE TABLE %s.%s (k int, c int, v %s, PRIMARY KEY(k, c))", KEYSPACE, TABLE, UDT)).setTimeout(Duration.ofMinutes(1)));

            // Prepare a SELECT query
            PreparedStatement select = session.prepare(SimpleStatement.newInstance(format("SELECT * FROM %s.%s WHERE k = ?", KEYSPACE, TABLE)));

            // Alter the type
            session.execute(SimpleStatement.newInstance(format("ALTER TYPE %s.%s ADD c int", KEYSPACE, UDT)).setTimeout(Duration.ofMinutes(1)));

            // Insert some data
            session.execute(SimpleStatement.newInstance(format("INSERT INTO %s.%s (k, c, v) VALUES (0, 0, {a:1,b:2,c:3})", KEYSPACE, TABLE)));

            // Run and consume the prepared SELECT query
            session.execute(select.bind(0))
                   .all()
                   .stream()
                   .map(row -> row.getUdtValue("v")) // Too many fields in encoded UDT value, expected 2
                   .filter(Objects::nonNull)
                   .map(UdtValue::getFormattedContents)
                   .forEach(System.out::println);
        }
    }
}
