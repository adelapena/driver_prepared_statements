package com.adelapena;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import edu.umd.cs.findbugs.annotations.NonNull;
import junit.framework.Assert;

public class TestPreparedStatements
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tab";
    private static final String UDT = "udt";

    public static void main(String[] args)
    {
        // Prepare a schema change listener to record schema updates
        AtomicBoolean tableChanged = new AtomicBoolean(false);
        AtomicBoolean userTypeChanged = new AtomicBoolean(false);
        SchemaChangeListener listener =
                new SchemaChangeListenerBase()
                {
                    @Override
                    public void onTableUpdated(@NonNull TableMetadata current, @NonNull TableMetadata previous)
                    {
                        tableChanged.set(true);
                    }

                    @Override
                    public void onUserDefinedTypeUpdated(@NonNull UserDefinedType current, @NonNull UserDefinedType previous)
                    {
                        userTypeChanged.set(true);
                    }
                };

        CqlSessionBuilder builder = new CqlSessionBuilder().withSchemaChangeListener(listener);

        try (CqlSession session = builder.build())
        {
            session.execute(SimpleStatement.newInstance(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE)).setTimeout(Duration.ofMinutes(1)));
            session.execute(SimpleStatement.newInstance(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", KEYSPACE)).setTimeout(Duration.ofMinutes(1)));
            session.execute(SimpleStatement.newInstance(String.format("CREATE TYPE %s.%s (a int, b int)", KEYSPACE, UDT)).setTimeout(Duration.ofMinutes(1)));
            session.execute(SimpleStatement.newInstance(String.format("CREATE TABLE %s.%s (k int PRIMARY KEY, v udt)", KEYSPACE, TABLE)).setTimeout(Duration.ofMinutes(1)));

            Metadata metadata = session.refreshSchema();
            UserDefinedType type = metadata.getKeyspace(KEYSPACE).flatMap(ksm -> ksm.getUserDefinedType(UDT)).orElseThrow(NoSuchElementException::new);

            // Prepare and run a insert query
            String insert = String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", KEYSPACE, TABLE);
            PreparedStatement prepared = session.prepare(insert);
            UdtValue udtValue = type.newValue().setInt(0, 1).setInt(1, 3);
            session.execute(prepared.bind(0, udtValue));

            // Alter the type and verify that the server notifies the client's listener
            session.execute(SimpleStatement.newInstance(String.format("ALTER TYPE %s.%s ADD c text", KEYSPACE, UDT)).setTimeout(Duration.ofMinutes(1)));
            Assert.assertTrue(tableChanged.get());
            Assert.assertTrue(userTypeChanged.get());

            // Verify that the type is correctly read from the updated schema
            metadata = session.refreshSchema();
            type = metadata.getKeyspace(KEYSPACE).flatMap(ksm -> ksm.getUserDefinedType(UDT)).orElseThrow(NoSuchElementException::new);
            Assert.assertEquals(3, type.getFieldTypes().size());

            // Prepare and run the same insert query as before
            prepared = session.prepare(insert);
            udtValue = type.newValue().setInt(0, 1).setInt(1, 3).setString(2, "abc");
            session.execute(prepared.bind(0, udtValue)); // FAILS: Invalid user defined type, expected UDT(ks.udt) but got UDT(ks.udt)
        }
    }
}
