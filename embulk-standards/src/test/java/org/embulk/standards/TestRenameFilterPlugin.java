package org.embulk.standards;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfigException;
import org.embulk.standards.RenameFilterPlugin.PluginTask;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRenameFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private final Schema SCHEMA = Schema.builder()
            .add("_c0", STRING)
            .add("_c1", TIMESTAMP)
            .add("_c2 (test test)", STRING)
            .build();

    private RenameFilterPlugin filter;

    @Before
    public void createFilter()
    {
        filter = new RenameFilterPlugin();
    }

    @Test
    public void checkDefaultValues()
    {
        PluginTask task = Exec.newConfigSource().loadConfig(PluginTask.class);
        assertTrue(task.getRenameMap().isEmpty());
    }

    @Test
    public void throwSchemaConfigExceptionIfColumnNotFound()
    {
        ConfigSource pluginConfig = Exec.newConfigSource()
                .set("columns", ImmutableMap.of("not_found", "any_name"));

        try {
            filter.transaction(pluginConfig, SCHEMA, new FilterPlugin.Control() {
                public void run(TaskSource task, Schema schema) { }
            });
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof SchemaConfigException);
        }
    }

    @Test
    public void checkRenaming()
    {
        ConfigSource pluginConfig = Exec.newConfigSource()
                .set("columns", ImmutableMap.of("_c0", "_c0_new"));

        filter.transaction(pluginConfig, SCHEMA, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource task, Schema newSchema)
            {
                // _c0 -> _c0_new
                Column old0 = SCHEMA.getColumn(0);
                Column new0 = newSchema.getColumn(0);
                assertEquals("_c0_new", new0.getName());
                assertEquals(old0.getType(), new0.getType());

                // _c1 is not changed
                Column old1 = SCHEMA.getColumn(1);
                Column new1 = newSchema.getColumn(1);
                assertEquals("_c1", new1.getName());
                assertEquals(old1.getType(), new1.getType());
            }
        });
    }

    @Test
    public void checkRenamingRegex()
    {
        ConfigSource pluginConfig = Exec.newConfigSource()
                .set("columns", ImmutableMap.of("_c0", "_c0_new"))
                .set("regex_conversions",
                        ImmutableList.of(
                                ImmutableMap.of("regex", " *\\(.*\\)", "replacement", ""),
                                ImmutableMap.of("regex", "_c0", "replacement", "c0c0c0"),
                                ImmutableMap.of("regex", "_c1", "replacement", "c_1"),
                                ImmutableMap.of("regex", "_c2.*", "replacement", "c2c2c2")
                        ));

        filter.transaction(pluginConfig, SCHEMA, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource task, Schema newSchema)
            {
                // columns precedes regex_conversions
                Column old0 = SCHEMA.getColumn(0);
                Column new0 = newSchema.getColumn(0);
                assertEquals("_c0_new", new0.getName());
                assertEquals(old0.getType(), new0.getType());

                // _c1 will be changed to c_1
                Column old1 = SCHEMA.getColumn(1);
                Column new1 = newSchema.getColumn(1);
                assertEquals("c_1", new1.getName());
                assertEquals(old1.getType(), new1.getType());

                // _c2 will be changed to _c2
                Column old2 = SCHEMA.getColumn(2);
                Column new2 = newSchema.getColumn(2);
                assertEquals("_c2", new2.getName());
                assertEquals(old2.getType(), new2.getType());
            }
        });
    }

}
