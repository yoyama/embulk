package org.embulk.standards;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

import java.util.List;
import java.util.Map;

public class RenameFilterPlugin
        implements FilterPlugin
{

    public interface RegexConversionParam
            extends Task
    {
        @Config("regex")
        String getRegex();

        @Config("replacement")
        String getReplacement();
    }

    public interface PluginTask
            extends Task
    {
        @Config("columns")
        @ConfigDefault("{}")
        Map<String, String> getRenameMap();

        @Config("regex_conversions")
        @ConfigDefault("[]")
        List<RegexConversionParam> getRegexConversions();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
                            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        Map<String, String> renameMap = task.getRenameMap();
        List<RegexConversionParam> regexConversions = task.getRegexConversions();

        // check column_options is valid or not
        for (String columnName : renameMap.keySet()) {
            inputSchema.lookupColumn(columnName); // throws SchemaConfigException
        }

        Schema.Builder builder = Schema.builder();
        for (Column column : inputSchema.getColumns()) {
            String name = column.getName();
            if (renameMap.containsKey(name)) {
                name = renameMap.get(name);
            } else {
                name = getRegexConvertedName(name, regexConversions);
            }
            builder.add(name, column.getType());
        }

        control.run(task.dump(), builder.build());
    }

    protected String getRegexConvertedName(String orgColumn, List<RegexConversionParam> regexConversions) {
        for(RegexConversionParam conv: regexConversions){
            String newColumn = orgColumn.replaceAll(conv.getRegex(), conv.getReplacement());
            if(!newColumn.equals(orgColumn)){
                return newColumn;
            }
        }
        return orgColumn;
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
                           Schema outputSchema, PageOutput output)
    {
        return output;
    }
}