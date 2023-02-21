The KairosDB CSV Plugin is a post-processing plugin that converts the JSON response from KairosDB to a CSV file.
Columns are comma delimited. 
A column is added for each groupBy.

Note: Currently only supports groupBy for tags.

The output looks like the following:

The plugin will be called when added to the query. The syntax is as follows:

    plugins": [ { "name":"CsvConverter"}]

This is added to the root of the query. For example

    {
    "plugins": [ 
        { 
            "name": "CsvConverter"
        }
    ]
    "start_absolute": 1357023600000,
    "end_relative": {
    "value": "5",
    "unit": "days"
    },
    ...

The plugin supports the following parameters:

* showMetricName - Whether to include a column for the metric name. The default is true.

Parameters are added to the plugin as additional JSON fields. For example:

    "plugins": [ 
        { 
            "name": "CsvConverter", 
            "show_metric_name": "false"
        }
    ]

