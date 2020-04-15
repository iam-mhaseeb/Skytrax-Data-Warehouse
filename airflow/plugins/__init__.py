from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class SkytaxPlugin(AirflowPlugin):
    name = "skytrax_plugin"
    operators = [
        operators.SourceToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.CreateTablesOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
