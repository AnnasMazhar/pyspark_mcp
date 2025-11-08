"""AWS Glue integration module with DynamicFrame support and advanced job templates."""

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union


class GlueJobType(Enum):
    """Types of AWS Glue jobs."""

    ETL = "etl"
    STREAMING = "streaming"
    PYTHON_SHELL = "pythonshell"


class DataFormat(Enum):
    """Supported data formats."""

    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    AVRO = "avro"
    ORC = "orc"
    DELTA = "delta"


@dataclass
class GlueJobConfig:
    """Configuration for AWS Glue job generation."""

    job_name: str
    job_type: GlueJobType = GlueJobType.ETL
    source_format: DataFormat = DataFormat.PARQUET
    target_format: DataFormat = DataFormat.PARQUET
    include_bookmarking: bool = True
    use_dynamic_frame: bool = True
    enable_continuous_logging: bool = True
    enable_metrics: bool = True
    enable_spark_ui: bool = True
    worker_type: str = "G.1X"
    number_of_workers: int = 2
    max_retries: int = 0
    timeout: int = 2880  # 48 hours in minutes
    glue_version: str = "4.0"


@dataclass
class ColumnInfo:
    """Represents a column in a Data Catalog table."""

    name: str
    type: str
    comment: Optional[str] = None
    nullable: bool = True


@dataclass
class PartitionInfo:
    """Represents partition information for a table."""

    key: str
    type: str
    comment: Optional[str] = None


@dataclass
class DataCatalogTable:
    """Represents a table in AWS Glue Data Catalog."""

    database_name: str
    table_name: str
    s3_location: Optional[str] = None
    format: Optional[DataFormat] = None
    partition_keys: Optional[List[str]] = None
    columns: Optional[List[ColumnInfo]] = None
    partitions: Optional[List[PartitionInfo]] = None
    table_type: str = "EXTERNAL_TABLE"
    storage_descriptor: Optional[Dict[str, Any]] = None
    parameters: Optional[Dict[str, str]] = None
    created_by: Optional[str] = None
    create_time: Optional[str] = None
    update_time: Optional[str] = None


@dataclass
class SchemaEvolutionConfig:
    """Configuration for schema evolution handling."""

    enable_schema_evolution: bool = True
    merge_behavior: str = "merge"  # merge, overwrite, fail
    case_sensitive: bool = False
    enable_partition_projection: bool = False
    projection_config: Optional[Dict[str, str]] = field(default_factory=dict)


@dataclass
class S3OptimizationConfig:
    """Configuration for S3 optimization patterns."""

    target_file_size_mb: int = 128  # Target file size in MB
    max_files_per_partition: int = 1000
    compression_type: str = "snappy"  # snappy, gzip, lz4, zstd
    enable_small_file_optimization: bool = True
    enable_partition_pruning: bool = True
    enable_predicate_pushdown: bool = True
    coalesce_partitions: bool = True
    partition_strategy: str = "hive"  # hive, custom
    lifecycle_policy: Optional[Dict[str, Any]] = None


@dataclass
class S3PartitioningStrategy:
    """S3 partitioning strategy configuration."""

    partition_columns: List[str]
    partition_format: str = "hive"  # hive, custom
    max_partitions: int = 10000
    partition_projection: bool = False
    projection_config: Optional[Dict[str, str]] = None
    partition_filtering: bool = True


@dataclass
class S3OptimizationResult:
    """Result of S3 optimization analysis."""

    current_issues: List[str]
    optimization_recommendations: List[str]
    estimated_improvements: Dict[str, str]
    implementation_code: str
    configuration_changes: Dict[str, Any]


class AWSGlueIntegration:
    """Enhanced AWS Glue integration with DynamicFrame support."""

    def __init__(self):
        """Initialize AWS Glue integration."""
        self.supported_formats = {
            DataFormat.PARQUET: {
                "read_options": {},
                "write_options": {"compression": "snappy"},
            },
            DataFormat.CSV: {
                "read_options": {"header": "true", "inferSchema": "true"},
                "write_options": {"header": "true"},
            },
            DataFormat.JSON: {
                "read_options": {"multiLine": "true"},
                "write_options": {},
            },
            DataFormat.AVRO: {"read_options": {}, "write_options": {}},
            DataFormat.ORC: {
                "read_options": {},
                "write_options": {"compression": "zlib"},
            },
            DataFormat.DELTA: {"read_options": {}, "write_options": {}},
        }

    def generate_enhanced_glue_job_template(
        self,
        config: GlueJobConfig,
        source_table: Optional[DataCatalogTable] = None,
        target_table: Optional[DataCatalogTable] = None,
        transformation_sql: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate an enhanced AWS Glue job template with DynamicFrame support.

        Args:
            config: Job configuration
            source_table: Source table configuration
            target_table: Target table configuration
            transformation_sql: Optional SQL transformation logic

        Returns:
            Dictionary containing the enhanced Glue job template
        """
        try:
            # Generate imports section
            imports = self._generate_imports(config)

            # Generate job initialization
            job_init = self._generate_job_initialization(config)

            # Generate data reading logic
            data_reading = self._generate_data_reading_logic(config, source_table)

            # Generate transformation logic
            transformation = self._generate_transformation_logic(
                config, transformation_sql
            )

            # Generate data writing logic
            data_writing = self._generate_data_writing_logic(config, target_table)

            # Generate error handling and cleanup
            error_handling = self._generate_error_handling(config)

            template = f"""{imports}

{job_init}

try:
{data_reading}

{transformation}

{data_writing}

    print(f"Job {{args['JOB_NAME']}} completed successfully")
    
{error_handling}
"""

            return {
                "status": "success",
                "job_name": config.job_name,
                "job_type": config.job_type.value,
                "template": template,
                "features": {
                    "dynamic_frame_support": config.use_dynamic_frame,
                    "error_handling": True,
                    "parameter_support": True,
                    "bookmarking": config.include_bookmarking,
                    "catalog_integration": True,
                    "continuous_logging": config.enable_continuous_logging,
                    "metrics": config.enable_metrics,
                    "spark_ui": config.enable_spark_ui,
                },
                "job_parameters": self._get_job_parameters(
                    config, source_table, target_table
                ),
                "glue_job_properties": self._get_glue_job_properties(config),
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _generate_imports(self, config: GlueJobConfig) -> str:
        """Generate import statements based on configuration."""
        imports = [
            "import sys",
            "from awsglue.transforms import *",
            "from awsglue.utils import getResolvedOptions",
            "from pyspark.context import SparkContext",
            "from awsglue.context import GlueContext",
            "from awsglue.job import Job",
        ]

        if config.use_dynamic_frame:
            imports.extend(
                [
                    "from awsglue.dynamicframe import DynamicFrame",
                    "from awsglue import DynamicFrame",
                ]
            )

        imports.extend(
            [
                "from pyspark.sql.functions import *",
                "from pyspark.sql.types import *",
            ]
        )

        if config.enable_continuous_logging:
            imports.append("import logging")

        return "\n".join(imports)

    def _generate_job_initialization(self, config: GlueJobConfig) -> str:
        """Generate job initialization code."""
        params = [
            "'JOB_NAME'",
            "'SOURCE_PATH'",
            "'TARGET_PATH'",
        ]

        if config.use_dynamic_frame:
            params.extend(
                [
                    "'SOURCE_DATABASE'",
                    "'SOURCE_TABLE'",
                    "'TARGET_DATABASE'",
                    "'TARGET_TABLE'",
                ]
            )

        if config.include_bookmarking:
            params.append("'JOB_BOOKMARK_OPTION'")

        params_str = ",\n    ".join(params)

        init_code = f"""# Get job parameters
args = getResolvedOptions(sys.argv, [
    {params_str}
])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)"""

        if config.enable_continuous_logging:
            init_code += """

# Configure logging
logger = glueContext.get_logger()
logger.info(f"Starting job: {args['JOB_NAME']}")"""

        return init_code

    def _generate_data_reading_logic(
        self, config: GlueJobConfig, source_table: Optional[DataCatalogTable]
    ) -> str:
        """Generate data reading logic."""
        if config.use_dynamic_frame and source_table:
            return f"""    # Read source data using DynamicFrame from Data Catalog
    source_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=args.get('SOURCE_DATABASE', '{source_table.database_name}'),
        table_name=args.get('SOURCE_TABLE', '{source_table.table_name}'),
        transformation_ctx="source_dynamic_frame"
    )
    
    # Convert to DataFrame for transformations if needed
    source_df = source_dynamic_frame.toDF()"""
        else:
            format_options = self.supported_formats.get(config.source_format, {}).get(
                "read_options", {}
            )
            options_str = ", ".join(
                [f'.option("{k}", "{v}")' for k, v in format_options.items()]
            )

            return f"""    # Read source data using DataFrame
    source_df = spark.read.format("{config.source_format.value}"){options_str}.load(args['SOURCE_PATH'])"""

    def _generate_transformation_logic(
        self, config: GlueJobConfig, transformation_sql: Optional[str]
    ) -> str:
        """Generate transformation logic."""
        if transformation_sql:
            return f"""    # Apply SQL transformations
    source_df.createOrReplaceTempView("source_table")
    transformed_df = spark.sql('''
    ''')"""
        else:
            return """    # Apply transformations here
    # Example transformations:
    # transformed_df = source_df.filter(col("status") == "active")
    # transformed_df = source_df.withColumn("processed_date", current_date())
    # transformed_df = source_df.dropDuplicates(["id"])
    
    # For now, pass through the data unchanged
    transformed_df = source_df"""

    def _generate_data_writing_logic(
        self, config: GlueJobConfig, target_table: Optional[DataCatalogTable]
    ) -> str:
        """Generate data writing logic."""
        if config.use_dynamic_frame and target_table:
            return f"""    # Convert back to DynamicFrame for writing
    target_dynamic_frame = DynamicFrame.fromDF(
        transformed_df, 
        glueContext, 
        "target_dynamic_frame"
    )
    
    # Write to Data Catalog
    glueContext.write_dynamic_frame.from_catalog(
        frame=target_dynamic_frame,
        database=args.get('TARGET_DATABASE', '{target_table.database_name}'),
        table_name=args.get('TARGET_TABLE', '{target_table.table_name}'),
        transformation_ctx="target_dynamic_frame"
    )"""
        else:
            format_options = self.supported_formats.get(config.target_format, {}).get(
                "write_options", {}
            )
            options_str = ", ".join(
                [f'.option("{k}", "{v}")' for k, v in format_options.items()]
            )

            return f"""    # Write transformed data
    transformed_df.write \\
        .format("{config.target_format.value}") \\
        .mode("overwrite"){options_str} \\
        .save(args['TARGET_PATH'])"""

    def _generate_error_handling(self, config: GlueJobConfig) -> str:
        """Generate error handling and cleanup code."""
        error_code = """except Exception as e:
    print(f"Job failed with error: {str(e)}")"""

        if config.enable_continuous_logging:
            error_code += """
    logger.error(f"Job failed with error: {str(e)}")"""

        error_code += """
    raise e
    
finally:"""

        if config.include_bookmarking:
            error_code += """
    # Commit job for bookmarking
    job.commit()"""
        else:
            error_code += """
    # Job bookmarking disabled"""

        return error_code

    def _get_job_parameters(
        self,
        config: GlueJobConfig,
        source_table: Optional[DataCatalogTable],
        target_table: Optional[DataCatalogTable],
    ) -> Dict[str, str]:
        """Get job parameters for the Glue job."""
        params = {
            "JOB_NAME": config.job_name,
            "SOURCE_PATH": "s3://your-bucket/source/",
            "TARGET_PATH": "s3://your-bucket/target/",
        }

        if config.use_dynamic_frame:
            if source_table:
                params.update(
                    {
                        "SOURCE_DATABASE": source_table.database_name,
                        "SOURCE_TABLE": source_table.table_name,
                    }
                )
            if target_table:
                params.update(
                    {
                        "TARGET_DATABASE": target_table.database_name,
                        "TARGET_TABLE": target_table.table_name,
                    }
                )

        if config.include_bookmarking:
            params["JOB_BOOKMARK_OPTION"] = "job-bookmark-enable"

        return params

    def _get_glue_job_properties(self, config: GlueJobConfig) -> Dict[str, Any]:
        """Get Glue job properties for job creation."""
        properties = {
            "Name": config.job_name,
            "Role": "arn:aws:iam::ACCOUNT_ID:role/GlueServiceRole",
            "Command": {
                "Name": (
                    "glueetl"
                    if config.job_type == GlueJobType.ETL
                    else config.job_type.value
                ),
                "ScriptLocation": f"s3://your-glue-scripts-bucket/{config.job_name}.py",
                "PythonVersion": "3",
            },
            "DefaultArguments": {
                "--job-language": "python",
                "--job-bookmark-option": (
                    "job-bookmark-enable"
                    if config.include_bookmarking
                    else "job-bookmark-disable"
                ),
                "--enable-continuous-cloudwatch-log": (
                    "true" if config.enable_continuous_logging else "false"
                ),
                "--enable-metrics": "true" if config.enable_metrics else "false",
                "--enable-spark-ui": "true" if config.enable_spark_ui else "false",
                "--spark-event-logs-path": "s3://your-spark-logs-bucket/",
            },
            "MaxRetries": config.max_retries,
            "Timeout": config.timeout,
            "GlueVersion": config.glue_version,
            "WorkerType": config.worker_type,
            "NumberOfWorkers": config.number_of_workers,
        }

        return properties

    def generate_dynamic_frame_conversion_template(
        self,
        pyspark_code: str,
        source_table: DataCatalogTable,
        target_table: DataCatalogTable,
    ) -> Dict[str, Any]:
        """
        Convert existing PySpark DataFrame code to use DynamicFrames.

        Args:
            pyspark_code: Existing PySpark DataFrame code
            source_table: Source table configuration
            target_table: Target table configuration

        Returns:
            Dictionary containing DynamicFrame-based code
        """
        try:
            # Extract DataFrame operations from the code
            df_operations = self._extract_dataframe_operations(pyspark_code)

            # Generate DynamicFrame equivalent
            dynamic_frame_code = self._convert_to_dynamic_frame_operations(
                df_operations, source_table, target_table
            )

            return {
                "status": "success",
                "original_code": pyspark_code,
                "dynamic_frame_code": dynamic_frame_code,
                "conversion_notes": [
                    "Converted DataFrame operations to DynamicFrame equivalents",
                    "Added Glue Data Catalog integration",
                    "Maintained transformation logic compatibility",
                ],
                "benefits": [
                    "Better schema evolution handling",
                    "Automatic data type inference",
                    "Integration with Glue Data Catalog",
                    "Support for nested and semi-structured data",
                ],
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _extract_dataframe_operations(self, pyspark_code: str) -> List[str]:
        """Extract DataFrame operations from PySpark code."""
        # This is a simplified extraction - in practice, you'd use AST parsing
        operations = []
        lines = pyspark_code.split("\n")

        for line in lines:
            line = line.strip()
            if any(
                op in line
                for op in [
                    ".filter(",
                    ".select(",
                    ".withColumn(",
                    ".groupBy(",
                    ".join(",
                ]
            ):
                operations.append(line)

        return operations

    def _convert_to_dynamic_frame_operations(
        self,
        operations: List[str],
        source_table: DataCatalogTable,
        target_table: DataCatalogTable,
    ) -> str:
        """Convert DataFrame operations to DynamicFrame equivalents."""
        template = f"""# Read from Glue Data Catalog using DynamicFrame
source_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="{source_table.database_name}",
    table_name="{source_table.table_name}",
    transformation_ctx="source_dynamic_frame"
)

# Apply transformations using DynamicFrame transforms
transformed_dynamic_frame = source_dynamic_frame"""

        # Convert common DataFrame operations to DynamicFrame equivalents
        for operation in operations:
            if ".filter(" in operation:
                # Convert filter to DynamicFrame filter
                template += f"""
# Converted from: {operation}
transformed_dynamic_frame = Filter.apply(
    frame=transformed_dynamic_frame,
    f=lambda x: {self._extract_filter_condition(operation)},
    transformation_ctx="filter_transform"
)"""
            elif ".select(" in operation:
                # Convert select to DynamicFrame select
                template += f"""
# Converted from: {operation}
transformed_dynamic_frame = SelectFields.apply(
    frame=transformed_dynamic_frame,
    paths={self._extract_select_fields(operation)},
    transformation_ctx="select_transform"
)"""

        template += f"""

# Write to Glue Data Catalog
glueContext.write_dynamic_frame.from_catalog(
    frame=transformed_dynamic_frame,
    database="{target_table.database_name}",
    table_name="{target_table.table_name}",
    transformation_ctx="write_transform"
)"""

        return template

    def _extract_filter_condition(self, filter_operation: str) -> str:
        """Extract filter condition from DataFrame filter operation."""
        # Simplified extraction - would need more sophisticated parsing
        if "col(" in filter_operation:
            return "True  # TODO: Convert filter condition"
        return "True  # TODO: Convert filter condition"

    def _extract_select_fields(self, select_operation: str) -> List[str]:
        """Extract select fields from DataFrame select operation."""
        # Simplified extraction - would need more sophisticated parsing
        return ["field1", "field2"]  # TODO: Parse actual fields

    def generate_glue_job_with_sql_conversion(
        self,
        sql_query: str,
        config: GlueJobConfig,
        source_table: DataCatalogTable,
        target_table: DataCatalogTable,
    ) -> Dict[str, Any]:
        """
        Generate a Glue job template that includes SQL to PySpark conversion.

        Args:
            sql_query: SQL query to convert and include
            config: Job configuration
            source_table: Source table configuration
            target_table: Target table configuration

        Returns:
            Dictionary containing the complete Glue job with SQL conversion
        """
        try:
            # This would integrate with the existing SQL converter
            # For now, we'll create a placeholder that shows the integration point

            template = self.generate_enhanced_glue_job_template(
                config=config,
                source_table=source_table,
                target_table=target_table,
                transformation_sql=sql_query,
            )

            # Add SQL conversion metadata
            template["sql_integration"] = {
                "original_sql": sql_query,
                "conversion_method": "integrated_sql_transformation",
                "sql_compatibility": "full",
            }

            return template

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def generate_data_catalog_table_definition(
        self,
        table: DataCatalogTable,
        schema_evolution_config: Optional[SchemaEvolutionConfig] = None,
    ) -> Dict[str, Any]:
        """
        Generate AWS Glue Data Catalog table definition.

        Args:
            table: Table configuration
            schema_evolution_config: Schema evolution configuration

        Returns:
            Dictionary containing table definition for Glue Data Catalog
        """
        try:
            # Build storage descriptor
            storage_descriptor = {
                "Columns": [
                    {
                        "Name": col.name,
                        "Type": col.type,
                        "Comment": col.comment or "",
                    }
                    for col in (table.columns or [])
                ],
                "Location": table.s3_location or "",
                "InputFormat": self._get_input_format(table.format),
                "OutputFormat": self._get_output_format(table.format),
                "SerdeInfo": {
                    "SerializationLibrary": self._get_serde_library(table.format),
                    "Parameters": self._get_serde_parameters(table.format),
                },
                "Compressed": table.format in [DataFormat.PARQUET, DataFormat.ORC],
                "StoredAsSubDirectories": False,
            }

            # Add partition information
            partition_keys = []
            if table.partitions:
                partition_keys = [
                    {
                        "Name": part.key,
                        "Type": part.type,
                        "Comment": part.comment or "",
                    }
                    for part in table.partitions
                ]

            # Build table definition
            table_definition = {
                "Name": table.table_name,
                "StorageDescriptor": storage_descriptor,
                "PartitionKeys": partition_keys,
                "TableType": table.table_type,
                "Parameters": table.parameters or {},
            }

            # Add schema evolution parameters if configured
            if (
                schema_evolution_config
                and schema_evolution_config.enable_schema_evolution
            ):
                table_definition["Parameters"].update(
                    {
                        "projection.enabled": (
                            "true"
                            if schema_evolution_config.enable_partition_projection
                            else "false"
                        ),
                        "storage.location.template": table.s3_location or "",
                    }
                )

                if schema_evolution_config.projection_config:
                    table_definition["Parameters"].update(
                        schema_evolution_config.projection_config
                    )

            # Generate AWS CLI command
            aws_cli_command = f"""aws glue create-table \\
  --database-name {table.database_name} \\
  --table-input '{json.dumps(table_definition, indent=2)}'"""

            # Generate Terraform resource
            terraform_resource = self._generate_terraform_table_resource(
                table, table_definition
            )

            return {
                "status": "success",
                "table_definition": table_definition,
                "aws_cli_command": aws_cli_command,
                "terraform_resource": terraform_resource,
                "schema_info": {
                    "column_count": len(table.columns or []),
                    "partition_count": len(table.partitions or []),
                    "data_format": table.format.value if table.format else "unknown",
                    "schema_evolution_enabled": (
                        schema_evolution_config.enable_schema_evolution
                        if schema_evolution_config
                        else False
                    ),
                },
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def detect_schema_from_sample_data(
        self,
        sample_data: Dict[str, Any],
        table_name: str,
        infer_partitions: bool = True,
    ) -> Dict[str, Any]:
        """
        Detect schema from sample data and generate table definition.

        Args:
            sample_data: Sample data dictionary or list of records
            table_name: Name for the table
            infer_partitions: Whether to infer partition keys from data

        Returns:
            Dictionary containing detected schema and table definition
        """
        try:
            columns = []
            partition_keys = []

            # Handle different sample data formats
            if isinstance(sample_data, dict):
                # Single record
                records = [sample_data]
            elif isinstance(sample_data, list) and sample_data:
                # List of records
                records = sample_data
            else:
                return {"status": "error", "message": "Invalid sample data format"}

            # Analyze all records to detect schema
            all_fields = {}
            for record in records:
                if isinstance(record, dict):
                    for key, value in record.items():
                        field_type = self._infer_data_type(value)
                        if key in all_fields:
                            # Check for type consistency
                            if all_fields[key] != field_type:
                                all_fields[key] = (
                                    "string"  # Default to string for mixed types
                                )
                        else:
                            all_fields[key] = field_type

            # Create column definitions
            for field_name, field_type in all_fields.items():
                # Check if field might be a partition key
                is_partition = False
                if infer_partitions and self._is_likely_partition_key(
                    field_name, records
                ):
                    partition_keys.append(
                        PartitionInfo(
                            key=field_name,
                            type=field_type,
                            comment=f"Inferred partition key from data pattern",
                        )
                    )
                    is_partition = True

                if not is_partition:
                    columns.append(
                        ColumnInfo(
                            name=field_name,
                            type=field_type,
                            comment=f"Inferred from sample data",
                        )
                    )

            # Generate schema statistics
            schema_stats = {
                "total_fields": len(all_fields),
                "data_columns": len(columns),
                "partition_columns": len(partition_keys),
                "sample_records_analyzed": len(records),
                "field_types": {
                    field_type: sum(1 for t in all_fields.values() if t == field_type)
                    for field_type in set(all_fields.values())
                },
            }

            return {
                "status": "success",
                "detected_schema": {
                    "columns": [
                        {"name": col.name, "type": col.type, "comment": col.comment}
                        for col in columns
                    ],
                    "partitions": [
                        {"key": part.key, "type": part.type, "comment": part.comment}
                        for part in partition_keys
                    ],
                },
                "schema_statistics": schema_stats,
                "recommendations": self._generate_schema_recommendations(
                    columns, partition_keys, records
                ),
                "table_name": table_name,
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def generate_schema_evolution_strategy(
        self,
        current_schema: List[ColumnInfo],
        new_schema: List[ColumnInfo],
        evolution_config: SchemaEvolutionConfig,
    ) -> Dict[str, Any]:
        """
        Generate schema evolution strategy for handling schema changes.

        Args:
            current_schema: Current table schema
            new_schema: New/target schema
            evolution_config: Evolution configuration

        Returns:
            Dictionary containing evolution strategy and migration steps
        """
        try:
            # Analyze schema differences
            current_columns = {
                (
                    col.name.lower()
                    if not evolution_config.case_sensitive
                    else col.name
                ): col
                for col in current_schema
            }
            new_columns = {
                (
                    col.name.lower()
                    if not evolution_config.case_sensitive
                    else col.name
                ): col
                for col in new_schema
            }

            added_columns = []
            removed_columns = []
            modified_columns = []
            unchanged_columns = []

            # Find added columns
            for name, col in new_columns.items():
                if name not in current_columns:
                    added_columns.append(col)

            # Find removed and modified columns
            for name, col in current_columns.items():
                if name not in new_columns:
                    removed_columns.append(col)
                else:
                    new_col = new_columns[name]
                    if col.type != new_col.type:
                        modified_columns.append(
                            {
                                "column": col,
                                "old_type": col.type,
                                "new_type": new_col.type,
                            }
                        )
                    else:
                        unchanged_columns.append(col)

            # Generate evolution strategy
            strategy = {
                "evolution_type": evolution_config.merge_behavior,
                "changes_detected": len(added_columns)
                + len(removed_columns)
                + len(modified_columns)
                > 0,
                "backward_compatible": len(removed_columns) == 0
                and len(modified_columns) == 0,
                "migration_required": len(modified_columns) > 0,
            }

            # Generate migration steps
            migration_steps = []

            if evolution_config.merge_behavior == "merge":
                if added_columns:
                    migration_steps.append(
                        {
                            "step": "add_columns",
                            "description": f"Add {len(added_columns)} new columns",
                            "columns": [col.name for col in added_columns],
                            "sql": self._generate_add_columns_sql(added_columns),
                        }
                    )

                if modified_columns:
                    migration_steps.append(
                        {
                            "step": "modify_columns",
                            "description": f"Modify {len(modified_columns)} column types",
                            "changes": modified_columns,
                            "sql": self._generate_modify_columns_sql(modified_columns),
                        }
                    )

            elif evolution_config.merge_behavior == "overwrite":
                migration_steps.append(
                    {
                        "step": "recreate_table",
                        "description": "Recreate table with new schema",
                        "backup_required": True,
                        "sql": self._generate_recreate_table_sql(new_schema),
                    }
                )

            # Generate DynamicFrame transformation code
            transformation_code = self._generate_schema_evolution_code(
                added_columns, modified_columns, evolution_config
            )

            return {
                "status": "success",
                "evolution_strategy": strategy,
                "schema_changes": {
                    "added_columns": len(added_columns),
                    "removed_columns": len(removed_columns),
                    "modified_columns": len(modified_columns),
                    "unchanged_columns": len(unchanged_columns),
                },
                "migration_steps": migration_steps,
                "transformation_code": transformation_code,
                "recommendations": self._generate_evolution_recommendations(
                    added_columns, removed_columns, modified_columns
                ),
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _get_input_format(self, format: Optional[DataFormat]) -> str:
        """Get input format for Glue table definition."""
        format_mapping = {
            DataFormat.PARQUET: "org.apache.hadoop.mapred.TextInputFormat",
            DataFormat.CSV: "org.apache.hadoop.mapred.TextInputFormat",
            DataFormat.JSON: "org.apache.hadoop.mapred.TextInputFormat",
            DataFormat.AVRO: "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
            DataFormat.ORC: "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            DataFormat.DELTA: "org.apache.hadoop.mapred.TextInputFormat",
        }
        return format_mapping.get(format, "org.apache.hadoop.mapred.TextInputFormat")

    def _get_output_format(self, format: Optional[DataFormat]) -> str:
        """Get output format for Glue table definition."""
        format_mapping = {
            DataFormat.PARQUET: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            DataFormat.CSV: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            DataFormat.JSON: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            DataFormat.AVRO: "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
            DataFormat.ORC: "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
            DataFormat.DELTA: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        }
        return format_mapping.get(
            format, "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        )

    def _get_serde_library(self, format: Optional[DataFormat]) -> str:
        """Get SerDe library for Glue table definition."""
        serde_mapping = {
            DataFormat.PARQUET: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            DataFormat.CSV: "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            DataFormat.JSON: "org.openx.data.jsonserde.JsonSerDe",
            DataFormat.AVRO: "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
            DataFormat.ORC: "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
            DataFormat.DELTA: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        }
        return serde_mapping.get(
            format, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        )

    def _get_serde_parameters(self, format: Optional[DataFormat]) -> Dict[str, str]:
        """Get SerDe parameters for Glue table definition."""
        params_mapping = {
            DataFormat.PARQUET: {},
            DataFormat.CSV: {"field.delim": ",", "skip.header.line.count": "1"},
            DataFormat.JSON: {"ignore.malformed.json": "true"},
            DataFormat.AVRO: {},
            DataFormat.ORC: {},
            DataFormat.DELTA: {},
        }
        return params_mapping.get(format, {})

    def _infer_data_type(self, value: Any) -> str:
        """Infer Glue data type from Python value."""
        if value is None:
            return "string"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "bigint"
        elif isinstance(value, float):
            return "double"
        elif isinstance(value, str):
            # Try to detect date/timestamp patterns
            if re.match(r"\d{4}-\d{2}-\d{2}", value):
                if "T" in value or " " in value:
                    return "timestamp"
                else:
                    return "date"
            return "string"
        elif isinstance(value, (list, tuple)):
            if value:
                element_type = self._infer_data_type(value[0])
                return f"array<{element_type}>"
            return "array<string>"
        elif isinstance(value, dict):
            return "struct<>"  # Would need deeper analysis for struct fields
        else:
            return "string"

    def _is_likely_partition_key(self, field_name: str, records: List[Dict]) -> bool:
        """Determine if a field is likely a partition key based on patterns."""
        # Common partition key patterns
        partition_patterns = [
            r".*year.*",
            r".*month.*",
            r".*day.*",
            r".*date.*",
            r".*partition.*",
            r".*bucket.*",
            r".*region.*",
        ]

        # Check field name patterns
        field_lower = field_name.lower()
        if any(re.match(pattern, field_lower) for pattern in partition_patterns):
            return True

        # Check value cardinality (low cardinality suggests partition key)
        if len(records) > 10:
            values = [
                record.get(field_name) for record in records if field_name in record
            ]
            unique_values = len(set(str(v) for v in values if v is not None))
            cardinality_ratio = unique_values / len(values) if values else 0

            # If less than 20% unique values, likely a partition key
            return cardinality_ratio < 0.2

        return False

    def _generate_schema_recommendations(
        self,
        columns: List[ColumnInfo],
        partitions: List[PartitionInfo],
        records: List[Dict],
    ) -> List[str]:
        """Generate schema optimization recommendations."""
        recommendations = []

        # Check for potential optimizations
        if len(columns) > 50:
            recommendations.append(
                "Consider using columnar format (Parquet/ORC) for better performance with wide tables"
            )

        if not partitions and len(records) > 1000:
            recommendations.append(
                "Consider adding partition keys for better query performance"
            )

        # Check for nested data
        nested_fields = [
            col for col in columns if "struct" in col.type or "array" in col.type
        ]
        if nested_fields:
            recommendations.append(
                "Consider flattening nested structures for better query performance"
            )

        # Check for string fields that might be better as other types
        string_fields = [col for col in columns if col.type == "string"]
        if len(string_fields) > len(columns) * 0.8:
            recommendations.append(
                "Review string fields - some might benefit from more specific data types"
            )

        return recommendations

    def _generate_terraform_table_resource(
        self, table: DataCatalogTable, table_definition: Dict
    ) -> str:
        """Generate Terraform resource for Glue table."""
        resource_name = f"{table.database_name}_{table.table_name}".replace("-", "_")

        columns_tf = "\n".join(
            [
                f'    {{\n      name = "{col["Name"]}"\n      type = "{col["Type"]}"\n      comment = "{col["Comment"]}"\n    }}'
                for col in table_definition["StorageDescriptor"]["Columns"]
            ]
        )

        partition_keys_tf = ""
        if table_definition["PartitionKeys"]:
            partition_keys_tf = "\n".join(
                [
                    f'    {{\n      name = "{part["Name"]}"\n      type = "{part["Type"]}"\n      comment = "{part["Comment"]}"\n    }}'
                    for part in table_definition["PartitionKeys"]
                ]
            )

        return f'''resource "aws_glue_catalog_table" "{resource_name}" {{
  name          = "{table.table_name}"
  database_name = "{table.database_name}"
  table_type    = "{table_definition["TableType"]}"

  storage_descriptor {{
    location      = "{table_definition["StorageDescriptor"]["Location"]}"
    input_format  = "{table_definition["StorageDescriptor"]["InputFormat"]}"
    output_format = "{table_definition["StorageDescriptor"]["OutputFormat"]}"
    compressed    = {str(table_definition["StorageDescriptor"]["Compressed"]).lower()}

    ser_de_info {{
      serialization_library = "{table_definition["StorageDescriptor"]["SerdeInfo"]["SerializationLibrary"]}"
      parameters = {{
{chr(10).join([f'        "{k}" = "{v}"' for k, v in table_definition["StorageDescriptor"]["SerdeInfo"]["Parameters"].items()])}
      }}
    }}

    columns {{
{columns_tf}
    }}
  }}

{f"""  partition_keys {{
{partition_keys_tf}
  }}""" if partition_keys_tf else ""}

  parameters = {{
{chr(10).join([f'    "{k}" = "{v}"' for k, v in table_definition["Parameters"].items()])}
  }}
}}'''

    def _generate_add_columns_sql(self, columns: List[ColumnInfo]) -> str:
        """Generate SQL for adding columns."""
        column_defs = [f"{col.name} {col.type}" for col in columns]
        return f"ALTER TABLE table_name ADD COLUMNS ({', '.join(column_defs)})"

    def _generate_modify_columns_sql(self, modified_columns: List[Dict]) -> str:
        """Generate SQL for modifying columns."""
        modifications = []
        for mod in modified_columns:
            col = mod["column"]
            new_type = mod["new_type"]
            modifications.append(
                f"ALTER TABLE table_name CHANGE {col.name} {col.name} {new_type}"
            )
        return "; ".join(modifications)

    def _generate_recreate_table_sql(self, schema: List[ColumnInfo]) -> str:
        """Generate SQL for recreating table with new schema."""
        column_defs = [f"{col.name} {col.type}" for col in schema]
        return f"CREATE TABLE new_table ({', '.join(column_defs)})"

    def _generate_schema_evolution_code(
        self,
        added_columns: List[ColumnInfo],
        modified_columns: List[Dict],
        config: SchemaEvolutionConfig,
    ) -> str:
        """Generate PySpark/DynamicFrame code for schema evolution."""
        code_lines = [
            "# Schema evolution transformation",
            "from awsglue.transforms import *",
            "",
        ]

        if added_columns:
            code_lines.extend(
                [
                    "# Add new columns with default values",
                    "def add_missing_columns(rec):",
                ]
            )
            for col in added_columns:
                default_value = self._get_default_value_for_type(col.type)
                code_lines.append(f'    rec["{col.name}"] = {default_value}')
            code_lines.extend(
                [
                    "    return rec",
                    "",
                    "transformed_df = Map.apply(frame=source_df, f=add_missing_columns)",
                    "",
                ]
            )

        if modified_columns:
            code_lines.extend(
                [
                    "# Convert column types",
                    "def convert_column_types(rec):",
                ]
            )
            for mod in modified_columns:
                col_name = mod["column"].name
                new_type = mod["new_type"]
                code_lines.append(
                    f'    # Convert {col_name} from {mod["old_type"]} to {new_type}'
                )
                code_lines.append(f'    if "{col_name}" in rec:')
                code_lines.append(
                    f'        rec["{col_name}"] = convert_to_{new_type.replace("<", "_").replace(">", "_")}(rec["{col_name}"])'
                )
            code_lines.extend(
                [
                    "    return rec",
                    "",
                    "transformed_df = Map.apply(frame=transformed_df, f=convert_column_types)",
                ]
            )

        return "\n".join(code_lines)

    def _get_default_value_for_type(self, data_type: str) -> str:
        """Get default value for a data type."""
        type_defaults = {
            "string": '""',
            "bigint": "0",
            "int": "0",
            "double": "0.0",
            "float": "0.0",
            "boolean": "False",
            "date": "None",
            "timestamp": "None",
        }

        if data_type.startswith("array"):
            return "[]"
        elif data_type.startswith("struct"):
            return "{}"
        else:
            return type_defaults.get(data_type, "None")

    def _generate_evolution_recommendations(
        self,
        added_columns: List[ColumnInfo],
        removed_columns: List[ColumnInfo],
        modified_columns: List[Dict],
    ) -> List[str]:
        """Generate recommendations for schema evolution."""
        recommendations = []

        if removed_columns:
            recommendations.append(
                "Removed columns detected - ensure downstream applications can handle missing fields"
            )

        if modified_columns:
            recommendations.append(
                "Column type changes detected - validate data compatibility before migration"
            )

        if len(added_columns) > 10:
            recommendations.append(
                "Many new columns added - consider impact on query performance"
            )

        if any(
            "string" in mod["old_type"] and "int" in mod["new_type"]
            for mod in modified_columns
        ):
            recommendations.append(
                "String to numeric conversions detected - validate data format consistency"
            )

        return recommendations

    def analyze_s3_optimization_opportunities(
        self,
        s3_location: str,
        table_info: DataCatalogTable,
        query_patterns: Optional[List[str]] = None,
        data_size_gb: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Analyze S3 data layout and suggest optimization opportunities.

        Args:
            s3_location: S3 location of the data
            table_info: Table information
            query_patterns: Common query patterns for optimization
            data_size_gb: Estimated data size in GB

        Returns:
            Dictionary containing optimization analysis and recommendations
        """
        try:
            issues = []
            recommendations = []
            estimated_improvements = {}

            # Analyze partitioning strategy
            partition_analysis = self._analyze_partitioning_strategy(
                table_info, query_patterns
            )
            issues.extend(partition_analysis["issues"])
            recommendations.extend(partition_analysis["recommendations"])

            # Analyze file size optimization
            file_size_analysis = self._analyze_file_size_optimization(
                s3_location, data_size_gb
            )
            issues.extend(file_size_analysis["issues"])
            recommendations.extend(file_size_analysis["recommendations"])

            # Analyze compression optimization
            compression_analysis = self._analyze_compression_optimization(table_info)
            issues.extend(compression_analysis["issues"])
            recommendations.extend(compression_analysis["recommendations"])

            # Analyze small files problem
            small_files_analysis = self._analyze_small_files_problem(
                s3_location, data_size_gb
            )
            issues.extend(small_files_analysis["issues"])
            recommendations.extend(small_files_analysis["recommendations"])

            # Generate estimated improvements
            estimated_improvements = {
                "query_performance": "20-50% faster queries with proper partitioning",
                "storage_cost": "10-30% reduction with optimal compression",
                "scan_efficiency": "Up to 90% reduction in data scanned",
                "file_operations": "50-80% fewer S3 operations with file consolidation",
            }

            # Generate implementation code
            implementation_code = self._generate_s3_optimization_code(
                table_info, recommendations
            )

            return {
                "status": "success",
                "s3_location": s3_location,
                "analysis_summary": {
                    "total_issues": len(issues),
                    "optimization_opportunities": len(recommendations),
                    "priority_level": self._calculate_priority_level(issues),
                },
                "current_issues": issues,
                "optimization_recommendations": recommendations,
                "estimated_improvements": estimated_improvements,
                "implementation_code": implementation_code,
                "next_steps": self._generate_optimization_next_steps(recommendations),
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def generate_s3_optimization_strategy(
        self,
        table_info: DataCatalogTable,
        optimization_config: S3OptimizationConfig,
        query_patterns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Generate comprehensive S3 optimization strategy.

        Args:
            table_info: Table information
            optimization_config: Optimization configuration
            query_patterns: Common query patterns

        Returns:
            Dictionary containing optimization strategy and implementation
        """
        try:
            # Generate partitioning strategy
            partitioning_strategy = self._generate_partitioning_strategy(
                table_info, optimization_config, query_patterns
            )

            # Generate file size optimization strategy
            file_optimization_strategy = self._generate_file_optimization_strategy(
                optimization_config
            )

            # Generate compression strategy
            compression_strategy = self._generate_compression_strategy(
                table_info.format, optimization_config
            )

            # Generate small files consolidation strategy
            consolidation_strategy = self._generate_consolidation_strategy(
                optimization_config
            )

            # Generate complete implementation code
            implementation_code = self._generate_complete_optimization_code(
                table_info,
                partitioning_strategy,
                file_optimization_strategy,
                compression_strategy,
                consolidation_strategy,
            )

            # Generate Glue job configuration
            glue_job_config = self._generate_optimization_glue_job_config(
                table_info, optimization_config
            )

            return {
                "status": "success",
                "optimization_strategy": {
                    "partitioning": partitioning_strategy,
                    "file_optimization": file_optimization_strategy,
                    "compression": compression_strategy,
                    "consolidation": consolidation_strategy,
                },
                "implementation_code": implementation_code,
                "glue_job_config": glue_job_config,
                "monitoring_queries": self._generate_monitoring_queries(table_info),
                "performance_benchmarks": self._generate_performance_benchmarks(),
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def generate_small_files_consolidation_job(
        self,
        source_table: DataCatalogTable,
        target_table: DataCatalogTable,
        target_file_size_mb: int = 128,
        consolidation_strategy: str = "coalesce",  # coalesce, repartition
    ) -> Dict[str, Any]:
        """
        Generate Glue job for small files consolidation.

        Args:
            source_table: Source table with small files
            target_table: Target table for consolidated files
            target_file_size_mb: Target file size in MB
            consolidation_strategy: Strategy for consolidation

        Returns:
            Dictionary containing consolidation job template
        """
        try:
            # Calculate optimal partition count based on data size
            estimated_partitions = self._calculate_optimal_partitions(
                target_file_size_mb, consolidation_strategy
            )

            # Generate consolidation job template
            job_template = f"""import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_DATABASE',
    'SOURCE_TABLE',
    'TARGET_DATABASE', 
    'TARGET_TABLE',
    'TARGET_FILE_SIZE_MB'
])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # Read source data
    source_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=args['SOURCE_DATABASE'],
        table_name=args['SOURCE_TABLE'],
        transformation_ctx="source_dynamic_frame"
    )
    
    # Convert to DataFrame for optimization
    source_df = source_dynamic_frame.toDF()
    
    # Calculate optimal partitions based on data size
    total_size_mb = source_df.count() * 0.001  # Rough estimate
    target_size_mb = int(args.get('TARGET_FILE_SIZE_MB', {target_file_size_mb}))
    optimal_partitions = max(1, int(total_size_mb / target_size_mb))
    
    print(f"Consolidating files: estimated {{total_size_mb}}MB into {{optimal_partitions}} partitions")
    
    # Apply consolidation strategy
    if "{consolidation_strategy}" == "coalesce":
        # Coalesce reduces partitions without full shuffle
        optimized_df = source_df.coalesce(optimal_partitions)
    else:
        # Repartition for better data distribution
        if source_df.schema.names and len({source_table.partition_keys or []}) > 0:
            # Repartition by partition keys if available
            partition_cols = {source_table.partition_keys or ['rand()']}
            optimized_df = source_df.repartition(optimal_partitions, *partition_cols)
        else:
            optimized_df = source_df.repartition(optimal_partitions)
    
    # Convert back to DynamicFrame
    optimized_dynamic_frame = DynamicFrame.fromDF(
        optimized_df,
        glueContext,
        "optimized_dynamic_frame"
    )
    
    # Write consolidated data
    glueContext.write_dynamic_frame.from_catalog(
        frame=optimized_dynamic_frame,
        database=args['TARGET_DATABASE'],
        table_name=args['TARGET_TABLE'],
        transformation_ctx="write_optimized"
    )
    
    print(f"Small files consolidation completed successfully")
    
except Exception as e:
    print(f"Consolidation job failed: {{str(e)}}")
    raise e
    
finally:
    job.commit()
"""

            return {
                "status": "success",
                "job_template": job_template,
                "job_name": f"{source_table.table_name}_consolidation",
                "estimated_partitions": estimated_partitions,
                "consolidation_strategy": consolidation_strategy,
                "target_file_size_mb": target_file_size_mb,
                "job_parameters": {
                    "SOURCE_DATABASE": source_table.database_name,
                    "SOURCE_TABLE": source_table.table_name,
                    "TARGET_DATABASE": target_table.database_name,
                    "TARGET_TABLE": target_table.table_name,
                    "TARGET_FILE_SIZE_MB": str(target_file_size_mb),
                },
                "performance_tips": [
                    "Run during low-traffic periods to minimize impact",
                    "Monitor Spark UI for partition distribution",
                    "Consider incremental consolidation for large datasets",
                    "Validate data integrity after consolidation",
                ],
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _analyze_partitioning_strategy(
        self, table_info: DataCatalogTable, query_patterns: Optional[List[str]]
    ) -> Dict[str, List[str]]:
        """Analyze current partitioning strategy."""
        issues = []
        recommendations = []

        if not table_info.partitions or len(table_info.partitions) == 0:
            issues.append("No partitioning detected - all data in single partition")
            recommendations.append(
                "Implement partitioning by date, region, or other high-cardinality columns"
            )

        if table_info.partitions and len(table_info.partitions) > 3:
            issues.append("Over-partitioning detected - too many partition levels")
            recommendations.append(
                "Reduce partition levels to 1-3 for optimal performance"
            )

        if query_patterns:
            # Analyze query patterns for partition pruning opportunities
            date_queries = any(
                "date" in query.lower() or "timestamp" in query.lower()
                for query in query_patterns
            )
            if date_queries and not any(
                "date" in p.key.lower() for p in (table_info.partitions or [])
            ):
                recommendations.append(
                    "Add date-based partitioning for time-series queries"
                )

        return {"issues": issues, "recommendations": recommendations}

    def _analyze_file_size_optimization(
        self, s3_location: str, data_size_gb: Optional[float]
    ) -> Dict[str, List[str]]:
        """Analyze file size optimization opportunities."""
        issues = []
        recommendations = []

        if data_size_gb:
            # Estimate number of files based on typical patterns
            if data_size_gb > 100:  # Large dataset
                issues.append(
                    "Large dataset detected - may have file size optimization opportunities"
                )
                recommendations.append(
                    "Implement file size optimization with 128-256MB target files"
                )

            if data_size_gb < 1:  # Small dataset
                recommendations.append("Consider single file output for small datasets")

        # General recommendations
        recommendations.extend(
            [
                "Use coalesce() or repartition() to control output file sizes",
                "Target 128-256MB files for optimal S3 and Spark performance",
                "Monitor file count and sizes in S3 console",
            ]
        )

        return {"issues": issues, "recommendations": recommendations}

    def _analyze_compression_optimization(
        self, table_info: DataCatalogTable
    ) -> Dict[str, List[str]]:
        """Analyze compression optimization opportunities."""
        issues = []
        recommendations = []

        if table_info.format == DataFormat.CSV:
            issues.append(
                "CSV format detected - poor compression and query performance"
            )
            recommendations.append(
                "Convert to Parquet format for better compression and performance"
            )

        if table_info.format == DataFormat.JSON:
            issues.append("JSON format detected - suboptimal for analytics workloads")
            recommendations.append("Convert to Parquet format for columnar analytics")

        if table_info.format in [DataFormat.PARQUET, DataFormat.ORC]:
            recommendations.append(
                "Enable compression (Snappy/GZIP) for storage cost reduction"
            )

        return {"issues": issues, "recommendations": recommendations}

    def _analyze_small_files_problem(
        self, s3_location: str, data_size_gb: Optional[float]
    ) -> Dict[str, List[str]]:
        """Analyze small files problem."""
        issues = []
        recommendations = []

        # Heuristic: if we have small data but it's likely spread across many files
        if data_size_gb and data_size_gb < 10:
            issues.append(
                "Potential small files problem - many small files impact performance"
            )
            recommendations.extend(
                [
                    "Implement file consolidation job to merge small files",
                    "Use coalesce() to reduce number of output files",
                    "Consider batch processing to accumulate data before writing",
                ]
            )

        return {"issues": issues, "recommendations": recommendations}

    def _generate_s3_optimization_code(
        self, table_info: DataCatalogTable, recommendations: List[str]
    ) -> str:
        """Generate PySpark code for S3 optimizations."""
        code_lines = [
            "# S3 Optimization Implementation",
            "from pyspark.sql.functions import *",
            "",
            "# Read data",
            f'df = spark.read.format("{table_info.format.value if table_info.format else "parquet"}").load("s3://your-bucket/path/")',
            "",
        ]

        # Add partitioning optimization
        if any("partitioning" in rec for rec in recommendations):
            code_lines.extend(
                [
                    "# Optimize partitioning",
                    "# Repartition by commonly queried columns",
                    "optimized_df = df.repartition(col('year'), col('month'))",
                    "",
                ]
            )

        # Add file size optimization
        if any("file size" in rec for rec in recommendations):
            code_lines.extend(
                [
                    "# Optimize file sizes",
                    "# Target 128-256MB files",
                    "optimized_df = optimized_df.coalesce(10)  # Adjust based on data size",
                    "",
                ]
            )

        # Add compression optimization
        if any("compression" in rec for rec in recommendations):
            code_lines.extend(
                [
                    "# Write with compression",
                    "optimized_df.write \\",
                    "    .mode('overwrite') \\",
                    "    .option('compression', 'snappy') \\",
                    f"    .format('{table_info.format.value if table_info.format else 'parquet'}') \\",
                    "    .save('s3://your-bucket/optimized-path/')",
                ]
            )

        return "\n".join(code_lines)

    def _calculate_priority_level(self, issues: List[str]) -> str:
        """Calculate optimization priority level."""
        if len(issues) >= 5:
            return "HIGH"
        elif len(issues) >= 3:
            return "MEDIUM"
        elif len(issues) >= 1:
            return "LOW"
        else:
            return "NONE"

    def _generate_optimization_next_steps(
        self, recommendations: List[str]
    ) -> List[str]:
        """Generate next steps for optimization implementation."""
        next_steps = [
            "1. Backup existing data before optimization",
            "2. Test optimization on a subset of data first",
            "3. Monitor query performance before and after optimization",
            "4. Implement optimizations during low-traffic periods",
            "5. Validate data integrity after optimization",
        ]

        if any("partitioning" in rec for rec in recommendations):
            next_steps.append(
                "6. Update Glue Data Catalog with new partition structure"
            )

        if any("compression" in rec for rec in recommendations):
            next_steps.append("7. Monitor storage cost reduction after compression")

        return next_steps

    def _generate_partitioning_strategy(
        self,
        table_info: DataCatalogTable,
        config: S3OptimizationConfig,
        query_patterns: Optional[List[str]],
    ) -> Dict[str, Any]:
        """Generate partitioning strategy."""
        strategy = {
            "current_partitions": len(table_info.partitions or []),
            "recommended_partitions": [],
            "partition_pruning_enabled": config.enable_partition_pruning,
        }

        # Analyze query patterns for partition recommendations
        if query_patterns:
            for pattern in query_patterns:
                if "date" in pattern.lower() or "timestamp" in pattern.lower():
                    strategy["recommended_partitions"].append("date-based partitioning")
                if "region" in pattern.lower() or "country" in pattern.lower():
                    strategy["recommended_partitions"].append("geographic partitioning")

        return strategy

    def _generate_file_optimization_strategy(
        self, config: S3OptimizationConfig
    ) -> Dict[str, Any]:
        """Generate file optimization strategy."""
        return {
            "target_file_size_mb": config.target_file_size_mb,
            "max_files_per_partition": config.max_files_per_partition,
            "coalesce_enabled": config.coalesce_partitions,
            "small_file_optimization": config.enable_small_file_optimization,
        }

    def _generate_compression_strategy(
        self, data_format: Optional[DataFormat], config: S3OptimizationConfig
    ) -> Dict[str, Any]:
        """Generate compression strategy."""
        return {
            "compression_type": config.compression_type,
            "current_format": data_format.value if data_format else "unknown",
            "recommended_format": (
                "parquet" if data_format != DataFormat.PARQUET else data_format.value
            ),
        }

    def _generate_consolidation_strategy(
        self, config: S3OptimizationConfig
    ) -> Dict[str, Any]:
        """Generate consolidation strategy."""
        return {
            "enabled": config.enable_small_file_optimization,
            "target_file_size_mb": config.target_file_size_mb,
            "consolidation_method": (
                "coalesce" if config.coalesce_partitions else "repartition"
            ),
        }

    def _generate_complete_optimization_code(
        self,
        table_info: DataCatalogTable,
        partitioning_strategy: Dict[str, Any],
        file_strategy: Dict[str, Any],
        compression_strategy: Dict[str, Any],
        consolidation_strategy: Dict[str, Any],
    ) -> str:
        """Generate complete optimization implementation code."""
        return f"""# Complete S3 Optimization Implementation
from pyspark.sql.functions import *

# Read source data
source_df = spark.read.format("{table_info.format.value if table_info.format else 'parquet'}").load("s3://source-bucket/path/")

# Apply partitioning optimization
if {partitioning_strategy['partition_pruning_enabled']}:
    # Enable partition pruning
    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# Apply file size optimization
target_partitions = max(1, int(source_df.count() * 0.001 / {file_strategy['target_file_size_mb']}))
optimized_df = source_df.coalesce(target_partitions)

# Write with optimizations
optimized_df.write \\
    .mode('overwrite') \\
    .option('compression', '{compression_strategy['compression_type']}') \\
    .format('{compression_strategy['recommended_format']}') \\
    .save('s3://optimized-bucket/path/')

print("S3 optimization completed successfully")
"""

    def _generate_optimization_glue_job_config(
        self, table_info: DataCatalogTable, config: S3OptimizationConfig
    ) -> Dict[str, Any]:
        """Generate Glue job configuration for optimization."""
        return {
            "job_name": f"{table_info.table_name}_s3_optimization",
            "worker_type": "G.1X",
            "number_of_workers": 2,
            "max_retries": 1,
            "timeout": 2880,
            "default_arguments": {
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--job-bookmark-option": "job-bookmark-disable",
                "--target-file-size-mb": str(config.target_file_size_mb),
                "--compression-type": config.compression_type,
            },
        }

    def _generate_monitoring_queries(self, table_info: DataCatalogTable) -> List[str]:
        """Generate monitoring queries for optimization validation."""
        return [
            f"-- Check file count and sizes\nSELECT COUNT(*) as file_count FROM {table_info.database_name}.{table_info.table_name}$files",
            f"-- Check partition distribution\nSHOW PARTITIONS {table_info.database_name}.{table_info.table_name}",
            f"-- Analyze table statistics\nANALYZE TABLE {table_info.database_name}.{table_info.table_name} COMPUTE STATISTICS",
        ]

    def _generate_performance_benchmarks(self) -> Dict[str, str]:
        """Generate performance benchmarks for optimization validation."""
        return {
            "query_performance": "Measure query execution time before and after optimization",
            "storage_cost": "Compare S3 storage costs monthly",
            "scan_efficiency": "Monitor data scanned vs. data returned ratio",
            "file_operations": "Count S3 LIST and GET operations per query",
        }

    def _calculate_optimal_partitions(
        self, target_file_size_mb: int, strategy: str
    ) -> int:
        """Calculate optimal number of partitions for consolidation."""
        # This is a simplified calculation - in practice would analyze actual data
        base_partitions = max(1, 1000 // target_file_size_mb)  # Rough estimate

        if strategy == "coalesce":
            return base_partitions
        else:
            return base_partitions * 2  # Repartition allows for better distribution

    def generate_incremental_processing_job(
        self,
        source_table: DataCatalogTable,
        target_table: DataCatalogTable,
        incremental_column: str,
        incremental_strategy: str = "timestamp",  # timestamp, watermark, bookmark
        transformation_sql: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate Glue job with incremental processing and job bookmarking.

        Args:
            source_table: Source table configuration
            target_table: Target table configuration
            incremental_column: Column used for incremental processing
            incremental_strategy: Strategy for incremental processing
            transformation_sql: Optional SQL transformation logic

        Returns:
            Dictionary containing incremental processing job template
        """
        try:
            # Generate job template based on incremental strategy
            if incremental_strategy == "timestamp":
                job_template = self._generate_timestamp_incremental_job(
                    source_table, target_table, incremental_column, transformation_sql
                )
            elif incremental_strategy == "watermark":
                job_template = self._generate_watermark_incremental_job(
                    source_table, target_table, incremental_column, transformation_sql
                )
            elif incremental_strategy == "bookmark":
                job_template = self._generate_bookmark_incremental_job(
                    source_table, target_table, incremental_column, transformation_sql
                )
            else:
                raise ValueError(
                    f"Unsupported incremental strategy: {incremental_strategy}"
                )

            return {
                "status": "success",
                "job_template": job_template,
                "job_name": f"{source_table.table_name}_incremental_{incremental_strategy}",
                "incremental_strategy": incremental_strategy,
                "incremental_column": incremental_column,
                "job_parameters": {
                    "SOURCE_DATABASE": source_table.database_name,
                    "SOURCE_TABLE": source_table.table_name,
                    "TARGET_DATABASE": target_table.database_name,
                    "TARGET_TABLE": target_table.table_name,
                    "INCREMENTAL_COLUMN": incremental_column,
                    "JOB_BOOKMARK_OPTION": "job-bookmark-enable",
                },
                "monitoring_setup": self._generate_incremental_monitoring_setup(),
                "best_practices": self._generate_incremental_best_practices(
                    incremental_strategy
                ),
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def generate_job_bookmark_configuration(
        self,
        job_name: str,
        bookmark_strategy: str = "enable",  # enable, disable, pause
        transformation_context_keys: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Generate job bookmark configuration for Glue jobs.

        Args:
            job_name: Name of the Glue job
            bookmark_strategy: Bookmark strategy (enable, disable, pause)
            transformation_context_keys: List of transformation context keys

        Returns:
            Dictionary containing bookmark configuration
        """
        try:
            bookmark_config = {
                "job_name": job_name,
                "bookmark_option": f"job-bookmark-{bookmark_strategy}",
                "transformation_contexts": transformation_context_keys or [],
            }

            # Generate bookmark management commands
            management_commands = {
                "reset_bookmark": f"aws glue reset-job-bookmark --job-name {job_name}",
                "get_bookmark": f"aws glue get-job-bookmark --job-name {job_name}",
                "update_bookmark": f'aws glue update-job --job-name {job_name} --job-update \'{{"DefaultArguments": {{"--job-bookmark-option": "job-bookmark-{bookmark_strategy}"}}}}\'',
            }

            # Generate bookmark monitoring queries
            monitoring_queries = [
                f"-- Check bookmark status\nSELECT * FROM information_schema.job_bookmarks WHERE job_name = '{job_name}'",
                f"-- Monitor incremental processing\nSELECT COUNT(*) as processed_records, MAX(last_modified) as last_processed FROM target_table",
            ]

            return {
                "status": "success",
                "bookmark_configuration": bookmark_config,
                "management_commands": management_commands,
                "monitoring_queries": monitoring_queries,
                "troubleshooting_guide": self._generate_bookmark_troubleshooting_guide(),
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def generate_change_data_capture_job(
        self,
        source_table: DataCatalogTable,
        target_table: DataCatalogTable,
        cdc_column: str = "last_modified",
        cdc_strategy: str = "upsert",  # upsert, append, merge
        primary_keys: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Generate Glue job for Change Data Capture (CDC) processing.

        Args:
            source_table: Source table configuration
            target_table: Target table configuration
            cdc_column: Column indicating change timestamp
            cdc_strategy: CDC processing strategy
            primary_keys: Primary key columns for upsert operations

        Returns:
            Dictionary containing CDC job template
        """
        try:
            job_template = f"""import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_DATABASE',
    'SOURCE_TABLE',
    'TARGET_DATABASE',
    'TARGET_TABLE',
    'CDC_COLUMN',
    'LAST_PROCESSED_TIMESTAMP'
])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # Get last processed timestamp from job bookmark or parameter
    last_processed = args.get('LAST_PROCESSED_TIMESTAMP', '1970-01-01 00:00:00')
    
    print(f"Processing changes since: {{last_processed}}")
    
    # Read source data with incremental filter
    source_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=args['SOURCE_DATABASE'],
        table_name=args['SOURCE_TABLE'],
        transformation_ctx="source_dynamic_frame",
        push_down_predicate=f"{{args['CDC_COLUMN']}} > '{{last_processed}}'"
    )
    
    # Convert to DataFrame
    source_df = source_dynamic_frame.toDF()
    
    # Check if there are new records to process
    new_records_count = source_df.count()
    print(f"Found {{new_records_count}} new/changed records")
    
    if new_records_count > 0:
        # Get current max timestamp for next run
        max_timestamp = source_df.agg(max(col(args['CDC_COLUMN']))).collect()[0][0]
        
        # Apply CDC strategy
        if "{cdc_strategy}" == "upsert":
            # Read existing target data
            try:
                target_df = spark.read.format("delta").table(f"{{args['TARGET_DATABASE']}}.{{args['TARGET_TABLE']}}")
                
                # Perform upsert using Delta Lake
                delta_table = DeltaTable.forName(spark, f"{{args['TARGET_DATABASE']}}.{{args['TARGET_TABLE']}}")
                
                # Build merge condition based on primary keys
                primary_keys = {primary_keys or ['id']}
                merge_condition = " AND ".join([f"target.{{key}} = source.{{key}}" for key in primary_keys])
                
                delta_table.alias("target") \\
                    .merge(source_df.alias("source"), merge_condition) \\
                    .whenMatchedUpdateAll() \\
                    .whenNotMatchedInsertAll() \\
                    .execute()
                    
                print(f"Upserted {{new_records_count}} records")
                
            except Exception as e:
                print(f"Target table doesn't exist or not Delta format, creating new table: {{e}}")
                # Create new Delta table
                source_df.write.format("delta").mode("overwrite").saveAsTable(f"{{args['TARGET_DATABASE']}}.{{args['TARGET_TABLE']}}")
                
        elif "{cdc_strategy}" == "append":
            # Simple append strategy
            source_df.write.format("delta").mode("append").saveAsTable(f"{{args['TARGET_DATABASE']}}.{{args['TARGET_TABLE']}}")
            print(f"Appended {{new_records_count}} records")
            
        elif "{cdc_strategy}" == "merge":
            # Custom merge logic with deduplication
            # Remove duplicates within the batch
            deduplicated_df = source_df.dropDuplicates({primary_keys or ['id']})
            
            # Write to target
            deduplicated_df.write.format("delta").mode("append").saveAsTable(f"{{args['TARGET_DATABASE']}}.{{args['TARGET_TABLE']}}")
            print(f"Merged {{deduplicated_df.count()}} unique records")
        
        # Update job bookmark with max timestamp
        job.commit()
        print(f"Job bookmark updated with timestamp: {{max_timestamp}}")
        
    else:
        print("No new records to process")
        job.commit()
    
except Exception as e:
    print(f"CDC job failed: {{str(e)}}")
    raise e
    
finally:
    print("CDC processing completed")
"""

            return {
                "status": "success",
                "job_template": job_template,
                "job_name": f"{source_table.table_name}_cdc_{cdc_strategy}",
                "cdc_strategy": cdc_strategy,
                "cdc_column": cdc_column,
                "primary_keys": primary_keys or ["id"],
                "job_parameters": {
                    "SOURCE_DATABASE": source_table.database_name,
                    "SOURCE_TABLE": source_table.table_name,
                    "TARGET_DATABASE": target_table.database_name,
                    "TARGET_TABLE": target_table.table_name,
                    "CDC_COLUMN": cdc_column,
                    "JOB_BOOKMARK_OPTION": "job-bookmark-enable",
                },
                "prerequisites": [
                    "Target table should use Delta Lake format for upsert operations",
                    "Source table should have a reliable timestamp column",
                    "Primary keys should be defined for upsert strategy",
                ],
                "monitoring_metrics": self._generate_cdc_monitoring_metrics(),
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _generate_timestamp_incremental_job(
        self,
        source_table: DataCatalogTable,
        target_table: DataCatalogTable,
        timestamp_column: str,
        transformation_sql: Optional[str],
    ) -> str:
        """Generate timestamp-based incremental processing job."""
        return f"""import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from datetime import datetime, timedelta

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_DATABASE',
    'SOURCE_TABLE',
    'TARGET_DATABASE',
    'TARGET_TABLE',
    'INCREMENTAL_COLUMN',
    'LOOKBACK_HOURS'
])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # Calculate incremental window
    lookback_hours = int(args.get('LOOKBACK_HOURS', 24))
    current_time = datetime.now()
    start_time = current_time - timedelta(hours=lookback_hours)
    
    print(f"Processing data from {{start_time}} to {{current_time}}")
    
    # Read incremental data
    source_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=args['SOURCE_DATABASE'],
        table_name=args['SOURCE_TABLE'],
        transformation_ctx="source_dynamic_frame",
        push_down_predicate=f"{{args['INCREMENTAL_COLUMN']}} >= '{{start_time.strftime('%Y-%m-%d %H:%M:%S')}}'"
    )
    
    # Convert to DataFrame
    source_df = source_dynamic_frame.toDF()
    
    # Apply transformations
    if transformation_sql:
        # Apply SQL transformations
        source_df.createOrReplaceTempView("source_table")
        transformed_df = spark.sql(transformation_sql)
    else:
        # Apply default transformations
        transformed_df = source_df.withColumn("processed_timestamp", current_timestamp())
    
    # Convert back to DynamicFrame
    target_dynamic_frame = DynamicFrame.fromDF(
        transformed_df,
        glueContext,
        "target_dynamic_frame"
    )
    
    # Write to target
    glueContext.write_dynamic_frame.from_catalog(
        frame=target_dynamic_frame,
        database=args['TARGET_DATABASE'],
        table_name=args['TARGET_TABLE'],
        transformation_ctx="write_target"
    )
    
    print(f"Processed {{transformed_df.count()}} records")
    
except Exception as e:
    print(f"Incremental job failed: {{str(e)}}")
    raise e
    
finally:
    job.commit()
"""

    def _generate_watermark_incremental_job(
        self,
        source_table: DataCatalogTable,
        target_table: DataCatalogTable,
        watermark_column: str,
        transformation_sql: Optional[str],
    ) -> str:
        """Generate watermark-based incremental processing job."""
        return f"""import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_DATABASE',
    'SOURCE_TABLE',
    'TARGET_DATABASE',
    'TARGET_TABLE',
    'INCREMENTAL_COLUMN',
    'WATERMARK_TABLE'
])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # Get last watermark value
    watermark_table = args.get('WATERMARK_TABLE', 'job_watermarks')
    job_name = args['JOB_NAME']
    
    try:
        watermark_df = spark.sql(f"SELECT max_value FROM {{watermark_table}} WHERE job_name = '{{job_name}}'")
        last_watermark = watermark_df.collect()[0]['max_value'] if watermark_df.count() > 0 else '1970-01-01'
    except:
        last_watermark = '1970-01-01'
        print(f"Watermark table not found, starting from beginning")
    
    print(f"Processing data since watermark: {{last_watermark}}")
    
    # Read incremental data
    source_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=args['SOURCE_DATABASE'],
        table_name=args['SOURCE_TABLE'],
        transformation_ctx="source_dynamic_frame",
        push_down_predicate=f"{{args['INCREMENTAL_COLUMN']}} > '{{last_watermark}}'"
    )
    
    # Convert to DataFrame
    source_df = source_dynamic_frame.toDF()
    
    if source_df.count() > 0:
        # Get new watermark value
        new_watermark = source_df.agg(max(col(args['INCREMENTAL_COLUMN']))).collect()[0][0]
        
        # Apply transformations
        if transformation_sql:
            # Apply SQL transformations
            source_df.createOrReplaceTempView("source_table")
            transformed_df = spark.sql(transformation_sql)
        else:
            # Apply default transformations
            transformed_df = source_df.withColumn("processed_timestamp", current_timestamp())
        
        # Convert back to DynamicFrame
        target_dynamic_frame = DynamicFrame.fromDF(
            transformed_df,
            glueContext,
            "target_dynamic_frame"
        )
        
        # Write to target
        glueContext.write_dynamic_frame.from_catalog(
            frame=target_dynamic_frame,
            database=args['TARGET_DATABASE'],
            table_name=args['TARGET_TABLE'],
            transformation_ctx="write_target"
        )
        
        # Update watermark
        watermark_update_df = spark.createDataFrame([
            (job_name, str(new_watermark), current_timestamp())
        ], ["job_name", "max_value", "updated_at"])
        
        watermark_update_df.write.mode("overwrite").insertInto(watermark_table)
        
        print(f"Processed {{transformed_df.count()}} records, new watermark: {{new_watermark}}")
    else:
        print("No new data to process")
    
except Exception as e:
    print(f"Watermark job failed: {{str(e)}}")
    raise e
    
finally:
    job.commit()
"""

    def _generate_bookmark_incremental_job(
        self,
        source_table: DataCatalogTable,
        target_table: DataCatalogTable,
        bookmark_column: str,
        transformation_sql: Optional[str],
    ) -> str:
        """Generate Glue job bookmark-based incremental processing job."""
        return f"""import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_DATABASE',
    'SOURCE_TABLE',
    'TARGET_DATABASE',
    'TARGET_TABLE',
    'INCREMENTAL_COLUMN'
])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # Read source data with job bookmark
    # Glue automatically handles incremental reading based on job bookmark
    source_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=args['SOURCE_DATABASE'],
        table_name=args['SOURCE_TABLE'],
        transformation_ctx="source_dynamic_frame_with_bookmark"
    )
    
    # Convert to DataFrame
    source_df = source_dynamic_frame.toDF()
    
    print(f"Processing {{source_df.count()}} new records from job bookmark")
    
    if source_df.count() > 0:
        # Apply transformations
        if transformation_sql:
            # Apply SQL transformations
            source_df.createOrReplaceTempView("source_table")
            transformed_df = spark.sql(transformation_sql)
        else:
            # Apply default transformations
            transformed_df = source_df.withColumn("processed_timestamp", current_timestamp())
        
        # Convert back to DynamicFrame
        target_dynamic_frame = DynamicFrame.fromDF(
            transformed_df,
            glueContext,
            "target_dynamic_frame_with_bookmark"
        )
        
        # Write to target with job bookmark
        glueContext.write_dynamic_frame.from_catalog(
            frame=target_dynamic_frame,
            database=args['TARGET_DATABASE'],
            table_name=args['TARGET_TABLE'],
            transformation_ctx="write_target_with_bookmark"
        )
        
        print(f"Successfully processed {{transformed_df.count()}} records")
    else:
        print("No new data to process based on job bookmark")
    
except Exception as e:
    print(f"Bookmark job failed: {{str(e)}}")
    raise e
    
finally:
    # Commit job bookmark
    job.commit()
    print("Job bookmark committed successfully")
"""

    def _generate_incremental_monitoring_setup(self) -> Dict[str, Any]:
        """Generate monitoring setup for incremental processing."""
        return {
            "cloudwatch_metrics": [
                "RecordsProcessed",
                "ProcessingLatency",
                "BookmarkUpdateSuccess",
                "IncrementalWindowSize",
            ],
            "cloudwatch_alarms": [
                {
                    "name": "IncrementalProcessingFailure",
                    "metric": "JobRunState",
                    "threshold": "FAILED",
                    "action": "SNS notification",
                },
                {
                    "name": "ProcessingLatencyHigh",
                    "metric": "ProcessingLatency",
                    "threshold": "> 30 minutes",
                    "action": "SNS notification",
                },
            ],
            "dashboard_widgets": [
                "Processing volume over time",
                "Job success rate",
                "Average processing latency",
                "Bookmark progression",
            ],
        }

    def _generate_incremental_best_practices(self, strategy: str) -> List[str]:
        """Generate best practices for incremental processing."""
        common_practices = [
            "Always test incremental logic with historical data",
            "Monitor data freshness and processing latency",
            "Implement data quality checks on incremental data",
            "Set up alerting for job failures and data delays",
            "Document the incremental processing logic and dependencies",
        ]

        strategy_specific = {
            "timestamp": [
                "Ensure timestamp column is indexed for performance",
                "Handle timezone conversions consistently",
                "Account for late-arriving data with appropriate lookback window",
            ],
            "watermark": [
                "Implement watermark table with proper backup and recovery",
                "Use atomic updates for watermark values",
                "Monitor watermark progression for data pipeline health",
            ],
            "bookmark": [
                "Understand Glue job bookmark limitations and behavior",
                "Test bookmark reset procedures",
                "Monitor bookmark state in CloudWatch",
            ],
        }

        return common_practices + strategy_specific.get(strategy, [])

    def _generate_bookmark_troubleshooting_guide(self) -> Dict[str, str]:
        """Generate troubleshooting guide for job bookmarks."""
        return {
            "bookmark_not_advancing": "Check if transformation_ctx is unique and consistent across job runs",
            "duplicate_data_processing": "Verify job bookmark is enabled and transformation contexts are properly set",
            "bookmark_reset_needed": "Use 'aws glue reset-job-bookmark --job-name <job-name>' to reset bookmark state",
            "bookmark_corruption": "Check CloudWatch logs for bookmark-related errors and consider resetting bookmark",
            "performance_degradation": "Monitor bookmark metadata size and consider periodic cleanup",
        }

    def _generate_cdc_monitoring_metrics(self) -> Dict[str, str]:
        """Generate monitoring metrics for CDC processing."""
        return {
            "records_processed": "Number of records processed in each CDC run",
            "processing_latency": "Time between data change and processing completion",
            "change_detection_accuracy": "Percentage of actual changes detected by CDC logic",
            "upsert_success_rate": "Success rate of upsert operations",
            "data_freshness": "Age of the most recent processed record",
            "conflict_resolution": "Number of conflicts resolved during merge operations",
        }
