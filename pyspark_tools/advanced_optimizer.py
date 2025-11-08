"""Advanced optimization engine for PySpark code with data flow analysis and performance estimation."""

import ast
import hashlib
import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


class OptimizationType(Enum):
    """Types of optimizations that can be applied."""

    PARTITIONING = "partitioning"
    JOIN_STRATEGY = "join_strategy"
    CACHING = "caching"
    COLUMN_PRUNING = "column_pruning"
    PREDICATE_PUSHDOWN = "predicate_pushdown"
    BROADCAST_JOIN = "broadcast_join"
    COALESCE = "coalesce"
    REPARTITION = "repartition"


@dataclass
class DataFlowNode:
    """Represents a node in the data flow graph."""

    node_id: str
    operation: str
    table_name: Optional[str] = None
    columns_used: Set[str] = None
    estimated_rows: Optional[int] = None
    estimated_size_mb: Optional[float] = None
    dependencies: List[str] = None

    def __post_init__(self):
        if self.columns_used is None:
            self.columns_used = set()
        if self.dependencies is None:
            self.dependencies = []


@dataclass
class DataFlowAnalysis:
    """Results of data flow analysis."""

    nodes: Dict[str, DataFlowNode]
    execution_order: List[str]
    join_operations: List[Dict[str, Any]]
    aggregation_operations: List[Dict[str, Any]]
    filter_operations: List[Dict[str, Any]]
    column_lineage: Dict[str, Set[str]]
    estimated_total_cost: float


@dataclass
class PartitioningStrategy:
    """Partitioning strategy recommendation."""

    table_name: str
    partition_columns: List[str]
    partition_type: str  # "hash", "range", "custom"
    estimated_partitions: int
    reasoning: str
    performance_impact: float  # Expected improvement percentage


@dataclass
class JoinOptimization:
    """Join optimization recommendation."""

    join_id: str
    left_table: str
    right_table: str
    join_type: str
    join_keys: List[str]
    recommended_strategy: str  # "broadcast", "sort_merge", "shuffle_hash"
    estimated_left_size: Optional[float]
    estimated_right_size: Optional[float]
    reasoning: str
    performance_impact: float


@dataclass
class PerformanceEstimate:
    """Performance impact estimation for optimizations."""

    optimization_type: OptimizationType
    estimated_improvement: float  # Percentage improvement
    confidence_level: float  # 0.0 to 1.0
    resource_impact: Dict[str, str]  # CPU, Memory, Network, etc.
    implementation_complexity: str  # "low", "medium", "high"
    prerequisites: List[str]


@dataclass
class OptimizationRecommendation:
    """Complete optimization recommendation."""

    optimization_id: str
    optimization_type: OptimizationType
    title: str
    description: str
    code_changes: List[str]
    performance_estimate: PerformanceEstimate
    priority: str  # "high", "medium", "low"
    applicable_conditions: List[str]


class AdvancedOptimizer:
    """Advanced optimization engine for PySpark code."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # Performance impact models based on common patterns
        self.performance_models = {
            OptimizationType.BROADCAST_JOIN: {
                "small_table_threshold_mb": 200,
                "expected_improvement": 0.4,  # 40% improvement
                "confidence_base": 0.8,
            },
            OptimizationType.JOIN_STRATEGY: {
                "expected_improvement": 0.3,  # 30% improvement
                "confidence_base": 0.75,
            },
            OptimizationType.PARTITIONING: {
                "expected_improvement": 0.25,  # 25% improvement
                "confidence_base": 0.7,
            },
            OptimizationType.CACHING: {
                "reuse_threshold": 2,
                "expected_improvement": 0.3,  # 30% improvement
                "confidence_base": 0.9,
            },
            OptimizationType.COLUMN_PRUNING: {
                "expected_improvement": 0.15,  # 15% improvement
                "confidence_base": 0.85,
            },
            OptimizationType.COALESCE: {
                "expected_improvement": 0.2,  # 20% improvement
                "confidence_base": 0.7,
            },
            OptimizationType.REPARTITION: {
                "expected_improvement": 0.25,  # 25% improvement
                "confidence_base": 0.6,
            },
            OptimizationType.PREDICATE_PUSHDOWN: {
                "expected_improvement": 0.2,  # 20% improvement
                "confidence_base": 0.8,
            },
        }

        # Common table size estimates (in MB) for performance calculations
        self.default_table_sizes = {
            "small": 50,
            "medium": 500,
            "large": 5000,
            "xlarge": 50000,
        }

    def analyze_data_flow(
        self, pyspark_code: str, table_info: Optional[Dict] = None
    ) -> DataFlowAnalysis:
        """
        Analyze data flow patterns in PySpark code.

        Args:
            pyspark_code: PySpark code to analyze
            table_info: Optional table metadata for better analysis

        Returns:
            DataFlowAnalysis with comprehensive flow information
        """
        try:
            # Parse the code to extract operations
            nodes = self._extract_dataflow_nodes(pyspark_code, table_info)

            # Determine execution order
            execution_order = self._determine_execution_order(nodes)

            # Extract specific operation types
            join_ops = self._extract_join_operations(pyspark_code, nodes)
            agg_ops = self._extract_aggregation_operations(pyspark_code, nodes)
            filter_ops = self._extract_filter_operations(pyspark_code, nodes)

            # Build column lineage
            column_lineage = self._build_column_lineage(nodes)

            # Estimate total cost
            total_cost = self._estimate_total_cost(nodes, execution_order)

            return DataFlowAnalysis(
                nodes=nodes,
                execution_order=execution_order,
                join_operations=join_ops,
                aggregation_operations=agg_ops,
                filter_operations=filter_ops,
                column_lineage=column_lineage,
                estimated_total_cost=total_cost,
            )

        except Exception as e:
            self.logger.error(f"Error analyzing data flow: {str(e)}")
            # Return minimal analysis on error
            return DataFlowAnalysis(
                nodes={},
                execution_order=[],
                join_operations=[],
                aggregation_operations=[],
                filter_operations=[],
                column_lineage={},
                estimated_total_cost=0.0,
            )

    def suggest_partitioning_strategy(
        self, analysis: DataFlowAnalysis, table_info: Optional[Dict] = None
    ) -> List[PartitioningStrategy]:
        """
        Suggest optimal partitioning strategies based on data flow analysis.

        Args:
            analysis: Data flow analysis results
            table_info: Optional table metadata

        Returns:
            List of partitioning strategy recommendations
        """
        strategies = []

        try:
            # Analyze each table in the data flow
            for node_id, node in analysis.nodes.items():
                if node.table_name and node.operation in ["read", "load"]:
                    strategy = self._analyze_table_partitioning(
                        node, analysis, table_info
                    )
                    if strategy:
                        strategies.append(strategy)

            # Sort by performance impact
            strategies.sort(key=lambda x: x.performance_impact, reverse=True)

        except Exception as e:
            self.logger.error(f"Error suggesting partitioning strategies: {str(e)}")

        return strategies

    def recommend_join_strategy(
        self, analysis: DataFlowAnalysis, table_info: Optional[Dict] = None
    ) -> List[JoinOptimization]:
        """
        Recommend specific join strategies based on estimated table sizes and join patterns.

        Args:
            analysis: Data flow analysis results
            table_info: Optional table metadata

        Returns:
            List of join optimization recommendations
        """
        optimizations = []

        try:
            for join_op in analysis.join_operations:
                optimization = self._analyze_join_optimization(
                    join_op, analysis, table_info
                )
                if optimization:
                    optimizations.append(optimization)

            # Sort by performance impact
            optimizations.sort(key=lambda x: x.performance_impact, reverse=True)

        except Exception as e:
            self.logger.error(f"Error recommending join strategies: {str(e)}")

        return optimizations

    def estimate_performance_impact(
        self, optimizations: List[OptimizationRecommendation]
    ) -> Dict[str, PerformanceEstimate]:
        """
        Estimate performance impact for a list of optimization recommendations.

        Args:
            optimizations: List of optimization recommendations

        Returns:
            Dictionary mapping optimization IDs to performance estimates
        """
        estimates = {}

        try:
            for opt in optimizations:
                estimate = self._calculate_performance_estimate(opt)
                estimates[opt.optimization_id] = estimate

        except Exception as e:
            self.logger.error(f"Error estimating performance impact: {str(e)}")

        return estimates

    def generate_comprehensive_recommendations(
        self, pyspark_code: str, table_info: Optional[Dict] = None
    ) -> List[OptimizationRecommendation]:
        """
        Generate comprehensive optimization recommendations for PySpark code.

        Args:
            pyspark_code: PySpark code to optimize
            table_info: Optional table metadata

        Returns:
            List of optimization recommendations sorted by priority
        """
        recommendations = []

        try:
            # Perform data flow analysis
            analysis = self.analyze_data_flow(pyspark_code, table_info)

            # Generate partitioning recommendations
            partitioning_strategies = self.suggest_partitioning_strategy(
                analysis, table_info
            )
            for strategy in partitioning_strategies:
                rec = self._create_partitioning_recommendation(strategy)
                recommendations.append(rec)

            # Generate join optimization recommendations
            join_optimizations = self.recommend_join_strategy(analysis, table_info)
            for join_opt in join_optimizations:
                rec = self._create_join_recommendation(join_opt)
                recommendations.append(rec)

            # Generate caching recommendations
            caching_recs = self._generate_caching_recommendations(
                analysis, pyspark_code
            )
            recommendations.extend(caching_recs)

            # Generate column pruning recommendations
            pruning_recs = self._generate_column_pruning_recommendations(
                analysis, pyspark_code
            )
            recommendations.extend(pruning_recs)

            # Generate coalescing recommendations
            coalesce_recs = self._generate_coalescing_recommendations(
                analysis, pyspark_code
            )
            recommendations.extend(coalesce_recs)

            # Sort by priority and performance impact
            recommendations.sort(
                key=lambda x: (
                    {"high": 3, "medium": 2, "low": 1}[x.priority],
                    x.performance_estimate.estimated_improvement,
                ),
                reverse=True,
            )

        except Exception as e:
            self.logger.error(
                f"Error generating comprehensive recommendations: {str(e)}"
            )

        return recommendations

    def _extract_dataflow_nodes(
        self, pyspark_code: str, table_info: Optional[Dict] = None
    ) -> Dict[str, DataFlowNode]:
        """Extract data flow nodes from PySpark code."""
        nodes = {}

        # Extract table reads
        read_patterns = [
            r'(\w+)_df\s*=\s*spark\.table\([\'"](\w+)[\'"]',
            r'(\w+)_df\s*=\s*spark\.read\..*\([\'"]([^\'\"]+)[\'"]',
            r"(\w+)\s*=\s*spark\.read\..*",
        ]

        for pattern in read_patterns:
            matches = re.findall(pattern, pyspark_code)
            for match in matches:
                if len(match) >= 2:
                    var_name, table_name = match[0], match[1]
                    node_id = f"read_{var_name}"

                    # Estimate table size from table_info or defaults
                    estimated_size = self._estimate_table_size(table_name, table_info)
                    estimated_rows = self._estimate_table_rows(table_name, table_info)

                    nodes[node_id] = DataFlowNode(
                        node_id=node_id,
                        operation="read",
                        table_name=table_name,
                        estimated_size_mb=estimated_size,
                        estimated_rows=estimated_rows,
                    )

        # Extract transformations
        transformation_patterns = [
            (r"(\w+)\s*=\s*(\w+)\.filter\(", "filter"),
            (r"(\w+)\s*=\s*(\w+)\.select\(", "select"),
            (r"(\w+)\s*=\s*(\w+)\.groupBy\(", "groupBy"),
            (r"(\w+)\s*=\s*(\w+)\.join\(", "join"),
            (r"(\w+)\s*=\s*(\w+)\.orderBy\(", "orderBy"),
            (r"(\w+)\s*=\s*(\w+)\.withColumn\(", "withColumn"),
            (r"(\w+)\s*=\s*(\w+)\.drop\(", "drop"),
            (r"(\w+)\s*=\s*(\w+)\.cache\(\)", "cache"),
            (r"(\w+)\s*=\s*(\w+)\.coalesce\(", "coalesce"),
            (r"(\w+)\s*=\s*(\w+)\.repartition\(", "repartition"),
        ]

        for pattern, operation in transformation_patterns:
            matches = re.findall(pattern, pyspark_code)
            for match in matches:
                result_var, source_var = match
                node_id = f"{operation}_{result_var}"
                source_node_id = self._find_source_node(source_var, nodes)

                nodes[node_id] = DataFlowNode(
                    node_id=node_id,
                    operation=operation,
                    dependencies=[source_node_id] if source_node_id else [],
                )

        return nodes

    def _determine_execution_order(self, nodes: Dict[str, DataFlowNode]) -> List[str]:
        """Determine execution order based on dependencies."""
        order = []
        visited = set()

        def visit(node_id: str):
            if node_id in visited or node_id not in nodes:
                return

            visited.add(node_id)
            node = nodes[node_id]

            # Visit dependencies first
            for dep in node.dependencies:
                visit(dep)

            order.append(node_id)

        # Visit all nodes
        for node_id in nodes:
            visit(node_id)

        return order

    def _extract_join_operations(
        self, pyspark_code: str, nodes: Dict[str, DataFlowNode]
    ) -> List[Dict[str, Any]]:
        """Extract join operations from code."""
        joins = []

        # Multiple patterns to match different join syntaxes
        join_patterns = [
            r'(\w+)\s*=\s*(\w+)\.join\((\w+),\s*([^,]+)(?:,\s*[\'"](\w+)[\'"])?\)',
            r'\.join\((\w+),\s*([^,\)]+)(?:,\s*[\'"](\w+)[\'"])?\)',
            r'(\w+)\.join\((\w+),\s*[\'"](\w+)[\'"](?:,\s*[\'"](\w+)[\'"])?\)',
        ]

        for pattern in join_patterns:
            matches = re.findall(pattern, pyspark_code, re.MULTILINE)

            for match in matches:
                if len(match) >= 3:
                    if len(match) == 5:  # Full match with result variable
                        result_var, left_var, right_var, condition, join_type = match
                    elif len(match) == 4:  # Match without result variable
                        if pattern.startswith(r"\.join"):
                            right_var, condition, join_type, _ = match
                            result_var = f"join_result_{len(joins)}"
                            left_var = "unknown"
                        else:
                            left_var, right_var, condition, join_type = match
                            result_var = f"join_result_{len(joins)}"
                    elif len(match) == 3:
                        right_var, condition, join_type = match
                        result_var = f"join_result_{len(joins)}"
                        left_var = "unknown"
                    else:
                        continue

                    joins.append(
                        {
                            "join_id": f"join_{len(joins)}",
                            "result_var": result_var,
                            "left_table": left_var,
                            "right_table": right_var,
                            "condition": condition,
                            "join_type": join_type or "inner",
                            "left_size": self._estimate_dataframe_size(left_var, nodes),
                            "right_size": self._estimate_dataframe_size(
                                right_var, nodes
                            ),
                        }
                    )

        return joins

    def _extract_aggregation_operations(
        self, pyspark_code: str, nodes: Dict[str, DataFlowNode]
    ) -> List[Dict[str, Any]]:
        """Extract aggregation operations from code."""
        aggregations = []

        # Multiple patterns to match different aggregation syntaxes
        aggregation_patterns = [
            # Pattern 1: Simple groupBy with agg
            r"(\w+)\s*=\s*(\w+)\.groupBy\(([^)]+)\)(?:\.agg\(([^)]+)\))?",
            # Pattern 2: Multi-line aggregations with backslashes
            r"\.groupBy\(([^)]+)\)\s*\\?\s*\n?\s*\.agg\s*\(",
            # Pattern 3: Just groupBy (will catch multi-line ones)
            r"\.groupBy\(([^)]+)\)",
            # Pattern 4: Direct agg calls
            r"\.agg\s*\(",
        ]

        # First, look for any groupBy operations
        groupby_matches = re.findall(
            r"\.groupBy\(([^)]+)\)", pyspark_code, re.MULTILINE
        )

        for i, group_cols in enumerate(groupby_matches):
            aggregations.append(
                {
                    "agg_id": f"agg_{len(aggregations)}",
                    "result_var": f"agg_result_{len(aggregations)}",
                    "source_var": "unknown",
                    "group_columns": group_cols,
                    "agg_functions": "",
                    "estimated_reduction": 0.1,  # Assume 10x reduction on average
                }
            )

        # Also look for standalone agg operations
        agg_matches = re.findall(r"\.agg\s*\(", pyspark_code, re.MULTILINE)
        if agg_matches and not groupby_matches:
            # Found agg without groupBy (global aggregation)
            aggregations.append(
                {
                    "agg_id": f"agg_{len(aggregations)}",
                    "result_var": f"global_agg_{len(aggregations)}",
                    "source_var": "unknown",
                    "group_columns": "",
                    "agg_functions": "global_aggregation",
                    "estimated_reduction": 0.01,  # Global agg reduces to single row
                }
            )

        return aggregations

    def _extract_filter_operations(
        self, pyspark_code: str, nodes: Dict[str, DataFlowNode]
    ) -> List[Dict[str, Any]]:
        """Extract filter operations from code."""
        filters = []

        # Pattern to match filter operations
        filter_pattern = r"(\w+)\s*=\s*(\w+)\.filter\(([^)]+)\)"
        matches = re.findall(filter_pattern, pyspark_code)

        for i, match in enumerate(matches):
            result_var, source_var, condition = match

            # Estimate selectivity based on condition type
            selectivity = self._estimate_filter_selectivity(condition)

            filters.append(
                {
                    "filter_id": f"filter_{i}",
                    "result_var": result_var,
                    "source_var": source_var,
                    "condition": condition,
                    "estimated_selectivity": selectivity,
                }
            )

        return filters

    def _build_column_lineage(
        self, nodes: Dict[str, DataFlowNode]
    ) -> Dict[str, Set[str]]:
        """Build column lineage mapping."""
        lineage = {}

        # This is a simplified implementation
        # In practice, this would require more sophisticated AST analysis
        for node_id, node in nodes.items():
            if node.operation == "select":
                # Extract selected columns
                lineage[node_id] = (
                    node.columns_used.copy() if node.columns_used else set()
                )

        return lineage

    def _estimate_total_cost(
        self, nodes: Dict[str, DataFlowNode], execution_order: List[str]
    ) -> float:
        """Estimate total execution cost."""
        total_cost = 0.0

        # Cost model based on operation types and data sizes
        operation_costs = {
            "read": 1.0,
            "filter": 0.5,
            "select": 0.3,
            "join": 3.0,
            "groupBy": 2.0,
            "orderBy": 2.5,
            "cache": 0.1,
            "coalesce": 1.0,
            "repartition": 1.5,
        }

        for node_id in execution_order:
            if node_id in nodes:
                node = nodes[node_id]
                base_cost = operation_costs.get(node.operation, 1.0)

                # Scale by estimated data size
                size_multiplier = 1.0
                if node.estimated_size_mb:
                    size_multiplier = max(
                        1.0, node.estimated_size_mb / 100.0
                    )  # Scale by 100MB units

                total_cost += base_cost * size_multiplier

        return total_cost

    def _analyze_table_partitioning(
        self,
        node: DataFlowNode,
        analysis: DataFlowAnalysis,
        table_info: Optional[Dict] = None,
    ) -> Optional[PartitioningStrategy]:
        """Analyze partitioning strategy for a table."""
        if not node.table_name:
            return None

        # Look for common partitioning patterns in downstream operations
        partition_candidates = []

        # Check for date/time columns in filters
        for filter_op in analysis.filter_operations:
            condition = filter_op.get("condition", "")
            if any(
                date_pattern in condition.lower()
                for date_pattern in ["date", "timestamp", "year", "month"]
            ):
                # Extract potential date column
                date_match = re.search(
                    r"(\w*(?:date|time|year|month)\w*)", condition.lower()
                )
                if date_match:
                    partition_candidates.append(date_match.group(1))

        # Check for high-cardinality columns in joins
        for join_op in analysis.join_operations:
            condition = join_op.get("condition", "")
            # Extract join keys as potential partition candidates
            key_match = re.search(r'col\([\'"](\w+)[\'"]', condition)
            if key_match:
                partition_candidates.append(key_match.group(1))

        if not partition_candidates:
            return None

        # Select best partition column (prefer date columns)
        best_column = partition_candidates[0]
        for candidate in partition_candidates:
            if any(
                date_pattern in candidate.lower()
                for date_pattern in ["date", "time", "year", "month"]
            ):
                best_column = candidate
                break

        # Estimate performance impact
        estimated_size = node.estimated_size_mb or self.default_table_sizes["medium"]
        performance_impact = min(
            0.4, estimated_size / 10000.0
        )  # Up to 40% improvement for large tables

        return PartitioningStrategy(
            table_name=node.table_name,
            partition_columns=[best_column],
            partition_type="hash" if "date" not in best_column.lower() else "range",
            estimated_partitions=min(200, max(10, int(estimated_size / 100))),
            reasoning=f"Partitioning by {best_column} will improve query performance for filtered operations",
            performance_impact=performance_impact,
        )

    def _analyze_join_optimization(
        self,
        join_op: Dict[str, Any],
        analysis: DataFlowAnalysis,
        table_info: Optional[Dict] = None,
    ) -> Optional[JoinOptimization]:
        """Analyze join optimization opportunities."""
        left_size = join_op.get("left_size", self.default_table_sizes["medium"])
        right_size = join_op.get("right_size", self.default_table_sizes["medium"])

        # Determine optimal join strategy
        broadcast_threshold = self.performance_models[OptimizationType.BROADCAST_JOIN][
            "small_table_threshold_mb"
        ]

        if min(left_size, right_size) <= broadcast_threshold:
            strategy = "broadcast"
            smaller_table = (
                join_op["left_table"]
                if left_size < right_size
                else join_op["right_table"]
            )
            reasoning = f"Broadcast {smaller_table} (estimated {min(left_size, right_size):.1f}MB) for optimal performance"
            performance_impact = self.performance_models[
                OptimizationType.BROADCAST_JOIN
            ]["expected_improvement"]
        elif max(left_size, right_size) > 1000:  # Large tables
            strategy = "sort_merge"
            reasoning = "Use sort-merge join for large tables to minimize shuffling"
            performance_impact = 0.2
        else:
            strategy = "shuffle_hash"
            reasoning = "Use shuffle hash join for medium-sized tables"
            performance_impact = 0.1

        return JoinOptimization(
            join_id=join_op["join_id"],
            left_table=join_op["left_table"],
            right_table=join_op["right_table"],
            join_type=join_op.get("join_type", "inner"),
            join_keys=[join_op.get("condition", "")],
            recommended_strategy=strategy,
            estimated_left_size=left_size,
            estimated_right_size=right_size,
            reasoning=reasoning,
            performance_impact=performance_impact,
        )

    def _calculate_performance_estimate(
        self, optimization: OptimizationRecommendation
    ) -> PerformanceEstimate:
        """Calculate performance estimate for an optimization."""
        opt_type = optimization.optimization_type

        if opt_type in self.performance_models:
            model = self.performance_models[opt_type]
            base_improvement = model["expected_improvement"]
            base_confidence = model["confidence_base"]
        else:
            base_improvement = 0.1
            base_confidence = 0.5

        # Adjust based on optimization complexity
        complexity_adjustments = {
            "low": {"improvement": 1.0, "confidence": 1.0},
            "medium": {"improvement": 0.8, "confidence": 0.9},
            "high": {"improvement": 0.6, "confidence": 0.7},
        }

        complexity = optimization.performance_estimate.implementation_complexity
        adjustment = complexity_adjustments.get(
            complexity, complexity_adjustments["medium"]
        )

        final_improvement = base_improvement * adjustment["improvement"]
        final_confidence = base_confidence * adjustment["confidence"]

        # Resource impact estimation
        resource_impact = {
            "CPU": (
                "medium"
                if opt_type
                in [OptimizationType.JOIN_STRATEGY, OptimizationType.PARTITIONING]
                else "low"
            ),
            "Memory": "high" if opt_type == OptimizationType.CACHING else "medium",
            "Network": "high" if opt_type == OptimizationType.BROADCAST_JOIN else "low",
            "Storage": "medium" if opt_type == OptimizationType.PARTITIONING else "low",
        }

        return PerformanceEstimate(
            optimization_type=opt_type,
            estimated_improvement=final_improvement,
            confidence_level=final_confidence,
            resource_impact=resource_impact,
            implementation_complexity=complexity,
            prerequisites=optimization.performance_estimate.prerequisites,
        )

    def _create_partitioning_recommendation(
        self, strategy: PartitioningStrategy
    ) -> OptimizationRecommendation:
        """Create optimization recommendation from partitioning strategy."""
        opt_id = hashlib.md5(
            f"partition_{strategy.table_name}_{strategy.partition_columns[0]}".encode()
        ).hexdigest()[:8]

        code_changes = [
            f"# Partition {strategy.table_name} by {strategy.partition_columns[0]}",
            f"{strategy.table_name}_df = {strategy.table_name}_df.repartition(col('{strategy.partition_columns[0]}'))",
            f"# Or use .partitionBy('{strategy.partition_columns[0]}') when writing to storage",
        ]

        performance_estimate = PerformanceEstimate(
            optimization_type=OptimizationType.PARTITIONING,
            estimated_improvement=strategy.performance_impact,
            confidence_level=0.7,
            resource_impact={
                "CPU": "medium",
                "Memory": "low",
                "Network": "medium",
                "Storage": "low",
            },
            implementation_complexity="medium",
            prerequisites=["Table should be written to partitioned storage format"],
        )

        return OptimizationRecommendation(
            optimization_id=opt_id,
            optimization_type=OptimizationType.PARTITIONING,
            title=f"Partition {strategy.table_name} by {strategy.partition_columns[0]}",
            description=strategy.reasoning,
            code_changes=code_changes,
            performance_estimate=performance_estimate,
            priority="high" if strategy.performance_impact > 0.3 else "medium",
            applicable_conditions=[
                f"Table {strategy.table_name} is frequently filtered by {strategy.partition_columns[0]}"
            ],
        )

    def _create_join_recommendation(
        self, join_opt: JoinOptimization
    ) -> OptimizationRecommendation:
        """Create optimization recommendation from join optimization."""
        opt_id = hashlib.md5(
            f"join_{join_opt.join_id}_{join_opt.recommended_strategy}".encode()
        ).hexdigest()[:8]

        if join_opt.recommended_strategy == "broadcast":
            smaller_table = (
                join_opt.left_table
                if join_opt.estimated_left_size < join_opt.estimated_right_size
                else join_opt.right_table
            )
            code_changes = [
                f"# Use broadcast join for {smaller_table}",
                f"from pyspark.sql.functions import broadcast",
                f"result = {join_opt.left_table}.join(broadcast({join_opt.right_table}), {join_opt.join_keys[0]}, '{join_opt.join_type}')",
            ]
        else:
            code_changes = [
                f"# Use {join_opt.recommended_strategy} join strategy",
                f"# Configure join strategy in Spark configuration:",
                f"# spark.conf.set('spark.sql.adaptive.joinSelectionStrategy', '{join_opt.recommended_strategy}')",
            ]

        performance_estimate = PerformanceEstimate(
            optimization_type=OptimizationType.JOIN_STRATEGY,
            estimated_improvement=join_opt.performance_impact,
            confidence_level=0.8,
            resource_impact={
                "CPU": "medium",
                "Memory": "high",
                "Network": "high",
                "Storage": "low",
            },
            implementation_complexity=(
                "low" if join_opt.recommended_strategy == "broadcast" else "medium"
            ),
            prerequisites=["Ensure smaller table fits in memory for broadcast joins"],
        )

        return OptimizationRecommendation(
            optimization_id=opt_id,
            optimization_type=OptimizationType.JOIN_STRATEGY,
            title=f"Optimize join using {join_opt.recommended_strategy} strategy",
            description=join_opt.reasoning,
            code_changes=code_changes,
            performance_estimate=performance_estimate,
            priority="high" if join_opt.performance_impact > 0.3 else "medium",
            applicable_conditions=[
                f"Join between {join_opt.left_table} and {join_opt.right_table}"
            ],
        )

    def _generate_caching_recommendations(
        self, analysis: DataFlowAnalysis, pyspark_code: str
    ) -> List[OptimizationRecommendation]:
        """Generate caching recommendations based on DataFrame reuse patterns."""
        recommendations = []

        # Find DataFrames that are used multiple times
        df_usage = {}
        for line in pyspark_code.split("\n"):
            # Find DataFrame variable references
            df_matches = re.findall(r"(\w+_df|\w+)\s*\.", line)
            for df_var in df_matches:
                if df_var not in df_usage:
                    df_usage[df_var] = 0
                df_usage[df_var] += 1

        # Recommend caching for frequently used DataFrames
        for df_var, usage_count in df_usage.items():
            if usage_count >= 2:  # Used more than once
                opt_id = hashlib.md5(f"cache_{df_var}".encode()).hexdigest()[:8]

                code_changes = [
                    f"# Cache {df_var} for reuse",
                    f"{df_var} = {df_var}.cache()",
                    f"# Optionally persist to disk: {df_var}.persist(StorageLevel.DISK_ONLY)",
                ]

                performance_estimate = PerformanceEstimate(
                    optimization_type=OptimizationType.CACHING,
                    estimated_improvement=min(
                        0.5, usage_count * 0.15
                    ),  # Up to 50% improvement
                    confidence_level=0.9,
                    resource_impact={
                        "CPU": "low",
                        "Memory": "high",
                        "Network": "low",
                        "Storage": "medium",
                    },
                    implementation_complexity="low",
                    prerequisites=["Sufficient memory available for caching"],
                )

                recommendations.append(
                    OptimizationRecommendation(
                        optimization_id=opt_id,
                        optimization_type=OptimizationType.CACHING,
                        title=f"Cache {df_var} for reuse",
                        description=f"DataFrame {df_var} is used {usage_count} times - caching will improve performance",
                        code_changes=code_changes,
                        performance_estimate=performance_estimate,
                        priority="high" if usage_count > 3 else "medium",
                        applicable_conditions=[
                            f"{df_var} is accessed multiple times in the pipeline"
                        ],
                    )
                )

        return recommendations

    def _generate_column_pruning_recommendations(
        self, analysis: DataFlowAnalysis, pyspark_code: str
    ) -> List[OptimizationRecommendation]:
        """Generate column pruning recommendations."""
        recommendations = []

        # Look for SELECT * patterns and other column selection issues
        select_patterns = [
            r"\.select\(\s*\*\s*\)",
            r"\.select\(\s*['\"]?\*['\"]?\s*\)",
            r"spark\.sql\([^)]*SELECT\s+\*[^)]*\)",
        ]

        has_select_star = any(
            re.search(pattern, pyspark_code, re.IGNORECASE)
            for pattern in select_patterns
        )

        if has_select_star:
            opt_id = hashlib.md5("column_pruning_select_star".encode()).hexdigest()[:8]

            code_changes = [
                "# Replace SELECT * with specific columns",
                "# Example: df.select('col1', 'col2', 'col3') instead of df.select('*')",
                "# This reduces data transfer and memory usage",
            ]

            performance_estimate = PerformanceEstimate(
                optimization_type=OptimizationType.COLUMN_PRUNING,
                estimated_improvement=0.15,
                confidence_level=0.85,
                resource_impact={
                    "CPU": "low",
                    "Memory": "medium",
                    "Network": "medium",
                    "Storage": "low",
                },
                implementation_complexity="low",
                prerequisites=["Know which columns are actually needed"],
            )

            recommendations.append(
                OptimizationRecommendation(
                    optimization_id=opt_id,
                    optimization_type=OptimizationType.COLUMN_PRUNING,
                    title="Replace SELECT * with specific columns",
                    description="Selecting all columns (*) transfers unnecessary data - specify only needed columns",
                    code_changes=code_changes,
                    performance_estimate=performance_estimate,
                    priority="medium",
                    applicable_conditions=["SELECT * is used in the query"],
                )
            )

        return recommendations

    def _generate_coalescing_recommendations(
        self, analysis: DataFlowAnalysis, pyspark_code: str
    ) -> List[OptimizationRecommendation]:
        """Generate coalescing recommendations for small files."""
        recommendations = []

        # Look for write operations that might benefit from coalescing
        write_patterns = [r"\.write\.", r"\.save\(", r"\.saveAsTable\("]
        has_write = any(re.search(pattern, pyspark_code) for pattern in write_patterns)

        if has_write and "coalesce" not in pyspark_code.lower():
            opt_id = hashlib.md5("coalesce_before_write".encode()).hexdigest()[:8]

            code_changes = [
                "# Coalesce partitions before writing to reduce small files",
                "# For small datasets: df.coalesce(1).write...",
                "# For larger datasets: df.coalesce(num_partitions).write...",
                "# Where num_partitions = total_size_mb / target_partition_size_mb",
            ]

            performance_estimate = PerformanceEstimate(
                optimization_type=OptimizationType.COALESCE,
                estimated_improvement=0.2,
                confidence_level=0.7,
                resource_impact={
                    "CPU": "medium",
                    "Memory": "medium",
                    "Network": "low",
                    "Storage": "high",
                },
                implementation_complexity="low",
                prerequisites=["Output will be written to storage"],
            )

            recommendations.append(
                OptimizationRecommendation(
                    optimization_id=opt_id,
                    optimization_type=OptimizationType.COALESCE,
                    title="Coalesce partitions before writing",
                    description="Reduce small file problems by coalescing partitions before write operations",
                    code_changes=code_changes,
                    performance_estimate=performance_estimate,
                    priority="medium",
                    applicable_conditions=[
                        "Data is being written to storage",
                        "No coalescing is currently used",
                    ],
                )
            )

        return recommendations

    # Helper methods
    def _estimate_table_size(
        self, table_name: str, table_info: Optional[Dict] = None
    ) -> float:
        """Estimate table size in MB."""
        if table_info and table_name in table_info:
            return table_info[table_name].get(
                "size_mb", self.default_table_sizes["medium"]
            )

        # Use heuristics based on table name
        if any(
            keyword in table_name.lower()
            for keyword in ["fact", "transaction", "event", "log"]
        ):
            return self.default_table_sizes["large"]
        elif any(
            keyword in table_name.lower()
            for keyword in ["dim", "lookup", "ref", "master"]
        ):
            return self.default_table_sizes["small"]
        else:
            return self.default_table_sizes["medium"]

    def _estimate_table_rows(
        self, table_name: str, table_info: Optional[Dict] = None
    ) -> int:
        """Estimate table row count."""
        if table_info and table_name in table_info:
            return table_info[table_name].get("row_count", 1000000)

        # Rough estimate based on size
        size_mb = self._estimate_table_size(table_name, table_info)
        return int(size_mb * 10000)  # Assume ~10K rows per MB

    def _estimate_dataframe_size(
        self, df_var: str, nodes: Dict[str, DataFlowNode]
    ) -> float:
        """Estimate DataFrame size from nodes."""
        # Find the node that creates this DataFrame
        for node in nodes.values():
            if df_var in node.node_id or (
                node.table_name and df_var.startswith(node.table_name)
            ):
                return node.estimated_size_mb or self.default_table_sizes["medium"]

        # Try to match by table name pattern (e.g., customers_df -> customers)
        for node in nodes.values():
            if node.table_name and (
                df_var.endswith("_df") and node.table_name in df_var
            ):
                return node.estimated_size_mb or self.default_table_sizes["medium"]

        return self.default_table_sizes["medium"]

    def _find_source_node(
        self, var_name: str, nodes: Dict[str, DataFlowNode]
    ) -> Optional[str]:
        """Find the source node for a variable."""
        for node_id, node in nodes.items():
            if var_name in node_id:
                return node_id
        return None

    def _estimate_filter_selectivity(self, condition: str) -> float:
        """Estimate filter selectivity based on condition."""
        # Simple heuristics for common filter patterns
        if "=" in condition:
            return 0.1  # Equality filters are typically selective
        elif any(op in condition for op in [">", "<", ">=", "<="]):
            return 0.3  # Range filters
        elif "like" in condition.lower() or "contains" in condition.lower():
            return 0.2  # String pattern matching
        elif "in" in condition.lower():
            return 0.15  # IN clauses
        else:
            return 0.5  # Default selectivity
