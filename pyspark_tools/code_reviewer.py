"""Code reviewer for PySpark code with AWS Glue best practices."""

import re
from typing import List, Dict, Tuple
from dataclasses import dataclass


@dataclass
class ReviewIssue:
    """Represents a code review issue."""
    severity: str  # "error", "warning", "info"
    category: str  # "performance", "best_practice", "aws_glue", "style"
    message: str
    line_number: int = 0
    suggestion: str = ""


class PySparkCodeReviewer:
    """Reviews PySpark code for best practices, optimizations, and AWS Glue compatibility."""
    
    def __init__(self):
        self.aws_glue_patterns = self._init_aws_glue_patterns()
        self.performance_patterns = self._init_performance_patterns()
        self.best_practice_patterns = self._init_best_practice_patterns()
    
    def review_code(self, code: str) -> Tuple[List[ReviewIssue], Dict[str, int]]:
        """
        Review PySpark code and return issues and metrics.
        
        Returns:
            Tuple of (issues_list, metrics_dict)
        """
        issues = []
        lines = code.split('\n')
        
        # Check AWS Glue compatibility
        issues.extend(self._check_aws_glue_compatibility(lines))
        
        # Check performance issues
        issues.extend(self._check_performance_issues(lines))
        
        # Check best practices
        issues.extend(self._check_best_practices(lines))
        
        # Check style issues
        issues.extend(self._check_style_issues(lines))
        
        # Generate metrics
        metrics = self._generate_metrics(issues, lines)
        
        return issues, metrics
    
    def _init_aws_glue_patterns(self) -> Dict[str, Dict]:
        """Initialize AWS Glue specific patterns."""
        return {
            "unsupported_functions": {
                "pattern": r"\b(collect|toPandas|foreach|foreachPartition)\s*\(",
                "message": "Function may not be supported or efficient in AWS Glue",
                "suggestion": "Use Glue-optimized alternatives or avoid collecting large datasets"
            },
            "missing_glue_context": {
                "pattern": r"SparkSession\.builder",
                "message": "Consider using GlueContext instead of SparkSession for AWS Glue",
                "suggestion": "Use: from awsglue.context import GlueContext; glueContext = GlueContext(SparkContext())"
            },
            "hardcoded_paths": {
                "pattern": r"['\"]s3://[^'\"]*['\"]",
                "message": "Hardcoded S3 paths found - consider using job parameters",
                "suggestion": "Use job parameters: args = getResolvedOptions(sys.argv, ['S3_BUCKET'])"
            },
            "missing_job_commit": {
                "pattern": r"job\.commit\(\)",
                "message": "Missing job.commit() for AWS Glue job bookmarking",
                "suggestion": "Add job.commit() at the end of your job for proper bookmarking"
            }
        }
    
    def _init_performance_patterns(self) -> Dict[str, Dict]:
        """Initialize performance-related patterns."""
        return {
            "expensive_operations": {
                "pattern": r"\.(count|collect|show)\(\)",
                "message": "Expensive operation detected",
                "suggestion": "Consider if this operation is necessary or can be optimized"
            },
            "missing_cache": {
                "pattern": r"\.groupBy\(.*\)\.agg\(",
                "message": "Consider caching before expensive operations",
                "suggestion": "Add .cache() before groupBy operations if the DataFrame is reused"
            },
            "inefficient_joins": {
                "pattern": r"\.join\(",
                "message": "Join operation detected",
                "suggestion": "Consider broadcast joins for small tables: broadcast(small_df)"
            },
            "select_star": {
                "pattern": r"\.select\(\s*\*\s*\)",
                "message": "SELECT * detected - consider column pruning",
                "suggestion": "Select only required columns to improve performance"
            }
        }
    
    def _init_best_practice_patterns(self) -> Dict[str, Dict]:
        """Initialize best practice patterns."""
        return {
            "missing_error_handling": {
                "pattern": r"spark\.read\.",
                "message": "Consider adding error handling for data reading operations",
                "suggestion": "Wrap data operations in try-except blocks"
            },
            "missing_schema": {
                "pattern": r"\.option\(['\"]inferSchema['\"],\s*['\"]true['\"]",
                "message": "Schema inference can be expensive",
                "suggestion": "Define explicit schema for better performance"
            },
            "inefficient_file_format": {
                "pattern": r"\.(csv|json)\(",
                "message": "Consider using more efficient file formats",
                "suggestion": "Use Parquet or Delta format for better performance"
            }
        }
    
    def _check_aws_glue_compatibility(self, lines: List[str]) -> List[ReviewIssue]:
        """Check for AWS Glue compatibility issues."""
        issues = []
        
        for i, line in enumerate(lines, 1):
            for pattern_name, pattern_info in self.aws_glue_patterns.items():
                if re.search(pattern_info["pattern"], line, re.IGNORECASE):
                    issues.append(ReviewIssue(
                        severity="warning",
                        category="aws_glue",
                        message=pattern_info["message"],
                        line_number=i,
                        suggestion=pattern_info["suggestion"]
                    ))
        
        # Check for missing imports
        code_text = '\n'.join(lines)
        if "from awsglue" not in code_text and "GlueContext" not in code_text:
            issues.append(ReviewIssue(
                severity="info",
                category="aws_glue",
                message="Consider using AWS Glue specific imports for better integration",
                suggestion="Add: from awsglue.context import GlueContext, from awsglue.job import Job"
            ))
        
        return issues
    
    def _check_performance_issues(self, lines: List[str]) -> List[ReviewIssue]:
        """Check for performance-related issues."""
        issues = []
        
        for i, line in enumerate(lines, 1):
            for pattern_name, pattern_info in self.performance_patterns.items():
                if re.search(pattern_info["pattern"], line, re.IGNORECASE):
                    severity = "error" if pattern_name == "expensive_operations" else "warning"
                    issues.append(ReviewIssue(
                        severity=severity,
                        category="performance",
                        message=pattern_info["message"],
                        line_number=i,
                        suggestion=pattern_info["suggestion"]
                    ))
        
        return issues
    
    def _check_best_practices(self, lines: List[str]) -> List[ReviewIssue]:
        """Check for best practice violations."""
        issues = []
        
        for i, line in enumerate(lines, 1):
            for pattern_name, pattern_info in self.best_practice_patterns.items():
                if re.search(pattern_info["pattern"], line, re.IGNORECASE):
                    issues.append(ReviewIssue(
                        severity="info",
                        category="best_practice",
                        message=pattern_info["message"],
                        line_number=i,
                        suggestion=pattern_info["suggestion"]
                    ))
        
        return issues
    
    def _check_style_issues(self, lines: List[str]) -> List[ReviewIssue]:
        """Check for style issues."""
        issues = []
        
        for i, line in enumerate(lines, 1):
            # Check line length
            if len(line) > 88:
                issues.append(ReviewIssue(
                    severity="info",
                    category="style",
                    message="Line too long (>88 characters)",
                    line_number=i,
                    suggestion="Break long lines for better readability"
                ))
            
            # Check for missing docstrings in functions
            if re.match(r'^\s*def\s+\w+', line) and i < len(lines):
                next_line = lines[i] if i < len(lines) else ""
                if not re.match(r'^\s*[\'\"]{3}', next_line):
                    issues.append(ReviewIssue(
                        severity="info",
                        category="style",
                        message="Function missing docstring",
                        line_number=i,
                        suggestion="Add docstring to document function purpose"
                    ))
        
        return issues
    
    def _generate_metrics(self, issues: List[ReviewIssue], lines: List[str]) -> Dict[str, int]:
        """Generate code metrics."""
        return {
            "total_lines": len(lines),
            "total_issues": len(issues),
            "errors": len([i for i in issues if i.severity == "error"]),
            "warnings": len([i for i in issues if i.severity == "warning"]),
            "info": len([i for i in issues if i.severity == "info"]),
            "aws_glue_issues": len([i for i in issues if i.category == "aws_glue"]),
            "performance_issues": len([i for i in issues if i.category == "performance"]),
            "best_practice_issues": len([i for i in issues if i.category == "best_practice"]),
            "style_issues": len([i for i in issues if i.category == "style"])
        }
    
    def generate_report(self, issues: List[ReviewIssue], metrics: Dict[str, int]) -> str:
        """Generate a formatted review report."""
        report_lines = [
            "# PySpark Code Review Report",
            "",
            "## Summary",
            f"- Total Lines: {metrics['total_lines']}",
            f"- Total Issues: {metrics['total_issues']}",
            f"- Errors: {metrics['errors']}",
            f"- Warnings: {metrics['warnings']}",
            f"- Info: {metrics['info']}",
            "",
            "## Issues by Category",
            f"- AWS Glue: {metrics['aws_glue_issues']}",
            f"- Performance: {metrics['performance_issues']}",
            f"- Best Practices: {metrics['best_practice_issues']}",
            f"- Style: {metrics['style_issues']}",
            "",
            "## Detailed Issues",
            ""
        ]
        
        # Group issues by severity
        for severity in ["error", "warning", "info"]:
            severity_issues = [i for i in issues if i.severity == severity]
            if severity_issues:
                report_lines.append(f"### {severity.upper()}S")
                for issue in severity_issues:
                    report_lines.append(f"- **Line {issue.line_number}** [{issue.category}]: {issue.message}")
                    if issue.suggestion:
                        report_lines.append(f"  - *Suggestion*: {issue.suggestion}")
                report_lines.append("")
        
        return "\n".join(report_lines)