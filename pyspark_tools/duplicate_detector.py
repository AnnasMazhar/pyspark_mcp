"""Duplicate code detection and pattern analysis for PySpark code."""

import ast
import difflib
import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class CodePattern:
    """Represents a detected code pattern."""

    pattern_id: str
    pattern_hash: str
    description: str
    code_template: str
    parameters: List[str]
    usage_count: int
    examples: List[str]
    similarity_threshold: float
    ast_structure: str


@dataclass
class PatternMatch:
    """Represents a match of a pattern in code."""

    pattern_id: str
    code_snippet: str
    start_line: int
    end_line: int
    confidence: float
    parameter_values: Dict[str, str]


@dataclass
class PatternAnalysis:
    """Results of pattern analysis."""

    patterns: List[CodePattern]
    matches: List[PatternMatch]
    duplicate_groups: List[List[str]]
    refactoring_opportunities: List[Dict[str, Any]]
    statistics: Dict[str, Any]


@dataclass
class UtilityFunction:
    """Represents a generated utility function."""

    function_name: str
    function_code: str
    parameters: List[str]
    description: str
    usage_examples: List[str]
    pattern_id: str


class ASTPatternExtractor:
    """Extracts patterns from AST nodes."""

    def __init__(self):
        self.node_types_to_ignore = (
            ast.Load,
            ast.Store,
            ast.Del,
            ast.AugLoad,
            ast.AugStore,
            ast.Param,
        )

    def extract_ast_pattern(self, node: ast.AST) -> str:
        """Extract a normalized pattern from an AST node."""
        if isinstance(node, self.node_types_to_ignore):
            return ""

        if isinstance(node, ast.Name):
            return f"Name({node.id})"
        elif isinstance(node, ast.Constant):
            return f"Constant({type(node.value).__name__})"
        elif isinstance(node, ast.Attribute):
            return f"Attr({self.extract_ast_pattern(node.value)}.{node.attr})"
        elif isinstance(node, ast.Call):
            func_pattern = self.extract_ast_pattern(node.func)
            args_pattern = [self.extract_ast_pattern(arg) for arg in node.args]
            return f"Call({func_pattern}({','.join(args_pattern)}))"
        elif isinstance(node, ast.Assign):
            targets = [self.extract_ast_pattern(target) for target in node.targets]
            value = self.extract_ast_pattern(node.value)
            return f"Assign({','.join(targets)}={value})"
        elif isinstance(node, ast.FunctionDef):
            return f"FunctionDef({node.name})"
        elif isinstance(node, ast.If):
            test = self.extract_ast_pattern(node.test)
            return f"If({test})"
        elif isinstance(node, ast.For):
            target = self.extract_ast_pattern(node.target)
            iter_pattern = self.extract_ast_pattern(node.iter)
            return f"For({target} in {iter_pattern})"
        else:
            # Generic handling for other node types
            return f"{type(node).__name__}({','.join(self.extract_ast_pattern(child) for child in ast.iter_child_nodes(node))})"

    def extract_code_blocks(self, code: str) -> List[Tuple[ast.AST, int, int, str]]:
        """Extract meaningful code blocks from Python code."""
        try:
            tree = ast.parse(code)
            blocks = []

            for node in ast.walk(tree):
                if isinstance(
                    node,
                    (
                        ast.FunctionDef,
                        ast.ClassDef,
                        ast.For,
                        ast.While,
                        ast.If,
                        ast.With,
                    ),
                ):
                    if hasattr(node, "lineno") and hasattr(node, "end_lineno"):
                        # Extract the source code for this block
                        lines = code.split("\n")
                        start_line = node.lineno - 1
                        end_line = (
                            node.end_lineno if node.end_lineno else start_line + 1
                        )
                        block_code = "\n".join(lines[start_line:end_line])

                        blocks.append((node, start_line + 1, end_line, block_code))

            return blocks
        except SyntaxError:
            return []


class DuplicateDetector:
    """Main class for detecting duplicate code patterns and generating utilities."""

    def __init__(self, similarity_threshold: float = 0.8):
        self.similarity_threshold = similarity_threshold
        self.ast_extractor = ASTPatternExtractor()
        self.detected_patterns: Dict[str, CodePattern] = {}
        self.pattern_matches: List[PatternMatch] = []

    def analyze_patterns(self, code_samples: List[str]) -> PatternAnalysis:
        """Analyze code samples to identify duplicate patterns."""
        all_blocks = []
        code_to_blocks = {}

        # Extract code blocks from all samples
        for i, code in enumerate(code_samples):
            blocks = self.ast_extractor.extract_code_blocks(code)
            all_blocks.extend([(block, i, code) for block in blocks])
            code_to_blocks[i] = blocks

        # Group similar blocks
        pattern_groups = self._group_similar_blocks(all_blocks)

        # Generate patterns from groups
        patterns = []
        matches = []
        duplicate_groups = []

        for group_id, group in enumerate(pattern_groups):
            if len(group) >= 2:  # Only consider groups with multiple instances
                pattern = self._create_pattern_from_group(group, group_id)
                patterns.append(pattern)

                # Create matches for each instance in the group
                group_matches = []
                for block_info, code_idx, original_code in group:
                    block, start_line, end_line, block_code = block_info
                    match = PatternMatch(
                        pattern_id=pattern.pattern_id,
                        code_snippet=block_code,
                        start_line=start_line,
                        end_line=end_line,
                        confidence=0.9,  # High confidence for grouped patterns
                        parameter_values=self._extract_parameter_values(
                            block_code, pattern
                        ),
                    )
                    matches.append(match)
                    group_matches.append(block_code)

                duplicate_groups.append(group_matches)

        # Generate refactoring opportunities
        refactoring_opportunities = self._generate_refactoring_opportunities(
            patterns, matches
        )

        # Calculate statistics
        statistics = self._calculate_statistics(code_samples, patterns, matches)

        return PatternAnalysis(
            patterns=patterns,
            matches=matches,
            duplicate_groups=duplicate_groups,
            refactoring_opportunities=refactoring_opportunities,
            statistics=statistics,
        )

    def _group_similar_blocks(
        self, all_blocks: List[Tuple[Tuple[ast.AST, int, int, str], int, str]]
    ) -> List[List[Tuple[Tuple[ast.AST, int, int, str], int, str]]]:
        """Group similar code blocks together."""
        groups = []
        used_blocks = set()

        for i, (block_info1, code_idx1, original_code1) in enumerate(all_blocks):
            if i in used_blocks:
                continue

            current_group = [(block_info1, code_idx1, original_code1)]
            used_blocks.add(i)

            block1, _, _, code1 = block_info1

            for j, (block_info2, code_idx2, original_code2) in enumerate(
                all_blocks[i + 1 :], i + 1
            ):
                if j in used_blocks:
                    continue

                block2, _, _, code2 = block_info2

                # Check structural similarity
                if self._are_blocks_similar(block1, block2, code1, code2):
                    current_group.append((block_info2, code_idx2, original_code2))
                    used_blocks.add(j)

            if len(current_group) >= 2:  # Only keep groups with duplicates
                groups.append(current_group)

        return groups

    def _are_blocks_similar(
        self, block1: ast.AST, block2: ast.AST, code1: str, code2: str
    ) -> bool:
        """Check if two code blocks are structurally similar."""
        # Check AST structure similarity
        pattern1 = self.ast_extractor.extract_ast_pattern(block1)
        pattern2 = self.ast_extractor.extract_ast_pattern(block2)

        if pattern1 == pattern2:
            return True

        # Check text similarity as fallback
        similarity = difflib.SequenceMatcher(None, code1, code2).ratio()
        return similarity >= self.similarity_threshold

    def _create_pattern_from_group(
        self, group: List[Tuple[Tuple[ast.AST, int, int, str], int, str]], group_id: int
    ) -> CodePattern:
        """Create a pattern from a group of similar blocks."""
        # Use the first block as the template
        first_block_info, _, _ = group[0]
        first_block, _, _, first_code = first_block_info

        # Generate pattern hash
        pattern_structure = self.ast_extractor.extract_ast_pattern(first_block)
        pattern_hash = hashlib.md5(pattern_structure.encode()).hexdigest()

        # Extract parameters (variables that differ between instances)
        parameters = self._extract_parameters_from_group(group)

        # Create template with parameter placeholders
        template = self._create_template(first_code, parameters)

        # Generate description
        description = self._generate_pattern_description(first_block, parameters)

        # Collect examples
        examples = [block_info[3] for block_info, _, _ in group[:3]]  # First 3 examples

        return CodePattern(
            pattern_id=f"pattern_{group_id}",
            pattern_hash=pattern_hash,
            description=description,
            code_template=template,
            parameters=parameters,
            usage_count=len(group),
            examples=examples,
            similarity_threshold=self.similarity_threshold,
            ast_structure=pattern_structure,
        )

    def _extract_parameters_from_group(
        self, group: List[Tuple[Tuple[ast.AST, int, int, str], int, str]]
    ) -> List[str]:
        """Extract parameters that vary between instances in a group."""
        if len(group) < 2:
            return []

        # Get all code snippets
        codes = [block_info[3] for block_info, _, _ in group]

        # Find variable names and values that differ
        parameters = set()

        # Simple approach: find identifiers that appear in different contexts
        for code in codes:
            # Extract variable names using regex
            var_names = re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", code)
            parameters.update(var_names)

        # Filter out common keywords and built-ins
        keywords = {
            "def",
            "class",
            "if",
            "else",
            "elif",
            "for",
            "while",
            "try",
            "except",
            "finally",
            "with",
            "as",
            "import",
            "from",
            "return",
            "yield",
            "break",
            "continue",
            "pass",
            "and",
            "or",
            "not",
            "in",
            "is",
            "True",
            "False",
            "None",
        }

        parameters = [
            p for p in parameters if p not in keywords and not p.startswith("__")
        ]

        return sorted(list(parameters))

    def _create_template(self, code: str, parameters: List[str]) -> str:
        """Create a template with parameter placeholders."""
        template = code

        # Replace specific values with parameter placeholders
        for i, param in enumerate(parameters):
            if param in template:
                template = template.replace(param, f"{{{param}}}")

        return template

    def _generate_pattern_description(
        self, block: ast.AST, parameters: List[str]
    ) -> str:
        """Generate a human-readable description of the pattern."""
        if isinstance(block, ast.FunctionDef):
            return (
                f"Function definition pattern with parameters: {', '.join(parameters)}"
            )
        elif isinstance(block, ast.For):
            return f"For loop pattern with parameters: {', '.join(parameters)}"
        elif isinstance(block, ast.If):
            return f"Conditional statement pattern with parameters: {', '.join(parameters)}"
        elif isinstance(block, ast.Assign):
            return f"Assignment pattern with parameters: {', '.join(parameters)}"
        else:
            return f"{type(block).__name__} pattern with parameters: {', '.join(parameters)}"

    def _extract_parameter_values(
        self, code: str, pattern: CodePattern
    ) -> Dict[str, str]:
        """Extract parameter values from code based on pattern."""
        values = {}

        # Simple extraction based on parameter names
        for param in pattern.parameters:
            # Find the value of this parameter in the code
            matches = re.findall(rf"\b{re.escape(param)}\s*=\s*([^,\n\)]+)", code)
            if matches:
                values[param] = matches[0].strip()
            else:
                # If not found in assignment, just use the parameter name
                if param in code:
                    values[param] = param

        return values

    def _generate_refactoring_opportunities(
        self, patterns: List[CodePattern], matches: List[PatternMatch]
    ) -> List[Dict[str, Any]]:
        """Generate refactoring opportunities based on detected patterns."""
        opportunities = []

        for pattern in patterns:
            if pattern.usage_count >= 2:
                pattern_matches = [
                    m for m in matches if m.pattern_id == pattern.pattern_id
                ]

                opportunity = {
                    "type": "extract_utility_function",
                    "pattern_id": pattern.pattern_id,
                    "description": f"Extract utility function for {pattern.description}",
                    "occurrences": len(pattern_matches),
                    "estimated_lines_saved": len(pattern_matches) * 5,  # Rough estimate
                    "suggested_function_name": self._suggest_function_name(pattern),
                    "complexity_reduction": (
                        "medium" if pattern.usage_count < 5 else "high"
                    ),
                }
                opportunities.append(opportunity)

        return opportunities

    def _suggest_function_name(self, pattern: CodePattern) -> str:
        """Suggest a function name based on the pattern."""
        if "function" in pattern.description.lower():
            return f"utility_function_{pattern.pattern_id}"
        elif "loop" in pattern.description.lower():
            return f"process_loop_{pattern.pattern_id}"
        elif "conditional" in pattern.description.lower():
            return f"check_condition_{pattern.pattern_id}"
        elif "assignment" in pattern.description.lower():
            return f"assign_value_{pattern.pattern_id}"
        else:
            return f"utility_{pattern.pattern_id}"

    def _calculate_statistics(
        self,
        code_samples: List[str],
        patterns: List[CodePattern],
        matches: List[PatternMatch],
    ) -> Dict[str, Any]:
        """Calculate statistics about the pattern analysis."""
        total_lines = sum(len(code.split("\n")) for code in code_samples)
        duplicate_lines = sum(len(match.code_snippet.split("\n")) for match in matches)

        return {
            "total_code_samples": len(code_samples),
            "total_lines": total_lines,
            "patterns_detected": len(patterns),
            "total_matches": len(matches),
            "duplicate_lines": duplicate_lines,
            "duplication_percentage": (
                (duplicate_lines / total_lines * 100) if total_lines > 0 else 0
            ),
            "average_pattern_usage": (
                sum(p.usage_count for p in patterns) / len(patterns) if patterns else 0
            ),
            "refactoring_potential": len([p for p in patterns if p.usage_count >= 3]),
        }

    def extract_common_functions(
        self, patterns: List[CodePattern]
    ) -> List[UtilityFunction]:
        """Extract common functions from detected patterns."""
        utility_functions = []

        for pattern in patterns:
            if pattern.usage_count >= 2:
                function = self._create_utility_function(pattern)
                utility_functions.append(function)

        return utility_functions

    def _create_utility_function(self, pattern: CodePattern) -> UtilityFunction:
        """Create a utility function from a pattern."""
        function_name = self._suggest_function_name(pattern)

        # Create function signature
        params = ", ".join(pattern.parameters) if pattern.parameters else ""

        # Create function body from template
        function_body = pattern.code_template

        # Wrap in function definition
        function_code = f"""def {function_name}({params}):
    \"\"\"
    {pattern.description}
    
    Generated from pattern: {pattern.pattern_id}
    Usage count: {pattern.usage_count}
    \"\"\"
{self._indent_code(function_body, 4)}
    return result  # Adjust return value as needed
"""

        # Create usage examples
        usage_examples = []
        for i, example in enumerate(pattern.examples[:2]):
            usage_examples.append(f"# Example {i+1}:\n# {function_name}(...)")

        return UtilityFunction(
            function_name=function_name,
            function_code=function_code,
            parameters=pattern.parameters,
            description=pattern.description,
            usage_examples=usage_examples,
            pattern_id=pattern.pattern_id,
        )

    def _indent_code(self, code: str, spaces: int) -> str:
        """Indent code by the specified number of spaces."""
        indent = " " * spaces
        return "\n".join(
            indent + line if line.strip() else line for line in code.split("\n")
        )

    def generate_utilities_module(
        self, utility_functions: List[UtilityFunction]
    ) -> str:
        """Generate a complete utilities module with all utility functions."""
        module_header = '''"""
Generated utility functions for common PySpark patterns.

This module contains utility functions extracted from duplicate code patterns
to improve code reusability and maintainability.

Auto-generated by PySpark Tools Duplicate Detector.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


'''

        function_codes = []
        for func in utility_functions:
            function_codes.append(func.function_code)

        module_footer = """

# Usage examples:
# from pyspark_utilities import *
# 
# # Use the utility functions in your PySpark code
# result = utility_function_pattern_0(df, "column_name")
"""

        return module_header + "\n\n".join(function_codes) + module_footer

    def refactor_code_with_utilities(
        self, code: str, utility_functions: List[UtilityFunction]
    ) -> str:
        """Refactor code to use utility functions instead of duplicate patterns."""
        refactored_code = code

        # Add import statement for utilities
        import_statement = "from pyspark_utilities import *\n\n"

        # Replace pattern instances with utility function calls
        for func in utility_functions:
            # This is a simplified approach - in practice, you'd need more sophisticated
            # pattern matching and replacement logic
            pattern_id = func.pattern_id

            # Find instances of this pattern in the code and replace with function calls
            # This would require more sophisticated AST manipulation in a real implementation
            function_call = f"{func.function_name}()"  # Simplified

            # For now, just add a comment indicating where refactoring could occur
            refactored_code += (
                f"\n# TODO: Replace pattern {pattern_id} with {func.function_name}()"
            )

        return import_statement + refactored_code

    def get_pattern_statistics(self) -> Dict[str, Any]:
        """Get statistics about detected patterns."""
        if not self.detected_patterns:
            return {"message": "No patterns detected yet"}

        patterns = list(self.detected_patterns.values())

        return {
            "total_patterns": len(patterns),
            "total_usage": sum(p.usage_count for p in patterns),
            "average_usage": sum(p.usage_count for p in patterns) / len(patterns),
            "high_usage_patterns": len([p for p in patterns if p.usage_count >= 5]),
            "refactoring_candidates": len([p for p in patterns if p.usage_count >= 3]),
        }
