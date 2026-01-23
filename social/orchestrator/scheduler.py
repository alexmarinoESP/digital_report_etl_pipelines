"""Platform Scheduler Module.

This module provides scheduling and dependency management for platform execution
in the Social Ads Orchestrator. It handles execution order, dependencies, and
parallel execution groups.

Key Features:
- Topological sorting for dependency resolution
- Parallel execution group management
- Dependency validation and cycle detection
- Execution readiness checks
- Priority-based ordering

Architecture:
- Dependency graph construction
- Topological sort for execution order
- Support for parallel execution groups
- Validation for circular dependencies
"""

from collections import defaultdict, deque
from typing import Dict, List, Set

from loguru import logger

from social.core.exceptions import ConfigurationError
from social.orchestrator.config import PlatformConfig


class PlatformScheduler:
    """Scheduler for coordinating platform execution order.

    This class manages the execution order of platforms based on:
    - Priority levels
    - Inter-platform dependencies
    - Parallel execution groups

    Example:
        ```python
        scheduler = PlatformScheduler()

        # Define dependencies
        dependencies = {
            "linkedin_insights": ["linkedin_campaigns"],
            "facebook_ads": [],
            "google_ads": []
        }

        # Get execution order
        execution_order = scheduler.schedule_platforms(platforms, dependencies)

        # Check if platform can execute
        completed = {"linkedin_campaigns"}
        if scheduler.can_execute("linkedin_insights", completed):
            # Run platform
            pass
        ```
    """

    def __init__(self):
        """Initialize the platform scheduler."""
        self._dependency_graph: Dict[str, List[str]] = {}
        self._reverse_graph: Dict[str, List[str]] = {}
        logger.debug("PlatformScheduler initialized")

    def schedule_platforms(
        self,
        platforms: List[PlatformConfig],
        explicit_dependencies: Optional[Dict[str, List[str]]] = None,
    ) -> List[List[str]]:
        """Schedule platforms for execution considering dependencies and priorities.

        This method performs topological sorting to determine execution order
        while respecting dependencies and grouping platforms that can run in parallel.

        Args:
            platforms: List of platform configurations
            explicit_dependencies: Optional additional dependencies not in platform configs

        Returns:
            List of execution groups (each group can run in parallel)
            Example: [["microsoft"], ["linkedin", "google"], ["facebook"]]

        Raises:
            ConfigurationError: If circular dependencies detected or invalid dependencies
        """
        if not platforms:
            return []

        # Build dependency graph from platform configs
        self._build_dependency_graph(platforms, explicit_dependencies)

        # Validate no circular dependencies
        self._validate_no_cycles()

        # Perform topological sort with priority grouping
        execution_groups = self._topological_sort_with_groups(platforms)

        logger.info(
            f"Scheduled {len(platforms)} platforms into {len(execution_groups)} execution group(s)"
        )
        for i, group in enumerate(execution_groups):
            logger.debug(f"  Group {i+1}: {', '.join(group)}")

        return execution_groups

    def can_execute(
        self,
        platform_name: str,
        completed_platforms: Set[str],
    ) -> bool:
        """Check if a platform can execute based on completed dependencies.

        Args:
            platform_name: Name of platform to check
            completed_platforms: Set of platform names that have completed

        Returns:
            True if all dependencies are satisfied
        """
        if platform_name not in self._dependency_graph:
            # No dependencies or not in graph
            return True

        dependencies = self._dependency_graph[platform_name]
        all_satisfied = all(dep in completed_platforms for dep in dependencies)

        if not all_satisfied:
            missing = [dep for dep in dependencies if dep not in completed_platforms]
            logger.debug(
                f"Platform '{platform_name}' cannot execute yet. "
                f"Missing dependencies: {', '.join(missing)}"
            )

        return all_satisfied

    def get_dependencies(self, platform_name: str) -> List[str]:
        """Get list of dependencies for a platform.

        Args:
            platform_name: Name of platform

        Returns:
            List of platform names that must complete before this one
        """
        return self._dependency_graph.get(platform_name, [])

    def get_dependents(self, platform_name: str) -> List[str]:
        """Get list of platforms that depend on this platform.

        Args:
            platform_name: Name of platform

        Returns:
            List of platform names that depend on this one
        """
        return self._reverse_graph.get(platform_name, [])

    def _build_dependency_graph(
        self,
        platforms: List[PlatformConfig],
        explicit_dependencies: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        """Build dependency graph from platform configurations.

        Args:
            platforms: List of platform configurations
            explicit_dependencies: Optional additional dependencies
        """
        # Initialize graphs
        self._dependency_graph = {}
        self._reverse_graph = defaultdict(list)

        # Add dependencies from platform configs
        for platform in platforms:
            platform_name = platform.name
            dependencies = platform.dependencies.copy()

            # Add explicit dependencies if provided
            if explicit_dependencies and platform_name in explicit_dependencies:
                dependencies.extend(explicit_dependencies[platform_name])

            # Remove duplicates
            dependencies = list(set(dependencies))

            # Store in graph
            self._dependency_graph[platform_name] = dependencies

            # Build reverse graph
            for dep in dependencies:
                self._reverse_graph[dep].append(platform_name)

        logger.debug(f"Built dependency graph with {len(self._dependency_graph)} platforms")

    def _validate_no_cycles(self) -> None:
        """Validate that dependency graph has no circular dependencies.

        Raises:
            ConfigurationError: If circular dependencies detected
        """
        # Use DFS to detect cycles
        visited = set()
        rec_stack = set()

        def has_cycle(node: str) -> bool:
            """Check if node is part of a cycle using DFS."""
            visited.add(node)
            rec_stack.add(node)

            # Visit all dependencies
            for neighbor in self._dependency_graph.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    # Found a cycle
                    return True

            rec_stack.remove(node)
            return False

        # Check all nodes
        for platform in self._dependency_graph:
            if platform not in visited:
                if has_cycle(platform):
                    raise ConfigurationError(
                        f"Circular dependency detected involving platform: '{platform}'. "
                        f"Check platform dependencies in configuration."
                    )

        logger.debug("No circular dependencies detected")

    def _topological_sort_with_groups(
        self,
        platforms: List[PlatformConfig],
    ) -> List[List[str]]:
        """Perform topological sort grouping platforms by execution level.

        Platforms in the same group have no dependencies on each other
        and can be executed in parallel.

        Args:
            platforms: List of platform configurations

        Returns:
            List of execution groups
        """
        # Calculate in-degree for each platform
        in_degree = {}
        platform_names = {p.name for p in platforms}

        for platform in platforms:
            in_degree[platform.name] = len(
                [dep for dep in self._dependency_graph.get(platform.name, [])
                 if dep in platform_names]
            )

        # Initialize queue with platforms that have no dependencies
        queue = deque([
            p.name for p in platforms
            if in_degree[p.name] == 0
        ])

        # Sort initial queue by priority
        queue = deque(sorted(queue, key=lambda name: self._get_priority(name, platforms)))

        execution_groups = []

        while queue:
            # Process all platforms in current level (can run in parallel)
            current_group = []
            group_size = len(queue)

            for _ in range(group_size):
                platform_name = queue.popleft()
                current_group.append(platform_name)

                # Reduce in-degree for dependent platforms
                for dependent in self._reverse_graph.get(platform_name, []):
                    if dependent in in_degree:
                        in_degree[dependent] -= 1
                        if in_degree[dependent] == 0:
                            queue.append(dependent)

            # Sort current group by priority before adding
            current_group.sort(key=lambda name: self._get_priority(name, platforms))

            execution_groups.append(current_group)

        # Verify all platforms were scheduled
        scheduled_count = sum(len(group) for group in execution_groups)
        if scheduled_count != len(platforms):
            raise ConfigurationError(
                f"Could not schedule all platforms. "
                f"Scheduled: {scheduled_count}, Total: {len(platforms)}. "
                f"Check for circular dependencies."
            )

        return execution_groups

    def _get_priority(self, platform_name: str, platforms: List[PlatformConfig]) -> int:
        """Get priority for a platform.

        Args:
            platform_name: Name of platform
            platforms: List of platform configurations

        Returns:
            Priority value (lower = higher priority)
        """
        for platform in platforms:
            if platform.name == platform_name:
                return platform.priority
        return 999  # Default low priority

    def get_parallel_groups(
        self,
        platforms: List[PlatformConfig],
        predefined_groups: List[List[str]],
    ) -> List[List[str]]:
        """Get parallel execution groups from predefined groups.

        This method validates predefined groups against dependencies
        and returns only groups where all platforms can run in parallel.

        Args:
            platforms: List of platform configurations
            predefined_groups: Predefined groups from configuration

        Returns:
            Validated parallel execution groups

        Raises:
            ConfigurationError: If predefined groups violate dependencies
        """
        validated_groups = []

        for group in predefined_groups:
            # Check if group platforms have dependencies on each other
            for platform_name in group:
                dependencies = self._dependency_graph.get(platform_name, [])

                # Check if any dependency is within the same group
                deps_in_group = [dep for dep in dependencies if dep in group]
                if deps_in_group:
                    raise ConfigurationError(
                        f"Invalid parallel group: '{platform_name}' depends on "
                        f"{deps_in_group} which are in the same group. "
                        f"Platforms in the same parallel group cannot depend on each other."
                    )

            validated_groups.append(group)

        logger.debug(f"Validated {len(validated_groups)} parallel execution groups")
        return validated_groups


# Import Optional at the top if not already imported
from typing import Optional
