#!/usr/bin/env python3
"""
Branch Name and Merge Validation Script

This script validates:
1. Branch names follow the pattern: feature/*, release/*, or main
2. Merge operations follow allowed patterns:
   - feature -> feature
   - feature -> release
   - release -> main

Usage:
    # Validate current branch name
    python branch_validator.py --validate-branch

    # Validate merge from source to target branch
    python branch_validator.py --validate-merge <source_branch> <target_branch>

    # Validate both current branch and a merge operation
    python branch_validator.py --validate-branch --validate-merge <source_branch> <target_branch>

Exit codes:
    0: Validation passed
    1: Validation failed
    2: Invalid arguments
"""

import argparse
import re
import subprocess
import sys
from typing import Tuple, Optional


class BranchValidator:
    """Validates branch names and merge operations according to git flow rules."""

    # Valid branch name patterns
    VALID_PATTERNS = [
        r'^feature/.*$',
        r'^release/.*$',
        r'^main$'
    ]

    # Valid merge operations (source -> target)
    VALID_MERGES = [
        ('feature', 'feature'),
        ('feature', 'release'),
        ('release', 'main')
    ]

    def __init__(self):
        self.compiled_patterns = [re.compile(pattern) for pattern in self.VALID_PATTERNS]

    def get_current_branch(self) -> Optional[str]:
        """Get the current git branch name."""
        try:
            result = subprocess.run(
                ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError:
            return None

    def get_branch_type(self, branch_name: str) -> Optional[str]:
        """Extract the branch type from a branch name."""
        if branch_name == 'main':
            return 'main'
        elif branch_name.startswith('feature/'):
            return 'feature'
        elif branch_name.startswith('release/'):
            return 'release'
        else:
            return None

    def is_valid_branch_name(self, branch_name: str) -> bool:
        """Check if a branch name matches valid patterns."""
        return any(pattern.match(branch_name) for pattern in self.compiled_patterns)

    def is_valid_merge(self, source_branch: str, target_branch: str) -> bool:
        """Check if a merge operation is allowed."""
        source_type = self.get_branch_type(source_branch)
        target_type = self.get_branch_type(target_branch)

        if not source_type or not target_type:
            return False

        return (source_type, target_type) in self.VALID_MERGES

    def validate_branch_name(self, branch_name: str) -> Tuple[bool, str]:
        """
        Validate a branch name.

        Returns:
            Tuple of (is_valid, message)
        """
        if self.is_valid_branch_name(branch_name):
            return True, f"✅ Branch name '{branch_name}' is valid"
        else:
            return False, f"❌ Branch name '{branch_name}' is invalid. Must match: feature/*, release/*, or main"

    def validate_merge_operation(self, source_branch: str, target_branch: str) -> Tuple[bool, str]:
        """
        Validate a merge operation.

        Returns:
            Tuple of (is_valid, message)
        """
        # First validate both branch names
        source_valid, source_msg = self.validate_branch_name(source_branch)
        target_valid, target_msg = self.validate_branch_name(target_branch)

        if not source_valid:
            return False, f"❌ Source branch invalid: {source_msg}"

        if not target_valid:
            return False, f"❌ Target branch invalid: {target_msg}"

        # Then validate the merge operation
        if self.is_valid_merge(source_branch, target_branch):
            source_type = self.get_branch_type(source_branch)
            target_type = self.get_branch_type(target_branch)
            return True, f"✅ Merge operation '{source_type}' -> '{target_type}' is allowed"
        else:
            source_type = self.get_branch_type(source_branch)
            target_type = self.get_branch_type(target_branch)
            return False, f"❌ Merge operation '{source_type}' -> '{target_type}' is not allowed. Valid merges: feature->feature, feature->release, release->main"


def main():
    """Main function to handle command line arguments and run validations."""
    parser = argparse.ArgumentParser(
        description="Validate git branch names and merge operations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --validate-branch
  %(prog)s --validate-merge feature/new-feature release/v1.0
  %(prog)s --validate-branch --validate-merge feature/fix main
        """
    )

    parser.add_argument(
        '--validate-branch',
        action='store_true',
        help='Validate the current branch name'
    )

    parser.add_argument(
        '--validate-merge',
        nargs=2,
        metavar=('SOURCE', 'TARGET'),
        help='Validate merge from SOURCE branch to TARGET branch'
    )

    parser.add_argument(
        '--current-branch',
        help='Override current branch name (useful for testing)'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed output'
    )

    args = parser.parse_args()

    # Require at least one validation type
    if not args.validate_branch and not args.validate_merge:
        parser.error("Must specify --validate-branch and/or --validate-merge")

    validator = BranchValidator()
    all_valid = True

    # Validate current branch if requested
    if args.validate_branch:
        if args.current_branch:
            current_branch = args.current_branch
        else:
            current_branch = validator.get_current_branch()

        if current_branch is None:
            print("❌ Error: Could not determine current branch", file=sys.stderr)
            return 2

        is_valid, message = validator.validate_branch_name(current_branch)
        print(message)

        if not is_valid:
            all_valid = False

    # Validate merge operation if requested
    if args.validate_merge:
        source_branch, target_branch = args.validate_merge
        is_valid, message = validator.validate_merge_operation(source_branch, target_branch)
        print(message)

        if not is_valid:
            all_valid = False

    # Show summary if verbose or if validation failed
    if args.verbose or not all_valid:
        print("\nBranch naming rules:")
        print("  ✅ feature/* (e.g., feature/user-auth, feature/bug-fix)")
        print("  ✅ release/* (e.g., release/v1.0, release/2024-01)")
        print("  ✅ main")
        print("\nAllowed merge operations:")
        print("  ✅ feature -> feature")
        print("  ✅ feature -> release")
        print("  ✅ release -> main")

    return 0 if all_valid else 1


if __name__ == '__main__':
    sys.exit(main())
