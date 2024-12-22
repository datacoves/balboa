import pytest
import sys
from pathlib import Path
from _pytest.config import Config
from _pytest.terminal import TerminalReporter
from _pytest.reports import TestReport

class CustomReporter:
    def __init__(self, config: Config):
        self.config = config
        self.stats = {}
        self.warnings = []
        self.errors = []

    def pytest_runtest_logreport(self, report: TestReport):
        if report.when == 'call':
            self.stats.setdefault(report.outcome, []).append(report)
        if hasattr(report, 'longrepr'):  # Capture error details
            self.errors.append(report)

    def pytest_warning_recorded(self, warning_message):
        self.warnings.append(warning_message)

    def _generate_report_content(self):
        """Generate the report content as a string"""
        lines = []
        
        # Tests Summary Section
        total = sum(len(v) for v in self.stats.values())
        passed = len(self.stats.get('passed', []))
        failed = len(self.stats.get('failed', []))
        skipped = len(self.stats.get('skipped', []))
        error = len(self.stats.get('error', []))

        lines.append("# Test Execution Report\n")
        
        # Add warning header only if warnings exist
        if self.warnings:
            lines.append("⚠️ **Warnings Detected**\n")
        
        lines.append("```\n")
        lines.append("Tests Summary:\n")
        lines.append(f"Total Tests: {total}\n")
        lines.append(f"✅ Passed: {passed}\n")
        if failed:
            lines.append(f"❌ Failed: {failed}\n")
        if error:
            lines.append(f"🔥 Errors: {error}\n")
        if skipped:
            lines.append(f"⏭️  Skipped: {skipped}\n")

        # Warnings Section (if any)
        if self.warnings:
            lines.append("\nWarnings:\n")
            for warning in self.warnings:
                lines.append(f"- {str(warning.message)}\n")
                lines.append(f"  Location: {warning.filename}:{warning.lineno}\n")

        # Errors Section (if any)
        if self.errors:
            lines.append("\nTest Failures/Errors:\n")
            for error in self.errors:
                if hasattr(error, 'longrepr'):
                    lines.append(f"- Test: {error.nodeid}\n")
                    lines.append(f"  Outcome: {error.outcome}\n")
                    lines.append(f"  Details: {str(error.longrepr)}\n")

        lines.append("```\n")
        return "".join(lines)

    def pytest_sessionfinish(self):
        try:
            output_file = self.config.getoption('output_file')
            if not output_file:
                return

            # Create parent directories if they don't exist
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Generate and write report
            report_content = self._generate_report_content()
            
            try:
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(report_content)
            except IOError as e:
                print(f"Error writing to file {output_file}: {e}", file=sys.stderr)
                # Attempt to write to a fallback location
                fallback_file = Path('pytest_report_fallback.md')
                with open(fallback_file, 'w', encoding='utf-8') as f:
                    f.write(report_content)
                print(f"Report written to fallback file: {fallback_file}", file=sys.stderr)

        except Exception as e:
            print(f"Error generating report: {str(e)}", file=sys.stderr)
            raise

def pytest_configure(config):
    try:
        output_file = config.getoption('output_file', None)
        if output_file:
            reporter = CustomReporter(config)
            config.pluginmanager.register(reporter)
    except Exception as e:
        print(f"Error configuring custom reporter: {str(e)}", file=sys.stderr)
        raise

def pytest_addoption(parser):
    parser.addoption(
        "--output-file",
        action="store",
        default=None,
        help="path to output file for custom formatted results"
    )