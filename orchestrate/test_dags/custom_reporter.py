import pytest
from _pytest.config import Config
from _pytest.terminal import TerminalReporter
from _pytest.reports import TestReport

class CustomReporter:
    def __init__(self, config: Config):
        self.config = config
        self.stats = {}
        self.warnings = []
        self._tw = config.get_terminal_writer()

    def pytest_runtest_logreport(self, report: TestReport):
        if report.when == 'call':
            self.stats.setdefault(report.outcome, []).append(report)

        # Capture warnings
        if hasattr(report, 'warnings'):
            self.warnings.extend(report.warnings)

    def pytest_sessionfinish(self):
        # Create the markdown output
        with open(self.config.option.output_file, 'w') as f:
            f.write("⚠️ **Test Warnings Detected:**\n\n")
            f.write("```\n")

            # Write test summary
            total = sum(len(v) for v in self.stats.values())
            passed = len(self.stats.get('passed', []))
            failed = len(self.stats.get('failed', []))
            skipped = len(self.stats.get('skipped', []))

            f.write(f"Tests Summary:\n")
            f.write(f"Total Tests: {total}\n")
            f.write(f"Passed: {passed}\n")
            if failed:
                f.write(f"Failed: {failed}\n")
            if skipped:
                f.write(f"Skipped: {skipped}\n")

            # Write warnings
            if self.warnings:
                f.write("\nWarnings:\n")
                for warning in self.warnings:
                    f.write(f"- {str(warning.message)}\n")

            f.write("```\n")

def pytest_configure(config):
    # Get output file from command line option
    output_file = getattr(config.option, 'output_file', None)
    if output_file:
        reporter = CustomReporter(config)
        config.pluginmanager.register(reporter)

def pytest_addoption(parser):
    parser.addoption(
        "--output-file",
        action="store",
        default=None,
        help="path to output file for custom formatted results"
    )
