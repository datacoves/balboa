import pytest
from _pytest.config import Config
from _pytest.terminal import TerminalReporter
from _pytest.reports import TestReport

class CustomReporter:
    def __init__(self, config: Config):
        self.config = config
        self.stats = {}
        self.warnings = []

    def pytest_runtest_logreport(self, report: TestReport):
        if report.when == 'call':
            self.stats.setdefault(report.outcome, []).append(report)

    def pytest_warning_recorded(self, warning_message):
        self.warnings.append(warning_message)

    def pytest_sessionfinish(self):
        output_file = self.config.getoption('output_file')
        if not output_file:
            return

        with open(output_file, 'w') as f:
            if self.warnings:
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
                    f.write(f"- {str(warning.message)} \n  at {warning.filename}:{warning.lineno}\n")

            f.write("```\n")

def pytest_configure(config):
    output_file = config.getoption('output_file', None)
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
