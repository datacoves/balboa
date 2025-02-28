import sys
from pathlib import Path

from _pytest.config import Config
from _pytest.reports import TestReport


class CustomReporter:
    def __init__(self, config: Config):
        self.config = config
        self.stats = {}
        self.warnings = []

    def pytest_runtest_logreport(self, report: TestReport):
        if report.when == "call":
            self.stats.setdefault(report.outcome, []).append(report)

    def pytest_warning_recorded(self, warning_message):
        self.warnings.append(warning_message)

    def pytest_sessionfinish(self):
        try:
            output_file = self.config.getoption("output_file")
            if not output_file:
                return

            Path(output_file).parent.mkdir(parents=True, exist_ok=True)

            with open(output_file, "w", encoding="utf-8") as f:
                # Only add warnings header if there are warnings
                if self.warnings:
                    f.write("⚠️ **Test Warnings Detected:**\n\n")

                f.write("```\n")

                # Simple test summary
                total = sum(len(v) for v in self.stats.values())
                passed = len(self.stats.get("passed", []))
                failed = len(self.stats.get("failed", []))
                skipped = len(self.stats.get("skipped", []))

                f.write("Tests Summary:\n")
                f.write(f"Total Tests: {total}\n")
                f.write(f"Passed: {passed}\n")
                if failed:
                    f.write(f"Failed: {failed}\n")
                if skipped:
                    f.write(f"Skipped: {skipped}\n")

                # Only show warnings if they exist
                if self.warnings:
                    f.write("\nWarnings:\n")
                    for warning in self.warnings:
                        f.write(f"- {str(warning.message)}\n")

                f.write("```\n")

        except Exception as e:
            print(f"Error writing report: {e}", file=sys.stderr)


def pytest_configure(config):
    output_file = config.getoption("output_file", None)
    if output_file:
        reporter = CustomReporter(config)
        config.pluginmanager.register(reporter)


def pytest_addoption(parser):
    parser.addoption(
        "--output-file",
        action="store",
        default=None,
        help="path to output file for custom formatted results",
    )
