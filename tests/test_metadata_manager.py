"""Unit tests for MetadataManager functionality."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar
from unittest import mock

from aind_data_schema.components.identifiers import Code, Person
from aind_data_schema.core.data_description import DataDescription, Funding
from aind_data_schema.core.processing import DataProcess, Processing
from aind_data_schema.core.quality_control import QCMetric, QualityControl
from aind_data_schema_models.data_name_patterns import DataLevel
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization

from aind_metadata_manager.metadata_manager import (
    MetadataManager,
    MetadataSettings,
)


class DummySettings(MetadataSettings):
    """Dummy settings for testing purposes."""

    cli_parse_args: ClassVar[bool] = False
    input_dir: Path
    output_dir: Path
    processor_full_name: str = "Test User"
    pipeline_version: str = "1.0"
    pipeline_url: str = "http://example.com"
    pipeline_name: str = "test-pipeline"
    data_summary: str = "Test summary"
    modality: str = "E"
    skip_ancillary_files: bool = True
    aggregate_quality_control: bool = False
    verbose: bool = False


class TestMetadataManager(unittest.TestCase):
    """Tests for MetadataManager functionality."""

    def test_find_matching_file_verbose(self):
        """Test finding a matching file with verbose output."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                (input_dir / "foo.txt").write_text("bar")
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir, verbose=True
                )
                manager = MetadataManager(settings)
                found = manager._find_matching_file("foo.txt")
                self.assertIsNotNone(found)

    def test_copy_file_error(self):
        """Test copying a file that does not exist."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                src = Path(tempdir) / "src.txt"
                dst = Path(tempdir) / "dst.txt"
                # src does not exist
                settings = DummySettings(
                    input_dir=Path(tempdir),
                    output_dir=Path(tempdir),
                    verbose=True,
                )
                manager = MetadataManager(settings)
                with self.assertRaises(FileNotFoundError):
                    manager._copy_file(src, dst, "src.txt")

    def test_handle_missing_file_verbose(self):
        """Test handling a missing file with verbose output."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                settings = DummySettings(
                    input_dir=Path(tempdir),
                    output_dir=Path(tempdir),
                    verbose=True,
                )
                manager = MetadataManager(settings)
                manager._handle_missing_file("notfound.txt")

    def test_find_data_description_file_multiple(self):
        """Test finding a data description file in multiple directories."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                (input_dir / "data_description.json").write_text("{}")
                (input_dir / "sub").mkdir()
                (input_dir / "sub" / "data_description.json").write_text("{}")
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir, verbose=True
                )
                manager = MetadataManager(settings)
                found = manager._find_data_description_file()
                self.assertIsNotNone(found)

    def test_write_derived_data_description_verbose(self):
        """Test writing derived data description with verbose output."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                output_dir = Path(tempdir)
                settings = DummySettings(
                    input_dir=output_dir, output_dir=output_dir, verbose=True
                )
                manager = MetadataManager(settings)
                dummy_upgrader = mock.Mock()
                with mock.patch(
                    "aind_metadata_manager.metadata_manager.DataDescription"  # noqa: E501
                ) as MockDerived:
                    instance = MockDerived.from_raw.return_value
                    instance.write_standard_file.return_value = None
                    manager._write_derived_data_description(dummy_upgrader)
                    self.assertTrue(MockDerived.from_raw.called)

    def test_copy_ancillary_files_verbose_and_skip(self):
        """Test copying ancillary files with verbose output and skipping."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                output_dir = Path(tempdir) / "out"
                output_dir.mkdir()
                settings = DummySettings(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    skip_ancillary_files=True,
                    verbose=True,
                )
                manager = MetadataManager(settings)
                manager.copy_ancillary_files()  # Should skip and not error

    def test_create_derived_data_description_missing_file(self):
        """Test creating derived data description when the file is missing."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                output_dir = Path(tempdir) / "out"
                output_dir.mkdir()
                settings = DummySettings(
                    input_dir=input_dir, output_dir=output_dir, verbose=True
                )
                manager = MetadataManager(settings)
                # Should not raise, just return
                manager.create_derived_data_description()

    def test_create_derived_data_description_error(self):
        """Test creating derived data description when an error occurs."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                output_dir = Path(tempdir) / "out"
                output_dir.mkdir()
                dd_path = input_dir / "data_description.json"
                dd_path.write_text("{}")
                settings = DummySettings(
                    input_dir=input_dir, output_dir=output_dir, verbose=True
                )
                manager = MetadataManager(settings)
                with mock.patch(
                    "aind_metadata_manager.metadata_manager.DataDescription",  # noqa: E501
                    side_effect=Exception("fail"),
                ):
                    with self.assertRaises(Exception):
                        manager.create_derived_data_description()

    def test_collect_json_objects_empty(self):
        """Test collecting JSON objects when no files are found."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir, verbose=True
                )
                manager = MetadataManager(settings)
                objs = manager.collect_json_objects("notfound")
                self.assertEqual(objs, [])

    def test_collect_metrics_invalid(self):
        """Test collecting metrics when the file is invalid."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                (input_dir / "foo_metric.json").write_text(json.dumps({}))
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir, verbose=True
                )
                manager = MetadataManager(settings)
                metrics = manager.collect_metrics()
                self.assertIsInstance(metrics, list)

    def test_create_quality_control_metadata_with_metric(self):
        """Test creating quality control metadata with a metric file."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                # Write a valid evaluation JSON if possible
                (input_dir / "foo_metric.json").write_text(
                    json.dumps(
                        {
                            "name": "test",
                            "modality": {"abbreviation": "behavior"},
                            "stage": "Processing",
                            "value": "1.5",
                            "status_history": [
                                {
                                    "evaluator": "John Doe",
                                    "status": "Pass",
                                    "timestamp": str(
                                        "2025-06-04T14:42:32.061702-07:00"
                                    ),
                                }
                            ],
                        }
                    )
                )

                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir, verbose=True
                )
                manager = MetadataManager(settings)
                qc = manager.create_quality_control_metadata()
                self.assertIsNotNone(qc)

    def test_run_function(self):
        """Test the run() function can be called without error in a minimal
        environment.
        """
        with mock.patch("sys.argv", [""]):
            with mock.patch(
                "aind_metadata_manager.metadata_manager.MetadataSettings"
            ) as MockSettings:
                mock_settings = MockSettings.return_value
                mock_settings.verbose = False
                mock_settings.input_dir = Path("/tmp")
                mock_settings.output_dir = Path("/tmp")
                with mock.patch(
                    "aind_metadata_manager.metadata_manager.MetadataManager"
                ) as MockManager:
                    mock_manager = MockManager.return_value
                    mock_manager.create_processing_metadata.return_value = (
                        mock.Mock(write_standard_file=lambda x: None)
                    )
                    mock_manager.copy_ancillary_files.return_value = None
                    from aind_metadata_manager.metadata_manager import run

                    run()

    def test_find_matching_file_and_handle_missing(self):
        """Test _find_matching_file and _handle_missing_file for found
        and not found cases.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                (input_dir / "foo.txt").write_text("bar")
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir
                )
                manager = MetadataManager(settings)
                # Should find the file
                found = manager._find_matching_file("foo.txt")
                self.assertIsNotNone(found)
                # Should not find a non-existent file
                not_found = manager._find_matching_file("baz.txt")
                self.assertIsNone(not_found)
                # _handle_missing_file just logs, but should not error
                manager._handle_missing_file("baz.txt")

    def test_copy_file(self):
        """Test _copy_file copies a file successfully."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                src = Path(tempdir) / "src.txt"
                dst = Path(tempdir) / "dst.txt"
                src.write_text("hello")
                settings = DummySettings(
                    input_dir=Path(tempdir), output_dir=Path(tempdir)
                )
                manager = MetadataManager(settings)
                manager._copy_file(src, dst, "src.txt")
                self.assertTrue(dst.exists())
                self.assertEqual(dst.read_text(), "hello")

    def test_find_data_description_file(self):
        """Test _find_data_description_file finds a single file."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                (input_dir / "data_description.json").write_text("{}")
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir
                )
                manager = MetadataManager(settings)
                found = manager._find_data_description_file()
                self.assertIsNotNone(found)

    def test_apply_overrides_and_validate_modality(self):
        """Test _apply_overrides sets fields and _validate_modality raises on
        bad input.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:

                class DummyUpgrader:
                    """Dummy upgrader class for testing _apply_overrides."""

                    data_summary = None
                    modalities = None

                input_dir = Path(tempdir)
                output_dir = Path(tempdir) / "out"
                output_dir.mkdir()
                settings = DummySettings(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    modality="pophys",
                    data_summary="summary",
                )
                manager = MetadataManager(settings)
                upgrader = DummyUpgrader()
                manager._apply_overrides(upgrader)
                self.assertEqual(upgrader.data_summary, "summary")
                self.assertTrue(upgrader.modalities)
                # Test _validate_modality raises on bad input
                with self.assertRaises(ValueError):
                    manager._validate_modality("not-a-modality")

    def test_collect_json_objects_and_metrics(self):
        """Test collect_json_objects and collect_metrics with a valid
        file.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                # Write a dummy evaluation file
                eval_path = input_dir / "foo_metric.json"
                eval_path.write_text(
                    json.dumps(
                        {
                            "name": "test",
                            "modality": {"abbreviation": "behavior"},
                            "stage": "Processing",
                            "value": "1.5",
                        }
                    )
                )

                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir
                )
                manager = MetadataManager(settings)
                objs = manager.collect_json_objects("metric")
                self.assertEqual(len(objs), 1)
                # collect_evaluations should not error on invalid data
                metrics = manager.collect_metrics()
                self.assertIsInstance(metrics, list)

    def test_create_quality_control_metadata(self):
        """Test create_quality_control_metadata raises ValueError when no
        metrics are found.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir
                )
                manager = MetadataManager(settings)
                with self.assertRaises(ValueError):
                    manager.create_quality_control_metadata()

    def test_copy_ancillary_files_missing(self):
        """Test copy_ancillary_files does not raise if files are missing."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                output_dir = Path(tempdir) / "out"
                output_dir.mkdir()
                settings = DummySettings(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    skip_ancillary_files=False,
                )
                manager = MetadataManager(settings)
                # Should not raise even if files are missing
                manager.copy_ancillary_files()

    def test_create_processing_metadata(self):
        """Test create_processing_metadata creates a Processing object from
        valid input.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir) / "input"
                output_dir = Path(tempdir) / "output"
                input_dir.mkdir()
                output_dir.mkdir()
                dp = {
                    "name": "Analysis",
                    "process_type": "Analysis",
                    "start_date_time": "2023-01-01T00:00:00Z",
                    "end_date_time": "2023-01-01T01:00:00Z",
                    "code": {
                        "url": "http://example.com/code",
                        "version": "1.0",
                        "parameters": {"param1": "value1"},
                    },
                    "stage": "Analysis",
                    "output_path": "/output/path",
                    "experimenters": ["John Doe"],
                    "output_parameters": {"param2": "value2"},
                    "notes": "Test process",
                }
                with open(input_dir / "data_process.json", "w") as f:
                    json.dump(dp, f)
                settings = DummySettings(
                    input_dir=input_dir, output_dir=output_dir
                )
                manager = MetadataManager(settings)
                processing = manager.create_processing_metadata()

                self.assertEqual(
                    str(processing.data_processes[0].name),
                    "Analysis",
                )

    def test_copy_ancillary_files(self):
        """Test copy_ancillary_files copies an ancillary file successfully."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir) / "input"
                output_dir = Path(tempdir) / "output"
                input_dir.mkdir()
                output_dir.mkdir()
                # Create all ancillary files
                ancillary_files = [
                    "procedures.json",
                    "subject.json",
                    "session.json",
                    "rig.json",
                    "instrument.json",
                    "acquisition.json",
                ]
                for ancillary in ancillary_files:
                    with open(input_dir / ancillary, "w") as f:
                        json.dump({"test": 1}, f)
                settings = DummySettings(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    skip_ancillary_files=False,
                )
                manager = MetadataManager(settings)
                manager.copy_ancillary_files()
                for ancillary in ancillary_files:
                    self.assertTrue((output_dir / ancillary).exists())

    @mock.patch("aind_data_schema.core.data_description.DataDescription")
    def test_create_derived_data_description(self, MockDerived):
        """Test create_derived_data_description writes a derived data
        description file.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir) / "input"
                output_dir = Path(tempdir) / "output"
                input_dir.mkdir()
                output_dir.mkdir()
                dd = DataDescription(
                    modalities=[Modality.ECEPHYS, Modality.BEHAVIOR_VIDEOS],
                    group="ephys",
                    restrictions="",
                    subject_id="123456",
                    creation_time=datetime(
                        2022, 2, 21, 16, 30, 1, tzinfo=timezone.utc
                    ),
                    institution=Organization.AIND,
                    investigators=[
                        Person(
                            name="John Doe",
                            registry_identifier="0000-0003-3748-6289",
                        )
                    ],  # Include ORCID IDs
                    funding_source=[Funding(funder=Organization.AI)],
                    project_name="Example project",
                    data_level=DataLevel.RAW,
                ).model_dump_json()

                with open(input_dir / "data_description.json", "w") as f:
                    f.write(dd)
                settings = DummySettings(
                    input_dir=input_dir, output_dir=output_dir
                )
                manager = MetadataManager(settings)
                dummy_upgrade = mock.Mock()
                dummy_upgrade.data_summary = None
                dummy_upgrade.modality = None
                dummy_derived = mock.Mock()
                MockDerived.from_raw.return_value = dummy_derived
                dummy_derived.write_standard_file.side_effect = (
                    lambda output_directory: (
                        Path(output_directory) / "data_description.json"
                    ).write_text("{}")
                )
                manager.create_derived_data_description()
                self.assertTrue(
                    (output_dir / "data_description.json").exists()
                )


    def _make_data_process_dict(self, name="TestProcess", **overrides):
        """Helper to create a valid data_process dict for tests."""
        dp = {
            "name": name,
            "process_type": "Analysis",
            "start_date_time": "2023-01-01T00:00:00Z",
            "end_date_time": "2023-01-01T01:00:00Z",
            "code": {
                "url": "http://example.com/code",
                "version": "1.0",
                "parameters": {"param1": "value1"},
            },
            "stage": "Analysis",
            "output_path": "/output/path",
            "experimenters": ["John Doe"],
            "output_parameters": {"param2": "value2"},
            "notes": "Test process",
        }
        dp.update(overrides)
        return dp

    def _make_metric_dict(self, name="test-metric", **overrides):
        """Helper to create a valid QCMetric dict for tests."""
        m = {
            "name": name,
            "modality": {"abbreviation": "behavior"},
            "stage": "Processing",
            "value": "1.5",
            "status_history": [
                {
                    "evaluator": "John Doe",
                    "status": "Pass",
                    "timestamp": "2025-06-04T14:42:32.061702-07:00",
                }
            ],
        }
        m.update(overrides)
        return m

    def test_create_processing_metadata_merges_existing(self):
        """Test that create_processing_metadata appends to an existing
        processing.json found in the input directory.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir) / "input"
                output_dir = Path(tempdir) / "output"
                input_dir.mkdir()
                output_dir.mkdir()

                # Create an existing processing.json with one process
                existing_dp = DataProcess.model_validate(
                    self._make_data_process_dict(
                        name="ExistingProcess",
                        pipeline_name="old-pipeline",
                    )
                )
                existing_processing = Processing(
                    data_processes=[existing_dp],
                    pipelines=[
                        Code(
                            url="http://old.com",
                            version="0.1",
                            name="old-pipeline",
                        )
                    ],
                    dependency_graph={"ExistingProcess": ["ExistingProcess"]},
                )
                processing_path = input_dir / "processing.json"
                processing_path.write_text(
                    existing_processing.model_dump_json(indent=3)
                )

                # Create a new data_process.json
                new_dp = self._make_data_process_dict(name="NewProcess")
                with open(input_dir / "data_process.json", "w") as f:
                    json.dump(new_dp, f)

                settings = DummySettings(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    verbose=True,
                )
                manager = MetadataManager(settings)
                processing = manager.create_processing_metadata()

                # Should have both existing and new processes
                self.assertEqual(len(processing.data_processes), 2)
                names = [p.name for p in processing.data_processes]
                self.assertIn("ExistingProcess", names)
                self.assertIn("NewProcess", names)

                # Should have both pipelines
                self.assertEqual(len(processing.pipelines), 2)

                # Dependency graph should contain both
                self.assertIn("ExistingProcess", processing.dependency_graph)
                self.assertIn("NewProcess", processing.dependency_graph)

    def test_create_processing_metadata_no_existing(self):
        """Test that create_processing_metadata works when there is no
        existing processing.json (original behavior).
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir) / "input"
                output_dir = Path(tempdir) / "output"
                input_dir.mkdir()
                output_dir.mkdir()

                dp = self._make_data_process_dict(name="OnlyProcess")
                with open(input_dir / "data_process.json", "w") as f:
                    json.dump(dp, f)

                settings = DummySettings(
                    input_dir=input_dir, output_dir=output_dir
                )
                manager = MetadataManager(settings)
                processing = manager.create_processing_metadata()

                self.assertEqual(len(processing.data_processes), 1)
                self.assertEqual(
                    processing.data_processes[0].name, "OnlyProcess"
                )
                self.assertEqual(len(processing.pipelines), 1)

    def test_create_quality_control_metadata_merges_existing(self):
        """Test that create_quality_control_metadata appends to an
        existing quality_control.json found in the input directory.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir) / "input"
                output_dir = Path(tempdir) / "output"
                input_dir.mkdir()
                output_dir.mkdir()

                # Create an existing quality_control.json
                existing_metric = QCMetric.model_validate(
                    self._make_metric_dict(name="existing-metric")
                )
                existing_qc = QualityControl(
                    metrics=[existing_metric],
                    default_grouping=[],
                    notes="Existing QC notes",
                )
                qc_path = input_dir / "quality_control.json"
                qc_path.write_text(
                    existing_qc.model_dump_json(indent=3)
                )

                # Create a new metric file
                new_metric = self._make_metric_dict(name="new-metric")
                with open(input_dir / "new_metric.json", "w") as f:
                    json.dump(new_metric, f)

                settings = DummySettings(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    verbose=True,
                )
                manager = MetadataManager(settings)
                qc = manager.create_quality_control_metadata()

                # Should have both existing and new metrics
                self.assertEqual(len(qc.metrics), 2)
                names = [m.name for m in qc.metrics]
                self.assertIn("existing-metric", names)
                self.assertIn("new-metric", names)

                # Should preserve existing notes
                self.assertEqual(qc.notes, "Existing QC notes")

    def test_create_quality_control_metadata_existing_only(self):
        """Test that existing QC metrics alone satisfy the non-empty
        requirement (no new metric files needed).
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir) / "input"
                output_dir = Path(tempdir) / "output"
                input_dir.mkdir()
                output_dir.mkdir()

                # Create an existing quality_control.json
                existing_metric = QCMetric.model_validate(
                    self._make_metric_dict(name="existing-metric")
                )
                existing_qc = QualityControl(
                    metrics=[existing_metric],
                    default_grouping=[],
                )
                qc_path = input_dir / "quality_control.json"
                qc_path.write_text(
                    existing_qc.model_dump_json(indent=3)
                )

                # No new metric files — should still succeed
                settings = DummySettings(
                    input_dir=input_dir,
                    output_dir=output_dir,
                )
                manager = MetadataManager(settings)
                qc = manager.create_quality_control_metadata()

                self.assertEqual(len(qc.metrics), 1)
                self.assertEqual(qc.metrics[0].name, "existing-metric")


if __name__ == "__main__":
    unittest.main()
