"""Unit tests for the from_metadata-based MetadataManager."""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar
from unittest import mock

from aind_data_schema.components.identifiers import Code, Person
from aind_data_schema.core.data_description import DataDescription, Funding
from aind_data_schema.core.processing import (
    DataProcess,
    Processing,
    ProcessStage,
)
from aind_data_schema.core.quality_control import (
    QCMetric,
    QCStatus,
    QualityControl,
    Status,
)
from aind_data_schema_models.data_name_patterns import DataLevel
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization

from aind_metadata_manager.metadata_manager import (
    MetadataManager,
    MetadataSettings,
)

TS = datetime(2023, 1, 1, tzinfo=timezone.utc)


class DummySettings(MetadataSettings):
    """Settings that do not parse argv."""

    cli_parse_args: ClassVar[bool] = False
    input_dir: Path
    output_dir: Path
    pipeline_url: str = "http://example.com/pipeline"
    pipeline_version: str = "1.0"
    pipeline_name: str = "test-pipeline"
    verbose: bool = False


# -- fixture builders --------------------------------------------------------


def _data_description(subject_id: str = "123456") -> DataDescription:
    """Build a minimal valid RAW DataDescription."""
    return DataDescription(
        modalities=[Modality.BEHAVIOR],
        subject_id=subject_id,
        creation_time=TS,
        institution=Organization.AIND,
        investigators=[Person(name="John Doe")],
        funding_source=[Funding(funder=Organization.AI)],
        project_name="proj",
        data_level=DataLevel.RAW,
    )


def _processing(name: str = "Analysis") -> Processing:
    """Build a Processing with one named DataProcess."""
    dp = DataProcess(
        name=name,
        process_type="Analysis",
        stage=ProcessStage.PROCESSING,
        start_date_time=TS,
        end_date_time=datetime(2023, 1, 1, 1, tzinfo=timezone.utc),
        code=Code(url="http://example.com/code", version="1.0"),
        experimenters=["Jane"],
    )
    return Processing(data_processes=[dp], dependency_graph={name: []})


def _quality_control() -> QualityControl:
    """Build a QualityControl with one passing metric."""
    metric = QCMetric(
        name="m1",
        modality=Modality.BEHAVIOR,
        stage="Processing",
        value=1.5,
        status_history=[
            QCStatus(evaluator="e", status=Status.PASS, timestamp=TS)
        ],
        tags=["g1"],
    )
    return QualityControl(metrics=[metric], default_grouping=["g1"])


def _write_source_asset(
    root: Path,
    name: str,
    subject_id: str = "123456",
    with_processing: bool = True,
    with_qc: bool = True,
) -> Path:
    """Write an upstream source-asset directory with core files."""
    asset_dir = root / name
    asset_dir.mkdir(parents=True, exist_ok=True)
    (asset_dir / "data_description.json").write_text(
        _data_description(subject_id).model_dump_json()
    )
    if with_processing:
        (asset_dir / "processing.json").write_text(
            _processing().model_dump_json()
        )
    if with_qc:
        (asset_dir / "quality_control.json").write_text(
            _quality_control().model_dump_json()
        )
    return asset_dir


def _manager(input_dir: Path, output_dir: Path, **kw) -> MetadataManager:
    """Construct a MetadataManager with DummySettings (no argv parsing)."""
    with mock.patch("sys.argv", [""]):
        settings = DummySettings(
            input_dir=input_dir, output_dir=output_dir, **kw
        )
    return MetadataManager(settings)


class TestSourceDiscovery(unittest.TestCase):
    """Discovery and assembly of source Metadata."""

    def test_source_asset_dirs_found(self):
        """Each dir with a data_description.json is one source asset."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA")
            _write_source_asset(root, "assetB")
            mgr = _manager(root, root)
            self.assertEqual(len(mgr._source_asset_dirs()), 2)

    def test_load_source_metadata_carries_processing_and_qc(self):
        """Loaded source Metadata carries validated processing and QC."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA")
            mgr = _manager(root, root)
            sources = mgr._load_source_metadata()
            self.assertEqual(len(sources), 1)
            self.assertIsNotNone(sources[0].processing)
            self.assertIsNotNone(sources[0].quality_control)
            self.assertEqual(sources[0].data_description.subject_id, "123456")

    def test_partial_source_without_qc(self):
        """A source with only data_description + processing still loads."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA", with_qc=False)
            mgr = _manager(root, root)
            sources = mgr._load_source_metadata()
            self.assertIsNone(sources[0].quality_control)
            self.assertIsNotNone(sources[0].processing)


class TestAggregation(unittest.TestCase):
    """build_derived_metadata behavior."""

    def test_single_source_derived(self):
        """One source -> DERIVED data_description, processing + QC carried."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA")
            derived = _manager(root, root).build_derived_metadata()
            self.assertEqual(
                derived.data_description.data_level, DataLevel.DERIVED
            )
            self.assertEqual(len(derived.processing.data_processes), 1)
            self.assertEqual(len(derived.quality_control.metrics), 1)

    def test_multi_source_same_subject_accumulates(self):
        """Two same-subject sources -> processing/QC accumulate via '+'."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA", subject_id="123456")
            _write_source_asset(root, "assetB", subject_id="123456")
            derived = _manager(root, root).build_derived_metadata()
            self.assertEqual(len(derived.processing.data_processes), 2)
            self.assertEqual(len(derived.quality_control.metrics), 2)

    def test_no_sources_raises(self):
        """No source assets -> a clear ValueError."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(ValueError):
                _manager(root, root).build_derived_metadata()

    def test_cross_acquisition_without_new_processing_raises(self):
        """Cross-subject merge with no new processing is rejected (rule 4)."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA", subject_id="111111")
            _write_source_asset(root, "assetB", subject_id="222222")
            with self.assertRaises(Exception):
                _manager(root, root).build_derived_metadata()


class TestNewWork(unittest.TestCase):
    """This run's new processing / QC from standalone files."""

    def test_new_processing_from_standalone_data_process(self):
        """Standalone data_process.json outside asset dirs -> new proc."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "0_data_process.json").write_text(
                _processing().data_processes[0].model_dump_json()
            )
            mgr = _manager(root, root)
            new_proc = mgr.build_new_processing()
            self.assertIsNotNone(new_proc)
            self.assertEqual(len(new_proc.data_processes), 1)
            self.assertEqual(
                new_proc.data_processes[0].pipeline_name, "test-pipeline"
            )

    def test_standalone_excludes_files_inside_source_assets(self):
        """A processing.json inside a source asset is not 'new' work."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA")
            mgr = _manager(root, root)
            # asset processing.json must not be picked up as standalone
            self.assertEqual(mgr._standalone_files("processing"), [])

    def test_cross_acquisition_with_new_processing_drops_sources(self):
        """Cross-subject merge keeps only new processing (sources dropped)."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA", subject_id="111111")
            _write_source_asset(root, "assetB", subject_id="222222")
            (root / "0_data_process.json").write_text(
                _processing("NewStep").data_processes[0].model_dump_json()
            )
            derived = _manager(root, root).build_derived_metadata()
            names = [p.name for p in derived.processing.data_processes]
            self.assertEqual(names, ["NewStep"])

    def test_new_quality_control_from_standalone_metric(self):
        """Standalone metric.json -> new_quality_control."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            metric = _quality_control().metrics[0]
            (root / "0_metric.json").write_text(metric.model_dump_json())
            mgr = _manager(root, root)
            new_qc = mgr.build_new_quality_control()
            self.assertIsNotNone(new_qc)
            self.assertEqual(len(new_qc.metrics), 1)


class TestOverridesAndOutput(unittest.TestCase):
    """DataDescription overrides and file output."""

    def test_modality_override_applied(self):
        """--modality overrides the derived data_description modalities."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA")
            derived = _manager(
                root, root, modality="pophys"
            ).build_derived_metadata()
            abbrevs = [
                m.abbreviation for m in derived.data_description.modalities
            ]
            self.assertIn("pophys", abbrevs)

    def test_invalid_modality_raises(self):
        """An unknown modality abbreviation raises ValueError."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mgr = _manager(root, root)
            with self.assertRaises(ValueError):
                mgr._validate_modality("not-a-modality")

    def test_run_writes_core_files(self):
        """run() writes the derived core files to output_dir."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_dir = root / "data"
            output_dir = root / "results"
            input_dir.mkdir()
            output_dir.mkdir()
            _write_source_asset(input_dir, "assetA")

            with mock.patch("sys.argv", [""]):
                settings = DummySettings(
                    input_dir=input_dir, output_dir=output_dir
                )
            with mock.patch(
                "aind_metadata_manager.metadata_manager.MetadataSettings",
                return_value=settings,
            ):
                from aind_metadata_manager.metadata_manager import run

                run()
            self.assertTrue((output_dir / "data_description.json").exists())
            self.assertTrue((output_dir / "processing.json").exists())
            self.assertTrue((output_dir / "quality_control.json").exists())


class TestBranches(unittest.TestCase):
    """Cover error, override, and verbose branches."""

    def test_invalid_core_file_is_skipped(self):
        """An unparseable processing.json is dropped, source still loads."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            asset = _write_source_asset(root, "assetA")
            (asset / "processing.json").write_text("{ not json")
            sources = _manager(root, root)._load_source_metadata()
            self.assertEqual(len(sources), 1)
            self.assertIsNone(sources[0].processing)

    def test_invalid_data_description_dir_skipped(self):
        """A dir whose data_description.json is invalid yields no source."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            asset = root / "assetA"
            asset.mkdir()
            (asset / "data_description.json").write_text("{}")
            mgr = _manager(root, root)
            self.assertEqual(mgr._load_source_metadata(), [])
            with self.assertRaises(ValueError):
                mgr.build_derived_metadata()

    def test_missing_pipeline_url_raises_for_new_processing(self):
        """Standalone data_process with no pipeline_url raises ValueError."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "0_data_process.json").write_text(
                _processing().data_processes[0].model_dump_json()
            )
            mgr = _manager(root, root, pipeline_url="")
            with self.assertRaises(ValueError):
                mgr.build_new_processing()

    def test_invalid_standalone_files_skipped(self):
        """Invalid standalone data_process/metric files are skipped."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "0_data_process.json").write_text("{}")
            (root / "0_metric.json").write_text("{}")
            mgr = _manager(root, root)
            self.assertIsNone(mgr.build_new_processing())
            self.assertIsNone(mgr.build_new_quality_control())

    def test_data_summary_override(self):
        """--data_summary flows into the derived data_description."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA")
            derived = _manager(
                root, root, data_summary="my summary"
            ).build_derived_metadata()
            self.assertEqual(
                derived.data_description.data_summary, "my summary"
            )

    def test_verbose_end_to_end(self):
        """Verbose run exercises logging branches and writes files."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_dir = root / "data"
            output_dir = root / "results"
            input_dir.mkdir()
            output_dir.mkdir()
            _write_source_asset(input_dir, "assetA")
            with mock.patch("sys.argv", [""]):
                settings = DummySettings(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    verbose=True,
                )
            with mock.patch(
                "aind_metadata_manager.metadata_manager.MetadataSettings",
                return_value=settings,
            ):
                from aind_metadata_manager.metadata_manager import run

                run()
            self.assertTrue((output_dir / "processing.json").exists())


if __name__ == "__main__":
    unittest.main()
