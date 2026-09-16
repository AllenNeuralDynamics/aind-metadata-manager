"""Tests for schema-native source aggregation."""

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
    pipeline_version: str = "1.0"
    pipeline_name: str = "test-pipeline"
    verbose: bool = False


def _data_description(subject_id: str = "123456") -> DataDescription:
    """Build a minimal valid raw DataDescription."""
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


def _processing(
    name: str = "Analysis", pipeline: Code | None = None
) -> Processing:
    """Build a Processing with one named DataProcess."""
    process = DataProcess(
        name=name,
        process_type="Analysis",
        stage=ProcessStage.PROCESSING,
        start_date_time=TS,
        end_date_time=datetime(2023, 1, 1, 1, tzinfo=timezone.utc),
        code=Code(url="http://example.com/code", version="1.0"),
        experimenters=["Jane"],
    )
    return Processing(
        data_processes=[process],
        pipelines=[pipeline] if pipeline else None,
        dependency_graph={name: []},
    )


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
    pipeline: Code | None = None,
) -> Path:
    """Write an upstream source-asset directory with core files."""
    asset_dir = root / name
    asset_dir.mkdir(parents=True, exist_ok=True)
    (asset_dir / "data_description.json").write_text(
        _data_description(subject_id).model_dump_json()
    )
    if with_processing:
        (asset_dir / "processing.json").write_text(
            _processing(pipeline=pipeline).model_dump_json()
        )
    if with_qc:
        (asset_dir / "quality_control.json").write_text(
            _quality_control().model_dump_json()
        )
    return asset_dir


def _manager(input_dir: Path, output_dir: Path, **kw) -> MetadataManager:
    """Construct a MetadataManager with no argv parsing."""
    with mock.patch("sys.argv", [""]):
        settings = DummySettings(
            input_dir=input_dir, output_dir=output_dir, **kw
        )
    return MetadataManager(settings)


class TestSourceDiscovery(unittest.TestCase):
    """Discovery and assembly of source Metadata."""

    def test_source_asset_dirs_found(self):
        """Each data description directory is one source asset."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetB")
            _write_source_asset(root, "assetA")
            self.assertEqual(
                _manager(root, root)._source_asset_dirs(),
                [root / "assetA", root / "assetB"],
            )

    def test_source_metadata_carries_processing_and_qc(self):
        """Loaded source Metadata carries validated processing and QC."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA")
            source = _manager(root, root)._load_source_metadata()[0]
            self.assertIsNotNone(source.processing)
            self.assertIsNotNone(source.quality_control)

    def test_partial_source_without_qc(self):
        """A source can omit optional quality-control metadata."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA", with_qc=False)
            source = _manager(root, root)._load_source_metadata()[0]
            self.assertIsNone(source.quality_control)
            self.assertIsNotNone(source.processing)


class TestAggregation(unittest.TestCase):
    """Schema-native build_derived_metadata behavior."""

    def test_single_source_derived(self):
        """One source becomes a derived data description."""
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
        """Two same-subject sources accumulate through schema addition."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA")
            _write_source_asset(root, "assetB")
            derived = _manager(root, root).build_derived_metadata()
            self.assertEqual(len(derived.processing.data_processes), 2)
            self.assertEqual(len(derived.quality_control.metrics), 2)

    def test_duplicate_pipelines_collapsed(self):
        """Identical source pipelines occur once in the derived object."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline = Code(url="http://pipe", name="P", version="1.0")
            _write_source_asset(root, "assetA", pipeline=pipeline)
            _write_source_asset(root, "assetB", pipeline=pipeline)
            derived = _manager(root, root).build_derived_metadata()
            self.assertEqual(len(derived.processing.pipelines), 1)

    def test_distinct_pipelines_same_name_preserved(self):
        """Distinct full Code identities sharing a name are preserved."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name, version in (("assetA", "1.0"), ("assetB", "2.0")):
                _write_source_asset(
                    root,
                    name,
                    pipeline=Code(
                        url="http://pipe", name="P", version=version
                    ),
                )
            derived = _manager(root, root).build_derived_metadata()
            self.assertEqual(len(derived.processing.pipelines), 2)

    def test_no_sources_raises(self):
        """No source assets produce a clear ValueError."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(ValueError):
                _manager(root, root).build_derived_metadata()

    def test_cross_acquisition_without_new_processing_raises(self):
        """Cross-subject merge without new processing is rejected."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA", subject_id="111111")
            _write_source_asset(root, "assetB", subject_id="222222")
            with self.assertRaises(Exception):
                _manager(root, root).build_derived_metadata()


class TestOverridesAndOutput(unittest.TestCase):
    """DataDescription overrides and standard-file output."""

    def test_modality_override_applied(self):
        """A configured modality replaces derived modalities."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_source_asset(root, "assetA")
            derived = _manager(
                root, root, modality="pophys"
            ).build_derived_metadata()
            self.assertEqual(
                [m.abbreviation for m in derived.data_description.modalities],
                ["pophys"],
            )

    def test_invalid_modality_raises(self):
        """An unknown modality abbreviation raises ValueError."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(ValueError):
                _manager(root, root)._validate_modality("not-a-modality")

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


if __name__ == "__main__":
    unittest.main()
