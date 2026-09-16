"""Tests for source asset discovery and schema object assembly."""

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
from aind_data_schema_models.data_name_patterns import DataLevel
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization

from aind_metadata_manager.metadata_manager import (
    MetadataManager,
    MetadataSettings,
)

TS = datetime(2023, 1, 1, tzinfo=timezone.utc)


class DummySettings(MetadataSettings):
    """Settings that do not parse command-line arguments."""

    cli_parse_args: ClassVar[bool] = False
    input_dir: Path
    output_dir: Path
    processor_full_name: str = "Test User"
    pipeline_url: str = "http://example.com"
    verbose: bool = False


def _data_description(subject_id: str) -> DataDescription:
    """Build a minimal valid raw data description."""
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


def _processing() -> Processing:
    """Build a minimal valid processing object."""
    process = DataProcess(
        name="Analysis",
        process_type="Analysis",
        stage=ProcessStage.PROCESSING,
        start_date_time=TS,
        end_date_time=datetime(2023, 1, 1, 1, tzinfo=timezone.utc),
        code=Code(url="http://example.com/code", version="1.0"),
        experimenters=["Jane"],
    )
    return Processing(
        data_processes=[process], dependency_graph={"Analysis": []}
    )


def _manager(input_dir: Path) -> MetadataManager:
    """Construct a manager without reading process arguments."""
    with mock.patch("sys.argv", [""]):
        settings = DummySettings(input_dir=input_dir, output_dir=input_dir)
    return MetadataManager(settings)


def _write_asset(root: Path, name: str, with_processing: bool = False) -> Path:
    """Write a source asset containing a valid data description."""
    asset_dir = root / name
    asset_dir.mkdir()
    (asset_dir / "data_description.json").write_text(
        _data_description(name).model_dump_json()
    )
    if with_processing:
        (asset_dir / "processing.json").write_text(
            _processing().model_dump_json()
        )
    return asset_dir


class TestSourceAssembly(unittest.TestCase):
    """Source discovery and assembly behavior."""

    def test_source_asset_dirs_are_sorted(self):
        """Each data description directory is discovered in stable order."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_asset(root, "assetB")
            _write_asset(root, "assetA")
            self.assertEqual(
                _manager(root)._source_asset_dirs(),
                [root / "assetA", root / "assetB"],
            )

    def test_source_metadata_carries_valid_core_object(self):
        """Optional core files are validated and attached to the source."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_asset(root, "assetA", with_processing=True)
            source = _manager(root)._load_source_metadata()[0]
            self.assertEqual(source.data_description.subject_id, "assetA")
            self.assertEqual(len(source.processing.data_processes), 1)

    def test_invalid_data_description_is_skipped(self):
        """An invalid data description does not produce a source object."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            asset = root / "assetA"
            asset.mkdir()
            (asset / "data_description.json").write_text("{}")
            self.assertEqual(_manager(root)._load_source_metadata(), [])

    def test_invalid_core_file_is_skipped(self):
        """An invalid optional core file leaves the source usable."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            asset = _write_asset(root, "assetA")
            (asset / "processing.json").write_text("{not json")
            source = _manager(root)._load_source_metadata()[0]
            self.assertIsNone(source.processing)


if __name__ == "__main__":
    unittest.main()
