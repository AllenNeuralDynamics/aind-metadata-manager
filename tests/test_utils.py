"""Unit tests for utils module."""

import json
import tempfile
import unittest
from pathlib import Path

from aind_metadata_manager.utils import (
    SchemaVersion,
    get_acquisition_metadata,
    get_major_schema_version,
    get_metadata,
)


class TestGetMajorSchemaVersion(unittest.TestCase):
    """Tests for get_major_schema_version."""

    def test_v2_schema(self):
        """Test v2 schema version returns SchemaVersion.V2."""
        data = {"schema_version": "2.4.0"}
        self.assertEqual(
            get_major_schema_version(data),
            SchemaVersion.V2,
        )

    def test_v2_schema_dot_zero(self):
        """Test v2.0.0 returns SchemaVersion.V2."""
        data = {"schema_version": "2.0.0"}
        self.assertEqual(
            get_major_schema_version(data),
            SchemaVersion.V2,
        )

    def test_v1_schema(self):
        """Test v1 schema version returns SchemaVersion.V1."""
        data = {"schema_version": "1.0.0"}
        self.assertEqual(
            get_major_schema_version(data),
            SchemaVersion.V1,
        )

    def test_missing_field(self):
        """Test missing schema_version defaults to V1."""
        data = {}
        self.assertEqual(
            get_major_schema_version(data),
            SchemaVersion.V1,
        )

    def test_empty_string(self):
        """Test empty schema_version defaults to V1."""
        data = {"schema_version": ""}
        self.assertEqual(
            get_major_schema_version(data),
            SchemaVersion.V1,
        )

    def test_none_value(self):
        """Test None schema_version defaults to V1."""
        data = {"schema_version": None}
        self.assertEqual(
            get_major_schema_version(data),
            SchemaVersion.V1,
        )

    def test_from_file_path_str(self):
        """Test loading from a file path string."""
        data = {"schema_version": "2.1.0"}
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(data, f)
            f.flush()
            result = get_major_schema_version(f.name)
        self.assertEqual(result, SchemaVersion.V2)

    def test_from_file_path_pathlib(self):
        """Test loading from a pathlib.Path."""
        data = {"schema_version": "1.0.0"}
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(data, f)
            f.flush()
            result = get_major_schema_version(
                Path(f.name)
            )
        self.assertEqual(result, SchemaVersion.V1)

    def test_file_not_found(self):
        """Test FileNotFoundError for nonexistent path."""
        with self.assertRaises(FileNotFoundError):
            get_major_schema_version(
                "/nonexistent/path.json"
            )


class TestGetMetadata(unittest.TestCase):
    """Tests for get_metadata."""

    def test_finds_file(self):
        """Test recursive search finds a metadata file."""
        with tempfile.TemporaryDirectory() as td:
            sub = Path(td) / "nested"
            sub.mkdir()
            data = {"key": "value"}
            (sub / "subject.json").write_text(
                json.dumps(data)
            )
            result = get_metadata(Path(td), "subject.json")
            self.assertEqual(result, data)

    def test_file_not_found(self):
        """Test FileNotFoundError when file is missing."""
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(FileNotFoundError):
                get_metadata(
                    Path(td), "nonexistent.json"
                )


class TestGetAcquisitionMetadata(unittest.TestCase):
    """Tests for get_acquisition_metadata."""

    def test_v2_loads_acquisition(self):
        """Test V2 loads acquisition.json."""
        with tempfile.TemporaryDirectory() as td:
            data = {"schema_version": "2.4.0"}
            (Path(td) / "acquisition.json").write_text(
                json.dumps(data)
            )
            result = get_acquisition_metadata(
                Path(td), SchemaVersion.V2
            )
            self.assertEqual(result, data)

    def test_v1_loads_session(self):
        """Test V1 loads session.json."""
        with tempfile.TemporaryDirectory() as td:
            data = {"schema_version": "1.0.0"}
            (Path(td) / "session.json").write_text(
                json.dumps(data)
            )
            result = get_acquisition_metadata(
                Path(td), SchemaVersion.V1
            )
            self.assertEqual(result, data)


if __name__ == "__main__":
    unittest.main()
