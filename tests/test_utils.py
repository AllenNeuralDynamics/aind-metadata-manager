"""Unit tests for utils module."""

import json
import tempfile
import unittest
from pathlib import Path

from aind_metadata_manager.utils import (
    SchemaVersion,
    get_compatible_frame_rate,
    get_major_schema_version,
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


class TestGetCompatibleFrameRate(unittest.TestCase):
    """Tests for get_compatible_frame_rate."""

    def test_v1_frame_rates(self):
        """Test extracting frame rates from v1 session data."""
        v1_data = {
            "data_streams": [
                {
                    "ophys_fovs": [
                        {"frame_rate": "19.4188"},
                        {"frame_rate": "19.4188"},
                    ]
                }
            ]
        }
        result = get_compatible_frame_rate(
            v1_data, SchemaVersion.V1
        )
        self.assertEqual(result, [19.4188, 19.4188])

    def test_v2_frame_rates(self):
        """Test extracting frame rates from v2 acquisition data."""
        v2_data = {
            "data_streams": [
                {
                    "configurations": [
                        {
                            "sampling_strategy": {
                                "frame_rate": 10.71
                            }
                        }
                    ]
                }
            ]
        }
        result = get_compatible_frame_rate(
            v2_data, SchemaVersion.V2
        )
        self.assertEqual(result, [10.71])

    def test_v1_multiple_streams(self):
        """Test v1 with multiple data streams."""
        v1_data = {
            "data_streams": [
                {
                    "ophys_fovs": [
                        {"frame_rate": "10.0"},
                    ]
                },
                {
                    "ophys_fovs": [
                        {"frame_rate": "20.0"},
                    ]
                },
            ]
        }
        result = get_compatible_frame_rate(
            v1_data, SchemaVersion.V1
        )
        self.assertEqual(result, [10.0, 20.0])

    def test_v2_multiple_configs(self):
        """Test v2 with multiple configurations."""
        v2_data = {
            "data_streams": [
                {
                    "configurations": [
                        {
                            "sampling_strategy": {
                                "frame_rate": 10.71
                            }
                        },
                        {
                            "sampling_strategy": {
                                "frame_rate": 52.13
                            }
                        },
                    ]
                }
            ]
        }
        result = get_compatible_frame_rate(
            v2_data, SchemaVersion.V2
        )
        self.assertEqual(result, [10.71, 52.13])

    def test_empty_data_streams(self):
        """Test empty data_streams returns empty list."""
        data = {"data_streams": []}
        self.assertEqual(
            get_compatible_frame_rate(
                data, SchemaVersion.V1
            ),
            [],
        )
        self.assertEqual(
            get_compatible_frame_rate(
                data, SchemaVersion.V2
            ),
            [],
        )

    def test_missing_data_streams(self):
        """Test missing data_streams key returns empty list."""
        data = {}
        self.assertEqual(
            get_compatible_frame_rate(
                data, SchemaVersion.V1
            ),
            [],
        )
        self.assertEqual(
            get_compatible_frame_rate(
                data, SchemaVersion.V2
            ),
            [],
        )

    def test_v1_missing_ophys_fovs(self):
        """Test v1 stream without ophys_fovs is skipped."""
        v1_data = {
            "data_streams": [
                {"stream_modalities": ["ecephys"]}
            ]
        }
        result = get_compatible_frame_rate(
            v1_data, SchemaVersion.V1
        )
        self.assertEqual(result, [])

    def test_v2_missing_sampling_strategy(self):
        """Test v2 config without sampling_strategy is skipped."""
        v2_data = {
            "data_streams": [
                {
                    "configurations": [
                        {"device_name": "Laser"}
                    ]
                }
            ]
        }
        result = get_compatible_frame_rate(
            v2_data, SchemaVersion.V2
        )
        self.assertEqual(result, [])

    def test_v2_missing_configurations(self):
        """Test v2 stream without configurations is skipped."""
        v2_data = {
            "data_streams": [
                {"stream_modalities": ["ecephys"]}
            ]
        }
        result = get_compatible_frame_rate(
            v2_data, SchemaVersion.V2
        )
        self.assertEqual(result, [])

    def test_from_file_path(self):
        """Test loading acquisition data from a file path."""
        v2_data = {
            "data_streams": [
                {
                    "configurations": [
                        {
                            "sampling_strategy": {
                                "frame_rate": 10.71
                            }
                        }
                    ]
                }
            ]
        }
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(v2_data, f)
            f.flush()
            result = get_compatible_frame_rate(
                f.name, SchemaVersion.V2
            )
        self.assertEqual(result, [10.71])


if __name__ == "__main__":
    unittest.main()
