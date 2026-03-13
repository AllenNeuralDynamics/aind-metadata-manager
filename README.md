
# AIND Metadata Manager

AIND Metadata Manager aggregates metadata produced by individual capsules in a Code Ocean pipeline. It collects serialized [aind-data-schema](https://github.com/AllenNeuralDynamics/aind-data-schema) `DataProcess` and `QCMetric` JSON objects from upstream capsules into `processing.json` and `quality_control.json`. It also derives a new `data_description.json` from the input data asset and copies ancillary metadata files to the output.

It is used by adding the [`aind-metadata-manager-capsule`](https://github.com/AllenNeuralDynamics/aind-metadata-manager-capsule) to the end of a pipeline.

## Features

**Aggregation** — Collect serialized JSON objects from upstream capsules into consolidated output files:
- `DataProcess` objects (`*data_process*.json`) into `processing.json`
- `QCMetric` objects (`*metric*.json`) into `quality_control.json`

**Derived data description** — Read `data_description.json` from the input data asset and create a processed derivative, with optional `--data_summary` and `--modality` overrides.

**Ancillary file copying** — Copy metadata files from the input data asset to the output directory: `procedures.json`, `subject.json`, `session.json`, `rig.json`, `instrument.json`, `acquisition.json`.

**CLI and programmatic interfaces** — Configure via Pydantic `BaseSettings` with CLI argument parsing and environment variable fallbacks.

## Installation

```sh
git clone https://github.com/AllenNeuralDynamics/aind-metadata-manager.git
cd aind-metadata-manager
pip install -e ".[dev]"
```

## Usage

### Environment Variables

Pipeline metadata can be provided via environment variables or CLI arguments. If a value is missing from both, the manager will fail with a clear error message.

| Environment Variable | CLI Argument | Description |
|---|---|---|
| `PIPELINE_VERSION` | `--pipeline_version` | Semantic version of the pipeline |
| `PIPELINE_URL` | `--pipeline_url` | URL to the pipeline repository |
| `PIPELINE_NAME` | `--pipeline_name` | Name of the pipeline |

### Command Line Interface

```sh
# With environment variables set (e.g. from a .env file):
export PIPELINE_VERSION="0.1.0"
export PIPELINE_URL="https://github.com/AllenNeuralDynamics/my-pipeline"
export PIPELINE_NAME="my-pipeline"

python -m aind_metadata_manager.metadata_manager \
  --processor_full_name "Jane Doe" \
  --input_dir /data \
  --output_dir /results \
  --verbose
```

```sh
# Or pass pipeline fields directly as CLI arguments:
python -m aind_metadata_manager.metadata_manager \
  --processor_full_name "Jane Doe" \
  --pipeline_version "0.1.0" \
  --pipeline_url "https://github.com/AllenNeuralDynamics/my-pipeline" \
  --pipeline_name "my-pipeline"
```

### As a Python Package

```python
from aind_metadata_manager.metadata_manager import MetadataManager, MetadataSettings

settings = MetadataSettings(
    input_dir="path/to/input",
    output_dir="path/to/output",
    processor_full_name="Jane Doe",
    pipeline_version="0.1.0",
    pipeline_url="https://github.com/AllenNeuralDynamics/my-pipeline",
    pipeline_name="my-pipeline",
)
manager = MetadataManager(settings)
processing = manager.create_processing_metadata()
```

### All Parameters

| Parameter | Default | Description |
|---|---|---|
| `input_dir` | `/data` | Input directory containing data_process.json files |
| `output_dir` | `/results` | Output directory for processing.json and metadata |
| `processor_full_name` | *from file* | Name of person responsible (falls back to `input_dir/processor_full_name.txt`) |
| `pipeline_version` | `$PIPELINE_VERSION` | **Required.** Semantic version of the pipeline |
| `pipeline_url` | `$PIPELINE_URL` | **Required.** URL to the pipeline code |
| `pipeline_name` | `$PIPELINE_NAME` | **Required.** Pipeline name (propagated to all data processes) |
| `data_summary` | `""` | Data summary to set in derived data description |
| `modality` | `""` | Modality abbreviation to set in derived data description |
| `skip_ancillary_files` | `False` | Skip copying ancillary files to output |
| `aggregate_quality_control` | `True` | Aggregate quality control metrics from JSON files |
| `verbose` | `False` | Enable verbose logging output |

## Writing DataProcess Objects (Upstream Capsules)

Each capsule in the pipeline should serialize a [`DataProcess`](https://github.com/AllenNeuralDynamics/aind-data-schema/blob/e40aa071721fa6a882eec4e0f7f61bd8473971ea/src/aind_data_schema/core/processing.py#L52) object to JSON so that the metadata manager can collect it at the end of the pipeline.

```python
import json
from datetime import datetime
from pathlib import Path

from aind_data_schema.core.processing import DataProcess, ProcessName

data_proc = DataProcess(
    name=ProcessName.IMAGE_PROCESSING,
    software_version=os.getenv("VERSION", ""),
    start_date_time=start_time.isoformat(),
    end_date_time=end_time.isoformat(),
    input_location=str(input_dir),
    output_location=str(output_dir),
    code_url=os.getenv("REPO_URL", ""),
    parameters={"key": "value"},
)

output_path = Path(output_dir) / "my_capsule_data_process.json"
with open(output_path, "w") as f:
    f.write(data_proc.model_dump_json(indent=4))
```

The metadata manager discovers these files by recursively searching `input_dir` for any file matching `*data_process*.json`.

## Writing QCMetric Objects (Upstream Capsules)

Capsules that produce quality control results should serialize [`QCMetric`](https://github.com/AllenNeuralDynamics/aind-data-schema/blob/e40aa071721fa6a882eec4e0f7f61bd8473971ea/src/aind_data_schema/core/quality_control.py#L43) objects to JSON. The metadata manager aggregates these into a single `quality_control.json`.

```python
from pathlib import Path

from aind_data_schema.core.quality_control import QCMetric, Status

metric = QCMetric(
    name="My QC Check",
    value=0.95,
    status=Status.PASS,
    description="Signal-to-noise ratio above threshold",
    tags=["snr", "quality"],
)

output_path = Path(output_dir) / "my_capsule_metric.json"
with open(output_path, "w") as f:
    f.write(metric.model_dump_json(indent=4))
```

The metadata manager discovers these files by recursively searching `input_dir` for any file matching `*metric*.json`. Aggregation is enabled by default (`--aggregate_quality_control=True`).

## Derived Data Description

The metadata manager reads `data_description.json` from the input data asset and creates a **derived** copy in the output directory using `DataDescription.from_raw(data_description, process_name="processed")`. This marks the output as a processed derivative of the original data.

Optional overrides can be applied via CLI arguments:
- `--data_summary` — set a custom summary on the derived description
- `--modality` — override the modality abbreviation (e.g. `pophys`, `behavior-videos`)

If no `data_description.json` is found in the input, this step is skipped with a warning.

## Development & Testing

```sh
pip install -e ".[dev]"
pytest
```

## Project Structure
- `src/aind_metadata_manager/` — Main package code
- `tests/` — Unit tests and test resources

## Requirements
- Python 3.10+
- aind-data-schema
- aind-metadata-upgrader
- pydantic, pydantic-settings

## License
This project is licensed under the terms of the MIT license. See the `LICENSE` file for details.

## Citation
If you use this package, please cite as described in `CITATION.cff`.

## Contributing
See `CONTRIBUTING.md` for guidelines.

## Contact
For questions or support, please open an issue on GitHub or contact the Allen Institute for Neural Dynamics.
