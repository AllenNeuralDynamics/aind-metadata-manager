
# AIND Metadata Manager

AIND Metadata Manager is a Python package for managing, upgrading, and validating metadata files used in the Allen Institute for Neural Dynamics (AIND) data pipelines. It provides tools to upgrade metadata schemas, process and validate metadata, and support reproducible data workflows.

## Features
- Upgrade metadata files to the latest schema versions
- Validate and process metadata for AIND data pipelines
- Utilities for handling data descriptions, procedures, and processing metadata
- Command-line and programmatic interfaces

## Installation

1. Clone the repository:
	```sh
	git clone https://github.com/AllenNeuralDynamics/aind-metadata-manager.git
	cd aind-metadata-manager
	```
2. Create and activate a virtual environment (recommended):
	```sh
	python -m venv venv
	venv\Scripts\activate  # On Windows
	# or
	source venv/bin/activate  # On macOS/Linux
	```
3. Install dependencies:
	```sh
	pip install -e .
	pip install aind-data-schema aind-metadata-upgrader
	```

## Usage

### As a Python package
```python
from aind_metadata_manager.metadata_manager import MetadataManager, MetadataSettings
settings = MetadataSettings(input_dir='path/to/input', output_dir='path/to/output')
manager = MetadataManager(settings)
manager.create_processing_metadata()
```

### Command Line Interface
A CLI may be available (see `src/aind_metadata_manager/metadata_manager.py` for details):
```sh
python -m aind_metadata_manager.metadata_manager --help
```

## Development & Testing
- Tests are located in the `tests/` directory.
- To run tests:
  ```sh
  venv\Scripts\python -m unittest discover -s tests -p "test_*.py" -v
  ```

## Project Structure
- `src/aind_metadata_manager/` — Main package code
- `tests/` — Unit tests and test resources
- `docs/` — Documentation

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
