"""Metadata management script for processing pipeline aggregation"""

import json
import logging
import os
import shutil
from pathlib import Path
from typing import List

from aind_data_schema.components.identifiers import Code
from aind_data_schema.core.data_description import DataDescription
from aind_data_schema.core.processing import (
    DataProcess,
    Processing,
)
from aind_data_schema.core.quality_control import QCMetric, QualityControl
from aind_data_schema_models.modalities import Modality
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings

# Set up logging
logger = logging.getLogger(__name__)


class MetadataSettings(BaseSettings, cli_parse_args=True):
    """Command line arguments for the metadata management pipeline"""

    verbose: bool = Field(default=False, description="Print verbose output")

    # Input/Output directories - automatically converted to Path objects by
    # BaseSettings
    input_dir: Path = Field(
        default=Path("/data"),
        description="Input directory containing data_process.json files",
    )
    output_dir: Path = Field(
        default=Path("/results"),
        description="Output directory for processing.json and metadata",
    )

    # Required fields
    processor_full_name: str = Field(
        description=(
            "Name of person responsible for processing pipeline "
            "(defaults to input_dir/processor_full_name.txt)"
        )
    )

    # Pipeline information — sourced from env vars or CLI args
    pipeline_version: str = Field(
        default=os.getenv("PIPELINE_VERSION"),
        description=(
            "Version of the pipeline. "
            "Falls back to PIPELINE_VERSION env var. "
            "Required — fails if neither is provided."
        ),
    )

    pipeline_url: str = Field(
        default=os.getenv("PIPELINE_URL"),
        description=(
            "URL to the pipeline code. "
            "Falls back to PIPELINE_URL env var. "
            "Required — fails if neither is provided."
        ),
    )

    pipeline_name: str = Field(
        default=os.getenv("PIPELINE_NAME"),
        description=(
            "Name of the pipeline (used on all data processes). "
            "Falls back to PIPELINE_NAME env var. "
            "Required — fails if neither is provided."
        ),
    )

    @field_validator(
        "pipeline_version",
        "pipeline_url",
        "pipeline_name",
        mode="before",
    )
    @classmethod
    def validate_pipeline_fields(cls, v, info):
        """Ensure pipeline fields are provided via CLI arg or env var."""
        if v is None or (isinstance(v, str) and not v.strip()):
            env_var_map = {
                "pipeline_version": "PIPELINE_VERSION",
                "pipeline_url": "PIPELINE_URL",
                "pipeline_name": "PIPELINE_NAME",
            }
            env_var = env_var_map.get(info.field_name, info.field_name.upper())
            raise ValueError(
                f"{info.field_name} is required. "
                f"Provide it via --{info.field_name} "
                f"or set the {env_var} environment variable."
            )
        return v

    # Data description fields
    data_summary: str = Field(
        default="",
        description=("Data summary to overwrite in the \
            derived data description"),
    )
    modality: str = Field(
        default="",
        description="Modality to overwrite in the derived data description",
    )

    # File management - copy ancillary files by default, with opt-out
    skip_ancillary_files: bool = Field(
        default=False,
        description=("Skip copying ancillary files \
            (procedures.json, subject.json, session.json, rig.json, \
            instrument.json, and acquisition.json)"),
    )
    # Quality control options
    aggregate_quality_control: bool = Field(
        default=True,
        description="Aggregate quality control evaluations from JSON files",
    )

    @field_validator("processor_full_name", mode="before")
    @classmethod
    def validate_processor_name(cls, v, info):
        """Validate processor_full_name is provided or can be read from file"""
        if not v:
            # Try to get input_dir from the validation info context
            input_dir_raw = (
                info.data.get("input_dir", "/data") if info.data else "/data"
            )
            # Ensure input_dir is a Path object
            input_dir = (
                Path(input_dir_raw)
                if not isinstance(input_dir_raw, Path)
                else input_dir_raw
            )
            try:
                processor_file = input_dir / "processor_full_name.txt"
                if processor_file.exists():
                    return processor_file.read_text().strip()
                else:
                    raise ValueError(
                        f"processor_full_name not provided via args and "
                        f"not found in {processor_file}"
                    )
            except Exception:
                raise ValueError(
                    f"processor_full_name is required. Provide it via "
                    f"--processor_full_name or create {input_dir}/ "
                    "processor_full_name.txt"
                )
        return v


class MetadataManager:
    """Manages processing metadata aggregation and file operations"""

    def __init__(self, settings: MetadataSettings):
        """Initialize the MetadataManager with settings."""
        self.settings = settings
        self.ancillary_files = [
            "procedures.json",
            "subject.json",
            "session.json",
            "rig.json",
            "instrument.json",
            "acquisition.json",
        ]

    def _find_matching_file(self, file_name: str) -> Path | None:
        """Recursively search for a file in the input directory."""
        matches = list(self.settings.input_dir.rglob(file_name))
        return matches[0] if matches else None

    def _copy_file(
        self, source_path: Path, dest_path: Path, file_name: str
    ) -> None:
        """Copy a file and log the operation if verbose."""
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source_path, dest_path)
        if self.settings.verbose:
            logger.info(
                f"Copied {file_name} from {source_path} to {dest_path}"
            )

    def _handle_missing_file(self, file_name: str) -> None:
        """Log missing file if verbose."""
        if self.settings.verbose:
            logger.warning("(searched recursively)")

    def _find_data_description_file(self) -> Path | None:
        """Find data_description.json in input_dir recursively, with
        logging.
        """
        input_path = self.settings.input_dir
        matching_files = list(input_path.rglob("data_description.json"))
        if not matching_files:
            message = f"data_description.json not found in {input_path}"
            if self.settings.verbose:
                logger.warning(message)
                logger.info("Skipping derived data description creation")
                logger.info(
                    "To create a derived data description, ensure "
                    "data_description.json exists somewhere in the input "
                    "directory"
                )
            else:
                logger.info(
                    f"{message} - skipping derived data description creation"
                )
            return None
        if self.settings.verbose:
            logger.info(f"Found data_description.json at {matching_files[0]}")
            if len(matching_files) > 1:
                logger.info(
                    "Multiple data_description.json files found, "
                    f"using: {matching_files[0]}"
                )
        return matching_files[0]

    def _apply_overrides(self, data_description: DataDescription):
        """Apply data_summary and modality overrides, with logging."""
        if self.settings.data_summary:
            data_description.data_summary = self.settings.data_summary
            if self.settings.verbose:
                logger.info(f"Set data_summary: {self.settings.data_summary}")
        if self.settings.modality:
            validated_modalities = self._validate_modality(
                self.settings.modality
            )
            data_description.modalities = validated_modalities
            if self.settings.verbose:
                logger.info(
                    "Set modality: "
                    f"{[m.abbreviation for m in validated_modalities]}"
                )

    def _write_derived_data_description(
        self, data_description: DataDescription
    ):
        """Create and write the derived data description, with logging."""
        derived_data_description = DataDescription.from_data_description(
            data_description, process_name="processed"
        )
        output_dir_str = str(self.settings.output_dir)
        derived_data_description.write_standard_file(
            output_directory=output_dir_str
        )
        if self.settings.verbose:
            logger.info(
                "✓ Created derived data description at "
                f"{output_dir_str}/data_description.json"
            )

    def copy_ancillary_files(self) -> None:
        """
        Copy ancillary files from input_dir to output_dir using
        recursive search

        Raises
        ------
        FileNotFoundError
            If any required ancillary file is not found
        """
        if self.settings.skip_ancillary_files:
            if self.settings.verbose:
                logger.info(
                    "Skipping ancillary files copy "
                    "(--skip_ancillary_files=True)"
                )
            return

        copied_files = []
        missing_files = []
        output_path = self.settings.output_dir

        for file_name in self.ancillary_files:
            source_path = self._find_matching_file(file_name)
            dest_path = output_path / file_name
            if source_path:
                try:
                    self._copy_file(source_path, dest_path, file_name)
                    copied_files.append(file_name)
                except Exception as e:
                    raise FileNotFoundError(
                        f"Error copying {file_name} from {source_path} to "
                        f"{dest_path}: {e}"
                    )
            else:
                missing_files.append(file_name)
                self._handle_missing_file(file_name)

        if self.settings.verbose:
            logger.info(
                f"Successfully copied {len(copied_files)} ancillary files"
            )
            if missing_files:
                logger.info(f"Missing files: {missing_files}")
            if copied_files:
                logger.info("Copied files placed in output directory root")

    def create_derived_data_description(self) -> None:
        """
        Create a derived data description with optional modality override

        Raises
        ------
        FileNotFoundError
            If data_description.json is not found
        ValueError
            If specified modality is invalid
        """
        data_description_fp = self._find_data_description_file()
        if not data_description_fp:
            return

        with open(data_description_fp, "r") as f:
            data_description_dict = json.load(f)
        data_description = DataDescription(**data_description_dict)

        try:
            self._apply_overrides(data_description)
            self._write_derived_data_description(data_description)
        except Exception as e:
            logger.error(f"Error creating derived data description: {e}")
            if self.settings.verbose:
                import traceback

                logger.error(traceback.format_exc())
            raise

    def _validate_modality(self, modality_str: str) -> List[Modality]:
        """
        Validate and return modality objects

        Parameters
        ----------
        modality_str : str
            Modality abbreviation to validate

        Returns
        -------
        List[Modality]
            List of validated modality objects

        Raises
        ------
        ValueError
            If modality is not valid
        """
        validated_modalities = []
        modality_found = False

        for modality_class in Modality.ALL:
            if modality_str in modality_class().abbreviation:
                validated_modalities.append(modality_class())
                modality_found = True
                break

        if not modality_found:
            raise ValueError(
                f"Modality '{modality_str}' is not a valid modality. "
                f"Valid modalities are: {Modality.ONE_OF}"
            )

        return validated_modalities

    def collect_data_processes(self) -> List[DataProcess]:
        """
        Collect DataProcess objects from standalone *data_process*.json files.

        Returns
        -------
        List[DataProcess]
            List of DataProcess objects found in input directory
        """
        data_process_jsons = [
            data for _, data in self._iter_json_files("data_process")
        ]

        data_processes = []
        for json_data in data_process_jsons:
            try:
                data_process = DataProcess.model_validate(json_data)
                data_processes.append(data_process)
                if self.settings.verbose:
                    logger.info(
                        "Added data process: "
                        f"{data_process.name if hasattr(data_process, 'name') else 'unnamed'}"  # noqa: E501
                    )
            except Exception as e:
                logger.warning(f"Failed to validate data_process JSON: {e}")

        return data_processes

    def _propagate_pipeline_name(
        self, data_processes: List[DataProcess]
    ) -> None:
        """
        Set pipeline_name on all data processes from settings,
        warning if overriding an existing value.

        Parameters
        ----------
        data_processes : List[DataProcess]
            Data processes to update in place.
        """
        pipeline_name = self.settings.pipeline_name
        for data_process in data_processes:
            if (
                data_process.pipeline_name
                and data_process.pipeline_name != pipeline_name
            ):
                logger.warning(
                    f"Overriding pipeline_name "
                    f"'{data_process.pipeline_name}'"
                    f" with '{pipeline_name}' "
                    f"for process "
                    f"'{data_process.name}'"
                )
            data_process.pipeline_name = pipeline_name

    def collect_existing_processings(self) -> List[Processing]:
        """Collect Processing objects from pre-existing *processing.json
        files passed in by upstream sources, plus any prior processing.json
        already present in output_dir (so a re-run merges rather than
        clobbers).
        """
        processing_files = list(
            self.settings.input_dir.rglob("*processing.json")
        )
        prior_output = self.settings.output_dir / "processing.json"
        if prior_output.exists():
            processing_files.append(prior_output)

        seen: set = set()
        processings: List[Processing] = []
        for file_path in processing_files:
            try:
                key = file_path.resolve()
            except OSError:
                key = file_path
            if key in seen:
                continue
            seen.add(key)
            try:
                with open(file_path, "r") as f:
                    json_data = json.load(f)
                processings.append(Processing.model_validate(json_data))
                if self.settings.verbose:
                    logger.info(
                        f"Loaded existing processing.json: {file_path}"
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to load processing.json at {file_path}: {e}"
                )

        return processings

    def create_processing_metadata(self) -> Processing:
        """
        Create Processing object with collected data processes.

        Aggregates standalone *data_process*.json files and any pre-existing
        *processing.json files. Data processes, pipelines, and dependency
        graphs from existing processing.json files are preserved; standalone
        data processes are chained linearly in discovery order and have their
        pipeline_name enforced from settings. Raises ValueError on duplicate
        DataProcess names across sources.
        """
        standalone_processes = self.collect_data_processes()
        self._propagate_pipeline_name(standalone_processes)
        existing_processings = self.collect_existing_processings()

        data_processes: List[DataProcess] = []
        dependency_graph: dict = {}
        seen_names: set = set()

        pipelines: List[Code] = []
        seen_pipelines: set = set()

        def _register(process: DataProcess, deps: List[str]) -> None:
            """Register a DataProcess, guarding against duplicate names."""
            name = process.name
            if name in seen_names:
                raise ValueError(
                    f"Duplicate DataProcess name '{name}' found while "
                    "merging processing metadata"
                )
            seen_names.add(name)
            data_processes.append(process)
            dependency_graph[name] = deps

        def _register_pipeline(code: Code) -> None:
            """Register a pipeline Code, de-duplicating by name."""
            if code.name not in seen_pipelines:
                seen_pipelines.add(code.name)
                pipelines.append(code)

        for processing in existing_processings:
            existing_graph = processing.dependency_graph or {}
            for process in processing.data_processes:
                _register(process, list(existing_graph.get(process.name, [])))
            for code in processing.pipelines or []:
                _register_pipeline(code)

        for i, process in enumerate(standalone_processes):
            deps = [standalone_processes[i - 1].name] if i > 0 else []
            _register(process, deps)

        _register_pipeline(
            Code(
                url=self.settings.pipeline_url,
                version=self.settings.pipeline_version,
                name=self.settings.pipeline_name,
            )
        )

        processing = Processing(
            data_processes=data_processes,
            pipelines=pipelines,
            dependency_graph=dependency_graph,
        )

        if self.settings.verbose:
            logger.info(
                f"Created processing metadata with {len(data_processes)} "
                f"data processes and {len(pipelines)} pipelines"
            )
            logger.info(f"Pipeline version: {self.settings.pipeline_version}")
            logger.info(f"Processor: {self.settings.processor_full_name}")

        return processing

    def collect_json_objects(self, pattern: str) -> List:
        """
        Generic function to collect and parse JSON objects from files
        matching a pattern

        Parameters
        ----------
        pattern : str
            Pattern to search for in filenames
            (e.g., "data_process", "evaluation")

        Returns
        -------
        List
            List of parsed JSON objects from matching files
        """
        json_files = list(self.settings.input_dir.rglob(f"*{pattern}*.json"))

        if self.settings.verbose:
            logger.info(
                f"Found {len(json_files)} files matching pattern "
                f"'*{pattern}*.json'"
            )

        json_objects = []
        for file_path in json_files:
            if self.settings.verbose:
                logger.info(f"Processing: {file_path}")

            try:
                with open(file_path, "r") as f:
                    json_data = json.load(f)
                    json_objects.append(json_data)
            except Exception as e:
                logger.warning(f"Failed to load JSON from {file_path}: {e}")

        return json_objects

    def _collect_standalone_metrics(self) -> List[QCMetric]:
        """Validate QCMetrics from standalone *metric*.json files."""
        metrics: List[QCMetric] = []
        for path, json_data in self._iter_json_files("metric"):
            if path.name.endswith("quality_control.json"):
                continue
            try:
                metric = QCMetric.model_validate(json_data)
                metrics.append(metric)
                if self.settings.verbose:
                    logger.info(
                        f"Added metric from {path}: "
                        f"{metric.name if hasattr(metric, 'name') else 'unnamed'}"  # noqa: E501
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to validate metric JSON at {path}: {e}"
                )
        return metrics

    def _iter_qc_sources(self):
        """Yield (path, json_data) for every quality_control.json source,
        deduplicating across input_dir and a prior output_dir copy.
        """
        sources = list(self._iter_json_files("quality_control"))
        prior_output_qc = self.settings.output_dir / "quality_control.json"
        if prior_output_qc.exists():
            try:
                with open(prior_output_qc, "r") as f:
                    sources.append((prior_output_qc, json.load(f)))
            except Exception as e:
                logger.warning(
                    f"Failed to load prior quality_control.json at "
                    f"{prior_output_qc}: {e}"
                )

        seen: set = set()
        for path, json_data in sources:
            try:
                key = path.resolve()
            except OSError:
                key = path
            if key in seen:
                continue
            seen.add(key)
            yield path, json_data

    def collect_metrics(self) -> List[QCMetric]:
        """Collect QCMetric objects from standalone *metric*.json files,
        any pre-existing *quality_control.json files under input_dir, and
        a prior quality_control.json in output_dir (so a re-run merges
        rather than clobbers).
        """
        metrics = self._collect_standalone_metrics()

        for path, json_data in self._iter_qc_sources():
            try:
                qc = QualityControl.model_validate(json_data)
                metrics.extend(qc.metrics)
                if self.settings.verbose:
                    logger.info(
                        f"Loaded {len(qc.metrics)} metrics from "
                        f"existing quality_control.json: {path}"
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to load quality_control.json at {path}: {e}"
                )

        return metrics

    def _load_existing_quality_control(self) -> QualityControl | None:
        """
        Load an existing quality_control.json from the input directory.

        Returns
        -------
        QualityControl or None
            Existing QualityControl object if found, None otherwise.
        """
        qc_path = self._find_matching_file("quality_control.json")
        if qc_path is None:
            if self.settings.verbose:
                logger.info(
                    "No existing quality_control.json found in input — "
                    "creating from scratch"
                )
            return None

        try:
            with open(qc_path, "r") as f:
                qc_data = json.load(f)
            existing = QualityControl.model_validate(qc_data)
            if self.settings.verbose:
                logger.info(
                    f"Loaded existing quality_control.json from "
                    f"{qc_path} with "
                    f"{len(existing.metrics)} metrics"
                )
            return existing
        except Exception as e:
            logger.warning(
                f"Failed to load existing quality_control.json from "
                f"{qc_path}: {e}"
            )
            return None

    def _merge_qc_fields(
        self,
        existing: QualityControl,
    ) -> dict:
        """
        Extract preserved fields from an existing QualityControl object.

        Parameters
        ----------
        existing : QualityControl
            The existing QualityControl to extract fields from.

        Returns
        -------
        dict
            Keyword arguments to pass to the QualityControl constructor.
        """
        kwargs = {}
        if existing.key_experimenters:
            kwargs["key_experimenters"] = existing.key_experimenters
        if existing.allow_tag_failures:
            kwargs["allow_tag_failures"] = existing.allow_tag_failures
        if existing.notes:
            kwargs["notes"] = existing.notes

        # Merge default_grouping tags
        kwargs["extra_grouping"] = set(
            tuple(g) if isinstance(g, (list, tuple)) else g
            for g in existing.default_grouping
        )
        return kwargs

    def _iter_json_files(self, pattern: str):
        """Yield (path, parsed_json) for each *pattern*.json in input_dir."""
        for file_path in self.settings.input_dir.rglob(f"*{pattern}*.json"):
            try:
                with open(file_path, "r") as f:
                    yield file_path, json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load JSON from {file_path}: {e}")

    def create_quality_control_metadata(self) -> QualityControl:
        """
        Create QualityControl object with collected metrics.

        If an existing quality_control.json is found in the input
        directory, its metrics and configuration are preserved and the
        new metrics are appended.

        Returns
        -------
        QualityControl
            QualityControl object containing all metrics and notes

        Raises
        ------
        ValueError
            If no metrics are found (both existing and new)
        """
        # collect_metrics already folds in metrics from any existing
        # quality_control.json, so it must not be added again here.
        all_metrics = self.collect_metrics()

        # Preserve configuration fields (notes, grouping, experimenters)
        # from an existing quality_control.json, if present.
        existing = self._load_existing_quality_control()
        existing_kwargs = {}
        extra_grouping = set()
        if existing:
            merged = self._merge_qc_fields(existing)
            extra_grouping = merged.pop("extra_grouping", set())
            existing_kwargs = merged

        if not all_metrics:
            raise ValueError(
                "No metrics found. If quality control aggregation "
                "is enabled, metric files must exist in the input "
                "directory."
            )

        # Collect tags from all metrics (existing + new)
        tags = set()
        for metric in all_metrics:
            for tag in metric.tags:
                tags.add(tag)
        tags = tags.union(extra_grouping)

        quality_control = QualityControl(
            metrics=all_metrics,
            default_grouping=list(tags),
            **existing_kwargs,
        )

        if self.settings.verbose:
            logger.info(
                f"Created quality control metadata with "
                f"{len(all_metrics)} total metrics"
            )

        return quality_control


def run() -> None:
    """
    Main function to aggregate processing metadata and manage ancillary files.

    This function:
    1. Collects all DataProcess objects from input directory
    2. Creates a Processing object with pipeline metadata
    3. Copies ancillary files by default (unless skipped)
    4. Creates derived data description with optional modality override
    5. Creates quality control metadata from evaluation
       JSON files (unless skipped)
    """
    settings = MetadataSettings()

    # Configure logging based on verbose setting
    log_level = logging.INFO if settings.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    manager = MetadataManager(settings)

    if settings.verbose:
        logger.info("=== Metadata Management Pipeline ===")
        logger.info(f"Input directory: {settings.input_dir}")
        logger.info(f"Output directory: {settings.output_dir}")
        logger.info(f"Processor: {settings.processor_full_name}")
        logger.info(f"Pipeline version: {settings.pipeline_version}")

    # Create main processing metadata
    processing = manager.create_processing_metadata()
    # Ensure output_dir is a string for the API call
    processing.write_standard_file(str(settings.output_dir))

    manager.create_derived_data_description()

    if settings.aggregate_quality_control:
        quality_control = manager.create_quality_control_metadata()
        quality_control.write_standard_file(str(settings.output_dir))
        if settings.verbose:
            logger.info("✓ Written quality_control.json")

    if settings.verbose:
        logger.info("✓ Written processing.json")
    # Copy ancillary files (by default, unless skipped)
    if settings.skip_ancillary_files:
        pass
    else:
        manager.copy_ancillary_files()
