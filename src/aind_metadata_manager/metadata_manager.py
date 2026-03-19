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
            env_var = env_var_map.get(
                info.field_name, info.field_name.upper()
            )
            raise ValueError(
                f"{info.field_name} is required. "
                f"Provide it via --{info.field_name} "
                f"or set the {env_var} environment variable."
            )
        return v
    # Data description fields
    data_summary: str = Field(
        default="",
        description=(
            "Data summary to overwrite in the \
            derived data description"
        ),
    )
    modality: str = Field(
        default="",
        description="Modality to overwrite in the derived data description",
    )

    # File management - copy ancillary files by default, with opt-out
    skip_ancillary_files: bool = Field(
        default=False,
        description=(
            "Skip copying ancillary files \
            (procedures.json, subject.json, session.json, rig.json, \
            instrument.json, and acquisition.json)"
        ),
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
        derived_data_description = DataDescription.from_raw(
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
        Collect all DataProcess objects from data_process.json files

        Returns
        -------
        List[DataProcess]
            List of DataProcess objects found in input directory
        """
        data_process_jsons = self.collect_json_objects("data_process")

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

    def _load_existing_processing(self) -> Processing | None:
        """
        Load an existing processing.json from the input directory.

        Returns
        -------
        Processing or None
            Existing Processing object if found, None otherwise.
        """
        processing_path = self._find_matching_file("processing.json")
        if processing_path is None:
            if self.settings.verbose:
                logger.info(
                    "No existing processing.json found in input — "
                    "creating from scratch"
                )
            return None

        try:
            with open(processing_path, "r") as f:
                processing_data = json.load(f)
            existing = Processing.model_validate(processing_data)
            if self.settings.verbose:
                logger.info(
                    f"Loaded existing processing.json from "
                    f"{processing_path} with "
                    f"{len(existing.data_processes)} data processes"
                )
            return existing
        except Exception as e:
            logger.warning(
                f"Failed to load existing processing.json from "
                f"{processing_path}: {e}"
            )
            return None

    def create_processing_metadata(self) -> Processing:
        """
        Create Processing object with collected data processes.

        If an existing processing.json is found in the input directory,
        its data processes, pipelines, and dependency graph are preserved
        and the new data processes are appended.

        Returns
        -------
        Processing
            Processing object containing all data processes and pipeline info
        """
        existing = self._load_existing_processing()
        new_data_processes = self.collect_data_processes()
        self._propagate_pipeline_name(new_data_processes)

        # Merge with existing processing if present
        if existing:
            all_data_processes = (
                existing.data_processes + new_data_processes
            )
            existing_pipelines = existing.pipelines or []
            existing_dep_graph = existing.dependency_graph or {}
        else:
            all_data_processes = new_data_processes
            existing_pipelines = []
            existing_dep_graph = {}

        # Build dependency graph for new processes
        new_dep_graph = {}
        if new_data_processes:
            # First new process depends on itself (pipeline entry point)
            new_dep_graph[new_data_processes[0].name] = [
                new_data_processes[0].name
            ]
            for i in range(1, len(new_data_processes)):
                new_dep_graph[new_data_processes[i].name] = [
                    new_data_processes[i - 1].name
                ]

        # Merge dependency graphs
        merged_dep_graph = {**existing_dep_graph, **new_dep_graph}

        # Merge pipelines
        current_pipeline = Code(
            url=self.settings.pipeline_url,
            version=self.settings.pipeline_version,
            name=self.settings.pipeline_name,
        )
        merged_pipelines = existing_pipelines + [current_pipeline]

        processing = Processing(
            data_processes=all_data_processes,
            pipelines=merged_pipelines,
            dependency_graph=merged_dep_graph,
        )

        if self.settings.verbose:
            logger.info(
                f"Created processing metadata with "
                f"{len(all_data_processes)} total data processes "
                f"({len(new_data_processes)} new"
                f"{f', {len(existing.data_processes)} existing' if existing else ''})"  # noqa: E501
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

    def collect_metrics(self) -> List[QCMetric]:
        """
        Collect all QCMetrics objects from evaluation JSON files

        Returns
        -------
        List[QCMetric]
            List of QCMetric objects found in input directory
        """
        metric_jsons = self.collect_json_objects("metric")

        metrics = []
        for json_data in metric_jsons:
            try:
                metric = QCMetric.model_validate(json_data)
                metrics.append(metric)
                if self.settings.verbose:
                    logger.info(
                        f"Added evaluation: {metric.name if hasattr(metric, 'name') else 'unnamed'}"  # noqa: E501
                    )
            except Exception as e:
                logger.warning(f"Failed to validate evaluation JSON: {e}")

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
        existing = self._load_existing_quality_control()
        new_metrics = self.collect_metrics()

        # Merge with existing QC if present
        if existing:
            all_metrics = list(existing.metrics) + new_metrics
        else:
            all_metrics = new_metrics

        if not all_metrics:
            raise ValueError(
                "No metrics found. If quality control aggregation is enabled, "
                "metric files must exist in the input directory."
            )

        # Collect tags from all metrics (existing + new)
        tags = set()
        for metric in all_metrics:
            for tag in metric.tags:
                tags.add(tag)

        # Merge default_grouping
        if existing:
            existing_grouping = set(
                tuple(g) if isinstance(g, (list, tuple)) else g
                for g in existing.default_grouping
            )
            tags = tags.union(existing_grouping)

        # Preserve existing fields
        existing_kwargs = {}
        if existing:
            if existing.key_experimenters:
                existing_kwargs["key_experimenters"] = (
                    existing.key_experimenters
                )
            if existing.allow_tag_failures:
                existing_kwargs["allow_tag_failures"] = (
                    existing.allow_tag_failures
                )
            if existing.notes:
                existing_kwargs["notes"] = existing.notes

        quality_control = QualityControl(
            metrics=all_metrics,
            default_grouping=list(tags),
            **existing_kwargs,
        )

        if self.settings.verbose:
            logger.info(
                f"Created quality control metadata with "
                f"{len(all_metrics)} total metrics "
                f"({len(new_metrics)} new"
                f"{f', {len(existing.metrics)} existing' if existing else ''})"
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
