"""Aggregate derived-asset metadata via ``Metadata.from_metadata``.

This capsule combines the metadata of one or more upstream data assets mounted
under ``input_dir`` into the metadata of a single derived asset. The heavy
lifting is delegated to ``Metadata.from_metadata`` (aind-data-schema
>= 2.9.0), which builds the derived ``data_description``,
inherits ``subject``/``procedures``/``instrument``/``acquisition`` from the
sources, and accumulates their ``processing`` and ``quality_control`` using the
schema's own ``+`` operator.

See ``DESIGN_from_metadata.md`` for the input model and the rule-4 acquisition
caveat.
"""

import json
import logging
import os
from pathlib import Path
from typing import List, Optional

from aind_data_schema.components.identifiers import Code
from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.core.data_description import DataDescription
from aind_data_schema.core.instrument import Instrument
from aind_data_schema.core.metadata import Metadata
from aind_data_schema.core.procedures import Procedures
from aind_data_schema.core.processing import DataProcess, Processing
from aind_data_schema.core.quality_control import QCMetric, QualityControl
from aind_data_schema.core.subject import Subject
from aind_data_schema_models.modalities import Modality
from pydantic import Field
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)

# Core files loaded (beside a data_description.json) into a source Metadata.
# data_description is handled separately because it identifies the asset dir.
_SOURCE_CORE_FILES = {
    "subject": ("subject.json", Subject),
    "procedures": ("procedures.json", Procedures),
    "instrument": ("instrument.json", Instrument),
    "acquisition": ("acquisition.json", Acquisition),
    "processing": ("processing.json", Processing),
    "quality_control": ("quality_control.json", QualityControl),
}


class MetadataSettings(BaseSettings, cli_parse_args=True):
    """Command line arguments for the metadata aggregation pipeline."""

    verbose: bool = Field(default=False, description="Print verbose output")

    input_dir: Path = Field(
        default=Path("/data"),
        description="Input directory containing upstream data-asset metadata",
    )
    output_dir: Path = Field(
        default=Path("/results"),
        description="Output directory for the derived asset's core files",
    )

    # Pipeline information — sourced from env vars or CLI args. Only used when
    # this capsule contributes its own new processing (standalone
    # *data_process*.json files); pipeline_url maps to the schema-required
    # Code.url in that case.
    pipeline_version: str = Field(
        default=os.getenv("PIPELINE_VERSION", ""),
        description=(
            "Version of the pipeline (tags this run's new processing). "
            "Falls back to PIPELINE_VERSION env var. Optional."
        ),
    )
    pipeline_url: str = Field(
        default=os.getenv("PIPELINE_URL", ""),
        description=(
            "URL to the pipeline code (tags this run's new processing). "
            "Falls back to PIPELINE_URL env var. Required only when "
            "standalone data_process.json files are present."
        ),
    )
    pipeline_name: str = Field(
        default=os.getenv("PIPELINE_NAME", ""),
        description=(
            "Name of the pipeline (used on this run's new data processes). "
            "Falls back to PIPELINE_NAME env var. Optional."
        ),
    )

    # Derived data_description overrides (passed through to from_metadata).
    data_summary: str = Field(
        default="",
        description="Data summary to set on the derived data description",
    )
    modality: str = Field(
        default="",
        description="Modality to set on the derived data description",
    )
    process_name: str = Field(
        default="processed",
        description="Short process/analysis name used in the derived name",
    )
    location: str = Field(
        default="",
        description=(
            "Location (e.g. S3 URI) of the derived asset. Required by the "
            "Metadata schema; downstream indexers overwrite it. Defaults to "
            "output_dir when unset."
        ),
    )


class MetadataManager:
    """Aggregates upstream data-asset metadata into a derived asset."""

    def __init__(self, settings: MetadataSettings):
        """Initialize the MetadataManager with settings."""
        self.settings = settings

    # -- discovery -----------------------------------------------------------

    def _source_asset_dirs(self) -> List[Path]:
        """Directories under input_dir that contain a data_description.json.

        Each is treated as one upstream source asset. Sorted for deterministic
        ordering.
        """
        dirs = {
            path.parent
            for path in self.settings.input_dir.rglob("data_description.json")
        }
        return sorted(dirs)

    def _load_json(self, path: Path) -> Optional[dict]:
        """Load a JSON file, logging and returning None on failure."""
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(f"Failed to load JSON from {path}: {e}")
            return None

    def _load_source_metadata(self) -> List[Metadata]:
        """Assemble one source Metadata per source-asset directory."""
        sources = [
            self._assemble_source(asset_dir)
            for asset_dir in self._source_asset_dirs()
        ]
        return [source for source in sources if source is not None]

    def _load_core_object(self, asset_dir: Path, attr: str):
        """Load and validate one optional core file from an asset dir."""
        file_name, model = _SOURCE_CORE_FILES[attr]
        file_path = asset_dir / file_name
        if not file_path.exists():
            return None
        data = self._load_json(file_path)
        if data is None:
            return None
        try:
            return model.model_validate(data)
        except Exception as e:
            logger.warning(f"Skipping {file_name} in {asset_dir}: {e}")
            return None

    def _assemble_source(self, asset_dir: Path) -> Optional[Metadata]:
        """Build a source Metadata from an asset dir's core files.

        Sub-objects are validated individually; the Metadata container is built
        with ``model_construct`` so a partial asset does not trip
        Metadata-level cross-file validation (from_metadata only reads them).
        Returns None when the data_description is missing or invalid.
        """
        dd_data = self._load_json(asset_dir / "data_description.json")
        if dd_data is None:
            return None
        try:
            data_description = DataDescription.model_validate(dd_data)
        except Exception as e:
            logger.warning(
                f"Skipping source asset {asset_dir}: invalid "
                f"data_description.json: {e}"
            )
            return None

        fields: dict = {"data_description": data_description}
        for attr in _SOURCE_CORE_FILES:
            value = self._load_core_object(asset_dir, attr)
            if value is not None:
                fields[attr] = value

        if self.settings.verbose:
            logger.info(
                f"Loaded source asset {asset_dir} "
                f"(processing={'processing' in fields}, "
                f"quality_control={'quality_control' in fields})"
            )
        return Metadata.model_construct(
            name=data_description.name or asset_dir.name,
            location=str(asset_dir),
            **fields,
        )

    def _standalone_files(self, pattern: str) -> List[Path]:
        """*pattern* files under input_dir that are NOT inside a source-asset
        directory (i.e. this run's own outputs rather than an upstream asset).
        """
        source_dirs = self._source_asset_dirs()

        def _in_source(path: Path) -> bool:
            """True when path lives inside a source-asset directory."""
            return any(path == d or d in path.parents for d in source_dirs)

        return [
            path
            for path in self.settings.input_dir.rglob(f"*{pattern}*.json")
            if not _in_source(path)
            and not path.name.endswith("quality_control.json")
        ]

    # -- this run's new work -------------------------------------------------

    def _settings_pipeline(self) -> Code:
        """Build the Code describing this pipeline from settings/env vars."""
        if not self.settings.pipeline_url:
            raise ValueError(
                "pipeline_url is required to tag this run's new processing "
                "(standalone data_process.json files were found). Provide "
                "--pipeline_url or set the PIPELINE_URL environment variable."
            )
        return Code(
            url=self.settings.pipeline_url,
            version=self.settings.pipeline_version or None,
            name=self.settings.pipeline_name or None,
        )

    def build_new_processing(self) -> Optional[Processing]:
        """Build this run's new Processing from standalone data_process.json.

        Returns None when the capsule contributes no new processing (pure
        aggregation).
        """
        data_processes: List[DataProcess] = []
        for path in self._standalone_files("data_process"):
            data = self._load_json(path)
            if data is None:
                continue
            try:
                data_processes.append(DataProcess.model_validate(data))
            except Exception as e:
                logger.warning(f"Failed to validate {path}: {e}")

        if not data_processes:
            return None

        pipeline_name = self.settings.pipeline_name
        for data_process in data_processes:
            if pipeline_name and not data_process.pipeline_name:
                data_process.pipeline_name = pipeline_name

        dependency_graph = {
            process.name: ([data_processes[i - 1].name] if i > 0 else [])
            for i, process in enumerate(data_processes)
        }
        return Processing(
            data_processes=data_processes,
            pipelines=[self._settings_pipeline()],
            dependency_graph=dependency_graph,
        )

    def build_new_quality_control(self) -> Optional[QualityControl]:
        """Build this run's new QualityControl from standalone metric.json.

        Returns None when the capsule contributes no new metrics.
        """
        metrics: List[QCMetric] = []
        for path in self._standalone_files("metric"):
            data = self._load_json(path)
            if data is None:
                continue
            try:
                metrics.append(QCMetric.model_validate(data))
            except Exception as e:
                logger.warning(f"Failed to validate metric {path}: {e}")

        if not metrics:
            return None

        tags = sorted({tag for metric in metrics for tag in metric.tags})
        return QualityControl(metrics=metrics, default_grouping=tags)

    # -- derived data_description overrides ----------------------------------

    def _validate_modality(self, modality_str: str) -> List[Modality]:
        """Validate and return a modality list from an abbreviation."""
        for modality_class in Modality.ALL:
            if modality_str in modality_class().abbreviation:
                return [modality_class()]
        raise ValueError(
            f"Modality '{modality_str}' is not a valid modality. "
            f"Valid modalities are: {Modality.ONE_OF}"
        )

    def _data_description_overrides(self) -> dict:
        """Overrides forwarded to from_metadata's derive_data_description."""
        overrides: dict = {}
        if self.settings.data_summary:
            overrides["data_summary"] = self.settings.data_summary
        if self.settings.modality:
            overrides["modalities"] = self._validate_modality(
                self.settings.modality
            )
        return overrides

    # -- aggregation ---------------------------------------------------------

    def build_derived_metadata(self) -> Metadata:
        """Combine all source assets into the derived asset's Metadata.

        Delegates to ``Metadata.from_metadata``: derived data_description,
        subject/instrument inheritance, and processing/QC accumulation (via the
        schema's ``+``) all happen there.
        """
        sources = self._load_source_metadata()
        if not sources:
            raise ValueError(
                "No source assets found. Expected at least one directory "
                f"under {self.settings.input_dir} containing a "
                "data_description.json."
            )

        new_processing = self.build_new_processing()
        new_quality_control = self.build_new_quality_control()

        location = self.settings.location or str(self.settings.output_dir)
        derived = Metadata.from_metadata(
            sources,
            process_name=self.settings.process_name,
            location=location,
            new_processing=new_processing,
            new_quality_control=new_quality_control,
            **self._data_description_overrides(),
        )

        if self.settings.verbose:
            n_proc = (
                len(derived.processing.data_processes)
                if derived.processing
                else 0
            )
            n_metrics = (
                len(derived.quality_control.metrics)
                if derived.quality_control
                else 0
            )
            logger.info(
                f"Built derived metadata from {len(sources)} source asset(s): "
                f"{n_proc} data processes, {n_metrics} metrics"
            )
        return derived


def run() -> None:
    """Aggregate upstream metadata into the derived asset's core files."""
    settings = MetadataSettings()

    log_level = logging.INFO if settings.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if settings.verbose:
        logger.info("=== Metadata Aggregation Pipeline ===")
        logger.info(f"Input directory: {settings.input_dir}")
        logger.info(f"Output directory: {settings.output_dir}")

    manager = MetadataManager(settings)
    derived = manager.build_derived_metadata()
    derived.write_standard_files(output_directory=settings.output_dir)

    if settings.verbose:
        logger.info(f"✓ Wrote derived core files to {settings.output_dir}")
