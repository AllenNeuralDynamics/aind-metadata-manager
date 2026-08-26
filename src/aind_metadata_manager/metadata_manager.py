"""Metadata management script for processing pipeline aggregation"""

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

_SOURCE_CORE_FILES = {
    "subject": ("subject.json", Subject),
    "procedures": ("procedures.json", Procedures),
    "instrument": ("instrument.json", Instrument),
    "acquisition": ("acquisition.json", Acquisition),
    "processing": ("processing.json", Processing),
    "quality_control": ("quality_control.json", QualityControl),
}


class MetadataSettings(BaseSettings, cli_parse_args=True):
    """Command line arguments for the metadata management pipeline"""

    verbose: bool = Field(default=False, description="Print verbose output")
    input_dir: Path = Field(
        default=Path("/data"),
        description="Directory of upstream data-asset metadata",
    )
    output_dir: Path = Field(
        default=Path("/results"),
        description="Output directory for processing.json and metadata",
    )
    pipeline_version: str = Field(
        default=os.getenv("PIPELINE_VERSION", ""),
        description=(
            "Version of the pipeline. "
            "Falls back to PIPELINE_VERSION env var. Optional."
        ),
    )
    pipeline_url: str = Field(
        default=os.getenv("PIPELINE_URL", ""),
        description=(
            "URL to the pipeline code. "
            "Falls back to PIPELINE_URL env var. "
            "Required only when this run contributes new processing."
        ),
    )
    pipeline_name: str = Field(
        default=os.getenv("PIPELINE_NAME", ""),
        description=(
            "Name of the pipeline (used on all data processes). "
            "Falls back to PIPELINE_NAME env var. Optional."
        ),
    )
    data_summary: str = Field(
        default="",
        description=(
            "Data summary to overwrite in the derived data description"
        ),
    )
    modality: str = Field(
        default="",
        description="Modality to overwrite in the derived data description",
    )
    process_name: str = Field(
        default="processed",
        description=(
            "Process name to use when creating the derived data description"
        ),
    )
    location: str = Field(
        default="",
        description="Derived asset location; defaults to output_dir",
    )


class MetadataManager:
    """Manages processing metadata aggregation and file operations"""

    def __init__(self, settings: MetadataSettings):
        """Initialize the MetadataManager with settings."""
        self.settings = settings

    def _source_asset_dirs(self) -> List[Path]:
        """Return sorted dirs under input_dir holding a data_description.json.

        Returns
        -------
        List[Path]
            One directory per upstream source asset.
        """
        dirs = {
            path.parent
            for path in self.settings.input_dir.rglob("data_description.json")
        }
        return sorted(dirs)

    def _load_json(self, path: Path) -> Optional[dict]:
        """Load a JSON file.

        Parameters
        ----------
        path : Path
            File to read.

        Returns
        -------
        Optional[dict]
            Parsed JSON, or None on failure.
        """
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(f"Failed to load JSON from {path}: {e}")
            return None

    def _load_source_metadata(self) -> List[Metadata]:
        """Assemble one source Metadata per source-asset directory.

        Returns
        -------
        List[Metadata]
            Valid source assets (invalid ones dropped).
        """
        sources = [
            self._assemble_source(asset_dir)
            for asset_dir in self._source_asset_dirs()
        ]
        return [source for source in sources if source is not None]

    def _load_core_object(self, asset_dir: Path, attr: str):
        """Load and validate one optional core file.

        Parameters
        ----------
        asset_dir : Path
            Source-asset directory.
        attr : str
            Key into _SOURCE_CORE_FILES.

        Returns
        -------
        Optional[BaseModel]
            Validated core object, or None if absent/invalid.
        """
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
        """Build a source Metadata from one asset directory.

        Sub-objects are validated; the container uses model_construct so a
        partial asset does not trip Metadata cross-file validation.

        Parameters
        ----------
        asset_dir : Path
            Source-asset directory (must hold a data_description.json).

        Returns
        -------
        Optional[Metadata]
            Source Metadata, or None if the data_description is invalid.
        """
        dd_data = self._load_json(asset_dir / "data_description.json")
        if dd_data is None:
            return None
        try:
            data_description = DataDescription.model_validate(dd_data)
        except Exception as e:
            logger.warning(f"Skipping {asset_dir}: bad data_description: {e}")
            return None

        fields: dict = {"data_description": data_description}
        for attr in _SOURCE_CORE_FILES:
            value = self._load_core_object(asset_dir, attr)
            if value is not None:
                fields[attr] = value

        if self.settings.verbose:
            logger.info(f"Loaded source asset {asset_dir}: {sorted(fields)}")
        return Metadata.model_construct(
            name=data_description.name or asset_dir.name,
            location=str(asset_dir),
            **fields,
        )

    def _standalone_files(self, pattern: str) -> List[Path]:
        """Return *pattern* files outside any source-asset dir.

        Parameters
        ----------
        pattern : str
            Filename substring, e.g. "data_process" or "metric".

        Returns
        -------
        List[Path]
            This run's own output files (not upstream-asset files).
        """
        source_dirs = self._source_asset_dirs()

        def _in_source(path: Path) -> bool:
            """Return True when path is inside a source-asset dir."""
            return any(path == d or d in path.parents for d in source_dirs)

        return [
            path
            for path in self.settings.input_dir.rglob(f"*{pattern}*.json")
            if not _in_source(path)
            and not path.name.endswith("quality_control.json")
        ]

    def _settings_pipeline(self) -> Code:
        """Build the Code describing this pipeline from settings/env vars."""
        if not self.settings.pipeline_url:
            raise ValueError(
                "pipeline_url is required to tag new processing; set "
                "--pipeline_url or the PIPELINE_URL environment variable."
            )
        return Code(
            url=self.settings.pipeline_url,
            version=self.settings.pipeline_version or None,
            name=self.settings.pipeline_name or None,
        )

    def build_new_processing(self) -> Optional[Processing]:
        """Build this run's Processing from standalone data_process.json.

        Returns
        -------
        Optional[Processing]
            New processing, or None for pure aggregation.
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
        """Build this run's QualityControl from standalone metric.json.

        Returns
        -------
        Optional[QualityControl]
            New QC, or None when no standalone metrics exist.
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
        for modality_class in Modality.ALL:
            if modality_str in modality_class().abbreviation:
                return [modality_class()]
        raise ValueError(
            f"Modality '{modality_str}' is not a valid modality. "
            f"Valid modalities are: {Modality.ONE_OF}"
        )

    def _data_description_overrides(self) -> dict:
        """Return DataDescription overrides forwarded to from_metadata.

        Returns
        -------
        dict
            data_summary and/or modalities when configured.
        """
        overrides: dict = {}
        if self.settings.data_summary:
            overrides["data_summary"] = self.settings.data_summary
        if self.settings.modality:
            overrides["modalities"] = self._validate_modality(
                self.settings.modality
            )
        return overrides

    def build_derived_metadata(self) -> Metadata:
        """Combine source assets via Metadata.from_metadata.

        Returns
        -------
        Metadata
            Derived-asset metadata.

        Raises
        ------
        ValueError
            If no source assets are found.
        """
        sources = self._load_source_metadata()
        if not sources:
            raise ValueError(
                "No source assets found: no directory under "
                f"{self.settings.input_dir} has a data_description.json."
            )

        location = self.settings.location or str(self.settings.output_dir)
        derived = Metadata.from_metadata(
            sources,
            process_name=self.settings.process_name,
            location=location,
            new_processing=self.build_new_processing(),
            new_quality_control=self.build_new_quality_control(),
            **self._data_description_overrides(),
        )
        if self.settings.verbose:
            logger.info(
                f"Built derived metadata from {len(sources)} source asset(s)"
            )
        return derived


def run() -> None:
    """Aggregate upstream metadata into the derived asset's core files."""
    settings = MetadataSettings()
    logging.basicConfig(
        level=logging.INFO if settings.verbose else logging.WARNING,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    derived = MetadataManager(settings).build_derived_metadata()
    derived.write_standard_files(output_directory=settings.output_dir)
    if settings.verbose:
        logger.info(f"Wrote derived core files to {settings.output_dir}")
