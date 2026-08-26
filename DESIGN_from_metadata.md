# Design: aggregate via `Metadata.from_metadata`

Replaces the hand-rolled `_register`-style aggregation in `metadata_manager.py`
with `aind_data_schema.core.metadata.Metadata.from_metadata` (landed in
aind-data-schema **2.9.0**, PR AllenNeuralDynamics/aind-data-schema#1852).

## Why

`from_metadata` already owns everything this capsule was doing by hand:

- **Derived `data_description`** (DERIVED level; ANALYZED name pattern when the
  derived asset spans multiple sources) — replaces `create_derived_data_description`.
- **Inheritance** of `subject`/`procedures` and `instrument`/`acquisition` from the
  sources — replaces the ancillary-file copy (`copy_ancillary_files`). Note this is
  proper v2 inheritance, so the old v1 `session.json`/`rig.json` copies are dropped.
- **Processing + QualityControl accumulation** via the schema's own `+`
  (`Processing.__add__` / `QualityControl.__add__`) — replaces
  `create_processing_metadata`, `create_quality_control_metadata`, the manual
  dependency-graph build, `_dedupe_pipelines`, and the `+` reductions.

## Input model (the mapping onto `from_metadata`)

The capsule's primary inputs are **the core files of one or more upstream data
assets** mounted under `input_dir` (`/data`) — chiefly each asset's
`processing.json` and `quality_control.json`, alongside `data_description.json`
(and optionally `subject`/`procedures`/`instrument`/`acquisition`).

- **Source `Metadata` (the `metadata=` argument)** — one per **directory that
  contains a `data_description.json`**. Every core file found beside it is loaded
  (validated individually) and assembled into a `Metadata` via `model_construct`
  (so a partial asset — e.g. only `data_description` + `processing` +
  `quality_control` — does not trip Metadata-level cross-file validation;
  `from_metadata` only *reads* these attributes).
- **`new_processing` (optional)** — this pipeline's *own* new processing, built
  from standalone `*data_process*.json` files that are **not** inside a
  source-asset directory, chained linearly and tagged with the `PIPELINE_*`
  pipeline. `None` when the capsule is a pure aggregator.
- **`new_quality_control` (optional)** — same idea, from standalone
  `*metric*.json` files outside any source-asset directory.
- **DataDescription overrides** — `data_summary`, `modality`, and `process_name`
  flow through as `from_metadata`'s `process_name=` / `**data_description_kwargs`.

Output: `Metadata.from_metadata(...).write_standard_files(output_dir)` writes each
present core file (`data_description.json`, `processing.json`,
`quality_control.json`, and any inherited `subject`/`procedures`/`instrument`/
`acquisition`).

## Assumptions to confirm in review

1. **A source asset is identified by its `data_description.json`.** Standalone
   `data_process`/`metric` files (no sibling `data_description.json`) are treated
   as *this run's new* work, not as a source asset. If upstream assets can arrive
   without a `data_description.json`, that grouping needs revisiting.
2. **Rule-4 acquisition gate (the important one).** `from_metadata` only
   accumulates the *sources'* processing/QC when **all sources share a single
   acquisition** (`_is_single_acquisition`, keyed on the root raw-asset name
   parsed from each `data_description.name`). Multi-plane/one-session merges are
   single-acquisition, so they accumulate as expected. But a genuine
   **cross-acquisition** merge makes `from_metadata` **drop** the sources'
   processing/QC and keep only `new_processing`/`new_quality_control`. That is the
   schema's documented provenance rule, not a bug — but note the sharp edge:
   verified against 2.9.0, a cross-acquisition merge with **no** `new_processing`
   produces a derived Metadata with no `subject`/`processing`/`model` and
   `from_metadata` then **raises** (`Metadata must contain at least one of ...`).
   So a pure cross-subject aggregation that contributes no new processing is
   rejected outright. Confirm this matches intent for this capsule's real inputs.
3. **At least one source has a `data_description`.** `from_metadata` raises
   otherwise; the capsule surfaces a clear error rather than silently producing
   nothing.
4. **`location`.** The schema requires a non-null `Metadata.location`, but the
   real S3 location is not known until the downstream indexer registers the asset.
   The capsule passes `--location` if given, else `output_dir`, as a placeholder
   the indexer overwrites.
