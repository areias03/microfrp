# CLAUDE.md

Context for working in this repo across sessions. Update it as the project evolves.

## What this is

**microFRP** — a Snakemake workflow that predicts **microbial functional roles** from SPIRE
studies. It is the main project of the maintainer's PhD. Pipeline: extract MAGs for a SPIRE
item → reconstruct genome-scale metabolic models (gapseq or carve) → build per-sample
abundance manifests → simulate communities with MICOM (`build` + `grow`) → derive four
per-genome functional-role metrics whose classifications come from the score distributions.

Downstream `analysis/` notebooks join roles to HMP2 IBD metadata and test whether roles
differ by diagnosis (PCA, χ²/contingency, networks, P(role | species, diagnosis)).

## Commands

- `pixi install` — set up the env (pinned to `linux-64`; needs Gurobi + CPLEX licenses at runtime).
- `pixi run snakemake --cores 24` — run the full pipeline (top rule: `functional_roles`).
- `pixi run test` — Snakemake dry run (`-n`).
- `pixi run test-unit` — pytest suite for the scoring functions (`tests/`).

## Pipeline stages (and where they live)

1. **Study ingestion** — `Snakefile` (resolves SPIRE `item` → samples + MAG IDs via `spirepy`, caches them) and `rules/process_study.smk` (`download_mags` checkpoint).
2. **Reconstruction** — `rules/process_study.smk` `reconstruct` rule, dispatched on `config["reconstruction_tool"]` (`gapseq` | `carve`).
3. **Manifests** — `resource_generation/manifest_generation/{generate_sample_manifest,combine_manifests}.py` (abundance-weighted, per sample then combined).
4. **Simulation** — `resource_generation/simulation/simulation.py` (MICOM `build`+`grow`) + `interactions.py` (pairwise metabolic interactions). Emits growth_rates, exchanges, annotations, MES, interactions.
5. **Four role metrics** — `resource_generation/functional_roles/`:
   - `exchange_tendency.py` — signed Σ(flux × MES) → Producer / Consumer / Mixed (0.25/0.75 quantiles).
   - `cooperation_alignment.py` — positive vs co-consumed ratio → High / Low (mean split).
   - `provisioning_bias.py` — (provided − received)/total → Provider / Receiver / Balanced.
   - `interaction_synergy.py` — 2×3 combination of the above two → Primary / Mutualistic / Dependent / Marginal / Neutral / Passive.
6. **Combine** — `resource_generation/{combine_scores,combine_classifications}.py` → `results/combined_scores.tsv`, `results/functional_roles.tsv`.

## Path conventions (IMPORTANT — these have drifted before)

- Downloaded MAGs: `external_data/mags/`
- Reconstructed models: `intermediate_outputs/reconstructions/{mag}.xml`
- Simulation + per-role intermediates: `intermediate_outputs/simulation/`, `intermediate_outputs/functional_roles/`
- Final outputs: `results/`
- Study cache: `external_data/.study_cache.{item}.json` (namespaced per `item`)

Scripts must reference these exact paths. A past refactor left `generate_sample_manifest.py`
pointing at non-existent `results/mags/` and `results/reconstructions/` — verify path
consistency whenever touching manifest/simulation code.

## Conventions & gotchas

- **Config-driven** via `config/config.yaml`: `item`, `reconstruction_tool`, `tradeoff`, `growth_media` (default `config/western_diet_gut.csv`), `gapseq_medium`.
- **Three different media** currently in play: carve hardcodes `--gapfill M9`, gapseq uses `gapseq_medium`, simulation grows on `growth_media`. Reconciling these is open work.
- **`functional_role`** composite code: explicit per-axis codes joined with `-`, e.g. `Prod-Hi-Prov-Prim` (replaced first-letter concatenation, which collided — e.g. Primary/Passive → "P"). Notebooks group by this string; older runs used the 4-letter codes.
- **Reconstructed `.xml` models are cached on disk** and not rebuilt unless deleted — treat them as fixed inputs.
- **Determinism is not guaranteed**: all scores derive from MICOM exchange fluxes; the `0.01` cutoffs turn flux jitter into discrete label flips. Solver seed/method/threads are not pinned.
- Mixed **pandas** (simulation/interactions) and **polars** (functional roles).

## Open design questions (deferred — discuss before extending)

See the vault note `microFRP - Microbial functional role prediction workflow` for detail.

1. **Cohort-relative thresholds** — role cut points are computed per-run, so roles aren't portable across studies and are circular with the IBD-vs-control contrast. Plan: calibrate thresholds **per environment** (gut, soil, freshwater, …) over a stratified SPIRE subsample, frozen as versioned artifacts.
2. **Simulation determinism** — measure (run a sample twice, diff exchanges → labels), then pin the solver and record provenance.
3. **Gapfill medium inconsistency** — gapfill on the simulation medium; a prerequisite for per-environment calibration.

## Current state

- **PR #1** (https://github.com/areias03/microfrp/pull/1, merged) fixed a batch of correctness bugs and added the first tests: gapseq dispatch, manifest paths, suffix stripping, per-item cache, `exchange_tendency` `.drop`, decodable `functional_role`, the docstring, and the `tests/` pytest suite.
- Next up: confirm the pipeline runs end-to-end, then the deferred design questions above before starting the planned extensions.
