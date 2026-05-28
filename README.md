# microfrp

MicroFRP is a Snakemake pipeline for assigning microbial functional roles from SPIRE studies using genome-scale reconstructions, community simulations, and interaction scoring.

It downloads MAGs for a SPIRE item, reconstructs metabolic models with gapseq, simulates communities with MICOM, and derives functional role metrics (exchange tendency, cooperation alignment, provisioning bias, and interaction synergy).

## Requirements

- Linux (the Pixi environment is pinned to `linux-64`)
- [Pixi](https://pixi.sh/) for environment management
- Gurobi and CPLEX licenses available at runtime (required by MICOM/optimization)
- Network access to SPIRE endpoints for study metadata and MAG downloads

## Setup

Install the environment:

```bash
pixi install
```

Run commands via Pixi:

```bash
pixi run snakemake --cores 24
```

## Configuration

Edit `config/config.yaml` to target a specific SPIRE study and tune simulation settings:

- `item`: SPIRE item ID (e.g. `Lloyd-Price_2019_HMP2IBD`)
- `reconstruction_tool`: reconstruction backend (`carve` or `gapseq`)
- `tradeoff`: MICOM tradeoff value
- `growth_media`: CSV defining the growth medium
- `gapseq_medium`: optional gapseq medium argument (only used when `reconstruction_tool` is `gapseq`; empty string uses gapseq defaults)

The default growth medium is `config/western_diet_gut.csv`.

## Running the pipeline

From the repository root:

```bash
pixi run snakemake --cores 24
```

This targets the top-level rule and will:

1. Fetch SPIRE samples/MAGs for the configured item.
2. Reconstruct MAG models with gapseq.
3. Build and simulate community models with MICOM.
4. Compute functional role scores and classifications.

## Outputs

Key outputs are written to `results/`:

- `results/combined_scores.tsv`: numeric scores for each functional role metric
- `results/functional_roles.tsv`: combined classifications and functional role vectors

Intermediate artifacts are stored in `intermediate_outputs/` and downloaded MAGs in `external_data/`.

## Testing

Run a dry run of the workflow:

```bash
pixi run test
```

## License

MIT License. See [LICENSE](LICENSE).
