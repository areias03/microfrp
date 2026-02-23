import argparse
import glob
import gzip

import polars as pl
from Bio import SeqIO
from spirepy import Sample


def get_abundances(sample: Sample):
    abundances = {}
    depths = sample.get_contig_depths()
    for f in sorted(
        [
            mag
            for mag in glob.glob("results/mags/*.fa.gz")
            if mag.split("/")[-1].split("-")[1].strip(".fa.gz")
            in sample.get_mags()["genome_id"].to_list()
        ]
    ):
        with gzip.open(f, "rt") as handle:
            headers = set(rec.id for rec in SeqIO.parse(handle, "fasta"))
        abundance = depths.filter(pl.col("contigName").is_in(headers))[
            "totalAvgDepth"
        ].sum()
        abundances[f] = abundance
    return abundances


def generate_manifest(sample: Sample):
    manif = []
    abun = get_abundances(sample)
    for genome in sample.get_mags().iter_rows(named=True):
        manif.append(
            [
                genome["genome_id"],
                genome["domain"],
                genome["phylum"],
                genome["class"],
                genome["order"],
                genome["family"],
                genome["genus"],
                genome["species"],
                f"results/reconstructions/{genome['genome_id']}.xml",
                genome["derived_from_sample"],
                abun[f"{genome['genome_id']}.fa.gz",],
            ]
        )

    manifest = pl.DataFrame(
        manif,
        schema={
            "id": str,
            "kingdom": str,
            "phylum": str,
            "class": str,
            "order": str,
            "family": str,
            "genus": str,
            "species": str,
            "file": str,
            "sample_id": str,
            "abundance": float,
        },
        strict=False,
        orient="row",
    )
    manifest = manifest.with_columns(
        (pl.col("abundance") / pl.col("abundance").sum()).alias("abundance")
    )
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input",
        metavar="INPUT",
        help="Input item (sample ID)",
        type=str,
    )
    parser.add_argument("-o", "--output", dest="output", help="output file")
    args = parser.parse_args()
    sample = Sample(args.input)
    sample_manifest = generate_manifest(sample)
    sample_manifest.write_csv(args.output)
