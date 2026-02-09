import gzip
import sys

import polars as pl
from Bio import SeqIO
from spirepy import Sample

sample_file = sys.argv[1]
sample = Sample(sample_file.split("/")[-1].removesuffix(".tsv"))

egg_sample = pl.read_csv(sample_file, separator="\t")
for m in sample.get_mags()["genome_id"].to_list():
    with gzip.open(f"results/mags/{m}.fa.gz", "rt") as handle:
        headers = [rec.id for rec in SeqIO.parse(handle, "fasta")]
    split = egg_sample.filter((pl.col("#query").str.contains_any(headers)))
    split.write_csv(f"results/eggnog_splits/{m}.tsv", separator="\t")
