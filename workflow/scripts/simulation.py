import argparse

import pandas as pd
from micom.interaction import MES, interactions
from micom.qiime_formats import load_qiime_medium
from micom.workflows import build, grow

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input",
        metavar="INPUT",
        help="Manifest file",
        type=str,
    )
    parser.add_argument("-g", dest="growth_rates", help="growth rates file")
    parser.add_argument("-e", dest="exchanges", help="exchanges file")
    parser.add_argument("-a", dest="annotations", help="annotations file")
    parser.add_argument("-i", dest="interactions", help="interactions file")
    parser.add_argument("-m", dest="mes", help="MES file")
    parser.add_argument("-t", dest="threads", help="number of threads")
    parser.add_argument("--models", dest="models", help="model folder")
    parser.add_argument("--growth_medium", dest="medium", help="growth medium file")
    parser.add_argument("--tradeoff", dest="tradeoff", help="tradeoff value")
    args = parser.parse_args()
    data = pd.read_csv(args.input)
    manifest = build(
        data,
        model_db=None,
        out_folder=args.models,
        cutoff=0.0001,
        threads=args.threads,
        solver="gurobi",
    )
    medium = load_qiime_medium(args.medium)
    res = grow(
        manifest,
        model_folder=args.models,
        medium=medium,
        tradeoff=args.tradeoff,
        threads=args.threads,
    )
    res.growth_rates.to_csv(args.growth_rates)
    res.exchanges.to_csv(args.exchanges)
    res.annotations.to_csv(args.annotations)
    interactions(res, taxa=None, threads=args.threads).to_csv(args.interactions)
    MES(res).to_csv(args.mes)
