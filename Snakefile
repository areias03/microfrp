configfile: "config/config.yaml"

import os
import json
import requests
import logging as log
import polars as pl
import spirepy

ITEM_ID = config["item"]

CACHE_FILE = "external_data/.study_cache.json"

if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, 'r') as f:
        cache = json.load(f)
        SAMPLES = cache['samples']
        MAGS = cache['mags']
        print(f"Loaded from cache: {len(SAMPLES)} samples, {len(MAGS)} MAGs")
else:
    print("Fetching study data from API (this may take a moment)...")
    study = spirepy.Study(ITEM_ID)
    SAMPLES = [s.id for s in study.get_samples()]
    MAGS = study.get_mags()["genome_id"].to_list()
    os.makedirs("results", exist_ok=True)
    with open(CACHE_FILE, 'w') as f:
        json.dump({'samples': SAMPLES, 'mags': MAGS}, f)
    print(f"Cached study data: {len(SAMPLES)} samples, {len(MAGS)} MAGs")

print("Samples to process:", len(SAMPLES))
print("MAGs to process:", len(MAGS))

include: "rules/process_study.smk"


rule functional_roles:
    input:
        "intermediate_outputs/simulation/manifest.csv",
	"intermediate_outputs/reconstructions/all.done",
        "intermediate_outputs/simulation/growth_rates.csv",
        "intermediate_outputs/simulation/exchanges.csv",
        "intermediate_outputs/simulation/annotations.csv",
        "intermediate_outputs/simulation/interactions.csv",
        "intermediate_outputs/simulation/mes.csv",
        "intermediate_outputs/models",
        "results/combined_scores.tsv",
        "results/functional_roles.tsv",


rule simulate:
    input:
        manifest = "intermediate_outputs/simulation/manifest.csv",
        reconstructions_done = "intermediate_outputs/reconstructions/all.done"
    output:
        growth_rates = "intermediate_outputs/simulation/growth_rates.csv",
        exchanges = "intermediate_outputs/simulation/exchanges.csv",
        annotations = "intermediate_outputs/simulation/annotations.csv",
        interactions = "intermediate_outputs/simulation/interactions.csv",
        mes = "intermediate_outputs/simulation/mes.csv",
        models = directory("intermediate_outputs/models")
    resources:
            mem_mb=192000
    threads:
        24
    params:
        growth_media= config["growth_media"],
        tradeoff= config["tradeoff"]
    shell:
        "python resource_generation/simulation/simulation.py {input.manifest} -g {output.growth_rates} -e {output.exchanges} -a {output.annotations} -i {output.interactions} -m {output.mes} -t {threads} --models {output.models} --growth_medium {params.growth_media} --tradeoff {params.tradeoff}"

rule exchange_tendency:
    input:
        exchanges = "intermediate_outputs/simulation/exchanges.csv",
        mes = "intermediate_outputs/simulation/mes.csv",
    output:
        classifications = "intermediate_outputs/functional_roles/exchange_tendency.csv",
        scores = "intermediate_outputs/functional_roles/exchange_tendency_scores.csv"
    resources:
	    mem_mb=8000
    shell:
        "python resource_generation/functional_roles/exchange_tendency.py {input.exchanges} {input.mes} -o {output.classifications} -s {output.scores}"

rule cooperation_alignment:
    input:
        interactions = "intermediate_outputs/simulation/interactions.csv",
    output:
        classifications="intermediate_outputs/functional_roles/cooperation_alignment.csv",
        scores="intermediate_outputs/functional_roles/cooperation_alignment_scores.csv"
    resources:
            mem_mb=8000
    shell:
        "python resource_generation/functional_roles/cooperation_alignment.py {input.interactions} -o {output.classifications} -s {output.scores}"

rule provisioning_bias:
    input:
        interactions = "intermediate_outputs/simulation/interactions.csv",
    output:
        classifications="intermediate_outputs/functional_roles/provisioning_bias.csv",
        scores="intermediate_outputs/functional_roles/provisioning_bias_scores.csv"
    resources:
            mem_mb=8000
    shell:
        "python resource_generation/functional_roles/provisioning_bias.py {input.interactions} -o {output.classifications} -s {output.scores}"

rule interaction_synergy:
    input:
        cooperation_alignment = "intermediate_outputs/functional_roles/cooperation_alignment.csv",
        provisioning_bias = "intermediate_outputs/functional_roles/provisioning_bias.csv",
    output:
        "intermediate_outputs/functional_roles/interaction_synergy.csv"
    resources:
            mem_mb=8000
    shell:
        "python resource_generation/functional_roles/interaction_synergy.py -c {input.cooperation_alignment} -p {input.provisioning_bias} -o {output}"

rule combine_scores:
    input:
        exchange_tendency = "intermediate_outputs/functional_roles/exchange_tendency_scores.csv",
        cooperation_alignment = "intermediate_outputs/functional_roles/cooperation_alignment_scores.csv",
        provisioning_bias = "intermediate_outputs/functional_roles/provisioning_bias_scores.csv"
    output:
        "results/combined_scores.tsv"
    resources:
            mem_mb=8000
    shell:
        "python resource_generation/combine_scores.py {input.exchange_tendency} {input.cooperation_alignment} {input.provisioning_bias} -o {output}"

rule combine_classifications:
    input:
        exchange_tendency = "intermediate_outputs/functional_roles/exchange_tendency.csv",
        cooperation_alignment = "intermediate_outputs/functional_roles/cooperation_alignment.csv",
        provisioning_bias = "intermediate_outputs/functional_roles/provisioning_bias.csv",
        interaction_synergy = "intermediate_outputs/functional_roles/interaction_synergy.csv",
    output:
        "results/functional_roles.tsv"
    resources:
            mem_mb=8000
    shell:
        "python resource_generation/combine_classifications.py {input.exchange_tendency} {input.cooperation_alignment} {input.provisioning_bias} {input.interaction_synergy} -o {output}"
