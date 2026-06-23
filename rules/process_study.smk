configfile: "config/config.yaml"

checkpoint download_mags:
    output:
        mags_dir=directory("external_data/mags"),
        done_flag=touch("external_data/.mags_downloaded")
    resources:
        mem_mb=8000
    params:
        item_id=ITEM_ID,
        mags_to_keep=MAGS
    run:
        import subprocess
        import glob

        shell("mkdir -p {output.mags_dir}")
        shell("wget -O - https://swifter.embl.de/~fullam/spire/compiled/{params.item_id}_spire_v1_MAGs.tar | tar -xf - -C {output.mags_dir} --strip-components=1")

        all_files = glob.glob(os.path.join(output.mags_dir, "*.fa.gz"))
        all_files.extend(glob.glob(os.path.join(output.mags_dir, "*.tsv")))

        mags_to_keep_set = set(params.mags_to_keep)
        for filepath in all_files:
            filename = os.path.basename(filepath)
            mag_id = filename.replace(".fa.gz", "").replace(".tsv", "")

            if mag_id not in mags_to_keep_set:
                os.remove(filepath)
                print(f"Removed {filename} - not in MAGS list")

        kept_files = glob.glob(os.path.join(output.mags_dir, "*.fa.gz"))
        print(f"Kept {len(kept_files)} MAG files out of {len(all_files)} downloaded")


def get_mag_input(wildcards):
    checkpoint_output = checkpoints.download_mags.get(**wildcards).output.mags_dir
    return os.path.join(checkpoint_output, f"{wildcards.mag}.fa.gz")


def get_reconstructions(wildcards):
    checkpoints.download_mags.get(**wildcards)
    return expand("intermediate_outputs/reconstructions/{mag}.xml", mag=MAGS)

if config.get("reconstruction_tool") == "gapseq":
    rule reconstruct:
        input:
            get_mag_input
        output:
            "intermediate_outputs/reconstructions/{mag}.xml"
        params:
            workdir=lambda wc: os.path.abspath(f"intermediate_outputs/reconstructions/.gapseq/{wc.mag}"),
            output_xml=lambda wc: os.path.abspath(f"intermediate_outputs/reconstructions/{wc.mag}.xml"),
            gapseq_medium=config.get("gapseq_medium", "")
        resources:
            mem_mb=20000
        shell:
            r"""
            set -euo pipefail
            mkdir -p {params.workdir}
            cp {input} {params.workdir}/{wildcards.mag}.fa.gz
            cd {params.workdir}
            if [ -n "{params.gapseq_medium}" ]; then
                gapseq doall {wildcards.mag}.fa.gz {params.gapseq_medium}
            else
                gapseq doall {wildcards.mag}.fa.gz
            fi
            cp {wildcards.mag}.xml {params.output_xml}
            """

elif config.get("reconstruction_tool") == "carve":
    rule reconstruct:
        input:
            get_mag_input
        output:
            "intermediate_outputs/reconstructions/{mag}.xml"
        resources:
            mem_mb=20000
        shell:
            """
            carve --dna {input} --solver cplex --gapfill M9 --output {output}
            """
else:
    raise ValueError(f"Unsupported reconstruction tool: {config.get('reconstruction_tool')}")


rule reconstruct_done:
    input:
        get_reconstructions
    output:
        touch("intermediate_outputs/reconstructions/all.done")
    resources:
        mem_mb=8000
    run:
        pass


rule generate_sample_manifest:
    input:
        "intermediate_outputs/reconstructions/all.done"
    output:
        temp("intermediate_outputs/manifests/{sample}.csv"),
    resources:
        mem_mb=8000
    shell:
        "python resource_generation/manifest_generation/generate_sample_manifest.py {wildcards.sample} -o {output[0]}"


rule combine_manifests:
    input:
        expand("intermediate_outputs/manifests/{sample}.csv", sample=SAMPLES)
    output:
        "intermediate_outputs/simulation/manifest.csv"
    resources:
        mem_mb=8000
    shell:
        "python resource_generation/manifest_generation/combine_manifests.py {output} {input}"
