checkpoint download_mags:
    output:
        mags_dir=directory("results/mags"),
        done_flag=touch("results/.mags_downloaded")
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
    return expand("results/reconstructions/{mag}.xml", mag=MAGS)


rule reconstruct:
    input:
        get_mag_input
    output:
        "results/reconstructions/{mag}.xml"
    resources:
        mem_mb=20000
    shell:
        "carve --dna {input} -g M9 --solver cplex --output {output}"


rule reconstruct_done:
    input:
        get_reconstructions
    output:
        touch("results/reconstructions/all.done")
    resources:
        mem_mb=8000
    run:
        pass


rule generate_sample_manifest:
    input:
        "results/reconstructions/all.done"
    output:
        temp("results/manifests/{sample}.csv"),
    resources:
        mem_mb=8000
    shell:
        "python scripts/generate_sample_manifest.py {wildcards.sample} -o {output[0]}"


rule combine_manifests:
    input:
        expand("results/manifests/{sample}.csv", sample=SAMPLES)
    output:
        "results/manifest.csv"
    resources:
        mem_mb=8000
    shell:
        "python scripts/combine_manifests.py {output} {input}"


def get_mag_files_to_cleanup(wildcards):
    # Only list the .fa.gz files as dependencies (tsv files are side effects)
    checkpoint_output = checkpoints.download_mags.get(**wildcards).output.mags_dir
    mag_files = [os.path.join(checkpoint_output, f"{mag}.fa.gz") for mag in MAGS]
    return mag_files


rule cleanup_mags:
    input:
        manifest="results/manifest.csv",
        reconstructions="results/reconstructions/all.done",
        mags=get_mag_files_to_cleanup
    output:
        touch("results/.mags.cleaned")
    resources:
        mem_mb=8000
    run:
        import os
        import glob
        checkpoint_output = checkpoints.download_mags.get().output.mags_dir
        
        # Clean up all .fa.gz files
        for mag_file in input.mags:
            if os.path.exists(mag_file):
                os.remove(mag_file)
        
        # Clean up any .tsv files that were generated (not listed as inputs)
        tsv_files = glob.glob(os.path.join(checkpoint_output, "*.tsv"))
        for tsv_file in tsv_files:
            if os.path.exists(tsv_file):
                os.remove(tsv_file)
