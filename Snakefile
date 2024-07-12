'''
snakemake \
    --jobs 5 \
    --latency-wait 5 \
    --keep-going \
    --default-resource mem_mb=100000 \
    --cluster '
        sbatch \
            --partition pibu_el8 \
            --cpus-per-task {threads} \
            --mem {resources.mem_mb} \
            --time 3:00:00 \
            --job-name "remove duplicates" \
            --mail-user pascal.amrein@unifr.ch \
            --mail-type end,fail \
            --output /home/pamrein/2024_masterthesis/read-MINE-results/log/{rule}_%j.out \
            --error /home/pamrein/2024_masterthesis/read-MINE-results/log/{rule}_%j.err'
'''

            # --output /home/pamrein/2024_masterthesis/read-MINE-results/log/{rule}-{wildcards}_%j.out \
            # --error /home/pamrein/2024_masterthesis/read-MINE-results/log/{rule}-{wildcards}_%j.err'


import glob

# List of input files matching the pattern
INPUT_FILES = glob.glob("/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/all_original_files/*.parquet")


rule all:
    """
    Collect the outputfiles
    """
    input:
        expand("read_mine_results/db_files/cleaned_files/{input_file}", input_file=[file.split('/')[-1] for file in INPUT_FILES])


rule rename_compounds_and_reactions:
    input:
        'read_mine_results/db_files/all_original_files/{sample}.parquet'
    output:
        'read_mine_results/db_files/renamed_files/{sample}renamed.parquet'
    shell: '''
        time python read_mine_results/db_files/01_renaming_compounds_and_reaction.py {input} {output}
    '''

rule remove_duplicates:
    input:
        'read_mine_results/db_files/renamed_files/{sample}renamed.parquet'
    output:
        'read_mine_results/db_files/cleaned_files/{sample}.parquet'
    resources:
        mem_mb=850000
    shell: '''
        time python read_mine_results/db_files/02_remove_duplicates.py {input} {output}
    '''

    