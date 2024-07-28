#!/bin/bash

# Example to run:


#SBATCH --partition=pibu_el8
#SBATCH --cpus-per-task=75
#SBATCH --mem=500G
#SBATCH --time=5-10:00:00
#SBATCH --job-name="remove and rename duplicates"
#SBATCH --mail-user=pascal.amrein@unifr.ch
#SBATCH --mail-type=begin,end,fail
#SBATCH --output=/home/pamrein/2024_masterthesis/read-MINE-results/log/runAll_%j.out
#SBATCH --error=/home/pamrein/2024_masterthesis/read-MINE-results/log/runAll_%j.err


cd /home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/

# micromamba run -n read-MINEs 
eval "$(micromamba shell hook --shell=bash)"
micromamba activate read-MINEs

# Print information
echo "Running on host: $(hostname)"
echo "Allocated nodes: $SLURM_JOB_NODELIST"
echo "Current working directory: $(pwd)"



input_rename="/home/pamrein/2024_masterthesis/MINE-Database/data/output/metacyc_generalized/20240724_lotus_generalized_n50/"
output_rename="/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/20240728_renamed_files/"
time python 01_renaming_compounds_and_reaction.py ${input_rename} ${output_rename}


input_duplicates=$output_rename
output_duplicates="/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/20240728_cleaned_files/"
time python 02_remove_duplicates.py ${input_duplicates} ${output_duplicates}

# Deactivate the micromamba environment
micromamba deactivate

