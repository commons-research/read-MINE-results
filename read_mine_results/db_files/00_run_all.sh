#!/bin/bash

# Example to run:


#SBATCH --partition=pibu_el8
#SBATCH --mem=900G
#SBATCH --time=1-10:00:00
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



# input_rename="/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/all_original_files/"
# output_rename="/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/renamed_files/"
# time python 01_renaming_compounds_and_reaction.py ${input_rename} ${output_rename} > info.out 2>&1 


input_duplicates="/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/renamed_files/"
output_duplicates="/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/cleaned_files/"
time python 02_remove_duplicates.py ${input_duplicates} ${output_duplicates} > info.out 2>&1 

# Deactivate the micromamba environment
micromamba deactivate