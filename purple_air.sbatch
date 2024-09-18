#!/bin/bash
#
#SBATCH --job-name=download_PA
#SBATCH --partition=serc
#SBATCH --time=1:00:00
#SBATCH --mem=16GB
#SBATCH --output=purple_air.log
#SBATCH --mail-type=ALL
#SBATCH --output=test_job.%j.out
#SBATCH --error=test_job.%j.err

module load python/3.6.1

python3 purple_air.py