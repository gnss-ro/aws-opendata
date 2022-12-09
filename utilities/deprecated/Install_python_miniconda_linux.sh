#!/usr/bin/env bash
mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
~/miniconda3/bin/conda init bash
~/miniconda3/bin/conda init zsh
conda install -c -y anaconda numpy boto3 netcdf4 scipy pandas
conda install -c -y conda-forge matplotlib=3.5.1 cartopy=0.18.0
PATH=$PATH:~/miniconda3/bin/conda/python3
