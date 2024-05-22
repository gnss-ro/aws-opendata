# Radio Occultation Coverage Gap over Ukraine

This directory contains the python code used to do the analysis and 
compose the figures in the news item related to a GNSS radio occultation 
coverage gap over East Europe and West Asia due to battlefield GNSS 
signal jamming associated with the Russo-Ukrainian war. 

## Prerequisites

Note that this software is centrally dependent on the GNSS radio occultation 
metadata kept in the Amazon Web Services (AWS) Registry of Open Data 
repository of Earth Radio Occultation data. The PyPI package 
**awsgnssroutils** functions as a portal to that AWS repository of RO 
data and RO metadata. 

Reproducing the analysis is best done with a Miniconda/Anaconda Python 
installation. Create a working environment using the **config.yaml**: 

```
conda env create -f config.yaml
```

at the Linux command line. If you haven't previously implemented the 
**awsgnssroutils** Python package and set its defaults, be sure to 
change the "False" to a "True" in on line 530 of **ukraine_ro_gap.py** 
and change the paths that follow according to user wishes. 

## Analysis and Figure Generation

Analysis and figure generation can be done simply by executing the 
following on the Linux command line.

```
conda activate ro-distributions
python ukraine_ro_gap.py
```

## Scatterplot of RO Soundings

If you wish to experiment with the RO sounding coverage globally 
by a scatterplot of soundings on a map, use the jupyter notebook 
**distributions.ipynb**. It will color-code the points according 
to RO mission. 

