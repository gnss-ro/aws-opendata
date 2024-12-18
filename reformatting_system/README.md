# Radio Occultation Reformatting and Cataloging System 

The Radio Occultation Reformatting and Cataloging (RORefCat) system ingests 
GNSS radio occultation data for all missions from multiple independent RO 
processing centers. GNSS radio occultation (RO) refers to the remote sensing 
technique wherein the precisely timed signals of the Global Navigation 
Satellite Systems (GNSS), including the Global Positioning System, are 
tracked by GNSS receivers on satellites in low-Earth orbit as the transmitting 
GNSS satellites appear to rise or set on the Earth's horizon. The phase delays 
induced by refraction in the Earth's atmosphere can be inverted for ray bending 
in the atmosphere, profiles of the microwave index of refraction, and profiles 
of pressure, temperature, and water vapor. Historical Earth RO data are now 
manifested in the AWS Registry of Open Data S3 bucket 
[gnss-ro-data](https://registry.opendata.aws/gnss-ro-opendata). The archive and 
its background is documented in 
[an article in Earth and Space Sciences](https://doi.org/10.1029/2023EA003021). 
Descriptions of the GNSS RO technique can be found in 
[Kursinski et al. 2000](https://doi.org/10.3319/TAO.2000.11.1.53(COSMIC)) 
and in [Leroy 2015](https://doi.org/10.1016/B978-0-12-382225-3.00350-9). 

The RORefCat system requires access to an AWS account, AWS S3 and DynamoDB 
services. Authentication, if needed, is handled through environment variables 
according to AWS standards. The system can be easily installed either through 
a pip install, an conda environment build (for Anaconda/Miniconda Python), or 
through a Docker image build: 
```
(1) pip install ./rorefcat
(2) conda env create -f config.yaml
(3) ./build.sh
```
The second and third options are the most robust to the local environment. 
The third option requires that you have installed Docker Desktop and that 
its daemon is running in the background. The system can be deployed in AWS 
using the infrastructure-as-code formulation in Terraform, documented in 
the [terraform readme](terraform/README.md). 
RORefCat itself has [its own Readme page](rorefcat/README.md). 

The RORefCat system was built with funding from the NASA Advancing 
Collaborative Connections for Earth System Science (ACCESS) 2019 
program, grant 80NSSC21M0052. 

Authors: Stephen Leroy (sleroy-at-aer.com), Amy McVey (amcvey-at-aer.com)

Date: December 18, 2024

