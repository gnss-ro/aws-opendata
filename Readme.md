GNSS Radio Occultation Data in the AWS Cloud
============================================

**AWS Location**: s3://gnss-ro-data

**AWS Region**: us-east-1  

**Managing Organization**: Atmospheric and Environmental Research, Inc.

*Correspondence:* Stephen Leroy (sleroy@aer.com) or Amy McVey (amcvey@aer.com)


# Introduction

Radio occultation is the limb sounding of planetary atmospheres by measuring the 
bending of a well-timed, temporally coherent radiation transmitted from a 
spacecraft outside the atmosphere that then transits the limb of the atmosphere 
and is received on the other side of the atmosphere. The atmosphere itself 
bends the signal as it passes through the atmosphere because of vertical 
gradients of the index of refraction, and the receiver infers the amount of 
bending by measuring Doppler shifts in the carrier frequency of the transmitted 
signal. The received radiation field, both the amplitude and the phase, can be 
inverted for vertical profiles of the index of refraction. Empirical 
formulas relate the index of refraction to various constituents in the 
atmosphere, which enable retrieval of temperature, pressure, etc., with the 
a few additional constraints, such as the hydrostatic equation. 

Planetary radio occultation has yielded some of the strongest knowledge of the 
temperatures and pressures in planetary atmospheres since the 1960's. Radio 
occultation of the Earth's atmosphere using the already existing satellites of 
the Global Positioning System as transmitters commenced with the 
proof-of-concept mission GPS/MET in 1995. Since then, over 20 satellites have 
obtained radio occultation data using GPS. Modern Earth RO missions use the 
transmitters of other Global Navigation Satellite Systems (GNSS), such as 
the Russian GLONASS, the European Galileo, and the Chinese BeiDou. Still others 
use the transmitters of geo-synchronous regional navigation systems such as 
the Indian SBAS and the Japanese QZSS. Consequently, Earth radio occultation is
now referred to as GNSS RO. 

Because GNSS RO is a timing measurement as opposed to a radiance measurement, 
and because the amplitude and phase of the signal are tracked in the course 
of a sounding, GNSS RO begets two astonishing properties that set it aside 
from other remote sensing techniques for temperature and pressure. First, 
because the international definition of the unit of time can be scaled 
to an accuracy of one part in 10<sup>15</sup>, GNSS RO has the potential to 
benchmark the state of the climate to great accuracy. This makes GNSS RO a 
formidable climate monitoring technique. Second, because the phase of the 
radiation is measured in addition to the amplitude of the transmitted 
signal, holographic reconstruction methods become possible, and the result 
is extraordinary vertical resolution. The physical limitation on the 
vertical resolution is Fraunhofer diffraction, which yields a limit of 10 
meters in the vertical.  Other practical considerations provide the actual 
limitations, however --- the data rate, receiver electronic noise, and the 
horizontal-to-vertical aspect ratio of the atmospheric features being 
studied --- and the more practical result is that GNSS RO obtained 
~100-meter vertical resolution.  Such vertical resolution enables a host 
of interesting atmospheric process studies. 

The path and file naming convention follow in the second section. The 
third section contains descriptions of the file formats available in the 
GNSS RO repository in the AWS Open Data Registry. A [detailed PDF 
document](http://github.com/gnss-ro/aws-opendata/Data-Description.pdf) 
describes the rationale for the formats together with insights into 
their utility in addition to their actual contents. The following are all 
useful to a beginner at GNSS RO: 
* [A description of GNSS RO](http://doi.org/10.3319/TAO.2000.11.1.53(COSMIC)); 
* [A complete error analysis of GNSS RO](http://doi.org/10.1029/97JD01569); 
* [A description of a typical GNSS RO retrieval system](http://doi.org/10.1016/S1364-6826(01)00114-6); 
* [Vertical coordinates in GNSS RO](http://doi.org/10.1002/2016JD025902); 
* [Physical optics processing algorithms](http://doi.org/10.5194/amt-14-853-2021); 
* [Statistical optimization to smooth RO in the upper stratosphere](http://doi.org/10.1029/2000RS002370); 
* [A description of super-refraction](http://doi.org/10.1029/2002RS002728); 
* [Retrieving tropospheric water vapor in GNSS RO](http://doi.org/10.1175/JTECH-D-13-00233.1); and
* [Ionospheric calibration and the removal of ionospheric residual](http://doi.org/10.5194/amt-8-3385-2015). 


# Data availability and formats

The AWS Open Data Registry now makes available all GNSS RO obtained and 
processed by three independent RO processing centers: the COSMIC DAAC of the 
University Corporation for Atmospheric Research, the Jet Propulsion 
Laboratory of the California Institute of Technology, and the Radio Occultation 
Meteorology Satellite Application Facility (ROM SAF) of EUMETSAT. One other independent 
processing center is considering participation: the Wegener Center of the 
University of Graz. The data are available at three levels: calibrated 
satellites data (level 1b), retrievals of bending angle and refractivity 
(level 2a), and retrievals of atmospheric temperature, pressure, and water 
vapor (level 2b). 

All RO data is provided by at least three independent processing centers, 
but those centers did not process RO data from all GNSS RO missions, and 
each center implemented different versions of quality control. Consequently, 
there will be enormous overlap in RO sounding data between the different 
centers, but the overlaps are not supersets or subsets of one another. It 
will not be unusual at all to find multiple retrievals of the same RO 
sounding, nor will it be unusual to find only version even if multiple 
retrieval centers set out to process those soundings. 

In all files, time is provided in units of [GPS seconds](http://leapsecond.com/java/gpsclock.htm), 
which is the number of seconds elapsed since 00:00 UTC on 6 January 1980. It does
not add in leap seconds; thus, as of July of 2021, GPS time appears to lead 
UTC time by 18 seconds. Also, every file contains metadata on the processing 
center that provisioned the data, a pointer to the relevant data use license, 
and a list of references in the form of [digital object 
identifiers](http://doi.org) that document the processing system used to 
generate the data in each file. 


## Level 1b: Calibrated phase files, calibratedPhase

Calibrated GNSS RO data are provided as **calibratedPhase** files. The 
format is NetCDF4, one occultation sounding per file. The contents of each 
file includes the variables **time** (which is the independent 
coordinate), **snr**, **excessPhase**, 
**positionLEO**, and **positionGNSS** for an arbitrary number of signals 
tracked. Other variables define the 3-digit RINEX 3 observation codes 
that identify the signals (see section 5.1 of the [RINEX 3.04
documentation](http://acc.igs.org/misc/rinex304.pdf) for example), 
whether or not the navigation data bits have been removed when unwinding 
the excess phase, and the range and phase models used in open loop 
tracking. Each variable is well described in its variable attributes. 
Global attributes provided necessary metadata on the occultation sounding, 
including three-digit identifier of the transmitting GNSS, the names of 
the mission and the satellite carrying the receiver, the name of a 
reference GNSS satellite used to calibrate the receiver clock if 
calibration was performed by single- or double-differencing, the name of 
the ground station used to calibrate the GNSS transmitter clocks if the 
calibration was performed by double-differencing. Note that the 
**excessPhase** and the positions of the LEO (**positionLEO**) are given 
as a functions of **time** in the files, the **time** corresponding to 
the time of reception of the signal by the LEO satellite; however, the 
positions of the transmitter **positionGNSS** are specified at the 
times the received signals were *transmitted* by the GNSS satellite. 
The GNSS positions were computed by interpolating the GNSS positions 
backward in time from the receive **time** by an amount corresponding 
by the light travel time between the transmitter and the receiver. This 
is done because it is the positions of the transmitter at transmit time 
that directly enters into the RO retrieval process. 

## Level 2a: Bending angle, refractivity, and dry atmosphere retrievals, refractivityRetrieval

Retrievals of profiles of bending angle, microwave refractivity, and 
"dry" temperature and pressure are provided in **refractivityRetrieval** files. 
A dry atmospheric retrieval is a retrieval of temperature pressure 
that is obtained when the contribution water vapor to microwave 
refractivity is considered non-existent. This is a good approximation 
in the upper troposphere and stratosphere, but not so in the lower 
troposphere where water vapor contributes approximately 10% of the 
refractivity. Dry retrievals are generated because RO does not provide 
enough information by itself to distinguish between the 
contributions of water vapor and the "dry" atmospheric constituents 
(nitrogen, oxygen, carbon dioxide) to microwave refractivity. 

The variables include **impactParameter** (which is the independent 
coordinate for bending angle retrieval), **bendingAngle**, 
**combinedBendingAngle** (which is ionosphere-corrected) for 
bending angle retrievals; **altitude**, **geopotential**, **longitude**, 
**latitude**, **refractivity**, **dryPressure**, and **dryTemperature**
for dry atmospheric retrievals. Other useful auxiliary data define a 
reference geolocation for the sounding, the shape of the mean sea 
level geoid, and the effective center of curvature for the sounding. 

*Note:* Geopotential can be converted to geopotential height by dividing 
by the appropriate constant value of gravitational acceleration at 
the Earth's surface. In most cases, that constant is taken as 
the World Meteorological Organization standard of 
9.80665 J kg<sup>-1</sup> m<sup>-1</sup>. Some atmospheric models 
and processing algorithms for other atmospheric datasets have 
implemented different values for this constant. Their documentation 
must be searched for the value it incorporated in processing or 
execution and then used in the conversion to geopotential height when 
the data of this archive is compared to the atmospheric model output 
or atmospheric dataset, whether it be satellite or in-situ. 

## Level 2b: Full atmospheric retrievals, atmosphericRetrieval

Retrievals of profiles of temperature, pressure, and water vapor 
are provided in **atmosphericRetrieval** files. In each case, auxiliary 
information on temperature and/or water vapor has been used to 
disentangle the contributions of water vapor and the dry atmosphere 
to microwave refractivity. The auxiliary data usually comes from the 
forecasts or analyses of a numerical weather prediction model or 
atmospheric reanalysis. These profiles are usually of much coarser 
vertical resolution than the **refractivityRetrieval** files because the 
atmospheric model data used as an auxiliary input is much coarser 
in vertical resolution than RO is capable of. 

The variables include **geopotential** (the natural independent 
coordinate of RO atmospheric retrieval), **refractivity**, 
**temperature**, **pressure**, and **waterVaporPressure**. 
Other data provide geolocation information for the sounding 
and indicate whether whether or not super-refraction is 
present in the sounding and if a special algorithm was used to 
account for the influence of super-refraction in the retrieval. 

#  Paths and file naming

The path and file naming convention for all of the RO data is 
contributed/**version**/**center**/**mission**/**filetype**/**yyyy**/**mm**/**dd**/**filetype**\_**mission**\_**center**\_**version**\_**occid**.nc. 
The various mnemonics in this path are defined in the following table: 

| Mnemonic | Description | Examples |
| :------: | :---------- | :------- |
| center | The RO retrieval center that contributed the data | ucar, jpl, romsaf |
| mission | The RO mission | (See the next table) | 
| filetype | The file type | calibratedPhase, refractivityRetrieval, atmosphericRetrieval |
| yyyy | The year of the RO sounding | 1995, 1996, ... 2020, etc. |
| mm | The month of the RO sounding | 01, 02, ..., 12 |
| dd | The day of the month of the RO sounding | 01, 02, ..., 31 |
| hh | The hour of the RO sounding | 00, 01, ..., 23 | 
| nn | The minute of the RO sounding | 00, 01, ..., 59 | 
| version | A string defining the processing version | (Defined by the contributing center, no underscores) |
| occid | The occultation ID as registered in the AWS Open Data GNSS RO data repository | See definition below |

The occultation identifier **occid** is defined as **ttt-leo-yyyymmddhhnn** in which 
**ttt** is the three-digit RINEX standard identifier of the transmitting GNSS 
satellite and  **leo** is the name of the low-Earth orbiting receiving satellite. 
The remaining symbols denote the time of the occultation. Note that an occultation 
sounding is uniquely identified by the transmitter, receiver, and time of the occultation, 
and the precision of the time needs to be no better than a few minutes. 

The names of the GNSS RO missions and LEO/receiving satellites are given in the following 
table. 

| Mission | LEO/receiver | Long name |
| :-----: | :----------- | :-------- |
| gpsmet | gpsmet, gpsmetas | GPS/MET FORMOSAT-3 | 
| grace | gracea, graceb | Gravity Recovery and Climate Experiment (GRACE) | 
| sacc | sacc | Satellite de Aplicaciones Cientifico-C (SAC-C) | 
| champ | champ | Challenging Mini-satellite Payload (CHAMP) | 
| cosmic1 | cosmic1c1, cosmic1c2, cosmic1c3, cosmic1c4, cosmic1c5, cosmic1c6 | Constellation Observing System for Meteorology, Ionosphere and Climate (COSMIC) | 
| tsx | tsx | Terra Synthetic Aperture Radar - X (TerraSAR-X) | 
| tdx | tdx | TerraSAR add-on for Digital Elevation Measurement (TanDEM-X) | 
| cnofs | cnofs | Communications/Navigation Outage Forecasting System (C/NOFS) | 
| metop | metopa, metopb, metopc | Metop-A, Metop-B, Metop-C | 
| kompsat5 | kompsat5 | Korean Multi-Purpose Satellite 5 (KompSat 5) | 
| paz | paz | Radio Occultations and Heavy Precipitation with PAZ (ROHP-PAZ) | 
| cosmic2 | cosmic2e1, cosmic2e2, cosmic2e3, cosmic2e4, cosmic2e5, cosmic2e6 | Constellation Observing System for Meteorology, Ionosphere and Climate 2 (COSMIC-2) | 

Multiple satellites are listed for each mission if the mission consisted of 
multiple satellites (such as COSMIC-1 and COSMIC-2) or the same program deployed 
a series of similar environmental sounding satellites (such as Metop). 
The exception is GPS/MET, which is just one satellite. GPS/MET is a special case 
because it obtained RO soundings in two different modes: one when 
the GPS anti-spoofing encryption on L2 was turned off ("gpsmet") and one when 
the GPS anti-spoofing encryption on L2 was turned on ("gpsmetas"). The 
anti-spoofing encryption greatly inhibited the ability to track the L2 signal 
during RO soundings, 
and consequently special retrieval algorithms were applied to process these 
data at all. Since GPS/MET, flight GNSS RO receivers were enabled with greatly 
improved technology to track GPS L2 even when encrypted by anti-spoofing. 
Preently, most GPS satellites transmit a civil signal at L2 that requires 
no specialized tracking techniques. 

The following table defines the names of the contributing RO processing 
centers. 

| Processing center | center | 
| :---------------- | :----: |
| UCAR COSMIC Project Office | ucar | 
| Jet Propulsion Laboratory, Caltech | jpl | 
| EUMETSAT Radio Occultation Meteorology Satellite Application Facility | romsaf | 

#  Data use licenses, acknowledgments

The format definitions are the outcome of consultations of an international team 
of GNSS RO retreival scientists and experts. They were drawn largely from the 
International Radio Occultation Working Group (IROWG), from public and private 
concerns, from universities, government laboratories, and satellite agencies. 

The data use licenses for the various contributing centers are 
* The COSMIC Project Office at UCAR, a [creative commons license](https://www.ucar.edu/terms-of-use/data), 
* The NASA Jet Propulsion Laboratory, California Institute of Technology, a 
[creative commons license](https://creativecommmons.org/licenses/by/4.0/legalcode), and 
* The ROM SAF of EUMETSAT, the [EUMETSAT data use license](https://www.eumetsat.int/eumetsat-data-licensing). 

The repository of GNSS RO data in the AWS Open Data Registry was assembled and 
continues to be maintained by scientists and software engineers at Atmospheric 
and Environmental Research, Inc. Funding for this effort was provided by the NASA 
Advancing Collaborative Connections for Earth System Science (ACCESS) Program 2019, 
grant 80NSSC21M0052. 


*Last update: 3 September 2021*

