# ORNL-VOC

## Running

You can run the data reduction locally outside of INTERSECT with:

```
python dac.py data_file.xlsx sheet_name metadata_file.xlsx 0.1
```

Where:

data_file.xlsx is a file containing columns for the timestamp of each measurement ("PTR Time"), a 0/1 column that specifies rows to discard (0) or to include in calculations (1) ("DQ"), and a measurement for each gas (one per column). Each row represents one measurement by the device.

sheet_name is the name of the sheet within data_file.xlsx that contains the data.

metadata_file.xlsx is a file containing columns for plant names ("Plant Tag") where any blanks must have names beginning with "Blank", LICOR names ("LICOR"), and the date and time that the measurement took place ("PTR Start Time"). Each row represents one measurement by the experimenter, each of which will correspond to some number of device measurements in data_file.xlsx.

0.1 should be replaced by the desired cutoff value for what constitutes a gas with high variance. The list of high variance gases returned to the user will be those gases where the standard deviation of gas measurements for all measurements over all plants is greater than or equal to this value.

The program output will be:

A .csv file with the average of each gas over all valid measurements for each plant.

A list of plants with unusual data defined as PC Pressure outside the range 320-480 or gas 21 m/z greater/less than the LICOR's blanks' average by +/- 25% for at least one measurement.

A list for each plant of those gases with average values greater than one standard deviation of blank measurements for that LICOR above the average of the blank measurements for that LICOR for that gas.

A sorted table of gas names to the percentage of plants for which that gas was above the threshold in the above list.

A list of those gases where the standard deviation of gas measurements for all measurements over all plants is greater than or equal to the user provided cutoff value defined previously.