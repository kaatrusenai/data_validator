# trip-data-validator

We want to create a web application on which a user can login, enter the device name, enter the trip timing to get statistics, plots of the data collected.

For example, if I took a trip in Mumbai with device M6 from 26-May-2022 11:00:00 to 26-May-2022 12:00:00, 
I would like to see the following information:

1.  Column wise statistics the way pandas library shows us the information for a dataframe(pandas.DataFrame.info() and pandas.DataFrame.describe())

2.  The collected data has geographical coordinates in it, I would like to put these on map and get the data color coded as per some column like PM2.5, SOx, Temperature, or RH etc.

3.  For the IMU sensor's data, I would like to see the plots of average AcX, AcY, AcZ, Tmp, GcX, GcY, GcZ on map.