# Algorithm of analysis

First we take the data and construct 4 additional columns :"TankLeveldifference, TimeDifference, FuelRate, Situation".

Then two inputs given to the algorithm are: the maximum consumption rate and the sensor sensitivity.

To know the situation of the row, we use these conditions:

1. if the power is positive, then the situation is consumption represented by 'c'.

2. if the tankleveldifference is greater than 2*sensor_sesitivity then the situation is filling represented by 'f'.

3. if the power is NULL and the tanklevel difference is less than -sensor_sensitivity then the situation is consumption represented by c.

4. if power positive and tank leveldifference between -2*sensitivity and 0 then it is consumption represented by 'c'.

5. if power positive and tankleveldiff positive then it's the situation of idle represented by 'i'.

6. if power equal NULL and tankleveldifference between -*sensitivity and -2*sensitivity then it is idle represented by 'i'.

7. if fuelrate<max consumption rate then it is the situation of theft represented by 't'.

The output of the algorithm will construct the situation column in dataframe then after aplying the regular expressions on the string taken from situation table we get the events and we construct the dataframe 'events' which contains:

"SiteId, DeviceId, SiteKey, Event Flag, StartDate, EndDate, Quantity of Fuel Changed and Average Temperature.
