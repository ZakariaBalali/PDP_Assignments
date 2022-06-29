-- Load orders.csv
orders = LOAD '/user/maria_dev/diplomacy/orders.csv'  USING PigStorage(',')AS
	(game_id:chararray,
    unit_id:chararray,
    unit_order:chararray,
    location:chararray,
    target:chararray,
    target_dest:chararray,
    success:chararray,
    reason:chararray,
    turn_num:chararray);

--We only want to target Holland, so we create a filtered list
hollandList = Filter orders BY target == '"Holland"';

--Group orders by location
hollandListByLocation = GROUP hollandList BY (location, target);

--Counts the amount of holland targettings per group
hollandListByLocationCount = FOREACH hollandListByLocation GENERATE group, COUNT(hollandList);          

--Sort location alphabetically (Ascending)
orderedLocationList = ORDER hollandListByLocationCount BY $0 ASC;

--Show results
DUMP orderedLocationList;
