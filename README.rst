========
Python Power Service and Client
========

Package generated to interface with a trading system. Augmented with a prototypical client

Package will return a random set of open trades for a given day, where each trade contains data for hours, trading volume, and an unique ID.
Client will process the data aggregating them on an hourly base after data validation and QC

Note: The package will naively return series for a given date and not take into account for example if the date is in the future.
Further the numbers for the volume are random and do not have any relationship with previous or subsequent numbers as would normally be the case in real data.

Installation
============
Through a terminal navigate to the folder you have the powerservice folder and run

::

    pip install ./python-powerservice


Documentation
=============

The service will be part of the python environment and can be called in code
::

    from powerservice import trading

Example that will output some trades
::
    from powerservice import trading
    trades = trading.get_trades("29/06/2021")

    print(trades)

Example that will process some trades
::
    from powerservice import trading
    from powerservice import client
    trade_date = "29/06/2021"
    output_path = "/tmp/"

    trades = trading.get_trades(trade_date)

    # Configure and instantiate the processor
    processor = PetroineosChallenge(
        client.DataValidator(),
        client.MapReduce(),
        client.PersistenceUnit(output_path))

    # process data
    processor.process(trades, trade_date)

    # NOTE: The data are persisted according to the persistence unit injected
    # In this case they are saved on the filesystem
    # in the location specified by  output_path




Excercise
=========

The exercise requires to aggregate on hourly basis the data volumes provided by timeseries generator on a 5 minutes tick.
Additionally we are required to provide a data profiling and a data quality report.

Considerations
--------------
Without a properly defined business domain context, the requirements provided are not sufficient to
fully qualify the problem.
In a real worlds setup one would consider contacting the stakeholders for clarifications.
In this exercise it is not possible to interact with the stakeholders.
Therefore, one is forced to make some assumptions and justify them.

Input Data Structure
--------------------
The generator provides a list containing a random number of timeseries.
According to the identifier we can assume each time-series relates to the same trade.
Moreover, it is not clear what instrument does each time-series represent,
thus if they can be added together or not.
For sake of exercise we would consider those time-series homogeneous and handle them
as if they were all trades for the same instrument.
Extra to that, having so may data point is quite unusual for a single trade in the most common asset classes.
I.e. in a real world setup one might expect a time-series to be built of many trades with different IDs per trade
Possibly the same id mapping to a small number of  different events e.g. in case of partial fills.
Given there are no strong conditions on the id, we simply ignore the case and assume the volume needs to be aggregated
across all valid events in a time interval and across all "trades".
The example provided in the Additional Notes section shows the aggregate volume to be either 150 till 9:00 or 80
after 9:00am. The expected output is not consisten with the example input
Therefore, one should simply assume the volume data need to be added.

Null values
-----------
The generator deliberately sets 1 in 16 time values to NaN, knowing the generating model
allows to interpolate on the timeline and reinstate the missing values.
However, In a real world setup, this condition is not granted and missing values require to be handled adequately.
For sake of this exercise we'll report the row as a data quality issue and mark it for reconciliation
TO note, this approach results in the volume of trades for the  for the day to underestimate the volumes potentially traded as those rows are discarded
We cannot make any strong assertion about the trades aggregated on an hourly basis as we won't be able to impute any of those trades to a particular time slot
The user should consider discarded rows and validate the volumes on a case by case basis.
In real life a datasource with such big data quality issues should be handled with care and assess if it is suitable for purpose

Naming Conventions
------------------
The naming of the files is somehow confusing.
The Filename pattern hints at the fact we'll have a file per hour.
However, the example in the additional notes shows a file per day.
Given the situation we'll produce a file per date with the time part set to teh midnight GMT mapped to london time locale

Timezone Handling
-----------------
The Time Zone requirement is difficult to understand the phrasing is not clear and the description is conflicting
with the examples provided. The original phrasing below

```
All values must be aggregated per hour (local/wall clock time). Note that for a given day, the
actual local start time of the day is 23:00 (11 pm) on the previous day. Local time is London
Local time for the day.
```

should  be interpreted consistently with the usual issue of mapping
timezones on utc time e.g. midnight on British summer time maps to 11pm of the previous day in UTC
Therefore, one can interpret the requirement as

```
The API returns time in the Europe/London timezone,  the output should be returned in UTC timezone.
Therefore during British Summer Time the values returned by the API for midnight is mapped to 23:00 of the previous day
```

Implementation
==============

The implementation is quite trivial. Once the specs are clarified it reduces to a simple ETL process.
One can use a bridge pattern (https://en.wikipedia.org/wiki/Bridge_pattern) with an outer class acting as orchestrator and few handlers implementing the processing steps injected to the constructor.

This approach has multiple advantages. Firts of all the orchestrator is agnostic to the actual implementation of the processing steps.
Moreover, each processing step is self contained and loosely coupled with the rest of the system.
This approach keeps every component very lean,
each component can be tested in isolation,
components can be easily replaced with minor impact on the codebase.
e.g. if we want to persist in a database we just need to inject a new persistence unit writing to a DB.
The same happens on the validation, e.g. if we want to add or modify the validity constraints we can inject a different validator
and it will do the job seamlessly (even filtering for valid rows is carried out by the validator instance using inversion of control)


