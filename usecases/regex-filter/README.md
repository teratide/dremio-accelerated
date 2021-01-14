# Usecase: Regular Expression Filter

In this usecase, the evaluation of a regular expression filter is offloaded to FPGA.

> :warning: This usecase is currently a work in progress. Content may be incomplete and/or subject to change.

## Background

A SQL query, which includes a regular expression, is run on the 
[Chicago Taxi Dataset](https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew). 
The dataset is first converted to parquet such that it can be efficiently read by Dremio. 
The dataset contains 23 columns, but in this usecase we are only interested in the `Trip_Seconds` 
and `Company` columns.

TODO: Check these types

`Trip_Seconds` contains the duration of a given taxi trip in 32-bit integer values. The `Company` column 
contains the name of the company that carried out the trip as an UTF8 string.

Lets say we are interested in knowing the total duration of all taxi trips as carried out by a company 
whose name consists of two separate words. In that case we could construct the following SQL query:

    SELECT SUM(Trip_Seconds) FROM "taxi-trips.parquet" WHERE REGEXP_LIKE(Company, '\b[a-zA-Z]+\s[a-zA-Z]+\b')

Letting the Sabot engine perform it's planning phases for this query, the operators of the physical plan are
given by the figure below.

![Filter Operators](filter-operators.png)

The filter operator is where this regular expression matching takes place. This can be seen by further inspecting the `condition` property on the filter operator.

    condition = {
        op = "REGEXP_LIKE",
        operands = [
            "$1",
            "\b[a-zA-Z]+\s[a-zA-Z]+\b"
        ],
        type = "BOOLEAN",
        digest = "REGEXP_LIKE($1, '\b[a-zA-Z]+\s[a-zA-Z]+\b')"
    }

## Dremio Accelerated Setup

## Results
