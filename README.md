Dask Example Notebooks
======================

This repository includes easy-to-run example notebooks for Dask.
They are intended to be educational and give users a start on common workflows.

They should be easy to run locally if you download this repository.
They are also available on the cloud by clicking on the link below:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/dask/dask-examples/master)
[![Build Status](https://travis-ci.org/dask/dask-examples.svg?branch=master)](https://travis-ci.org/dask/dask-examples)


Contributing
------------

This repository is a great opportunity to start contributing to Dask.
Please note that examples submitted to this repository should follow these
guidelines:

1.  Run top-to-bottom without intervention from the user
2.  Not require external data sources that may disappear over time
    (external data sources that are highly unlikely to disappear are fine)
3.  Not be resource intensive, and should run within 2GB of memory
4.  Be clear and contain enough prose to explain the topic at hand
5.  Be concise and limited to one or two topics, such that a reader can
    get through the example within a few minutes of reading
6.  Be of general relevance to Dask users, and so not too specific on a
    particular problem or use case

    As an example "how to do dataframe joins" is a great topic while "how to
    do dataframe joins in the particular case when one column is a categorical
    and the other is object dtype" is probably too specific
