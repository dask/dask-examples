Dask Examples
=============

These examples show how to use Dask in a variety of situations.

First, there are some high level examples about various Dask APIs like arrays,
dataframes, and futures, then there are more in-depth examples about
particular features or use cases.

You can run these examples in a live session here: |Binder|

.. |Binder| image:: https://mybinder.org/badge.svg
   :target: https://mybinder.org/v2/gh/dask/dask-examples/master?urlpath=lab


.. toctree::
   :maxdepth: 1
   :caption: Basic Examples:

   array
   bag
   dataframe
   delayed
   futures
   machine-learning
   xarray

.. toctree::
   :maxdepth: 1
   :caption: Dataframes:

   dataframes/01-data-access
   dataframes/02-groupby

.. toctree::
   :maxdepth: 1
   :caption: Machine Learning:

   machine-learning/scale-scikit-learn
   machine-learning/parallel-prediction
   machine-learning/torch-prediction
   machine-learning/training-on-large-datasets
   machine-learning/incremental
   machine-learning/text-vectorization
   machine-learning/hyperparam-opt.ipynb
   machine-learning/xgboost
   machine-learning/voting-classifier
   machine-learning/tpot
   machine-learning/glm
   machine-learning/svd

.. toctree::
   :maxdepth: 1
   :caption: Applications:

   applications/json-data-on-the-web
   applications/async-await
   applications/async-web-server
   applications/embarrassingly-parallel
   applications/evolving-workflows
   applications/image-processing
   applications/prefect-etl
   applications/satellite-imagery-geotiff
   applications/stencils-with-numba

.. toctree::
   :maxdepth: 1
   :caption: User Surveys:

   surveys/2019.ipynb

.. raw:: html

   <!-- Google Analytics -->
   <script>
   (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
   (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
   m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
   })(window,document,'script','https://www.google-analytics.com/analytics.js','ga');

   ga('create', 'UA-18218874-5', 'auto');
   ga('send', 'pageview');
   </script>
   <!-- End Google Analytics -->
