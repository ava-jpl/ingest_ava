## Ingest AVA
Ingests AVA Products
----
There are 2 associated jobs:
- Ingest - AVA Metadata
- Ingest - AVA Products from Metadata

### Ingest - AVA Metadata
-----
Job is of type individual. It scrapes the AVA, as well as the CMR, and generates MET-AST_09T and MET-AST_L1B products that contain AVA urls, to allow for localization directly from the AVA without ordering.

### Ingest - AVA Product from Metadata
Job is of type iteration. It takes in an input MET-AST_09T or MET-AST_L1B product. It localizes and publishes the associated product from the AVA, using CMR metadata, provided the metadata.on_ava flag is True, and the metadata.ava_url field is filled and valid.


product specs are the followingc:

    MET-AST_<09T,L1B>-<sensing_start_datetime>_<sensing_end_datetime>-<version_number>

    AST_<09T,L1B>-<sensing_start_datetime>_<sensing_end_datetime>-<version_number>
