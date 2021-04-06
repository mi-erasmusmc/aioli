FAERS DRUG STANDARDIZER (AIOLI)
=======================

**The FAERS drug standardizer is a tool to be used in combination with AEOLUS for a larger and more accurate mapping of
the FEARS data to RxNORM CUIs and OMOP standard ingredient concept ids. It also allows for standardization to ATC rather
than RxNorm. Principally it can fully replace the AEOLUS drug_mapping scripts.**

Note:
The master branch of this project should work, but the project as whole is still under construction. Contributions,
comments and suggestions are very much welcome.

### REQUIREMENTS

- Aioli builds on the [AEOLUS](https://github.com/mi-erasmusmc/faersdbstats) system, so once you have used that to load
  the FEARS data up until the drug mapping section you can use this app to do the mapping, and then use the AEOLUS again
  for the final stats calculations.

- You will need the RxNORM database, in particular the RXNCONSO table. It is available
  [here](https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html). The app expects the table to be located in
  your faers schema with the name rxnconso

- A database containing the RxNORM vocabulary from [OHDSI](https://athena.ohdsi.org/vocabulary/list). The app expects
  this database to be located in the same db in a schema called staging_vocabulary (this is in line with the CEM
  information prep step, that this app was originally built to be used for)

- To build and run the app you will need [Rust](www.rust-lang.org)

- To make use of the feature to map to ATC codes you need to:
    - Load
      the [Article 57 Data](https://www.ema.europa.eu/en/human-regulatory/post-authorisation/data-medicines-iso-idmp-standards/public-data-article-57-database)
      from the xls file available there copy *name*, *ingredient* and *route* columns into a table called *article* (
      TODO: automate this step)
    - Obtain the mapping from RxNorm to ATC being developed at the ErasmusMC Medical Informatics department, get in
      touch for more details.

### HOW TO RUN

- Make sure the db is configured in accordance to your needs in the Settings.toml file.

- Use `cargo run --release` in the present folder to build and run the app (it runs for several hours, see options below
  for a faster run)

### OPTIONS

There are three settings that you can configure in the Settings.toml file.

1. *skip_normalizer* You can skip calling the RxNormalizer, the RxNormalizer is an API that allows to find RxNorm ids
   based on a string and accounts for common typos and alternative spellings, pulling all 600.000 unique strings in the
   FAERS data through this api takes over a day (currently we do only the 60.000 most common strings, to save some
   time (can change the limit in the rxnormalizer.sql file)) (rxnav throttle the amount of request to 20 per second,
   there is a docker image you could download and call as fast your computer can handle in stead, but loading that image
   also take a while). Skipping the api calls will still give you a pretty decent amount of matches.

2. *retain_multi* In the original mapping section of AEOLUS the idea was to include multi-ingredient drugs in the roll
   up, the present implementation splits the multi-ingredient drugs to single ingredient entries (you can still find the
   multi-ingredient entries in the mapping before they are converted to 'standardized' ids).

3. *use_atc* Set this option to true to normalize the drugs to ATC codes. This feature is still **experimental**

#### TODO:

- Add a configuration for rxnav limit.
- Replace aeolus in its entirety (with ability to update your existing data, with single command and without doing all
  the whole downloading and mapping steps from scratch).


  