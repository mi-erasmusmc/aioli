FAERS DRUG STANDARDIZER (AIOLI)
=======================

**The FAERS drug standardizer is a tool to be used in combination with AEOLUS for a larger and more accurate mapping of
the FEARS data to RxNORM CUIs and OMOP standard ingredient concept ids. It also allows for standardization to ATC rather
than RxNorm. Principally it can fully replace the AEOLUS drug_mapping scripts.**

Note:
The master branch of this project should work, but the project as whole is still under construction. Contributions,
comments and suggestions are very much welcome.

### REQUIREMENTS

- Java and Maven

- Aioli builds on the [AEOLUS](https://github.com/mi-erasmusmc/faersdbstats) system, so once you have used that to load
  the FEARS data up until the drug mapping section you can use this app to do the mapping, and then use the AEOLUS again
  for the final stats calculations.

- You will need the RxNORM database, in particular the RXNCONSO and RXNREL tables. The db is freely available
  [here](https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html). The app expects the tables to be located
  in the same db as the aeolus data and located in a schema named rxnorm

- To map towards OMOP concept ids:
    - A database containing the RxNORM vocabulary from [OHDSI](https://athena.ohdsi.org/vocabulary/list). The app
      expects this database to be located in the same db in a schema called staging_vocabulary (this is in line with the
      CEM information prep step, that this app was originally built to be used for)

- To map towards ATC you need a conversion table which is available on request

### HOW TO RUN

- Make sure the db is configured in accordance to your needs in the config.properties file located in scr/main/resources

- Use `mvn clean package assembly:single` in the present folder to build the app
- Run
  with `java -jar target/ailoi_java-1.0-SNAPSHOT-jar-with-dependencies.jar -tv <target vocabulary> -rmin <true or false>`

- Running time is approx 5 - 15 hours depending on target vocab (ATC slowest, RXNORM fastest) and the specs of your
  system.

### OPTIONS

*--target-vocab -tv* The vocabulary you wish to map to valid options are ATC, RXNORM and OMOP

*--retain-multi-ingredients -rmin* In the original mapping section of AEOLUS the idea was to include multi-ingredient
drugs in the roll up, the present implementation allows you to split the multi-ingredient drugs to single ingredient
entries if you please (This option only makes a difference when targeting RXNORM or OMOP).

*--skip-normalizer -sn* Whether you wish to use the RxNorm API, in order to run this you need to run the
RxNav-in-a-Box locally on port 4000. Otherwise, we overload the RxNav
servers https://lhncbc.nlm.nih.gov/RxNav/applications/RxNav-in-a-Box.html

  
