-- name: create_demo

truncate table faers.indi_legacy;


CREATE TABLE faers.demo_legacy
(
    ISR              INTEGER,
    "CASE"           INTEGER,
    I_F_COD          CHAR,
    FOLL_SEQ         VARCHAR,
    IMAGE            VARCHAR,
    EVENT_DT         INTEGER,
    MFR_DT           INTEGER,
    FDA_DT           INTEGER,
    REPT_COD         VARCHAR,
    MFR_NUM          VARCHAR,
    MFR_SNDR         VARCHAR,
    AGE              INTEGER,
    AGE_COD          VARCHAR,
    GNDR_COD         CHAR,
    E_SUB            CHAR,
    WT               INTEGER,
    WT_COD           VARCHAR,
    REPT_DT          INTEGER,
    OCCP_COD         VARCHAR,
    DEATH_DT         VARCHAR,
    TO_MFR           CHAR,
    CONFID           CHAR,
    REPORTER_COUNTRY VARCHAR
);