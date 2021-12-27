CREATE OR REPLACE PACKAGE MANAGE_PARTITIONS AS
 
/*********************************************************************************
   NAME:    Package - MANAGE_PARTITIONS 
   PURPOSE: This package Automates the process for partition management
      1. Creates new partitions
      2. Grants all privileges on new partition tables created 
      3. Marks old partitions as 'D' Delete which are older than partitions to be retained
      4. Re-creates the view to include new partition tables and exclude old partition tables
      5. Re-compiles all the triggers created on view
      6. Logs all the error into log table
             
  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      07/07/2020   AWS Professional Services   Initial Version

**********************************************************************************/

  /*****
    Type to hold the main table/view privileges
  *****/
  TYPE tab_privs IS TABLE OF ALL_TAB_PRIVS_MADE%rowtype INDEX BY PLS_INTEGER;

  /*****
    Type to hold the Synonym info on the view 
  *****/
  TYPE tab_synonyms IS TABLE OF ALL_SYNONYMS%rowtype INDEX BY PLS_INTEGER;

  /*****
    Type to hold the code of existing triggers on the view
  *****/
  TYPE tab_trigs IS TABLE OF VARCHAR(8000) INDEX BY PLS_INTEGER;


/*********************************************************************************
  NAME:    Procedure - SETUP_PARTITIONS
  PURPOSE: This procedure is to create necessary meta data entry and create partition tables
             
  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      08/24/2020   AWS Professional Services   Initial Version

**********************************************************************************/
  PROCEDURE SETUP_PARTITIONS (
      p_table_owner               VARCHAR2
    , p_table_name                VARCHAR2
    , p_table_alias               VARCHAR2
    , p_partition_column          VARCHAR2
    , p_pkey_column               VARCHAR2
    , p_view_name                 VARCHAR2
    , p_partition_type            VARCHAR2
    , p_start_date                VARCHAR2
    , p_precreate_partitions      NUMBER
    , p_retain_partitions         NUMBER
    , p_manage_partitions         VARCHAR2
    , p_drop_old_partitions       VARCHAR2
    , p_enable_logging            VARCHAR2
  );


/*********************************************************************************
  NAME:    Procedure - MAIN_PROCESS
  PURPOSE: This is the main procedure that is to be called to run the automation process
             
  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      07/07/2020   AWS Professional Services   Initial Version

**********************************************************************************/

  PROCEDURE MAIN_PROCESS (p_meta_data_id  NUMBER DEFAULT NULL);

END MANAGE_PARTITIONS;
/
