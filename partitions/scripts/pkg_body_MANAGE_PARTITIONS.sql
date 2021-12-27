CREATE OR REPLACE PACKAGE BODY MANAGE_PARTITIONS AS

/*********************************************************************************
   NAME:    Package - MANAGE_PARTITIONS 
   PURPOSE: This package Automates the process for partition management
      1. Creates new partitions
      2. Grants all privileges on new partition tables created 
      3. Marks old partitions as 'D' Delete which are older than partitions to be retained
      4. Re-creates the view to include new partition tables and exclude old partition tables
      5. Re-compiles all the triggers created on view
      6. Logs all the error into log table
             
  Ver    Date         Author                  		Description
  -----  ----------   -------------           		-----------------------
  1      07/07/2020   AWS Professional Services   	Initial Version
  1.1    08/03/2020   AWS Professional Services   	Procedures to create the INSER/UPDATE/DELETE triggers on view dynamically
													Procedures: (CREATE_INS_TRG, CREATE_UPD_TRG, CREATE_DEL_TRG)
  1.2    08/28/2020   AWS Professional Services   	Procedure to execute all necessary pre-steps required for partition automation
													Procedure: SETUP_PARTITIONS
  1.3    12/13/2020   AWS Professional Services   Modified for changing the column names as per review feedback 
													(PRIMARY_TABLE_OWNER -> TABLE_OWNER)
													(PRIMARY_TABLE_NAME -> TABLE_NAME)
													(PRIMARY_TABLE_SHORTNAME -> TABLE_ALIAS)
													(PRIMARY_VIEW_NAME -> VIEW_NAME)

**********************************************************************************/



/*********************************************************************************
  NAME:    Procedure - WRITE_TO_LOG
  PURPOSE: This procedure is used to log errors during the process
           Also logs the events during the process as debug if the ENABLE_LOGGING is set to 'Y'

  Parameters:
    p_log_type          Indicates ERROR or DEBUG
    p_meta_data_id      Partition main table unique identifier
    p_object_name       Object for which the log record is being created
    p_is_log_enabled    Flag to log the activity

             
  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      07/07/2020   AWS Professional Services   Initial Version

**********************************************************************************/
  PROCEDURE WRITE_TO_LOG (
    p_log_type           VARCHAR2,
    p_meta_data_id       NUMBER,
    p_object_name        VARCHAR2,
    p_log_msg            VARCHAR2
  )
  AS 
    PRAGMA AUTONOMOUS_TRANSACTION;

  BEGIN

    INSERT INTO PARTITION_LOG (LOG_TYPE, META_DATA_ID, OBJECT_NAME, LOG_MESSAGE)
    VALUES (p_log_type, p_meta_data_id, p_object_name, SUBSTR(p_log_msg, 1, 4000));
    
    COMMIT;

  END WRITE_TO_LOG;


/*********************************************************************************
  NAME:    Function - GET_OBJECT_PRIVILEGES
  PURPOSE: This function retrieves all the privileges on the given object and
            returns the same as collection

  Parameters:
    p_meta_data_id      Partition main table unique identifier
    p_table_owner        Owner of the partition tables
    p_object_name       Object for which the existing privileges are to be retrieved
    p_is_log_enabled    Flag to log the activity

             
  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      07/07/2020   AWS Professional Services   Initial Version

**********************************************************************************/

  FUNCTION GET_OBJECT_PRIVILEGES (
    p_meta_data_id      NUMBER,
    p_table_owner       VARCHAR2,
    p_object_name       VARCHAR2,
    p_is_log_enabled    BOOLEAN
  )
  RETURN tab_privs
  AS 
    t_privs           tab_privs;
  
  BEGIN

    DBMS_OUTPUT.PUT_LINE('----->>> START: GET_OBJECT_PRIVILEGES. meta_data_id: ' || p_meta_data_id || ', OWNER: ' || p_table_owner || ', OBJECT: ' || p_object_name);

    IF p_is_log_enabled THEN
      WRITE_TO_LOG ('DEBUG', p_meta_data_id, p_object_name, 'Getting privileges for ' || p_object_name);
    END IF;
    
    SELECT * 
      BULK COLLECT INTO t_privs
    FROM ALL_TAB_PRIVS_MADE 
    WHERE OWNER = p_table_owner
    AND TABLE_NAME = p_object_name;
  
    DBMS_OUTPUT.PUT_LINE('----->>> END: GET_OBJECT_PRIVILEGES. meta_data_id: ' || p_meta_data_id || ', OWNER: ' || p_table_owner || ', OBJECT: ' || p_object_name);

    RETURN t_privs;

  END GET_OBJECT_PRIVILEGES;



/*********************************************************************************
  NAME:    Procedure - GRANT_OBJECT_PRIVILEGES
  PURPOSE: This procedure grants all the privileges to the given object 

  Parameters:
    p_meta_data_id      Partition main table unique identifier
    p_object_privs      Collection of all privileges on main table or view got using GET_OBJECT_PRIVILEGES
    p_object_name       Object to which the privileges have to be granted
    p_is_log_enabled    Flag to log the activity

             
  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      07/07/2020   AWS Professional Services   Initial Version

**********************************************************************************/
  PROCEDURE GRANT_OBJECT_PRIVILEGES (
    p_meta_data_id      NUMBER,
    p_object_privs      tab_privs,
    p_object_name       VARCHAR2,
    p_is_log_enabled    BOOLEAN
  )
  AS
    v_sql           VARCHAR2(2000);
   
  BEGIN

    DBMS_OUTPUT.PUT_LINE('----->>> START: GRANT_OBJECT_PRIVILEGES. meta_data_id: ' || p_meta_data_id || ', OBJECT: ' || p_object_name);

    FOR i IN 1..p_object_privs.COUNT
    LOOP

    v_sql := 'GRANT ' || p_object_privs(i).PRIVILEGE || ' ON ' ||  p_object_privs(i).OWNER || '.' || p_object_name
         || ' TO ' || p_object_privs(i).GRANTEE;

    IF p_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', p_meta_data_id, p_object_name, 'Granting privilege - SQL: ' || v_sql);
    END IF;
   
    EXECUTE IMMEDIATE v_sql;
   
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('----->>> END: GRANT_OBJECT_PRIVILEGES. meta_data_id: ' || p_meta_data_id || ', OBJECT: ' || p_object_name);

  END GRANT_OBJECT_PRIVILEGES;


/*********************************************************************************
  NAME:    Function - GET_SYNONYMS
  PURPOSE: This function retrieves all the synonyms on the given object and returns the same as collection

  Parameters:
    p_meta_data_id      Partition main table unique identifier
    p_table_owner       Owner of the object
    p_object_name       Object name for which the synonyms are to be retrieved
    p_is_log_enabled    Flag to log the activity

             
  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      07/07/2020   AWS Professional Services   Initial Version

**********************************************************************************/

  FUNCTION GET_SYNONYMS (
    p_meta_data_id      NUMBER,
    p_table_owner       VARCHAR2,
    p_object_name       VARCHAR2,
    p_is_log_enabled    BOOLEAN
  )
  RETURN tab_synonyms
  AS
    t_synonyms        tab_synonyms;
  
  BEGIN

    DBMS_OUTPUT.PUT_LINE('----->>> START: GET_SYNONYMS. meta_data_id: ' || p_meta_data_id || ', OWNER: ' || p_table_owner || ', OBJECT: ' || p_object_name);

    IF p_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', p_meta_data_id, p_object_name, 'Getting Synonyms for: ' || p_object_name);
    END IF;

    SELECT * 
      BULK COLLECT INTO t_synonyms
    FROM ALL_SYNONYMS 
    WHERE TABLE_OWNER = p_table_owner
    AND TABLE_NAME = p_object_name;
  
    DBMS_OUTPUT.PUT_LINE('----->>> END: GET_SYNONYMS. meta_data_id: ' || p_meta_data_id || ', OWNER: ' || p_table_owner || ', OBJECT: ' || p_object_name);

    RETURN t_synonyms;

  END GET_SYNONYMS;



/*********************************************************************************
  NAME:    Procedure - CREATE_SYNONYMS
  PURPOSE: This procedure recreates all the synonyms using the data in p_object_synonyms collection

  Parameters:
    p_meta_data_id      Partition main table unique identifier
    p_object_synonyms   Collection if synonyms
    p_is_log_enabled    Flag to log the activity

             
  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      07/07/2020   AWS Professional Services   Initial Version

**********************************************************************************/

  PROCEDURE CREATE_SYNONYMS (
    p_meta_data_id        NUMBER,
    p_object_synonyms     tab_synonyms,
    p_is_log_enabled      BOOLEAN
  )
  AS
    v_sql                 VARCHAR2(2000);
    v_synonym             VARCHAR2(64);
   
  BEGIN

    DBMS_OUTPUT.PUT_LINE('----->>> START: CREATE_SYNONYMS. meta_data_id: ' || p_meta_data_id);

    FOR i IN 1..p_object_synonyms.COUNT
    LOOP
    
      v_synonym := p_object_synonyms(i).SYNONYM_NAME;

     IF p_object_synonyms(i).OWNER = 'PUBLIC' THEN
     
      v_sql := 'CREATE OR REPLACE PUBLIC SYNONYM  ' || p_object_synonyms(i).SYNONYM_NAME 
            || ' FOR ' ||  p_object_synonyms(i).TABLE_OWNER || '.' || p_object_synonyms(i).TABLE_NAME;

     ELSE
     
      v_sql := 'CREATE OR REPLACE SYNONYM  ' || p_object_synonyms(i).SYNONYM_NAME 
            || ' FOR ' ||  p_object_synonyms(i).TABLE_OWNER || '.' || p_object_synonyms(i).TABLE_NAME;

     END IF;
   
    IF p_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', p_meta_data_id, p_object_synonyms(i).TABLE_NAME, 'Creating Synonym: ' || v_sql);
    END IF;
   
    EXECUTE IMMEDIATE v_sql;
   
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('----->>> END: CREATE_SYNONYMS. meta_data_id: ' || p_meta_data_id);

  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error while creating Synonym : ' || v_synonym);
      DBMS_OUTPUT.PUT_LINE('SQLERRM : ' || SQLERRM);
      WRITE_TO_LOG ('ERROR', p_meta_data_id, v_synonym, SQLERRM);
      RAISE;
    
  END CREATE_SYNONYMS;



/*********************************************************************************
  NAME:    Procedure - DISABLE_OLD_PARTITIONS
  PURPOSE: This procedure updates the status to 'D' to all old partition tables
           which are older than partitions to be retained 
             
  Parameters:
    p_meta_data_id           Partition main table unique identifier
    p_table_owner             Owner of the partition tables
    p_partition_type         Type of partition
    p_num_retain_partitions  Number of old partitions to be retained
    p_is_log_enabled         Flag to log the activity

  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      07/07/2020   AWS Professional Services   Initial Version

**********************************************************************************/
  PROCEDURE DISABLE_OLD_PARTITIONS (
    p_meta_data_id            NUMBER,
    p_table_owner             VARCHAR2,
    p_partition_type          VARCHAR2,
    p_num_retain_partitions   NUMBER,
    p_is_log_enabled          BOOLEAN
  )
  AS
     v_view_sql           VARCHAR2(4000);
     v_view_cnt           NUMBER;
     v_last_partition_dt  DATE;
   
  BEGIN

    DBMS_OUTPUT.PUT_LINE('----->>> START: DISABLE_OLD_PARTITIONS. meta_data_id: ' || p_meta_data_id);

    IF p_partition_type = 'Y' THEN

      v_last_partition_dt := TRUNC(TO_DATE(EXTRACT(YEAR FROM SYSDATE) - p_num_retain_partitions, 'RRRR'), 'YEAR');
       
    ELSIF p_partition_type = 'M' THEN

      v_last_partition_dt := ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -p_num_retain_partitions);

    ELSIF p_partition_type = 'W' THEN

      v_last_partition_dt := (TRUNC(SYSDATE, 'IW')-1) - (p_num_retain_partitions * 7);

    ELSIF p_partition_type = 'D' THEN

      v_last_partition_dt := TRUNC(SYSDATE) - p_num_retain_partitions;

    END IF;
     
    DBMS_OUTPUT.PUT_LINE('----->>> DISABLE_OLD_PARTITIONS. v_last_partition_dt: ' || v_last_partition_dt);
  
    UPDATE PARTITION_TABLE_DATA 
       SET status = 'D' 
     WHERE meta_data_id = p_meta_data_id
       AND status = 'A' 
       AND partition_date_value < v_last_partition_dt;

    DBMS_OUTPUT.PUT_LINE('----->>> END: DISABLE_OLD_PARTITIONS. meta_data_id: ' || p_meta_data_id);

  END DISABLE_OLD_PARTITIONS;



/*********************************************************************************
  NAME:    Procedure - DROP_OLD_PARTITIONS
  PURPOSE: This procedure drops old partition tables with status = 'D' 
             and DROP_OLD_PARTITIONS flag is set to 'Y'
           Status is updated with 'D' by function 'DISABLE_OLD_PARTITIONS' 
             for partitions which are older than partitions to be retained

  Parameters:
    p_meta_data_id         Partition main table unique identifier
    p_table_owner           Owner of the partition tables
    p_is_log_enabled       Flag to log the activity

             
  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      07/07/2020   AWS Professional Services   Initial Version

**********************************************************************************/

  PROCEDURE DROP_OLD_PARTITIONS (
    p_meta_data_id          NUMBER,
    p_table_owner           VARCHAR2,
    p_is_log_enabled        BOOLEAN
  )
  AS
     v_drop_sql             VARCHAR2(2000);
     v_del_sql              VARCHAR2(2000);
   
  BEGIN

    DBMS_OUTPUT.PUT_LINE('----->>> START: DROP_OLD_PARTITIONS. meta_data_id: ' || p_meta_data_id);
  
    FOR rec IN (
      SELECT partition_table_name 
        FROM PARTITION_TABLE_DATA 
       WHERE meta_data_id = p_meta_data_id 
         AND status = 'D' 
    )
    LOOP

      BEGIN

      v_drop_sql := 'DROP TABLE ' || p_table_owner || '.' || rec.partition_table_name;

      IF p_is_log_enabled THEN      
        WRITE_TO_LOG ('DEBUG', p_meta_data_id, rec.partition_table_name, 'Dropping partition table: ' || rec.partition_table_name);
      END IF;
    
      EXECUTE IMMEDIATE v_drop_sql;

      EXCEPTION
         WHEN OTHERS THEN
             DBMS_OUTPUT.PUT_LINE('Error while dropping old partitions on table : ' || rec.partition_table_name);
             DBMS_OUTPUT.PUT_LINE('SQLERRM : ' || SQLERRM);
             WRITE_TO_LOG ('ERROR', p_meta_data_id, rec.partition_table_name, SQLERRM);
      END;

      BEGIN
      v_del_sql := 'DELETE FROM PARTITION_TABLE_DATA WHERE meta_data_id = ' || p_meta_data_id || ' AND partition_table_name = ''' || rec.partition_table_name || '''';
    
      IF p_is_log_enabled THEN      
          WRITE_TO_LOG ('DEBUG', p_meta_data_id, rec.partition_table_name, 'Delete record from PARTITION_TABLE_DATA for table: ' || rec.partition_table_name);
      END IF;

      EXECUTE IMMEDIATE v_del_sql;
  
      EXCEPTION
      WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('Error while dropping old partitions for  META_DATA_ID: ' || p_meta_data_id);
         DBMS_OUTPUT.PUT_LINE('SQLERRM : ' || SQLERRM);
         WRITE_TO_LOG ('ERROR', p_meta_data_id, rec.partition_table_name, SQLERRM);
      END;
  
    END LOOP;
  
    DBMS_OUTPUT.PUT_LINE('----->>> END: DROP_OLD_PARTITIONS. meta_data_id: ' || p_meta_data_id);

  END DROP_OLD_PARTITIONS;


/*********************************************************************************
  NAME:    Procedure - CREATE_DEL_TRG
  PURPOSE: This procedure creates INSTEAD OF DELETE trigger on the given view

  Parameters:
    p_meta_data_id      Partition main table unique identifier
    p_table_owner       Owner of the partition table
    p_table_name        Partition main table name
    p_partition_column  The column used for partitioning
    p_pkey_column       The primary key column of the main partition table
    p_view_name         Name of the view for which the triggers are to be retrieved
    p_partition_type    Type of partition (Y/M/W/D)
    p_is_log_enabled    Flag to log the activity

             
  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      08/03/2020   AWS Professional Services   Initial Version

**********************************************************************************/
  PROCEDURE CREATE_DEL_TRG (
      p_meta_data_id        NUMBER
    , p_table_owner         VARCHAR2
    , p_table_name          VARCHAR2
    , p_partition_column    VARCHAR2
    , p_pkey_column         VARCHAR2
    , p_view_name           VARCHAR2
    , p_partition_type      VARCHAR2
    , p_is_log_enabled      BOOLEAN
  )
  AS 
    trg_head                VARCHAR2(2000);
    del_stmt                VARCHAR2(4000);
    del_def_stmt            VARCHAR2(4000);
    stmt                    VARCHAR2(8000);
    exp_sql                 VARCHAR2(8000);
    part_dt_fmt             VARCHAR2(10);    

  BEGIN


    DBMS_OUTPUT.PUT_LINE('----->>> START: Creating INSTEAD OF DELETE trigger on: ' || p_view_name);

    IF p_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', p_meta_data_id, p_view_name, 'Creating INSTEAD OF DELETE trigger');
    END IF;

    trg_head := 'CREATE OR REPLACE TRIGGER TRG_' || p_view_name || '_DEL  INSTEAD OF DELETE ON ' || p_view_name || ' FOR EACH ROW ' || CHR(10) ||  
        ' DECLARE ' || CHR(10) || 
        '  v_part_val VARCHAR2(32); ' || CHR(10) || 
        '  v_part_table VARCHAR2(64); ' || CHR(10) || 
        '  table_not_found EXCEPTION;  ' || CHR(10) || 
        '  PRAGMA EXCEPTION_INIT (table_not_found, -942);  ' || CHR(10) || 
        ' BEGIN ' || CHR(10) || CHR(10);

     IF p_partition_type = 'Y' THEN
        part_dt_fmt := 'RRRR';
     ELSIF p_partition_type = 'M' THEN
        part_dt_fmt := 'RRRRMM'; 
     ELSIF p_partition_type = 'W' THEN
        part_dt_fmt := 'RRRRMMDD';
     ELSIF p_partition_type = 'D' THEN
        part_dt_fmt := 'RRRRMMDD';
     END IF; 
     
    IF p_partition_type = 'W' THEN
		 trg_head := trg_head || 
			'   v_part_val := TO_CHAR(TRUNC(:OLD.' || p_partition_column || ', ''IW'') - 1,''' || part_dt_fmt || '''); ' || CHR(10) || 
			'   v_part_table := ''' || p_table_name || '_'' || v_part_val; ' || CHR(10) || CHR(10);
        
    ELSE
		 trg_head := trg_head || 
			'   v_part_val := TO_CHAR(:OLD.' || p_partition_column || ',''' || part_dt_fmt || '''); ' || CHR(10) || 
			'   v_part_table := ''' || p_table_name || '_'' || v_part_val; ' || CHR(10) || CHR(10);
    END IF;
        
     del_stmt := ' EXECUTE IMMEDIATE '' DELETE FROM ' || p_table_owner || '.'' ||  v_part_table || '' WHERE ' || p_pkey_column || ' = :' || p_pkey_column || '''' || 
                ' USING :OLD.' || p_pkey_column || ';';
    
     del_def_stmt := ' EXECUTE IMMEDIATE  '' DELETE FROM ' || p_table_owner || '.' ||  p_table_name || '_DEFAULT WHERE ' || p_pkey_column || ' = :' || p_pkey_column || '''' || 
                ' USING :OLD.' || p_pkey_column || ';';
    
    
     exp_sql := CHR(10) || '  EXCEPTION ' || CHR(10) || 
               '     WHEN table_not_found THEN '  || CHR(10) ||
               '        ' || del_def_stmt;

     stmt := trg_head || del_stmt || CHR(10) || exp_sql || CHR(10) || ' END;' ;
               
    
     EXECUTE IMMEDIATE stmt;
    
--    DBMS_OUTPUT.PUT_LINE ( stmt);

    IF p_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', p_meta_data_id, p_view_name, 'Creating INSTEAD OF DELETE trigger - Successful');
    END IF;

     DBMS_OUTPUT.PUT_LINE('----->>> END: Creating INSTEAD OF DELETE trigger on: ' || p_view_name);

  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('Error while creating DELETE trigger on: ' || p_view_name);
     DBMS_OUTPUT.PUT_LINE('SQLERRM : ' || SQLERRM);
     WRITE_TO_LOG ('ERROR', p_meta_data_id, p_view_name, SQLERRM);
  END;


/*********************************************************************************
  NAME:    Procedure - CREATE_INS_TRG
  PURPOSE: This procedure creates INSTEAD OF INSERT trigger on the given view

  Parameters:
    p_meta_data_id      Partition main table unique identifier
    p_table_owner       Owner of the partition table
    p_table_name        Partition main table name
    p_partition_column  The column used for partitioning
    p_view_name         Name of the view for which the triggers are to be retrieved
    p_partition_type    Type of partition (Y/M/W/D)
    p_is_log_enabled    Flag to log the activity

             
  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      08/03/2020   AWS Professional Services   Initial Version

**********************************************************************************/
  PROCEDURE CREATE_INS_TRG(
      p_meta_data_id        NUMBER
    , p_table_owner         VARCHAR2
    , p_table_name          VARCHAR2
    , p_partition_column    VARCHAR2
    , p_view_name           VARCHAR2
    , p_partition_type      VARCHAR2
    , p_is_log_enabled      BOOLEAN
  ) 
  AS 
    trg_head                VARCHAR2(2000);
    ins_cols_def            VARCHAR2(4000);
    ins_cols                VARCHAR2(4000);
    i_cols                  VARCHAR2(4000);
    ins_vals                VARCHAR2(4000);
    exe_stmt                VARCHAR2(4000);
    stmt                    VARCHAR2(8000);
    
    exp_sql                 VARCHAR2(8000);
    part_dt_fmt             VARCHAR2(10);    

  BEGIN

    DBMS_OUTPUT.PUT_LINE('----->>> START: Creating INSTEAD OF INSERT trigger on: ' || p_view_name);

    IF p_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', p_meta_data_id, p_view_name, 'Creating INSTEAD OF INSERT trigger');
    END IF;

    trg_head := 'CREATE OR REPLACE TRIGGER TRG_' || p_view_name || '_INS  INSTEAD OF INSERT ON ' || p_view_name || ' FOR EACH ROW ' || CHR(10) ||  
        ' DECLARE ' || CHR(10) || 
        '  v_part_val VARCHAR2(32); ' || CHR(10) || 
        '  v_part_table VARCHAR2(64); ' || CHR(10) || 
        '  table_not_found EXCEPTION;  ' || CHR(10) || 
        '  PRAGMA EXCEPTION_INIT (table_not_found, -942);  ' || CHR(10) || 
        ' BEGIN ' || CHR(10) || CHR(10);

     IF p_partition_type = 'Y' THEN
        part_dt_fmt := 'RRRR';
     ELSIF p_partition_type = 'M' THEN
        part_dt_fmt := 'RRRRMM'; 
     ELSIF p_partition_type = 'W' THEN
        part_dt_fmt := 'RRRRMMDD';
     ELSIF p_partition_type = 'D' THEN
        part_dt_fmt := 'RRRRMMDD';
     END IF; 
     
    IF p_partition_type = 'W' THEN
		 trg_head := trg_head || 
			'   v_part_val := TO_CHAR(TRUNC(:NEW.' || p_partition_column || ', ''IW'') - 1,''' || part_dt_fmt || '''); ' || CHR(10) || 
			'   v_part_table := ''' || p_table_name || '_'' || v_part_val; ' || CHR(10) || CHR(10);
        
    ELSE
		 trg_head := trg_head || 
			'   v_part_val := TO_CHAR(:NEW.' || p_partition_column || ',''' || part_dt_fmt || '''); ' || CHR(10) || 
			'   v_part_table := ''' || p_table_name || '_'' || v_part_val; ' || CHR(10) || CHR(10);
    END IF;        
        
    ins_cols := ''' INSERT INTO ' || p_table_owner || '.'' ||  v_part_table || '' VALUES ( '|| CHR(10);
    
    ins_cols_def := ''' INSERT INTO ' || p_table_owner || '.' ||  p_table_name || '_DEFAULT VALUES ( '|| CHR(10); 
    
    ins_vals := ' USING ';
    i_cols := '';
    
    FOR rec in ( SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER = UPPER(p_table_owner) and table_name = UPPER(p_table_name)  ORDER BY COLUMN_ID )
    LOOP
       
      i_cols := i_cols || ' :' || rec.column_name || '  ,';

      ins_vals := ins_vals || ' :NEW.' || rec.column_name || '  ,';
    
    END LOOP;
    
    i_cols := SUBSTR(i_cols, 0, length(i_cols)-2);
    
    ins_cols := ins_cols || i_cols || ')''' || CHR(10);

--    DBMS_OUTPUT.PUT_LINE ('ins_cols: ' || ins_cols );

    ins_cols_def := ins_cols_def || i_cols || ')''' || CHR(10);

--    DBMS_OUTPUT.PUT_LINE ('ins_cols_def: ' || ins_cols_def);


    ins_vals := SUBSTR(ins_vals, 0, length(ins_vals)-2);
    
    exe_stmt := CHR(10) || '   EXECUTE IMMEDIATE ' || ins_cols || ins_vals || ';';
    
    exp_sql := CHR(10) || '  EXCEPTION ' || CHR(10) || 
               '     WHEN table_not_found THEN '  || CHR(10) ||
               '        EXECUTE IMMEDIATE ' || ins_cols_def  || ins_vals || ';';

    stmt := trg_head || exe_stmt || CHR(10) || exp_sql || CHR(10) || ' END;' ;
               
    
    execute IMMEDIATE stmt;
    
--    DBMS_OUTPUT.PUT_LINE ( stmt);

    IF p_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', p_meta_data_id, p_view_name, 'Creating INSTEAD OF INSERT trigger - Successful');
    END IF;

    DBMS_OUTPUT.PUT_LINE('----->>> END: Creating INSTEAD OF INSERT trigger on: ' || p_view_name);

  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('Error while creating INSERT trigger on: ' || p_view_name);
     DBMS_OUTPUT.PUT_LINE('SQLERRM : ' || SQLERRM);
     WRITE_TO_LOG ('ERROR', p_meta_data_id, p_view_name, SQLERRM);

  END;



/*********************************************************************************
  NAME:    Procedure - CREATE_UPD_TRG
  PURPOSE: This procedure creates INSTEAD OF UPDATE trigger on the given view

  Parameters:
    p_meta_data_id      Partition main table unique identifier
    p_table_owner       Owner of the partition table
    p_table_name        Partition main table name
    p_partition_column  The column used for partitioning
    p_pkey_column       The primary key column of the main partition table
    p_view_name         Name of the view for which the triggers are to be retrieved
    p_partition_type    Type of partition (Y/M/W/D)
    p_is_log_enabled    Flag to log the activity

             
  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      08/03/2020   AWS Professional Services   Initial Version

**********************************************************************************/
  PROCEDURE CREATE_UPD_TRG(
      p_meta_data_id        NUMBER
    , p_table_owner         VARCHAR2
    , p_table_name          VARCHAR2
    , p_partition_column    VARCHAR2
    , p_pkey_column         VARCHAR2
    , p_view_name           VARCHAR2
    , p_partition_type      VARCHAR2
    , p_is_log_enabled      BOOLEAN
  )
  AS 
    trg_head                VARCHAR2(2000);
    trg_code                VARCHAR2(24000);
    col_name                VARCHAR2(64);
    col_type                VARCHAR2(32);
    rid_sql                 VARCHAR2(4000);
    exe_stmt                VARCHAR2(4000);
    stmt                    VARCHAR2(8000);
    upd_cols                VARCHAR2(30000);
    v_upd_sql               VARCHAR2(30000);
    exp_sql                 VARCHAR2(8000);
    part_dt_fmt             VARCHAR2(10);    
    
    RID                     VARCHAR2(10) := 'RID';

  BEGIN

    DBMS_OUTPUT.PUT_LINE('----->>> START: Creating INSTEAD OF UPDATE trigger on: ' || p_view_name);

    IF p_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', p_meta_data_id, p_view_name, 'Creating INSTEAD OF UPDATE trigger');
    END IF;

    trg_head := 'CREATE OR REPLACE TRIGGER TRG_' || p_view_name || '_UPD  INSTEAD OF UPDATE ON ' || p_view_name || ' FOR EACH ROW ' || CHR(10) ||  
        ' DECLARE ' || CHR(10) || 
        '  v_part_val VARCHAR2(32); ' || CHR(10) || 
        '  v_part_table VARCHAR2(64); ' || CHR(10) || 
        '  v_upd_sql VARCHAR2(8000); ' || CHR(10) || 
        '  v_rowid_sql VARCHAR2(2000); ' || CHR(10) || 
        '  rid ROWID; ' || CHR(10) || 
        '  table_not_found EXCEPTION;  ' || CHR(10) || 
        '  PRAGMA EXCEPTION_INIT (table_not_found, -942);  ' || CHR(10) || 
        ' BEGIN ' || CHR(10) || CHR(10);

     IF p_partition_type = 'Y' THEN
        part_dt_fmt := 'RRRR';
     ELSIF p_partition_type = 'M' THEN
        part_dt_fmt := 'RRRRMM'; 
     ELSIF p_partition_type = 'W' THEN
        part_dt_fmt := 'RRRRMMDD';
     ELSIF p_partition_type = 'D' THEN
        part_dt_fmt := 'RRRRMMDD';
     END IF; 
     
    IF p_partition_type = 'W' THEN
		 trg_head := trg_head || 
			'   v_part_val := TO_CHAR(TRUNC(:OLD.' || p_partition_column || ', ''IW'') - 1,''' || part_dt_fmt || '''); ' || CHR(10) || 
			'   v_part_table := ''' || p_table_name || '_'' || v_part_val; ' || CHR(10) || CHR(10);
        
    ELSE
		 trg_head := trg_head || 
			'   v_part_val := TO_CHAR(:OLD.' || p_partition_column || ',''' || part_dt_fmt || '''); ' || CHR(10) || 
			'   v_part_table := ''' || p_table_name || '_'' || v_part_val; ' || CHR(10) || CHR(10);
    END IF;

    rid_sql := ' BEGIN ' || CHR(10) || CHR(10);
   
    rid_sql := rid_sql || ' v_rowid_sql := '' SELECT ROWID FROM '' || v_part_table || '' WHERE ' || p_pkey_column || ' = '' || :OLD.' || p_pkey_column || ';' || CHR(10);
   
    rid_sql := rid_sql || ' EXECUTE IMMEDIATE v_rowid_sql INTO rid;'  || CHR(10) || CHR(10);

    rid_sql := rid_sql || ' EXCEPTION '  || CHR(10);
   
    rid_sql := rid_sql || ' WHEN table_not_found THEN '  || CHR(10);

    rid_sql := rid_sql || ' v_part_table := ''' || p_table_name || '_DEFAULT '';'  || CHR(10);

    rid_sql := rid_sql || ' v_rowid_sql := '' SELECT ROWID FROM '' || v_part_table || '' WHERE ' || p_pkey_column || ' = '' || :OLD.' || p_pkey_column || ';' || CHR(10);
   
    rid_sql := rid_sql || ' EXECUTE IMMEDIATE v_rowid_sql INTO rid;'  || CHR(10) || CHR(10);

    rid_sql := rid_sql || ' END;'  || CHR(10) || CHR(10);
           
--      DBMS_OUTPUT.PUT_LINE ('rid_sql: ' || rid_sql );
        
    upd_cols := 'IF :OLD.' ||  p_partition_column || ' <> :NEW.' || p_partition_column || ' THEN  '|| CHR(10);

    upd_cols := upd_cols || ' RAISE_APPLICATION_ERROR(-20001, ''Partition key column ''''' || p_partition_column || ''''' cannot be modified'');'|| CHR(10);

    upd_cols := upd_cols || ' END IF;  ' || CHR(10) || CHR(10);
    
    FOR rec in ( 
        SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE 
        FROM all_tab_columns 
        WHERE owner = UPPER(p_table_owner) and table_name = UPPER(p_table_name)
        ORDER BY COLUMN_ID
    )
    LOOP
       
      col_name := rec.COLUMN_NAME;
      col_type := rec.DATA_TYPE;
      

    upd_cols := upd_cols || 'IF :OLD.' ||  col_name || ' <> :NEW.' || col_name || ' THEN  ' || CHR(10);

      IF col_type = 'NUMBER' THEN

        upd_cols := upd_cols || ' v_upd_sql := v_upd_sql || '', ' || col_name || ' = '' || :NEW.' || col_name || ';' || CHR(10);

      ELSIF col_type IN ('VARCHAR2', 'VARCHAR', 'CHAR', 'NVARCHAR2', 'NCHAR')  THEN

        upd_cols := upd_cols || ' v_upd_sql := v_upd_sql || '', ' || col_name || ' = '''''' || :NEW.' || col_name || ' || '''''''';' || CHR(10);


      ELSIF col_type IN ('DATE', 'TIMESTAMP')  THEN      

        upd_cols := upd_cols || ' v_upd_sql := v_upd_sql || '', ' || col_name || 
        ' = TO_DATE('''''' || TO_CHAR(:NEW.' || col_name || ', ''YYYYMMDD HH24:MI:SS.SSSSS'') || '''''',''''YYYYMMDD HH24:MI:SS.SSSSS'''')'';' || CHR(10) ;
      
      END IF;
      

    upd_cols := upd_cols || ' END IF;  ' || CHR(10) || CHR(10);

     
    END LOOP;
    
--      DBMS_OUTPUT.PUT_LINE ('upd_cols: ' || upd_cols );


   trg_code := ' v_upd_sql := '' UPDATE '' ||  v_part_table || '' SET '' || SUBSTR(v_upd_sql,2) || '' WHERE ROWID = '''''' || rid || ''''''''; ' || CHR(10);

   trg_code := trg_code || ' EXECUTE IMMEDIATE v_upd_sql;' || CHR(10);
   
   trg_code := rid_sql || upd_cols || trg_code || CHR(10);


--    DBMS_OUTPUT.PUT_LINE ('trg_code: ' || trg_code );


    stmt := trg_head || CHR(10) || trg_code || CHR(10) || ' END;' ;
               
        execute IMMEDIATE stmt;
    
--    DBMS_OUTPUT.PUT_LINE ( stmt);

    IF p_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', p_meta_data_id, p_view_name, 'Creating INSTEAD OF UPDATE trigger - Successful');
    END IF;

    DBMS_OUTPUT.PUT_LINE('----->>> END: Creating INSTEAD OF UPDATE trigger on: ' || p_view_name);

  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('Error while creating UPDATE trigger on: ' || p_view_name);
     DBMS_OUTPUT.PUT_LINE('SQLERRM : ' || SQLERRM);
     WRITE_TO_LOG ('ERROR', p_meta_data_id, p_view_name, SQLERRM);

END;



/*********************************************************************************
  NAME:    Procedure - RECREATE_VIEW
  PURPOSE: This procedure re-creates the view to include new partition tables and 
             exclude old partition tables 

     The activities performed:
     1. Get all privileges on the view
     2. Get synonyms on the view
     3. Recreates the view to include new partition tables and exclude old partition tables 
     4. Grant all privileges on the view
     5. Creates INSERT/UPDATE/DELETE triggers on the view
     6. Create synonyms on the view
             
  Parameters:
    p_meta_data_id         Partition main table unique identifier
    p_table_owner          Owner of the partition tables
    p_default_table_name   Default table used to have records that does not fall into any partition
    p_view_name            Name of the view to recreate
    p_is_log_enabled       Flag to log the activity


  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      07/07/2020   AWS Professional Services   Initial Version

**********************************************************************************/

  PROCEDURE RECREATE_VIEW (
    p_meta_data_id              NUMBER,
    p_table_owner               VARCHAR2,
    p_table_name                VARCHAR2,
    p_default_table_name        VARCHAR2,
    p_partition_column_name     VARCHAR2,
    p_primarykey_column_name    VARCHAR2,
    p_partition_type            VARCHAR2,
    p_view_name                 VARCHAR2,
    p_is_log_enabled            BOOLEAN
  )
  AS
     v_view_sql             VARCHAR2(8000);
     t_vw_privs             tab_privs;
     t_trig_code            tab_trigs;
     t_synonyms             tab_synonyms;

  BEGIN

    DBMS_OUTPUT.PUT_LINE('----->>> START: RECREATE_VIEW. meta_data_id: ' || p_meta_data_id || ', VIEW: ' || p_view_name);

    IF p_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', p_meta_data_id, p_view_name, 'Recreating view: ' || p_view_name);
    END IF;

    /*** Get privileges on view before recreating ***/
    t_vw_privs := GET_OBJECT_PRIVILEGES (p_meta_data_id, p_table_owner, p_view_name, p_is_log_enabled);

    /*** Get Synonyms on view before recreating ***/
    t_synonyms := GET_SYNONYMS (p_meta_data_id, p_table_owner, p_view_name, p_is_log_enabled);

    /*** Build the view query for all partition tables that are in Active state ***/  
    SELECT listagg(pmd.TABLE_OWNER || '.' || ptd.PARTITION_TABLE_NAME,' UNION ALL SELECT * FROM ') within group(order by ptd.PARTITION_TABLE_NAME) 
      INTO v_view_sql
     FROM PARTITION_META_DATA pmd
     JOIN PARTITION_TABLE_DATA ptd ON ptd.META_DATA_ID = pmd.META_DATA_ID
    WHERE pmd.META_DATA_ID = p_meta_data_id
      AND ptd.STATUS = 'A';
      
          v_view_sql := 'CREATE OR REPLACE VIEW ' || p_table_owner || '.' || p_view_name 
            || ' AS ' 
            || 'SELECT * FROM ' || p_table_owner || '.' || p_default_table_name || ' UNION ALL ' 
            || 'SELECT * FROM ' || v_view_sql; 

  --  DBMS_OUTPUT.PUT_LINE(v_view_sql);

    EXECUTE IMMEDIATE v_view_sql;

    /***  Grant all privileges on view after recreating  ***/
    GRANT_OBJECT_PRIVILEGES (p_meta_data_id, t_vw_privs, p_view_name, p_is_log_enabled);

    /***  Create insert trigger  ***/
    create_ins_trg(p_meta_data_id, p_table_owner, p_table_name, p_partition_column_name, p_view_name, p_partition_type, p_is_log_enabled);
    
    /***  Create delete trigger  ***/
    create_del_trg(p_meta_data_id, p_table_owner, p_table_name, p_partition_column_name, p_primarykey_column_name, p_view_name, p_partition_type, p_is_log_enabled);
    
    /***  Create update trigger  ***/
    create_upd_trg(p_meta_data_id, p_table_owner, p_table_name, p_partition_column_name, p_primarykey_column_name, p_view_name, p_partition_type, p_is_log_enabled);

    /***  Create all Synonyms on view after recreating  ***/
    CREATE_SYNONYMS(p_meta_data_id, t_synonyms, p_is_log_enabled);

    IF p_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', p_meta_data_id, p_view_name, 'Recreating view: ' || p_view_name || ' Completed');
    END IF;

    DBMS_OUTPUT.PUT_LINE('----->>> END: RECREATE_VIEW. meta_data_id: ' || p_meta_data_id || ', VIEW: ' || p_view_name);

  END RECREATE_VIEW;
  
  
  
  /*********************************************************************************
  NAME:    Procedure - MAIN_PROCESS
  PURPOSE: This is the main procedure that is to be called to run the automation process
     The activities performed:
     1. Get info of all partition tables for which AUTO_MANAGE_PARTITIONS flag is set to 'Y'
     2. For each of these partitions get the recent partition table date
     3. Based on the partition type Y/M/W/D and NUM_PRECREATE_PARTITIONS identify new partition tables to be created
     4. Get all the privileges on partition main table
     5. Loop to create new partition tables 
     6. Identify the new partition table name
     7. Get the partition main table DDL
     8. Replace the main table name with new partition table name
     9. Replace the constraints, indexes names using main table short name and partition date value
     10. Create the new partition table
     11. After all partition tables are created, disable old partitions which are older than partitions to be retained
     12. Recreate the view with new partition tables
     13. Drop old partition tables if DROP_OLD_PARTITIONS is set to 'Y'
     

  Parameters: 
    p_meta_data_id      Partition main table unique identifier, 
                          If provided the automation is run only on the given table
                          If not provided (default NULL) the automation is run on all the partition tables

             
  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      07/07/2020   AWS Professional Services   Initial Version

**********************************************************************************/

  PROCEDURE MAIN_PROCESS (p_meta_data_id  NUMBER DEFAULT NULL)
  AS
     t_tab_privs         tab_privs;
     t_trig_code         tab_trigs;

     v_meta_data_id            NUMBER;
     v_table_owner             VARCHAR2(32);
     v_table_name              VARCHAR2(64);
     v_table_alias             VARCHAR2(8);
     v_view_name               VARCHAR2(64);
     v_partition_column_name   VARCHAR2(64);
     v_primaykey_column_name   VARCHAR2(64);
     v_partition_type          CHAR(1);
     v_num_partitions          NUMBER;
     v_num_retain_partitions   NUMBER;
     v_enable_logging          CHAR(1);
     v_is_log_enabled          BOOLEAN;
   
     v_prev_partition_date_val   DATE;
     v_prev_partition_num_val    NUMBER;

     v_new_partition_table_name  VARCHAR2(64);
     v_new_partition_short_name  VARCHAR2(64);
     v_new_partition_char_val    VARCHAR2(32);
     v_start_dt            DATE;
     v_end_dt              DATE;
   
     v_part_char_fmt       VARCHAR2(8);
     v_ddl_sql             VARCHAR2(8000);
   
     v_new_partition_created   CHAR(1);
     v_drop_old_partitions     CHAR(1);

     CURSOR c_part IS 
       SELECT META_DATA_ID, TABLE_OWNER, TABLE_NAME, TABLE_ALIAS
         , PARTITION_COLUMN_NAME, PRIMARYKEY_COLUMN_NAME, VIEW_NAME
         , PARTITION_TYPE, NUM_PRECREATE_PARTITIONS, DROP_OLD_PARTITIONS
         , NUM_RETAIN_PARTITIONS, ENABLE_LOGGING
       FROM PARTITION_META_DATA
       WHERE AUTO_MANAGE_PARTITIONS = 'Y'
         AND meta_data_id = NVL(p_meta_data_id, meta_data_id);
   
  BEGIN

     DBMS_OUTPUT.PUT_LINE('----->>> START: main_process');

     DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SQLTERMINATOR', true);
     DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'PRETTY', true);
     DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', false);
     DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'STORAGE', false);


    OPEN c_part;
  
    LOOP 

      BEGIN
  
      FETCH c_part INTO 
        v_meta_data_id, v_table_owner, v_table_name, v_table_alias
        , v_partition_column_name, v_primaykey_column_name, v_view_name
        , v_partition_type, v_num_partitions, v_drop_old_partitions
        , v_num_retain_partitions, v_enable_logging;

       -- DBMS_OUTPUT.PUT_LINE('----->>> partition_table_name: ' || v_table_name);

      EXIT WHEN c_part%NOTFOUND;

      IF v_enable_logging = 'Y' THEN
        v_is_log_enabled := TRUE;
      ELSE 
        v_is_log_enabled := FALSE;
      END IF;

      IF v_is_log_enabled THEN
        WRITE_TO_LOG ('DEBUG', v_meta_data_id, UPPER(v_table_name), 'START process for ' || v_table_owner || '.' || v_table_name);
      END IF;
     
       SELECT NVL(MAX(PARTITION_DATE_VALUE), 
               (CASE v_partition_type 
                 WHEN 'Y' THEN TRUNC(TO_DATE(EXTRACT(YEAR FROM SYSDATE)-1, 'RRRR'), 'YEAR')
                 WHEN 'M' THEN ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1)
                 WHEN 'W' THEN TRUNC(SYSDATE, 'IW') - (7-1)
                 ELSE SYSDATE - 1 
              END)
            ) 
        INTO v_prev_partition_date_val 
       FROM PARTITION_TABLE_DATA
       WHERE META_DATA_ID = v_meta_data_id;

       DBMS_OUTPUT.PUT_LINE('----->>> prev_partition_date_val: ' || v_prev_partition_date_val);
     
       IF v_partition_type = 'Y' THEN
       
         v_part_char_fmt := 'RRRR';

         v_start_dt := TRUNC(TO_DATE(EXTRACT(YEAR FROM v_prev_partition_date_val)+1, 'RRRR'), 'YEAR');
         v_end_dt := TRUNC(TO_DATE(TO_NUMBER(TO_CHAR(SYSDATE, 'RRRR')) + v_num_partitions, 'RRRR'), 'YEAR');
               
       ELSIF v_partition_type = 'M' THEN

         v_part_char_fmt := 'RRRRMM';

         v_start_dt := ADD_MONTHS(TRUNC(v_prev_partition_date_val, 'MONTH'), 1);
         v_end_dt := ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), v_num_partitions);

       ELSIF v_partition_type = 'W' THEN

         v_part_char_fmt := 'RRRRMMDD';

         v_start_dt := TRUNC(NEXT_DAY(v_prev_partition_date_val, 'SUN'));
         v_end_dt := TRUNC(SYSDATE, 'IW') + (v_num_partitions * 7);

       ELSIF v_partition_type = 'D' THEN

         v_part_char_fmt := 'RRRRMMDD';

         v_start_dt := TRUNC(v_prev_partition_date_val) + 1;
         v_end_dt := TRUNC(SYSDATE + v_num_partitions);

       END IF;
     
       DBMS_OUTPUT.PUT_LINE('----->>> start_dt: ' || v_start_dt || ', end_dt: ' || v_end_dt);

       v_new_partition_created := 'N';

       /******
       If new partition table is to be created get the privileges of main table 
       Grant the same privileges on the newly created partition table
       ******/
       IF v_start_dt <= v_end_dt THEN
            t_tab_privs := GET_OBJECT_PRIVILEGES (v_meta_data_id, UPPER(v_table_owner), UPPER(v_table_name), v_is_log_enabled);
       END IF;

       WHILE v_start_dt <= v_end_dt
       LOOP 

         /*****
        This flag is to identify if any new partition table is created.
        This will be used to recreate the view to include new partition table
         *****/
         v_new_partition_created := 'Y';

         v_new_partition_char_val := to_char(v_start_dt, v_part_char_fmt);     

         v_new_partition_table_name := UPPER(v_table_name || '_' || v_new_partition_char_val);
         v_new_partition_short_name := UPPER(v_table_alias || '_' || v_new_partition_char_val);

         SELECT DBMS_METADATA.GET_DDL('TABLE', UPPER(v_table_name), UPPER(v_table_owner)) 
         INTO v_ddl_sql
         FROM DUAL;

         IF v_is_log_enabled THEN      
          WRITE_TO_LOG ('DEBUG', v_meta_data_id, v_new_partition_table_name, 'Creating partition table: ' || v_new_partition_table_name);
         END IF;

         v_ddl_sql := REGEXP_REPLACE (v_ddl_sql, v_table_name, v_new_partition_table_name, 1, 1, 'i');
         v_ddl_sql := REPLACE (v_ddl_sql, v_table_alias, v_new_partition_short_name);
         
         v_ddl_sql := substr(v_ddl_sql, 1, length(v_ddl_sql)-2);

         EXECUTE IMMEDIATE v_ddl_sql;

         -- DBMS_OUTPUT.PUT_LINE('----->>> creating: ' || new_partition_table_name);

         INSERT INTO PARTITION_TABLE_DATA(META_DATA_ID, PARTITION_TABLE_NAME, PARTITION_DATE_VALUE, CRT_DT, STATUS)
         VALUES (v_meta_data_id, UPPER(v_new_partition_table_name), v_start_dt, SYSDATE, 'A');
       
         /******
         Grant the main table privileges on the newly created partition table
         ******/
         GRANT_OBJECT_PRIVILEGES (v_meta_data_id, t_tab_privs, v_new_partition_table_name, v_is_log_enabled);
     
         IF v_partition_type = 'Y' THEN
     
           v_start_dt := TRUNC(TO_DATE(EXTRACT(YEAR FROM v_start_dt) + 1, 'RRRR'), 'YEAR');
        
         ELSIF v_partition_type = 'M' THEN

           v_start_dt := ADD_MONTHS(v_start_dt, 1);

         ELSIF v_partition_type = 'W' THEN

           v_start_dt := NEXT_DAY(v_start_dt, 'SUN');

         ELSIF v_partition_type = 'D' THEN

           v_start_dt := TRUNC(v_start_dt) + 1;

         END IF;

       END LOOP;
  
      /*****
      If any new partition table is created the we have to recreate the view to include new partition table(s)
      Get privileges and triggers on view before recreating
      After recreating the view grant all privileges and create the triggers
      *****/   

      IF v_new_partition_created = 'Y' THEN

       /******
      If the old partition tables are older than retain partitions count, then mark them as 'Delete'
       ******/
       DISABLE_OLD_PARTITIONS (v_meta_data_id, v_table_owner, v_partition_type, v_num_retain_partitions, v_is_log_enabled);

       /*** Recreate view with new partition table(s) ***/
       RECREATE_VIEW (v_meta_data_id, v_table_owner, v_table_name, v_table_name || '_DEFAULT'
                           , v_partition_column_name, v_primaykey_column_name, v_partition_type
                           , v_view_name, v_is_log_enabled);
             
       /******
      If the drop old partitions flag is Yes, then drop old partitions where status is 'Delete'
       ******/

       IF v_drop_old_partitions = 'Y' THEN
        DROP_OLD_PARTITIONS (v_meta_data_id, v_table_owner, v_is_log_enabled);
       END IF;
     
      END IF;
     
     EXCEPTION
     WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error in procedure MAIN_PROCESS while processing table: ' || v_table_name);
      DBMS_OUTPUT.PUT_LINE('SQLERRM : ' || SQLERRM);
      WRITE_TO_LOG ('ERROR', v_meta_data_id, v_table_name, SQLERRM);
     END;

     END LOOP;

     CLOSE c_part;

     DBMS_OUTPUT.PUT_LINE('----->>> END: main_process');

   EXCEPTION
   WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('Error in procedure MAIN_PROCESS while processing table: ' || v_table_name);
     DBMS_OUTPUT.PUT_LINE('SQLERRM : ' || SQLERRM);
     WRITE_TO_LOG ('ERROR', v_meta_data_id, v_table_name, SQLERRM);
     RAISE;

  END;



/*********************************************************************************
  NAME:    Procedure - SETUP_PARTITIONS
  PURPOSE: This procedure creates the meta data entry and all required partitions management tasks.
           This should be called only once when partition management is initialled.

     The activities performed:
     1. Checks if given table is already part of partition management. If yes, raises the error else continues with below activities 
     2. Insert record into PARTITION_META_DATA, which is required for managing the partitions 
     3. Create a default table '<main_table>_DEFAULT', this table used as overflow table to store records that do not fit into any partitions 
     4. Creates partition tables from the given start date (parameter: p_start_date) 
     5. Creates view with the given name (parameter: p_view_name) based on all required partition tables 
     6. Creates INSERT/UPDATE/DELETE triggers on view

             
  Parameters:
    p_table_owner                Owner of the table
    p_table_name                 Table to be partitioned
    p_table_alias                Short name of the partition table
    p_partition_column           Name of the column to be used for partitioning
    p_pkey_column                Name of primary key column
    p_view_name                  Name of the view to be created
    p_partition_type             Type of partition (Y/M/W/D)
    p_start_date                 Date from which partitions have to be created
    p_precreate_partitions       No. of future partitions to be created from current date
    p_retain_partitions          Number indicating how many old partitions to be retained 
    p_manage_partitions          Flag to auto manage partitions
    p_drop_old_partitions        Flag to indicate if old partitions to be dropped
    p_enable_logging             Flag to log the activity


  Ver    Date         Author                  Description
  -----  ----------   -------------           -----------------------
  1      08/28/2020   AWS Professional Services   Initial Version

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
  )
  AS 
    v_meta_data_id              NUMBER;
    t_tab_privs                 tab_privs;    
    v_part_dt_fmt               VARCHAR2(10);  
    v_is_log_enabled            BOOLEAN;  
    v_table_name                VARCHAR2(64);
    v_table_alias               VARCHAR2(32);
    v_ddl_sql                   VARCHAR(8000);
    v_start_dt                  DATE;
    v_partition_char_val        VARCHAR2(10);  
    v_part_char_fmt             VARCHAR2(10);
    v_cnt                       NUMBER;
    v_start_date                DATE;

  BEGIN


    DBMS_OUTPUT.PUT_LINE('----->>> START: setup_partitions p_table_name: ' || p_table_name);

    SELECT COUNT(1)  INTO  v_cnt
      FROM PARTITION_META_DATA
     WHERE TABLE_OWNER = UPPER(p_table_owner)
       AND TABLE_NAME = UPPER(p_table_name);
    
    IF v_cnt > 0 THEN
       RAISE_APPLICATION_ERROR(-20001, 'The table ' || p_table_owner || '.' || p_table_name || ' is already part of partition management');
    END IF;
    
    v_start_date := TO_DATE(p_start_date, 'YYYY-MM-DD');
    
    INSERT INTO PARTITION_META_DATA(
        TABLE_OWNER, TABLE_NAME, TABLE_ALIAS, VIEW_NAME
        , PARTITION_COLUMN_NAME, PRIMARYKEY_COLUMN_NAME, PARTITION_TYPE
        , NUM_PRECREATE_PARTITIONS, NUM_RETAIN_PARTITIONS, AUTO_MANAGE_PARTITIONS
        , DROP_OLD_PARTITIONS, ENABLE_LOGGING)
    VALUES (
          UPPER(p_table_owner), UPPER(p_table_name), UPPER(p_table_alias), UPPER(p_view_name)
        , UPPER(p_partition_column), UPPER(p_pkey_column), UPPER(p_partition_type)
        , p_precreate_partitions, p_retain_partitions, UPPER(p_manage_partitions)
        , UPPER(p_drop_old_partitions), UPPER(p_enable_logging))
    RETURNING meta_data_id 
    INTO v_meta_data_id;

    DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SQLTERMINATOR', true);
    DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'PRETTY', true);
    DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', false);
    DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'STORAGE', false);


      IF p_enable_logging = 'Y' THEN
        v_is_log_enabled := TRUE;
      ELSE 
        v_is_log_enabled := FALSE;
      END IF;

    t_tab_privs := GET_OBJECT_PRIVILEGES (v_meta_data_id, UPPER(p_table_owner), UPPER(p_table_name), v_is_log_enabled);

   /* START - Create DRFAULT partition */
     v_table_name := UPPER(p_table_name) || '_DEFAULT';
     v_table_alias := UPPER(p_table_alias) || '_DFT';

     SELECT DBMS_METADATA.GET_DDL('TABLE', UPPER(p_table_name), UPPER(p_table_owner)) 
     INTO v_ddl_sql
     FROM DUAL;

--    DBMS_OUTPUT.PUT_LINE('----->>> Original v_ddl_sql: ' || v_ddl_sql);

     IF v_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', v_meta_data_id, UPPER(v_table_name), 'Creating default table: ' || v_table_name);
     END IF;

     v_ddl_sql := REGEXP_REPLACE (v_ddl_sql, p_table_name, v_table_name, 1, 1, 'i');
     v_ddl_sql := REPLACE (v_ddl_sql, p_table_alias, v_table_alias);
     
     v_ddl_sql := substr(v_ddl_sql, 1, length(v_ddl_sql)-2);

--    DBMS_OUTPUT.PUT_LINE('----->>> DEFAULT v_ddl_sql: ' || v_ddl_sql);

     EXECUTE IMMEDIATE v_ddl_sql;

     IF v_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', v_meta_data_id, UPPER(v_table_name), 'Creating default table: ' || v_table_name || ' - Successful');
     END IF;

     /******
     Grant the main table privileges on the newly created partition table
     ******/
     GRANT_OBJECT_PRIVILEGES (v_meta_data_id, t_tab_privs, UPPER(v_table_name), v_is_log_enabled);

   /* END - Create DRFAULT partition */


       IF p_partition_type = 'Y' THEN
       
         v_part_char_fmt := 'RRRR';

         v_start_dt := TRUNC(TO_DATE(EXTRACT(YEAR FROM v_start_date), 'RRRR'), 'YEAR');
               
       ELSIF p_partition_type = 'M' THEN

         v_part_char_fmt := 'RRRRMM';

         v_start_dt := TRUNC(v_start_date, 'MONTH');

       ELSIF p_partition_type = 'W' THEN

         v_part_char_fmt := 'RRRRMMDD';

         v_start_dt := TRUNC(v_start_date, 'IW') - 1;

       ELSIF p_partition_type = 'D' THEN

         v_part_char_fmt := 'RRRRMMDD';

         v_start_dt := TRUNC(v_start_date);

       END IF;

     v_partition_char_val := to_char(v_start_dt, v_part_char_fmt);     

     v_table_name := UPPER(p_table_name || '_' || v_partition_char_val);
     v_table_alias := UPPER(p_table_alias || '_' || v_partition_char_val);

     SELECT DBMS_METADATA.GET_DDL('TABLE', UPPER(p_table_name), UPPER(p_table_owner)) 
     INTO v_ddl_sql
     FROM DUAL;

     IF v_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', v_meta_data_id, UPPER(v_table_name), 'Creating partition table: ' || v_table_name);
     END IF;

     v_ddl_sql := REGEXP_REPLACE (v_ddl_sql, p_table_name, v_table_name, 1, 1, 'i');
     v_ddl_sql := REPLACE (v_ddl_sql, p_table_alias, v_table_alias);
     
     v_ddl_sql := substr(v_ddl_sql, 1, length(v_ddl_sql)-2);

--    DBMS_OUTPUT.PUT_LINE('----->>> 1ST PART v_ddl_sql: ' || v_ddl_sql);

     EXECUTE IMMEDIATE v_ddl_sql;

     INSERT INTO PARTITION_TABLE_DATA(META_DATA_ID, PARTITION_TABLE_NAME, PARTITION_DATE_VALUE, CRT_DT, STATUS)
     VALUES (v_meta_data_id, UPPER(v_table_name), v_start_dt, SYSDATE, 'A');
   
     /******
     Grant the main table privileges on the newly created partition table
     ******/
     GRANT_OBJECT_PRIVILEGES (v_meta_data_id, t_tab_privs, UPPER(v_table_name), v_is_log_enabled);

     IF v_is_log_enabled THEN      
      WRITE_TO_LOG ('DEBUG', v_meta_data_id, UPPER(v_table_name), 'Creating partition table: ' || v_table_name || ' - Successful');
     END IF;

    MANAGE_PARTITIONS.main_process (v_meta_data_id);

    DBMS_OUTPUT.PUT_LINE('----->>> END: setup_partitions p_table_name: ' || p_table_name);

  EXCEPTION
     WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error in procedure SETUP_PARTITIONS while processing table: ' || v_table_name);
      DBMS_OUTPUT.PUT_LINE('SQLERRM : ' || SQLERRM);
      WRITE_TO_LOG ('ERROR', v_meta_data_id, v_table_name, SQLERRM);

  END;

END MANAGE_PARTITIONS;
/
