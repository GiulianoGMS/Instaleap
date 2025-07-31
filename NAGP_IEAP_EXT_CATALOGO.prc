CREATE OR REPLACE PROCEDURE NAGP_IEAP_EXT_CATALOGO IS

    v_file UTL_FILE.file_type;
    v_line VARCHAR2(32767);
    v_Cabecalho VARCHAR2(32767);
    v_Periodo VARCHAR2(100);
    v_buffer CLOB;
    v_chunk_size CONSTANT PLS_INTEGER := 32000; -- Ajuste conforme necessário

BEGIN
  
    SELECT REPLACE(TO_CHAR(SYSDATE-1, 'DD/MM'),'/','_') 
      INTO v_Periodo
      FROM DUAL;
    -- Abre o arquivo para escrita
    v_file := UTL_FILE.fopen('PLUSOFT', 'catalogos_instaleap.csv', 'w', 32767);

    -- Pega o nome das colunas para inserir no cabecalho pq tenho preguica
    SELECT 'PLU;LOJA;STATUS;ESTOQUE;PRECO' 
      INTO v_Cabecalho 
      FROM DUAL;
    -- Nao utiliza pq nao deu certo na variavel   
       /*    
    SELECT 'vda.'||LISTAGG(COLUMN_NAME,'||'||;||'||vda.') WITHIN GROUP (ORDER BY COLUMN_ID)
      INTO v_LineConteudo
      FROM ALL_TAB_COLUMNS A
     WHERE A.table_name = 'NAGV_PLUSOFT_OFERTAS'
       AND COLUMN_NAME != 'DATA';
       */
    -- Escreve o cabe¿alho do CSV
    UTL_FILE.put_line(v_file, v_Cabecalho);

    -- Executa a query e escreve os resultados

      FOR x IN ( SELECT /*+OPTIMIZER_FEATURES_ENABLE('11.2.0.4')*/ 
                        X.PRODUCT_SKU,
                        X.STORE_REFERENCE LOJA,
                        X.ISACTIVE STATUS,
                        X.STOCK ESTOQUE,
                        X.PRICE PRECO
                  FROM NAGV_IEAP_CATALOGO_BASE X WHERE 1=1
                     ) 

      LOOP
                
      v_line := X.PRODUCT_SKU||';'||X.LOJA||';'||X.STATUS||';'||X.ESTOQUE||';'||X.PRECO;
            UTL_FILE.put_line(v_file, v_line); -- Escreve o buffer no arquivo
            
        
    END LOOP;
    
   
    UTL_FILE.fclose(v_file);

COMMIT;
EXCEPTION

    WHEN OTHERS THEN
        IF UTL_FILE.is_open(v_file) THEN
            UTL_FILE.fclose(v_file);
        END IF;
        DBMS_OUTPUT.PUT_LINE('Error Code: ' || SQLCODE);
        DBMS_OUTPUT.PUT_LINE('Error Message: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('Error Stack: ' || DBMS_UTILITY.FORMAT_ERROR_STACK);
        DBMS_OUTPUT.PUT_LINE('Error Backtrace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        DBMS_OUTPUT.PUT_LINE('Call Stack: ' || DBMS_UTILITY.FORMAT_CALL_STACK);
        RAISE;

END;
