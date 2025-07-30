CREATE OR REPLACE PROCEDURE NAGP_CERTACOM_HISTORICO_NFS (vsDtaInicial DATE, vsDtaFinal DATE, NRO_EMPRESA NUMBER) IS

    v_file UTL_FILE.file_type;
    v_line VARCHAR2(32767);
    v_Targetcharset varchar2(40 BYTE);
    v_Dbcharset varchar2(40 BYTE);
    v_Cabecalho VARCHAR2(4000);
    v_Periodo VARCHAR2(10);
    v_buffer CLOB;
    v_chunk_size CONSTANT PLS_INTEGER := 32000; -- Ajuste conforme necessário

BEGIN

    FOR t IN (SELECT X.ANOMESDESCRICAO,
                     MIN(TO_DATE(LAST_DAY(ADD_MONTHS(X.DTA, -1)) + 1, 'DD/MM/RRRR')) DTA_INICIAL,
                     MAX(TO_DATE(LAST_DAY(X.DTA), 'DD/MM/RRRR')) DTA_FINAL
                FROM DIM_TEMPO X
               WHERE X.DTA BETWEEN vsDtaInicial AND vsDtaFinal
               GROUP BY X.ANOMESDESCRICAO
               ORDER BY 2)

    LOOP
    
    V_Periodo := REPLACE(t.ANOMESDESCRICAO, '/','_');

    -- Abre o arquivo para escrita
    v_file := UTL_File.Fopen('CERTACON','NFS_'||v_Periodo||'.csv', 'w');

    SELECT LISTAGG(COLUMN_NAME,';') WITHIN GROUP (ORDER BY COLUMN_ID)
      INTO v_Cabecalho
      FROM ALL_TAB_COLUMNS A
     WHERE A.table_name = 'NAGV_CTCON_NFS';

    -- Escreve o cabe¿alho do CSV
    UTL_FILE.put_line(v_file, v_Cabecalho);

    -- Executa a query e escreve os resultados

      FOR vda IN (         select TO_CHAR(x.DTPERIODO,'DD/MM/RRRR') ||';'||
                 X.NUMDOC||';'||
       X.SERIE||';'||
       X.CODMODELO||';'||
       X.INDENTRADASAIDA||';'||
       X.CODEMITENTE||';'||
       X.RAZAOSOCIALEMITENTE||';'||
       X.UFEMITENTE||';'||
       X.CODDESTINATARIO||';'||
       X.RAZAOSOCIALDESTINATARIO||';'||
       X.UFDESTINATARIO||';'||
       X.CHAVENFE||';'||
       X.SEQITEM||';'||
       X.CPROD||';'||
       X.DESCRICAO||';'||
       X.CFOP||';'||
       X.CSTORIG||';'||
       X.CSTICMS||';'||
       X.NCM||';'||
       X.CEST||';'||
       X.CODEFETIVADOEAN||';'||
       X.QUANTIDADE||';'||
       X.UNITARIO||';'||
       X.VALORUNITARIO||';'||
       X.VALORTOTAL||';'||
       X.MVA||';'||
       X.VLRBASEICMSPROP||';'||
       X.VLRICMS||';'||
       X.PERALIQUOTAICMSST||';'||
       X.VLRICMSRET||';'||
       X.VLRBASEICMSRETIDO||';'||
       X.INDPAUTAST||';'||
       X.VLRBCFCPST||';'||
       X.VLRPERCFCPST||';'||
       X.VLRFCPST||';'||
       X.DESCONTO||';'||
       X.VLRBASEICMSST||';'||
       X.VLRICMSST||';'||
       X.REDBC||';'||
       X.ALIQUOTAICMS   as linha
          from consinco.NAGV_CTCON_NFS x
			 	WHERE (X.CODEMITENTE = NRO_EMPRESA OR X.CODDESTINATARIO = NRO_EMPRESA)
          and x.DTPERIODO BETWEEN T.DTA_INICIAL AND T.DTA_FINAL)

      LOOP

        v_line := vda.linha;

        v_buffer := v_buffer || v_line || CHR(10); -- Adiciona nova linha ao buffer
        
        IF LENGTH(v_buffer) > v_chunk_size THEN
            UTL_FILE.put_line(v_file, v_buffer); -- Escreve o buffer no arquivo
            v_buffer := ''; -- Limpe o buffer
            
        END IF;
        
    END LOOP;
    
    -- Grava o restante do buffer no final (burro esqueceu)
    IF v_buffer IS NOT NULL THEN
        UTL_FILE.put_line(v_file, v_buffer);
        v_buffer := '';
    END IF;
    
    -- Fecha o arquivo
    UTL_FILE.fclose(v_file);

    END LOOP;

EXCEPTION

    WHEN OTHERS THEN
        IF UTL_FILE.is_open(v_file) THEN
            UTL_FILE.fclose(v_file);
        END IF;
        RAISE;

END;
