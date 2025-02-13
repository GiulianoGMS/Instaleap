-- Essa tabela será utilizada como fonte de parametros em todas as views
-- Desta forma, nao é preciso alterar em cada uma, uma a uma, seus filtros

CREATE TABLE CONSINCO.NAGT_PARAMETROS_IEAP (COD_PD    VARCHAR2(3),
                                            PARAMETRO VARCHAR2(100),
                                            VALOR_PD  VARCHAR2(1));
                                            
SELECT * FROM NAGT_PARAMETROS_IEAP FOR UPDATE;
