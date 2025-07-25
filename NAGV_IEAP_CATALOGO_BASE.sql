CREATE OR REPLACE VIEW NAGV_IEAP_CATALOGO_BASE AS
SELECT S.SEQPRODUTO PRODUCT_SKU,
       S.NROEMPRESA STORE_REFERENCE,
/*       LISTAGG(SEQCATEGORIAN1||CASE WHEN SEQCATEGORIAN2 IS NOT NULL THEN ', '||SEQCATEGORIAN2 END
                             ||CASE WHEN SEQCATEGORIAN3 IS NOT NULL THEN ', '||SEQCATEGORIAN3 END)
       WITHIN GROUP (ORDER BY C.SEQFAMILIA, SEQCATEGORIAN1, SEQCATEGORIAN2, SEQCATEGORIAN3),*/
			 NAGF_CATEGORIA_IEAP(P.SEQFAMILIA) CATEGORY_REFERENCE,
       S.Precogernormal PRICE,
       -- Alterado por Giuliano em 07/04/2025
       -- Solic Roger - Email
       -- HortiFruti e nao existe ean = Nao Envia Estoque
       -- Acougue -- Se existir na DEPARA = Nao envia estoque

       CASE WHEN C.CATEGORIAN1 = 'HORTIFRUTI' AND NOT EXISTS (SELECT 1 FROM MAP_PRODCODIGO E WHERE E.SEQPRODUTO = S.SEQPRODUTO AND E.TIPCODIGO = 'E') THEN 1000
            WHEN C.CATEGORIAN1 IN ('AÇOUGUE', 'PADARIA')   AND EXISTS (SELECT 1 FROM NAGT_DEPARA_STOCK_IEAP DP WHERE DP.SEQPRODUTO = S.SEQPRODUTO) THEN 1000
            ELSE fc5estoquedisponivel(S.SEQPRODUTO,S.NROEMPRESA) END STOCK,

       NULL MAXQTD,
       NULL MINQTD,
       CASE WHEN S.STATUSVENDA = 'A' AND P.INDINTEGRAECOMMERCE = 'S' THEN 'TRUE' ELSE 'FALSE' END ISACTIVE,
       COALESCE(CATEGORIAN1, CATEGORIAN2, CATEGORIAN3) LOCATION,
       4 SECURITYSTOCK, -- Definido por Roger
       NULL TAGS

  FROM  MRL_PRODEMPSEG S          INNER JOIN MAP_PRODUTO P ON P.SEQPRODUTO = S.SEQPRODUTO
                                                               INNER JOIN ETLV_CATEGORIA C ON C.SEQFAMILIA = P.SEQFAMILIA AND C.NRODIVISAO = 1
WHERE 1=1
  AND S.QTDEMBALAGEM = 1
	AND S.STATUSVENDA = 'A'
	AND S.NROSEGMENTO IN (5,8)
  AND EXISTS (SELECT 1 FROM NAGV_IEAP_REGRA_PRODUTOS W WHERE W.seqproduto = S.SEQPRODUTO AND W.NROSEGMENTO = S.NROSEGMENTO AND W.NROEMPRESA = S.NROEMPRESA)
  AND C.SEQCATEGORIAN1 != 40054
;
