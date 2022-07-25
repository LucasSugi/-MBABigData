-- Consulta para usar como modelo
-- Substituir [questao] pela questao que deseja-se analisar
-- Também é possível alterar a métrica das notas
select
	ano_prova
	, [questao]
	, (percentile(nu_nota_cn,0.5) + percentile(nu_nota_ch,0.5) + percentile(nu_nota_lc,0.5) + percentile(nu_nota_mt,0.5) + percentile(nu_nota_redacao,0.5)) / 5 as nota
from
	generic_sandbox.enem_fact
	join generic_sandbox.enem_dimensio_questionario_socio_economico on (enem_fact.id_questionario_socio_economico == enem_dimensio_questionario_socio_economico.id_questionario_socio_economico)
group by
	1, 2
order by
	1, 2