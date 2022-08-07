select
	ano_prova as ano_prova_salario
	, case renda_familiar
	    when "Nenhuma renda." then "(A) Nenhuma renda"
	    when "Até R$ 954,00." then "(B) Até 1000"
	    when "De R$ 954,01 até R$ 1.431,00." then "(C) Entre 1000 e 1500"
	    when "De R$ 1.431,01 até R$ 1.908,00." then "(D) Entre 1500 e 2000"
	    when "De R$ 1.908,01 até R$ 2.385,00." then "(E) Entre 2000 e 2500"
	    else "(F) Acima 2500"
	end as salario
	, (percentile(nota_cn,0.5) + percentile(nota_ch,0.5) + percentile(nota_lc,0.5) + percentile(nota_mt,0.5) + percentile(nota_redacao,0.5)) / 5 as nota
from
	generic_sandbox.enem_fact
	join generic_sandbox.enem_dimensio_questionario_socio_economico on (enem_fact.id_questionario_socio_economico == enem_dimensio_questionario_socio_economico.id_questionario_socio_economico)
where
	Q006 is not null
group by
	1, 2
order by
	1, 2