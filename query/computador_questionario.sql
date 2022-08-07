select
	ano_prova as ano_prova_computador
	, case possui_computador
		when "Não." then "0"
        when "Sim, um." then "1"
        when "Sim, dois." then "2"
        when "Sim, três." then "3"
        else "4"
    end as computador
	, (mean(nota_cn) + mean(nota_ch) + mean(nota_lc) + mean(nota_mt) + mean(nota_redacao)) / 5 as nota
from
	generic_sandbox.enem_fact
	join generic_sandbox.enem_dimensio_questionario_socio_economico on (enem_fact.id_questionario_socio_economico == enem_dimensio_questionario_socio_economico.id_questionario_socio_economico)
where
	Q024 is not null
group by
	1, 2
order by
	1, 2