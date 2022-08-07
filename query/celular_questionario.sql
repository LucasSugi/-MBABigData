select
	ano_prova as ano_prova_celular
	, case possui_celular
		when "Não." then "0"
        when "Sim, um." then "1"
        when "Sim, dois." then "2"
        when "Sim, três." then "3"
        else "4"
    end as celular
	, (percentile(nota_cn,0.5) + percentile(nota_ch,0.5) + percentile(nota_lc,0.5) + percentile(nota_mt,0.5) + percentile(nota_redacao,0.5)) / 5 as nota
from
	generic_sandbox.enem_fact
	join generic_sandbox.enem_dimensio_questionario_socio_economico on (enem_fact.id_questionario_socio_economico == enem_dimensio_questionario_socio_economico.id_questionario_socio_economico)
where
	Q022 is not null
group by
	1, 2
order by
	1, 2