--drop view staging.v_etl_covid19_rki;
--create or replace view staging.v_etl_covid19_rki as
select "Datum"::date                        as date_of_day
     , "Landkreis"                          as county
     , "LandkreisTyp"                       as county_type
     , "Bundesland"                         as state
     , "Flaeche"                            as area
     , "Einwohner"                          as population
     , "Dichte"                             as population_density
     , "AnzahlFall"                         as cases
     , "AnzahlFallNeu"                      as cases_new
     , "AnzahlTodesfall"                    as deaths
     , "AnzahlTodesfallNeu"                 as deaths_new
     , "AnzahlGenesen"                      as recovered
     , "AnzahlGenesenNeu"                   as recovered_new
     , "InzidenzFallNeu_7TageSumme_R"       as r_incidence_new
     , "Fallsterblichkeit_Prozent"          as case_mortality_percent
     , "MeldeTag_AnzahlFall"                as rki_cases
     , "MeldeTag_AnzahlFallNeu"             as rki_cases_new
     , "MeldeTag_AnzahlTodesfall"           as rki_deaths
     , "MeldeTag_AnzahlTodesfallNeu"        as rki_deaths_new
     , "MeldeTag_InzidenzFallNeu_R"         as rki_r_incidence_new
     , "MeldeTag_Fallsterblichkeit_Prozent" as rki_case_mortality_percent
     , "Kontaktrisiko"                      as risk_of_contact
     , "InzidenzFallNeu_Tage_bis_50"        as days_until_incidence_of_50
     , "InzidenzFallNeu_Tage_bis_100"       as days_until_incidence_of_100

from staging.covid19_rki

order by county_type, state, date_of_day, risk_of_contact;