/*
Covid 19 Data Exploration 
Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types
*/

CREATE SCHEMA covid;
DROP TABLE covid.coviddeaths;
DROP TABLE covid.covidvaccinations;

CREATE TABLE covid.coviddeaths(
iso_code CHAR(50),	continent CHAR(50),	location CHAR(50),	date date,	population NUMERIC,	total_cases NUMERIC,new_cases NUMERIC, 
new_cases_smoothed NUMERIC,	
total_deaths NUMERIC,	new_deaths NUMERIC,	new_deaths_smoothed NUMERIC,	total_cases_per_million NUMERIC,
new_cases_per_million NUMERIC,	new_cases_smoothed_per_million NUMERIC,	total_deaths_per_million NUMERIC,	new_deaths_per_million	NUMERIC,
new_deaths_smoothed_per_million NUMERIC,	reproduction_rate NUMERIC,
icu_patients NUMERIC,icu_patients_per_million NUMERIC,hosp_patients NUMERIC,hosp_patients_per_million NUMERIC,
weekly_icu_admissions NUMERIC,	weekly_icu_admissions_per_million NUMERIC,	weekly_hosp_admissions NUMERIC,	weekly_hosp_admissions_per_million NUMERIC);

CREATE TABLE covid.covidvaccinations(iso_code CHAR(50),	continent CHAR(50),	location CHAR(50),
date date,total_tests NUMERIC,	new_tests NUMERIC,	total_tests_per_thousand NUMERIC,	new_tests_per_thousand NUMERIC,	new_tests_smoothed NUMERIC,	new_tests_smoothed_per_thousand NUMERIC,
positive_rate NUMERIC,	tests_per_case NUMERIC,	tests_units CHAR(50),	total_vaccinations NUMERIC,	people_vaccinated NUMERIC,	people_fully_vaccinated NUMERIC,
total_boosters NUMERIC,	new_vaccinations NUMERIC,	new_vaccinations_smoothed NUMERIC,	total_vaccinations_per_hundred NUMERIC,	people_vaccinated_per_hundred NUMERIC,	people_fully_vaccinated_per_hundred NUMERIC,
total_boosters_per_hundred NUMERIC,	new_vaccinations_smoothed_per_million NUMERIC,	new_people_vaccinated_smoothed NUMERIC,	new_people_vaccinated_smoothed_per_hundred NUMERIC,	stringency_index NUMERIC,	
population_density NUMERIC,	median_age NUMERIC,	aged_65_older NUMERIC,aged_70_older NUMERIC,gdp_per_capita NUMERIC,	extreme_poverty NUMERIC,cardiovasc_death_rate NUMERIC,diabetes_prevalence NUMERIC,female_smokers NUMERIC,male_smokers NUMERIC,
handwashing_facilities NUMERIC,	hospital_beds_per_thousand NUMERIC,	life_expectancy NUMERIC,human_development_index NUMERIC,excess_mortality_cumulative_absolute NUMERIC,excess_mortality_cumulative NUMERIC,
excess_mortality NUMERIC,excess_mortality_cumulative_per_million NUMERIC);

Copy covid.coviddeaths from 'C:\Users\15045\Desktop\data analyst portfolio\SQL\Coviddeath.csv' WITH CSV HEADER;

Copy covid.covidvaccinations from 'C:\Users\15045\Desktop\data analyst portfolio\SQL\covidvaccinations.csv' WITH CSV HEADER;

SELECT * FROM covid.coviddeaths order by 3,4;

SELECT * FROM covid.covidvaccinations order by 3,4;

/*select the data we are going to be using. */
select location,date,total_cases,new_cases,total_deaths,population from covid.coviddeaths order by 1,2;

/* Looking at total cases vs total death. */
/* Likelihood of dying if you contract covid in USA country*/

Select location,date,total_cases,total_deaths,(total_deaths/total_cases )* 100 
AS DeathPercentage from covid.coviddeaths 
where location like  '%States%' and continent is not null order by 1,2;


/* Looking at total cases vs population. */
/* shows the percentage of population infected by covid*/

Select location,date,population,total_cases,(total_cases /population)* 100 
AS InfectedPercentage from covid.coviddeaths
where location like  '%States%' and continent is not null  order by 1,2;

/* Looking for countries with highest infection rate compared to population*/


Select location,population,max(total_cases) as HighestInfectedcount,
max((total_cases /population)* 100) AS InfectedPercentage from covid.coviddeaths 
where continent is not null 
group by location,population order by InfectedPercentage  desc NULLS LAST ;



/* Showing countries with highest death count per  population*/


Select location ,max(total_deaths) as Highesdeathcount from covid.coviddeaths where  continent is not null
group by location order by Highesdeathcount desc NULLS LAST ;


/* Showing Continents with highest death count per  population*/


Select continent ,max(total_deaths) as Highesdeathcount
from covid.coviddeaths where  continent is not null
group by continent order by Highesdeathcount desc NULLS LAST ;

-- Global numbers

Select date, sum(new_cases) as Global_new_cases_on_each_day,sum(new_deaths) 
as Global_new_deaths_on_each_day ,(sum(new_deaths)/sum(new_cases ))* 100 AS World_DeathPercentage from covid.coviddeaths 
where continent is not null 
group by date  ;

-- Looking at total population vs vaccination

Select dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations,sum(vac.new_vaccinations)
over(Partition by dea.location order by dea.location,dea.date) as Cummulative_of_people_vaccinated from covid.coviddeaths dea
join covid.covidvaccinations vac on dea.location=vac.location and dea.date=vac.date where dea.continent is not null 
;

--Creating a temporary view (CTE) to perform Calculation on Partition By in previous query

--Looking at total population vs vaccination

with popvsvac(continent,location,date,population,new_vaccinations,Cummulative_of_people_vaccinated) as
(Select dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations,sum(vac.new_vaccinations)
over(Partition by dea.location order by dea.location,dea.date) as Cummulative_of_people_vaccinated 
from covid.coviddeaths dea
join covid.covidvaccinations vac on dea.location=vac.location and dea.date=vac.date
where dea.continent is not null)
select *,(Cummulative_of_people_vaccinated/population)*100 as CummulativePercentpopulationvaccinated from popvsvac;


--Creating a temporary view 

Drop table if exists Percentpopulationvaccinated;
Create Table Percentpopulationvaccinated(
continent CHAR(50),location CHAR(50),date date,
population numeric,new_vaccinations numeric,Cummulative_of_people_vaccinated numeric)
insert into Percentpopulationvaccinated
Select dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations,sum(vac.new_vaccinations)
over(Partition by dea.location order by dea.location,dea.date) as Cummulative_of_people_vaccinated from covid.coviddeaths dea
join covid.covidvaccinations vac on dea.location=vac.location and dea.date=vac.date where dea.continent is not null 
;
select *,(Cummulative_of_people_vaccinated/population)*100 as CummulativePercentpopulationvaccinated from Percentpopulationvaccinated;


--Creating view to store data for later visualisations

Drop view if exists Percent_of_populationvaccinated;
Create view Percent_of_populationvaccinated as Select dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations,sum(vac.new_vaccinations)
over(Partition by dea.location order by dea.location,dea.date) as Cummulative_of_people_vaccinated
from covid.coviddeaths dea join covid.covidvaccinations vac on dea.location=vac.location and dea.date=vac.date where dea.continent is not null 
;

select * from Percent_of_populationvaccinated;
