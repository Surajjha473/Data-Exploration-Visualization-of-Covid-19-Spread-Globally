-- data querying
-- data exploration 
select * 
from [Covid_data_project]..Coviddeaths
--where continent is not null -- because in some place the cotinentis say asia and then in some place say location is asia(continent)
order by 3,4

--select * 
--from [Covid_data_project]..covidvaccinations
--order by 3,4

--now selecting the data we need
select location, date, total_cases, new_cases, total_deaths, population
from [Covid_data_project]..Coviddeaths
where continent is not null
order by 1,2
 
 -- now just to compare we are loking at total cases vs total deaths
 -- percentage deaths out of total cases
select location, date, total_cases, total_deaths, (CONVERT(float, total_deaths) / NULLIF(CONVERT(float, total_cases), 0))*100 as deathpercentage
from [Covid_data_project]..Coviddeaths
--where location like '%india%'
where continent is not null
order by 1,2
 
  -- now just to compare we are loking at total cases vs the population of that country
  -- to see what percentage of the population were affected by the covid
select location, date, population, total_cases,  (CONVERT(float, total_cases) / NULLIF(population, 0))*100 as percentage_population_infected
--select location, date, population, total_cases,  (CONVERT(float, total_cases) / NULLIF(CONVERT(float, population), 0))*100 as percentage_population_infected
from [Covid_data_project]..Coviddeaths
--where location like '%states%'
where continent is not null
order by 1,2
 
 -- now to check country with highest infection rate with respect to population
 select location, population, Max(total_cases) as highest_infection_count,  Max(CONVERT(float, total_cases) / NULLIF(population, 0))*100 as percentage_population_infected
from [Covid_data_project]..Coviddeaths
--where location like '%states%'
where continent is not null
group by  location, population
order by percentage_population_infected desc

-- now to check countries with highest death count vs population
 select location, Max(cast(total_deaths as int)) as total_death_count
from [Covid_data_project]..Coviddeaths
--where location like '%states%'
where continent is not null
group by  location
order by total_death_count desc

-- now to check highest death count vs population for all the categories in location  
select location, Max(cast(total_deaths as int)) as total_death_count
from [Covid_data_project]..Coviddeaths
--where location like '%states%'
where continent is null
group by  location
order by total_death_count desc

-- now to check highest death count vs population in every continent
select continent, Max(cast(total_deaths as int)) as total_death_count
from [Covid_data_project]..Coviddeaths
--where location like '%states%'
where continent is not null
group by  continent
order by total_death_count desc

-- now to check highest death count vs total deaths across the world except for the world obviously(this is perday)
select date, sum(new_cases) as total_cases,sum(new_deaths) as total_deaths, (sum(new_deaths)/nullif(sum(new_cases),0))*100 as deathpercentage
--select date, sum(new_cases),sum(cast(new_deaths as int)), coalesce(sum(cast(new_deaths as int))/nullif(sum(new_cases),0)*100, null) as deathpercentage
from [Covid_data_project]..Coviddeaths
--where location like '%states%'
where continent is not null
group by date
order by 1,2

-- now total cases vs total deaths overall
select sum(new_cases) as total_cases,sum(new_deaths) as total_deaths, (sum(new_deaths)/nullif(sum(new_cases),0))*100 as deathpercentage
--select date, sum(new_cases),sum(cast(new_deaths as int)), coalesce(sum(cast(new_deaths as int))/nullif(sum(new_cases),0)*100, null) as deathpercentage
from [Covid_data_project]..Coviddeaths
--where location like '%states%'
where continent is not null
--group by date
order by 1,2
--so death percentage across the world is about 0.9034 %


-- joining covid_deaths & covid_vaccinations
select * 
from [Covid_data_project]..Coviddeaths as cdea
	join [Covid_data_project]..covidvaccinations as cvac
	on cdea.location = cvac.location
	and cdea.date = cvac.date

-- now checking total number of people vaccinated in the world(total population vs total vaccination)
select cdea.continent, cdea.location, cdea.date, cdea.population, cvac.new_vaccinations
from [Covid_data_project]..Coviddeaths as cdea
	join [Covid_data_project]..covidvaccinations as cvac
	on cdea.location = cvac.location
	and cdea.date = cvac.date
where cdea.continent is not null
order by 2,3


-- rolling count of new_vaccination from previous query
-- we are goin to use "new vaccination per" day which we get from previous query to get the rolling count
select cdea.continent, cdea.location, cdea.date, cdea.population, cvac.new_vaccinations, sum(convert(bigint, cvac.new_vaccinations)) 
over (partition by cdea.location order by cdea.location, cdea.date) as people_vaccinated_rollingcount
-- here partion is done by location & date and not by continent to get the correct figuress 
-- for every location the count needs to restart 
-- if partition on location only the the rolling count will be the total vaccinatoin and not truly a rolling count so 
-- in order by we are doing it by location and date as well
from [Covid_data_project]..Coviddeaths as cdea
	join [Covid_data_project]..covidvaccinations as cvac
	on cdea.location = cvac.location
	and cdea.date = cvac.date
where cdea.continent is not null
order by 2,3

-- now to look at total population vs vaccination using the rolling count(max number-the end value of rolling count; because of rolling count)
-- then dividing that max number by population to get percentage population vaccinated 
-- over here Common table expression(CTE) is used
with popvsvac (continent, location, date, population, new_vacciantions, people_vaccinated_rollingcount)
as
(
select cdea.continent, cdea.location, cdea.date, cdea.population, cvac.new_vaccinations, sum(convert(bigint, cvac.new_vaccinations)) 
over (partition by cdea.location order by cdea.location, cdea.date) as people_vaccinated_rollingcount
from [Covid_data_project]..Coviddeaths as cdea
	join [Covid_data_project]..covidvaccinations as cvac
	on cdea.location = cvac.location
	and cdea.date = cvac.date
where cdea.continent is not null
--order by 2,3
)
select *,(people_vaccinated_rollingcount/population)*100 percentof_rollingpopulation_vaccinated
from popvsvac

-- same thing as above but with creating a Temporary table

drop table if exists #percent_population_vaccinated --adding a if else statement in the beggining of this query if we want to run this query again
create table #percent_population_vaccinated
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
people_vaccinated_rollingcount numeric
)

insert into #percent_population_vaccinated
select cdea.continent, cdea.location, cdea.date, cdea.population, cvac.new_vaccinations, sum(convert(bigint, cvac.new_vaccinations)) 
over (partition by cdea.location order by cdea.location, cdea.date) as people_vaccinated_rollingcount
from [Covid_data_project]..Coviddeaths as cdea
	join [Covid_data_project]..covidvaccinations as cvac
	on cdea.location = cvac.location
	and cdea.date = cvac.date
where cdea.continent is not null
--order by 2,3

select *,(people_vaccinated_rollingcount/population)*100 as percentof_rollingpopulation_vaccinated
from #percent_population_vaccinated
--drop table #percent_population_vaccinated --- using this drops the table if we want to run this above query 
--or just add a if  statement in the beggining of this query

-- CReating a view to store data and then creating visualizations
drop view if exists percent_population_vaccinated 
use [Covid_data_project]  --selcting the database in which we want to create view
go
create view percent_population_vaccinated as
select cdea.continent, cdea.location, cdea.date, cdea.population, cvac.new_vaccinations, sum(convert(bigint, cvac.new_vaccinations)) 
over (partition by cdea.location order by cdea.location, cdea.date) as people_vaccinated_rollingcount
from [Covid_data_project]..Coviddeaths as cdea
	join [Covid_data_project]..covidvaccinations as cvac
	on cdea.location = cvac.location
	and cdea.date = cvac.date
where cdea.continent is not null
--order by 2,3

select *
from percent_population_vaccinated