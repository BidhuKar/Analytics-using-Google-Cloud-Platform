-- SELECT * FROM `portfolioproject01-420105.coviddata.covidatavacs` 
-- where continent is not null
-- order by 3,4;
-- limit 100;

--SELECT * FROM `portfolioproject01-420105.coviddata.coviddatadeaths` 
-- where continent is not null
--order by 3,4
--limit 100;


-- Select the data that we're going to use 

select location, 
        date, 
        total_cases, 
        new_cases, 
        total_deaths, 
        population
from `portfolioproject01-420105.coviddata.coviddatadeaths`
where continent is not null 
order by 1,2;


-- Total Cases vs Total Deaths
-- Shows likelihood of dying if you contract covid in your country

select location, 
        date, 
        total_cases,
        total_deaths, 
        (total_deaths/total_cases)*100 as death_percentage
from `portfolioproject01-420105.coviddata.coviddatadeaths`
where location like '%States%'
and continent is not null 
order by 1,2;


-- Looking at countries with highest infection rate compares to population

select location,
        population,
        max(total_cases) as highest_infection,
        max(total_cases/population)*100 as percent_population_infected
from `portfolioproject01-420105.coviddata.coviddatadeaths`
group by location,population
order by percent_population_infected desc;

-- Showing countries with the highest death count per population

select location,
        max(total_deaths) as total_death_count
from `portfolioproject01-420105.coviddata.coviddatadeaths`
group by location
order by total_death_count desc;

-- To remove the garbage values

select location,
        max(total_deaths) as total_death_count
from `portfolioproject01-420105.coviddata.coviddatadeaths`
where continent is not null
group by location
order by total_death_count desc;


-- Grouping base on continent

select continent,
        max(total_deaths) as total_death_count
from `portfolioproject01-420105.coviddata.coviddatadeaths`
where continent is not null
group by continent
order by total_death_count desc;


select location,
        max(total_deaths) as total_death_count
from `portfolioproject01-420105.coviddata.coviddatadeaths`
where continent is null
group by location
order by total_death_count desc;

-- showing the contnents with highest death counts

select continent,
        max(total_deaths) as total_death_count
from `portfolioproject01-420105.coviddata.coviddatadeaths`
where continent is not null
group by continent
order by total_death_count desc;

-- global numbers

select location, 
        date, 
        total_cases,
        total_deaths, 
        (total_deaths/total_cases)*100 as death_percentage
from `portfolioproject01-420105.coviddata.coviddatadeaths`
-- where location like '%States%'
where continent is not null
order by 1,2;

-- select  date, 
--        total_cases,
--        total_deaths, 
--        (total_deaths/total_cases)*100 as death_percentage
-- from `portfolioproject01-420105.coviddata.coviddatadeaths`
-- where location like '%States%'
-- where continent is not null
-- group by date
-- order by 1,2;


-- BigQuery division by zero - How to do safe error handling

select  date, 
       sum(new_cases) as sum_new_cases,
       sum(new_deaths) as sum_new_deaths,
       SAFE_DIVIDE(sum(new_deaths),sum(new_cases))*100
--       (sum((new_deaths))/sum(new_cases)*100) as new_death_percentage
from `portfolioproject01-420105.coviddata.coviddatadeaths`
-- where location like '%States%'
where continent is not null
group by date
order by 1,2;

   
 select sum(new_cases) as sum_new_cases,
       sum(new_deaths) as sum_new_deaths,
       SAFE_DIVIDE(sum(new_deaths),sum(new_cases))*100
--       (sum((new_deaths))/sum(new_cases)*100) as new_death_percentage
from `portfolioproject01-420105.coviddata.coviddatadeaths`
-- where location like '%States%'
where continent is not null
-- group by date
order by 1,2;


select dea.continent,
        dea.location,
        dea.date,
        dea.population,
        vac.new_vaccinations,
        sum(vac.new_vaccinations) OVER (Partition by dea.location 
        Order by dea.location,dea.date) as new_vaccine_added,
 --       (new_vaccine_added/population)*100 
from `portfolioproject01-420105.coviddata.coviddatadeaths` dea
join `portfolioproject01-420105.coviddata.covidatavacs` vac
  on dea.location = vac.location
  and dea.date = vac.date
where dea.continent is not null
order by 2,3;

-- Using CTE to perform Calculation on Partition By in previous query

with popsvsvac AS(
select dea.continent as Continent,
        dea.location as Location,
        dea.date as Date,
        dea.population as Population,
        vac.new_vaccinations New_Vaccinations,
        sum(vac.new_vaccinations) OVER (Partition by dea.location 
        Order by dea.location,dea.date) as New_Vaccine_Added,
 --       (new_vaccine_added/population)*100 
from `portfolioproject01-420105.coviddata.coviddatadeaths` dea
join `portfolioproject01-420105.coviddata.covidatavacs` vac
on dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null
order by 2,3
)
select *, (New_Vaccine_Added/Population)*100 as Percentage
from popsvsvac;



-- Using Temp Table to perform Calculation on Partition By in previous query

DROP Table if exists coviddata.PercentPopulationVaccinated1
Create Table coviddata.PercentPopulationVaccinated1
(
Continent string(50),
Location string(50),
Date datetime,
Population numeric,
New_vaccinations numeric,
New_Vaccine_Added numeric,
Percentage float64
);

Insert into coviddata.PercentPopulationVaccinated1
(
        Select dea.continent, 
                dea.location, 
                dea.date, 
                dea.population, 
                vac.new_vaccinations,
                SUM(vac.new_vaccinations) OVER (Partition by dea.location Order by dea.location, dea.Date),
                (SUM(vac.new_vaccinations) OVER (Partition by dea.location Order by dea.location, dea.Date)/population)*100
        from
        `portfolioproject01-420105.coviddata.coviddatadeaths` dea
        join `portfolioproject01-420105.coviddata.covidatavacs` vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
order by 2,3
);
Select * from coviddata.PercentPopulationVaccinated1






-- Creating View to store data for later visualizations

Create View coviddata.PercentPopulationVaccinated as
Select dea.continent as Continent,
        dea.location as Location,
        dea.date as Date,
        dea.population as Population, 
        vac.new_vaccinations as New_Vaccinations,
        SUM(vac.new_vaccinations) OVER (Partition by dea.Location Order by dea.location, dea.Date) as New_Vaccine_Added,
        (vac.new_vaccinations/dea.population)*100 as Percentage
        from
        `portfolioproject01-420105.coviddata.coviddatadeaths` dea
        join `portfolioproject01-420105.coviddata.covidatavacs` vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null; 

Select * from coviddata.PercentPopulationVaccinated;

# Queries for craeting dashboards

#View1
create view coviddata.dataquery1 as
        Select sum(new_cases) as total_cases, 
                sum(new_deaths) as total_deaths, 
                SAFE_DIVIDE(sum(new_deaths),sum(new_cases))*100 as death_percentage
                from
        `portfolioproject01-420105.coviddata.coviddatadeaths`
where continent is not null 
order by 1,2;
select * from coviddata.dataquery1;

#View2
create view coviddata.dataquery2 as
        select location, 
                sum(new_deaths) as TotalDeathCount
        from `portfolioproject01-420105.coviddata.coviddatadeaths`
        where continent is null 
        and location not in ('World', 'European Union', 'International','Low income','High income','Upper middle income','Lower middle income')
        group by location
order by TotalDeathCount desc;
select * from coviddata.dataquery2;

#View3
create view coviddata.dataquery3 as
        Select location, 
                population, 
                MAX(total_cases) as HighestInfectionCount,  
                Max((total_cases/population))*100 as PercentPopulationInfected
        From `portfolioproject01-420105.coviddata.coviddatadeaths`
Group by location, population
order by PercentPopulationInfected desc;
select * from coviddata.dataquery3;


#View4
create view coviddata.dataquery4 as
        Select location, 
        population,
        date, 
        MAX(total_cases) as HighestInfectionCount,  
        Max((total_cases/population))*100 as PercentPopulationInfected
        From `portfolioproject01-420105.coviddata.coviddatadeaths`
Group by location, population, date
order by PercentPopulationInfected desc;
select * from coviddata.dataquery4;











