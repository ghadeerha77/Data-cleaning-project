-- DATA CLEANING PROJECT :
-- 1 Create table to work
select *
from layoffs;
CREATE TABLE layoffs_staging
LIKE layoffs;
INSERT layoffs_staging
SELECT * FROM layoffs;
select *
from layoffs_staging;
-- 2 Removing Duplicates
WITH duplicate_cte AS
(
select *,
row_number() over(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS ROW_NUM
from layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE ROW_NUM > 1;

-- NOTE : Deleting directly from CTE works in MYSOL
-- But not standard across all databases
DELETE
FROM duplicate_cte
WHERE ROW_NUM > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
SELECT *
FROM layoffs_staging2;
INSERT INTO layoffs_staging2
select *,
row_number() over(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS ROW_NUM
FROM layoffs_staging;
SELECT *
FROM layoffs_staging2;

-- 3 Standaring Data

Update layoffs_staging2
SET company = TRIM(company);

update layoffs_staging2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

update layoffs_staging2
SET country = trim(trailing '.' FROM country)
WHERE country LIKE 'United States%';

update layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
modify column `date` DATE;

-- 4 التعامل مع القيم NULL
SELECT *
FROM layoffs_staging2
WHERE total_laid_off is NULL AND percentage_laid_off IS NULL ;

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
		ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = null
WHERE industry ='';

Update layoffs_staging2 t1
JOIN layoffs_staging2 t2
		ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

Select *
FROM layoffs_staging2
Where company = 'Airbnb';

Select *
FROM layoffs_staging2
Where company LIKE 'Bally%';

Select *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- because they are not useful
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- we do not need that any more
Alter table layoffs_staging2
DROP column row_num;
-- THE END --
-- --------------------------------------------------------------------------
-- --------------------------------------------------------------------------
-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

Select max(total_laid_off), max(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, sum(total_laid_off) AS total
FROM layoffs_staging2
GROUP BY company
ORDER BY total DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, sum(total_laid_off) AS total
FROM layoffs_staging2
GROUP BY industry
ORDER BY total DESC;

SELECT country, sum(total_laid_off) AS total
FROM layoffs_staging2
GROUP BY country
ORDER BY total DESC;

SELECT YEAR(`date`) AS `year`, sum(total_laid_off) AS total
FROM layoffs_staging2
GROUP BY `year`
ORDER BY `year` DESC;

SELECT stage, sum(total_laid_off) AS total
FROM layoffs_staging2
GROUP BY stage
ORDER BY stage DESC;

WITH Rolling_total AS
(
Select substring(`date`,1,7) AS `month`,
sum(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `month`
ORDER BY `month` ASC)
SELECT `month`, SUM(total_off) over(order by `month`) AS rolling_total
FROM Rolling_total;

SELECT company, YEAR(`date`), sum(total_laid_off) AS total
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY company ASC;

WITH Company_Year (company, years,total_laid_off)  AS
(
SELECT company, YEAR(`date`), sum(total_laid_off) AS total
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY total DESC
), Company_year_RANK AS
(
select *, 
dense_rank() OVER(partition by years ORDER BY total_laid_off DESC) AS COMPANY_RANK
FROM Company_Year
WHERE years IS NOT NULL
ORDER BY COMPANY_RANK
)
SELECT *
FROM Company_year_RANK
WHERE COMPANY_RANK <=5;