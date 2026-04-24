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











