-- DATA CLEANSING
SELECT count(*)
FROM layoffs_cleaned;

CREATE TABLE layoff_staging
LIKE layoffs_cleaned;

SELECT *
FROM layoff_staging;

INSERT INTO layoff_staging
SELECT *
FROM layoffs_cleaned;

SELECT *
FROM layoff_staging;

SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry,total_laid_off, percentage_laid_off, `date`,stage,country, funds_raised_millions
) AS row_num
FROM layoff_staging;

WITH duplicates_cte AS(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, 
location, industry,total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num
FROM layoff_staging
)
SELECT *
FROM duplicates _cte
WHERE row_num > 1;

CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` double DEFAULT NULL,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` double DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoff_staging2;

INSERT INTO layoff_staging2
SELECT*,
ROW_NUMBER() OVER (PARTITION BY company, 
location, industry,total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num
FROM layoff_staging;


DELETE
FROM layoff_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;

DELETE FROM layoff_staging2 WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 1;

SELECT *
FROM layoff_staging2;

-- STANDARDISING DATA
SELECT TRIM(company)
FROM layoff_staging2;
UPDATE layoff_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoff_staging2
ORDER BY 1 ;

SELECT * 
FROM layoff_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoff_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';


SELECT *
FROM layoff_staging2
WHERE industry = 'Crypto';

SELECT DISTINCT country
FROM layoff_staging2
ORDER BY 1;

UPDATE layoff_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

SELECT DISTINCT country, TRIM(TRAILING ' . ' FROM Country)
FROM layoff_staging2
ORDER BY 1;

SELECT *
FROM layoff_staging2;

SELECT * 
FROM layoff_staging2;

ALTER TABLE layoff_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoff_staging2
WHERE company = 'Airbnb';


SELECT *
FROM layoff_staging2
WHERE industry IS NULL 
OR industry = ' ';

UPDATE Layoff.layoff_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM Layoff.layoff_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

UPDATE layoff_staging2 t1
JOIN layoff_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t2.industry IS NULL
AND t1.industry IS NOT NULL;

SELECT company, industry
FROM layoff_staging2;

SELECT company,industry
FROM layoff_staging2;

ALTER TABLE layoff_staging2
DROP COLUMN row_num;

SELECT *
FROM layoff_staging2;

-- DATA EXPLORATORY ANALYSIS

SELECT *
FROM layoff_staging2;
-- Total laid off in accordance with the companies
SELECT company, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company
ORDER BY 2 DESC;
-- rolling total laid off with the month and date

SELECT SUBSTRING(`date`,6,2) AS Month, SUM(total_laid_off) AS tot_off
FROM layoff_staging2
WHERE SUBSTRING(`date`,6,2) IS NOT NULL
GROUP BY Month
ORDER BY 1 DESC;

WITH rolling_layoff AS
(SELECT SUBSTRING(`date`,1,7) AS Month, SUM(total_laid_off) AS tot_off
FROM layoff_staging2
WHERE SUBSTRING(`date`,1,7)IS NOT NULL
GROUP BY Month
ORDER BY 1 DESC
)
SELECT `Month`, tot_off, SUM(tot_off) OVER(ORDER BY `Month`) AS rolling_tot
FROM rolling_layoff
;

-- Total number of employees laid off by the respective company in accordance with the month and date

SELECT company,YEAR(`date`) AS YEAR, SUM(total_laid_off) AS sum_tot_lay
FROM layoff_staging2
GROUP BY company, YEAR
HAVING sum_tot_lay IS NOT NULL
ORDER BY 2 DESC;
WITH company_year AS
(SELECT company,YEAR(`date`) AS YEAR, SUM(total_laid_off) AS sum_tot_lay
FROM layoff_staging2
GROUP BY company, YEAR
),Company_year_rank AS
(SELECT *,
DENSE_RANK() OVER(PARTITION BY YEAR ORDER BY sum_tot_lay DESC ) AS ranking
FROM company_year
)
SELECT *
FROM company_year_rank
HAVING ranking <= 5 ;







