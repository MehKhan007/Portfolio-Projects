use saudi_maroof;

select * from fact_stores;

select * from dim_categories;

-- 1. Total number of stores
SELECT COUNT(*) as total_stores from fact_stores;

-- 2. How many stores per category
SELECT c.category_name, COUNT(*) AS num_stores
FROM fact_stores f
JOIN dim_categories c ON f.category_id = c.category_id
GROUP BY c.category_name
ORDER BY num_stores DESC;

-- 3. Average rating per category
SELECT c.category_name, ROUND(AVG(f.rating), 2) AS avg_rating
FROM fact_stores f
JOIN dim_categories c ON f.category_id = c.category_id
GROUP BY c.category_name
ORDER BY avg_rating DESC;

-- 4. How many stores have each digital channel
SELECT
  SUM(has_website)   AS have_website,
  SUM(has_instagram) AS have_instagram,
  SUM(has_twitter)   AS have_twitter,
  SUM(has_email)     AS have_email,
  SUM(has_phone)     AS have_phone
FROM fact_stores;

-- 5. Digital score breakdown
SELECT digital_score, COUNT(*) AS num_stores
FROM fact_stores
GROUP BY digital_score
ORDER BY digital_score;

-- 6. Active vs Not Active stores
SELECT activity_status, COUNT(*) AS count
FROM fact_stores
GROUP BY activity_status;

-- 7. Top 10 highest rated stores
SELECT name_eng, name_ar, rating, num_ratings
FROM fact_stores
WHERE rating IS NOT NULL
ORDER BY rating DESC
LIMIT 10;

-- 8. Stores that are fully digital (score = 5)
SELECT name_eng, name_ar, rating
FROM fact_stores
WHERE digital_score = 5
ORDER BY rating DESC;

-- 9. Stores with no email AND no phone (hardest to contact)
SELECT name_eng, name_ar, activity_status
FROM fact_stores
WHERE has_email = 0 AND has_phone = 0;

-- 10. Average digital score per category
SELECT c.category_name, ROUND(AVG(f.digital_score), 2) AS avg_digital_score
FROM fact_stores f
JOIN dim_categories c ON f.category_id = c.category_id
GROUP BY c.category_name
ORDER BY avg_digital_score DESC;