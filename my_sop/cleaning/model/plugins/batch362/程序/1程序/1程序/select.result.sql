-- SELECT COUNT(*) 
SELECT m.newno, m.c4, c.L3CN, m.c6, m.name, m.url 
FROM makeupall2 m
JOIN category c ON c.L3EN = m.SubCategorySegment 
WHERE m.name like "%素颜面霜%" and m.c6 not like "%乳液面霜%" and m.c6 not like "%爽肤水%"

-- 爽肤水，排除，素颜面霜；乳液面霜，加入，素颜面霜