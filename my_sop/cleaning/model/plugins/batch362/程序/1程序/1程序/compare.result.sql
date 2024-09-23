-- SELECT COUNT(*) 
SELECT m.newno, m.c4, c.L3CN, m2.c6, m2.name, m2.url 
FROM makeupall2 m
JOIN category c ON c.L3EN = m.SubCategorySegment 
JOIN makeupall22 m2 ON m.newno = m2.newno
WHERE m.c6 != m2.c6 and c.L3CN != m2.c6
ORDER BY m2.name DESC
