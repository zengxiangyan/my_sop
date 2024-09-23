-- select count(*) 
SELECT m2.c6, m2.newno, m2.name, m2.shopname, m2.brand, t.`BrandName-人工`, b.BrandName, b.BrandCN, b.BrandEN, m2.url
FROM makeupall2 m2
LEFT JOIN category c
ON m2.c6=c.L3CN 
LEFT JOIN makeupall3 m3
ON m2.newno=m3.newno
LEFT JOIN brand b
ON m3.brand7=b.BrandName
LEFT JOIN tag7 t 
on m2.newno = t.newno
WHERE m2.time = "2021-07-01" and not isnull(t.`BrandName-人工`) and t.`BrandName-人工` != ""
and t.`BrandName-人工`!= b.BrandName
order by m2.name desc
-- limit 100
 
