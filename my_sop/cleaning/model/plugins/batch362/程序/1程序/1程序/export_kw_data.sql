#只需要部分链接
SELECT m.no, m2.time, c.L1EN, c.L2EN, c.L3EN, m.c6 , m.c4 , m.name, m.brand, b.BrandName , b.BrandCN , b.BrandEN , m2.shopname , b.Manufacturer , b.User, b.Selectivity, m2.unit , m2.sales , m2.price ,m.url 
FROM kw.makeup3 m 
JOIN kw.makeup m2 on (m2.no=m.no)
LEFT JOIN kw.category c ON m.c6 = c.L3CN 
LEFT JOIN kw.brand b ON m.brand6 = b.BrandName 
where m.no>55686 and m.no<64757 limit 100000;

#所有标签
-- SELECT m.no, m2.time, c.L1EN, c.L2EN, c.L3EN, m.c6 , m.c4 , m.name, m.brand, b.BrandName , b.BrandCN , b.BrandEN , m2.shopname , b.Manufacturer , b.User, b.Selectivity, m2.unit , m2.sales , m2.price ,m.url 
-- FROM kw.makeup3 m 
-- JOIN kw.makeup m2 on (m2.no=m.no)
-- LEFT JOIN kw.category c ON m.c6 = c.L3CN 
-- LEFT JOIN kw.brand b ON m.brand6 = b.BrandName limit 100000;