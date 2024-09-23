#SELECT m.no, m2.time, c.L1EN, c.L2EN, c.L3EN, m.c6 , m.c4 , m.name, m.brand, b.BrandName , b.BrandCN , b.BrandEN , m2.shopname , b.Manufacturer , b.User, b.Selectivity, m2.unit , m2.sales , m2.price ,m.url 
select count(m.no)
FROM dy.makeup3 m 
JOIN dy.makeup m2 on (m2.no=m.no)
LEFT JOIN dy.category c ON m.c6 = c.L3CN 
LEFT JOIN dy.brand b ON m.brand6 = b.BrandName limit 1000000;