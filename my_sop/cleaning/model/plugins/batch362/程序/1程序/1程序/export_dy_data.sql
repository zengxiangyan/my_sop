#出6月的bottom链接
-- SELECT m.no, m.time, c.L1EN, c.L2EN, c.L3EN, m2.c6, m.Category, m.SubCategory, m.SubCategorySegment, m.c4, m.rank, m.name, m.url, 
-- m.Brand, m.Brand1, m.brandid, m.brand6, b.BrandName, b.BrandCN, b.BrandEN,
-- m.shopname, m.shopid, b.Manufacturer, b.User, b.Selectivity, m.unit, m.sales, m.price, m.top90, m.beforehave
-- FROM dy.makeup_all_month_6_2 m2
-- JOIN dy.makeup_all_month_6_3 m on m.no = m2.no
-- LEFT JOIN dy.category c ON m2.c6 = c.L3CN 
-- LEFT JOIN dy.brand b ON m.brand6 = b.BrandName


#只出尾部链接
-- SELECT m.no, m.time, c.L1EN, c.L2EN, c.L3EN, m2.c6, m.Category, m.SubCategory, m.SubCategorySegment, m.c4, m.rank, m.name, m.url, 
-- m.Brand, m.Brand1, m.brandid, m2.brand6, b.BrandName, b.BrandCN, b.BrandEN,
-- m.shopname, m.shopid, b.Manufacturer, b.User, b.Selectivity, m.unit, m.sales, m.price, m.top90, m.beforehave
-- FROM dy.makeupall2 m2
-- JOIN dy.makeupall m on m.no = m2.no
-- LEFT JOIN dy.category c ON m2.c6 = c.L3CN 
-- LEFT JOIN dy.brand b ON m2.brand6 = b.BrandName
-- where m2.top90="N" and m2.sales !=0 and m2.`time` like "%2021%"

#所有标签
SELECT m.no, m.time, c.L1EN, c.L2EN, c.L3EN, m2.c6, m.Category, m.SubCategory, m.SubCategorySegment, m.c4, m.rank, m.name, m.url, 
t.DYBrandName, t.DYBrandCN, t.DYBrandEN, t.DYBrandName, t.MCBrandCN, t.MCBrandEN, t.BrandTag, m.Brand, m.Brand1, m.brandid, m2.brand6, b.BrandName, b.BrandCN, b.BrandEN,
m.shopname, m.shopid, b.Manufacturer, b.User, b.Selectivity, m.unit, m.sales, m.price, m.top90, m.beforehave
FROM dy.makeupall2 m2
JOIN dy.makeupall m on m.no = m2.no
join dy.tagm t on t.no = m2.no
LEFT JOIN dy.category c ON m2.c6 = c.L3CN 
LEFT JOIN dy.brand b ON m2.brand6 = b.BrandName