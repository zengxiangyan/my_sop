SELECT m3.brand7 as Tempbrand, m3.brand6 as Namebrand, m32.brand6 as Brandbrand, m2.platform, m2.newno, m2.`time`, m2.c4, m2.name, m2.brand, 
m2.url, m2.shopname, m2.unit, m2.price, m2.sales, m2.c6, 
c.L1EN as Category, c.L2EN as SubCategory, c.L3EN as SubCategorySegment, 
b.BrandName, b.BrandCN, b.BrandEN, 
m2.`User`, m2.ShopType1, m2.ShopType2, m2.Manufacturer, m2.Division, m2.Selectivity, m2.BrandLRL
FROM makeupall2 m2
LEFT JOIN category c
ON m2.c6=c.L3CN 
LEFT JOIN makeupall3 m3
ON m2.newno=m3.newno
LEFT JOIN makeupall32 m32
ON m2.newno=m32.newno
LEFT JOIN brand b
ON m3.brand7=b.BrandName
WHERE m2.time = "2021-07-01" and m2.newno = "989092"



