-- 识别品牌
-- makeupall3 brand6为用name识别的品牌
-- makeupall32 brand6为用brand识别的品牌
-- makeupall3 brand7为综合处理品牌识别
-- p3 run.dy.brand.py --cpu_max 16  (运行rules.xls，sheet“brand.rule”，通过name字段识别品牌，写在表makeupall3)
-- p3 run.dy.brand2.py --cpu_max 16 (运行rules.xls，sheet“brand.rule2”，通过brand字段识别品牌，写在表makeupall32)

-- 1.用宝贝名字段识别brand，更新到brand7
use wq;
UPDATE makeupall3 m3
SET m3.brand7 = m3.brand6

-- 2.用品牌字段识别brand，更新到brand7为TBD的字段
UPDATE makeupall3 m3
JOIN makeupall32 m32 ON (m3.newno = m32.newno)
SET m3.brand7 = m32.brand6
WHERE m3.brand7 = "TBD"

-- 3.部分品牌用品牌字段识别brand，统一刷品牌
UPDATE makeupall3 m3
JOIN makeupall32 m32 ON (m3.newno = m32.newno)
SET m3.brand7 = m32.brand6
where m32.brand6 IN (SELECT * FROM tbrand)

-- 4.部分品牌用旗舰店名，统一刷品牌
UPDATE makeupall3 m3
JOIN tshop s ON (s.shopname = m3.shopname)
SET m3.brand7 = s.BrandName

-- 5.出数据

SELECT m2.platform, m2.newno, m2.`time`, m2.c4, m2.name, m2.brand, 
m2.url, m2.shopname, m2.unit, m2.price, m2.sales, m2.c6, 
c.L1EN as Category, c.L2EN as SubCategory, c.L3EN as SubCategorySegment, m3.brand7,m3.brand6,
b.BrandName, b.BrandCN, b.BrandEN, 
m2.User, m2.ShopType1, m2.ShopType2, m2.Manufacturer, m2.Division, m2.Selectivity, m2.BrandLRL
FROM makeupall2 m2
LEFT JOIN category c
ON m2.c6=c.L3CN 
LEFT JOIN makeupall3 m3
ON m2.newno=m3.newno
LEFT JOIN brand b
ON m3.brand7=b.BrandName
WHERE m2.time = "2022-01-01"







select * case when name like "%宝宝%" or "%宝贝%" or "%婴儿%" or "%青少年%" or "%幼儿%" then user='Child&Baby'
when 

SELECT m2.platform, m2.newno, m2.`time`, m2.c4, m2.name, m2.brand, 
m2.url, m2.shopname, m2.unit, m2.price, m2.sales, m2.c6, 
c.L1EN as Category, c.L2EN as SubCategory, c.L3EN as SubCategorySegment, m3.brand7,m3.brand6,
b.BrandName, b.BrandCN, b.BrandEN, 
m2.User, m2.ShopType1, m2.ShopType2, m2.Manufacturer, m2.Division, m2.Selectivity, m2.BrandLRL
FROM makeupall2 m2
LEFT JOIN category c
ON m2.c6=c.L3CN 
LEFT JOIN makeupall3 m3
ON m2.newno=m3.newno
LEFT JOIN brand b
ON m3.brand7=b.BrandName
WHERE m2.time = "2022-01-01"


-- 3.用shopname识别brand，更新到brand7


-- 4.品牌名为京东和网易，用宝贝名字段识别brand，更新到brand7
-- UPDATE makeupall3 m3
-- JOIN makeupall32 m32 ON (m3.newno = m32.newno)
-- SET m3.brand7 = m32.brand6
-- WHERE m3.brand like "%京东%" or m3.brand like "%网易%";


-- 5.修正易错品牌，宝贝名含“脚后跟、晒后、事后、浴后”，用宝贝名字段识别brand，更新到brand7
-- UPDATE makeupall3 m3
-- JOIN makeupall32 m32 ON (m3.newno = m32.newno)
-- SET m3.brand7 = "TBD"
-- WHERE (m3.name like "%脚后跟%" or m3.name like "%晒后%" or m3.name like "%事后%" or m3.name like "%浴后%") and m3.brand7 like "%Whoo%";
 
-- UPDATE makeupall3 m3
-- JOIN makeupall32 m32 ON (m3.newno = m32.newno)
-- SET m3.brand7 = m32.brand6
-- WHERE (m3.name like "%脚后跟%" or m3.name like "%晒后%" or m3.name like "%晒.后%" or m3.name like "%事后%" or m3.name like "%浴后%" 
-- or m3.name like "%薛后%" or m3.name like "%蝶后%"  or m3.name like "%太子后%") and m3.brand6 not like "%Whoo%";


