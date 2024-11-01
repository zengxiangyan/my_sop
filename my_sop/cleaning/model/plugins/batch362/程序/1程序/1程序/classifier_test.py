#!/usr/bin/env python3

import unittest
import logging

from classifier import Classifier, KeywordRule, SuiteRule, NegativeTest


def convertToRow(item, c4=""):
    if isinstance(item, str):
        return {"c4": c4, "name": item}
    return item


def splitText(text, c4=""):
    lines = text.split("\n")
    rows = []
    for l in lines:
        l = l.strip()
        if l and not l.startswith("#"):
            rows.append(convertToRow(l, c4))
    return rows


class BrandTest(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.classifier = Classifier()
        self.classifier.load_rules("rules.xlsx", sheet_name="brand.rule")

    def testBrand(self):
        row = {"c4": "", "name": "Estee Lauder洗发水"}
        result = self.classifier.test(row)
        self.assertEqual(result, "Estee Lauder/雅诗兰黛", row["name"])

        row = {"c4": "", "name": "EsteeLauder洗发水"}
        result = self.classifier.test(row)
        self.assertEqual(result, "Estee Lauder/雅诗兰黛", row["name"])

        row = {"c4": "", "name": "YSLD洗发水"}
        result = self.classifier.test(row)
        self.assertEqual(result, "Estee Lauder/雅诗兰黛", row["name"])


class SuiteTest(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.classifier = Classifier()
        self.classifier.add_rule(SuiteRule(0, 0, "护肤套装"))

    def _ruleTest(self, expect, positive=[], negative=[]):
        if isinstance(positive, str):
            positive = splitText(positive)
        if isinstance(negative, str):
            negative = splitText(negative)
        for row in positive:
            self.assertEqual(self.classifier.test(row), expect, row["name"])
        for row in negative:
            self.assertNotEqual(self.classifier.test(row), expect, row["name"])

    def testSuite(self):
        positive = """
            集合直播42-雅诗兰黛小棕瓶熬眼霜套装
            【陈姐姐】丸美宠爱珍藏礼盒
            可卡莉 蜂姿双萃水光精华组合 25ml+25ml
            【妮宝】奥蜜思芯悠【洁面乳120g+精华水180ml+精粹霜50g】
            【179元开抢】：极润水120ml+极润乳液50g 薇诺娜
            JAYJUN2盒水光+1盒樱花+1支洁面+5片樱花
            JAYJUN2黑色+1樱花+洁面90ml+五片樱花
            ZB-花印水漾含氨基酸洁面乳150g*2
            大利推荐 | 悦诗风吟 绿茶洁面补水保湿洗面奶*2支
            大璐璐推荐 | 绿茶洁面150ml+岩泥洁面150ml+蓝莓洁面100ml
            大璐璐推荐 | 悦诗风吟 绿茶洁面补水保湿洗面奶150ml*2支
            大璐璐推荐 | 悦诗风吟岩泥洗面奶洁面清洁毛孔*两支
            大璐璐推荐 | 悦诗风吟 绿茶精萃矿物质喷雾120ml*两支
            店铺自营｜innisfree/悦诗风吟 绿茶洁面150ml*2支
            后拱辰享气韵生润颜洁面膏*2 51103839
            艺博推荐 | 悦诗风吟绿茶洁面150ml+岩泥洁面150ml+蓝莓洁面100ml
            朱萧木推荐 | 悦诗风吟绿茶洁面150ml*两支
            joajota 玻尿酸水润洁净洗面奶三支装
            joajota西洋梨营养保湿洗面奶（三支装）
            氨基酸洁面乳 2支装各150g 多效净颜 井尚美
            一叶子黑松露洁面乳3支
            新3.22（呗呗兔专享）BB5 清透舒盈洁肤油双瓶装
            [旁氏]亮采净澈系列米粹润泽洁面乳120GX2
            【TR泡芙氨基酸洁面慕斯】Tabula Rasa洁面慕斯220ml*2瓶装
            【店铺专享】理肤泉清痘洁面泡沫50ml*2
            【老罗推荐】AOEO山茶花氨基酸洁面乳敏感肌温和  100g*2
            【直播间】AOEO山茶花洁面乳洗面奶氨基酸敏感肌温和*2
            【付鹏专属】UNISKIN优时颜优能平衡洁面乳-两支装
            【A哥】欧莱雅金致臻颜花蜜奢养滋润面霜+小黑瓶精华15ml*2
            冻干液两盒36支送一盒面膜
            4、六胜肽原液+定妆喷雾+除菌喷雾500ml+洗面奶+玻尿酸
            【索博士】抗蓝光眼霜（1+1）发两瓶15g*2（活动）            
            【5.5大促】英树神经酰胺舒妍细致洁颜慕斯洁面乳150ml 2支装
            梦希蓝三色彩虹隔离+防晒霜
            芦荟胶+原液+洗面奶+护手霜
            保湿提亮不卡粉双层气垫bb霜+遮瑕膏防水粉底液
            【橙姐专享】沁如春眼霜+空气霜二合一
            原液3个
        """
        negative = """
            亚姐--贝诗佳电动眼霜
            LAN兰时光凝萃精华油护肤美肤油 强韧修护屏障抵御初老舒缓15ml
            OLAY光感小白瓶30ml（面膜新老包装随机发货）
            【韩国直邮】VIDIVICI女神洁面乳氨基酸泡沫温和深层清洁120ml/支
            【娅米阿T】洗面奶 拍一发二
            汇美【厂家直销 买1送1】娅芝美白洗面奶
            雪玲妃氨基酸洗面奶500g【拍一发二】
            妮彩【买1送1】欧束菲珍珠美颜贵妇膏50g
            妮彩【拍1发2】夕碧泉贵妇膏
            妮维雅水活多效洁面乳100g买一送一
            （女王家）臻艾氨基酸洁面乳（买一送一）
            丝塔芙蓝朋友洁面473ml赠同款237ml洁面乳
            丝塔芙蓝朋友洁面乳473ml 赠同款洁面237ml
            【风清福利】日本咖思美马油洁面乳洗面奶130g*1支
            朵莉姿敏纯棉双效化妆棉卸妆棉 60片
            LPD旗舰店-缪斯女神卸妆贴100片
            # 敏家小黑清清无泡沫稻萃洗颜粉洁肤粉洗面奶2g*30袋
            # 【新年特惠39.9抢洗面奶2支】芮度富勒烯净润清透洁面乳100g
        """
        self._ruleTest("护肤套装", positive, negative)

    def testSuite2(self):
        positive = """
            【0318宠粉】玻尿酸安瓶精华液套组1.5ml*14支/盒
            资生堂护手*3个装
        """
        negative = """
            《甜甜专属》嫩肌清莹露精华水 拍一发三盒
            《甜甜专属》嫩肌清莹露精华水(拍一发三盒)
            《甜甜专属》蛇毒肽固态金箔水（拍1发3盒）
            《甜甜专属》嫩肌清莹露精华水 买一发三盒
            《甜甜专属》嫩肌清莹露精华水 拍一送三盒
            《甜甜专属》蛇毒肽固态金箔水（买一发两盒）
            《甜甜专属》蛇毒肽固态金箔水（拍一发两盒）
            【超人宝的妈妈】兰蔻大粉水400ml  送2瓶125ml
            《阿威专属》云锦熙 羊胎素冻干粉（发两盒送5片面膜）
            【0331送冻膜15g*4】冰肌如玉修护冻膜60g
            【送大礼包】梵蜜琳官方正品贵妇膏淡化干纹细纹皱纹提亮肤色面霜
            《甜甜专场》云锦熙羊胎素冻干粉（发两盒送10片面膜）
        """
        self._ruleTest("护肤套装", positive, negative)


class ProcessNextTest(unittest.TestCase):
    def testProcessNext(self):
        self.classifier = Classifier()
        rule = KeywordRule(1, 1, "面膜", process_next=True)
        rule.set_keywords(["面膜"])
        self.classifier.add_rule(rule)
        rule = KeywordRule(1, 2, "面膜", process_next=False)
        rule.set_keywords(["非面膜"])
        self.classifier.add_rule(NegativeTest(rule))
        rule = KeywordRule(2, 1, "洗发水")
        rule.set_keywords(["洗发水"])
        self.classifier.add_rule(rule)

        row = {"c4": "", "name": "YS兰黛洗发水 非面膜"}
        result = self.classifier.test(row)
        self.assertEqual(result, "洗发水")

        row = {"c4": "", "name": "YS兰黛面膜"}
        result = self.classifier.test(row)
        self.assertEqual(result, "面膜")

        row = {"c4": "", "name": "YS兰黛洗发水"}
        result = self.classifier.test(row)
        self.assertEqual(result, "洗发水")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
