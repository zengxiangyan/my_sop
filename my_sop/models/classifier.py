from typing import Optional
import pandas as pd
import pypinyin
import re
import logging

from sqlalchemy.sql.expression import true

logger = logging.getLogger(__name__)


class BaseRule:
    def __init__(self, group, order, result, process_next=False) -> None:
        self.group = group
        self.order = order
        self.result = result
        self.process_next = process_next

    def __str__(self) -> str:
        return f"{self.__class__.__name__}[{self.group}.{self.order} {self.result}]{'*' if self.process_next else ''}"

    def is_valid(self) -> bool:
        return False


class KeywordRule(BaseRule):
    def __init__(self, group, order, result, process_next=False) -> None:
        super().__init__(group, order, result, process_next)
        self.keywords = []

    def set_keywords(self, keywords):
        self.keywords = [str(k).lower() for k in keywords]

    def is_valid(self) -> bool:
        return len(self.keywords) > 0

    def test(self, row):
        if not self.keywords:
            return False

        name = row["name"]
        if not isinstance(name, str):
            return False
        name = name.lower()

        for kw in self.keywords:
            if name.find(kw) >= 0:
                return True
        return False


class RegexRule(BaseRule):
    def __init__(self, group, order, result, process_next=True) -> None:
        super().__init__(group, order, result, process_next)
        self.regex_list = []

    def set_regex_list(self, regex_list):
        for r in regex_list:
            try:
                self.regex_list.append(re.compile(r, re.IGNORECASE))
            except:
                logger.warning("ignored: wrong regex: %s", r)

    def is_valid(self) -> bool:
        return len(self.regex_list) > 0

    def test(self, row) -> Optional[str]:
        name = row["name"]
        if not isinstance(name, str):
            return False

        for r in self.regex_list:
            match = r.search(name)
            if match:
                return True
        return False


class PinyinRegexRule(RegexRule):
    REGEX_SEP = r"[ `,.。!@#$%^&*_\-+=|~、:;<>/?，《》【】￥%&]*"

    def __init__(self, group, order, result, process_next=True) -> None:
        super().__init__(group, order, result, process_next)
        self.regex_list = []

    def set_words(self, words, with_first_letter=True):
        for word in words:
            if not isinstance(word, str) or len(word) == 0:
                continue
            p = pypinyin.pinyin(word, style=pypinyin.Style.NORMAL)
            if len(p) != len(word):
                logger.warning("ignored: invalid pinyin for '%s': %s", word, p)
                continue
            if with_first_letter:
                rs = PinyinRegexRule.REGEX_SEP.join(
                    [fr"({c}|{l[0]}|{l[0][0]})" for c, l in zip(word, p)]
                )
            else:
                rs = PinyinRegexRule.REGEX_SEP.join(
                    [fr"({c}|{l[0]})" for c, l in zip(word, p)]
                )
            try:
                r = re.compile(rs, re.IGNORECASE)
                self.regex_list.append(r)
            except:
                logger.warning("ignored: wrong regex for word: %s", word)


class NegativeTest(BaseRule):
    def __init__(self, rule) -> None:
        super().__init__(rule.group, rule.order, rule.result, rule.process_next)
        self.rule = rule

    def is_valid(self) -> bool:
        return self.rule.is_valid()

    def test(self, row) -> Optional[str]:
        return not self.rule.test(row)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.rule})"


class WithCategoryTest(BaseRule):
    def __init__(self, rule, categories=[]) -> None:
        super().__init__(rule.group, rule.order, rule.result, rule.process_next)
        self.rule = rule
        self.categories = categories

    def is_valid(self) -> bool:
        return self.rule.is_valid()

    def test(self, row) -> Optional[str]:
        category = row["c4"]
        if category in self.categories:
            return self.rule.test(row)
        return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.rule})"


class SuiteRule(BaseRule):
# ========================================================================
# 识别套装
# ========================================================================

    def __init__(self, group, order, result, process_next=False) -> None:
        super().__init__(group, order, result, process_next)

        self.suite_regex = re.compile(
            r"""(套\s*[装组包盒餐]
            |(全|件|护理|沙龙|肤)套
            |礼[盒包]|组合|合集|套装|全家福|盘|洗/?护|水/?乳|洗发沐|洗发护|洗发护发|洗发沐浴
            |[二两三四五六七八九][步部]曲)
            """,
            re.X,
        )

        number = r"[0-9〇一二三四五六七八九十零壹贰叁肆伍陆柒捌玖拾两百千万亿单双]"
        unit = r"(?:ml|ML|毫升|g|G|克|盒|瓶|支|只|片|袋|款|套|个|件)"
        unit2 = r"(?:盒|瓶|支|只|袋|款|套|个|件)"
        ptype = (
            r"(?:"
            r"洗护|沐浴|洗发护|洗发沐|洗发|洗fa|洗头|菲诗蔻|洁发|干发喷雾|润发|xi发|洗F水|护发|护fa|养护秀发|蚕丝蛋白乳|发油|滋养液|发膜|洗发|染发|泡泡染|烫发|发蜡|发泥|发胶|弹力素|弹弹乳|"
            r"彩妆|气垫|氣垫|气墊|垫霜|900目|轻垫|BB|CC|DD|QQ|碧碧|遮瑕|橡皮擦|妆前|防护|素颜|美颜膏|美白霜|懒人霜|桃花霜|猪油膏|修颜液|粉饼|腮彩|胭脂|颊彩|腮红|定妆|持妆|粉底|散粉|散风|散&&粉|蜜粉|微笑保湿粉|修容|修颜|高光|gao光|贝壳粉|光影盘|鼻影|收颜粉|五花肉|粉底|粉厎|无痕粉|眼线|小金笔|胶笔|眼妆笔|双头笔|眼影|柠檬盘|美睫|婕髦液|睫毛|眉睫|捷毛|婕髦|眉笔|眉粉|眉膏|染眉|唇部护理|眼唇护理|唇膜|唇油|唇乳|唇部磨砂|唇部去角质|口红雨衣|固色雨衣|护唇|润唇|唇釉|唇釉|唇Y|唇you|唇彩|唇蜜|唇泥|染唇液|接吻棒|阿玛尼红管|丝绒|唇线笔|唇膏笔|唇笔线|唇膏|口红|口紅|口H|口荭|口hong|口-红|口 红|胡萝卜色号|子弹头|紅管|红管|甲油|底油|卧蚕笔|"
            r"脱毛|除毛|精油|按摩膏|精油|精华油|按摩油|草本油|美容油|艾精油|椰子油|椰油|橄榄油|玫瑰果油|荷荷巴油|角鲨烷精纯油|丽肤油|按摩膏|按摩啫喱|泰品|按摩霜|掌灸液|"
            r"香水|香氺|xiang水|浮力款|香体走珠露|香膏|反转香|真我|祖马龙|蓝风铃|"
            r"T区|鼻贴|鼻膜|眼膜|眼贴|眼纹贴|眼袋贴|颈膜|额头贴|泥膜|冻膜|冰膜|冷膜|粉膜|肤膜|护膜|嫩膜|仙膜|囧膜|双膜|软膜|m膜|M膜|紫膜|黑膜|白膜|绿膜|金膜|米膜|干膜|贴膜|膜膜|纹贴|护贴|面贴|膜贴|精华膜|紧致膜|冷敷膜|冰川膜|冻干膜|蓝藻膜|玫瑰膜|玫瑰茶膜|冻龄膜|抗皱膜|膜粉|面m|面M面膜|眼霜|眼膏|眼胶|眼部精华|眼精华|紫熨斗|复原蜜|yan霜|眼 霜|眼-霜|"
            r"护手|手霜|手乳|手膜|手蜡|半亩花田维E乳|走珠|隆力奇|凡士林|"
            r"卸妆|卸睫毛膏|卸Z|眼唇卸|"
            r"磨砂|去角质|祛死皮|"
            r"洁面|净面|洗面|洁脸|净脸|脸颜|洁颜|净颜|洗颜|小白刷|防S|清洁|净洗|洁肤|"
            r"芦荟胶|修复胶|芦荟凝胶|修复凝胶|马油|兰花|"
            r"隔离|ge离|G离|隔璃|ge璃|G璃|隔L|geL|GL|隔梨|紫隔|绿隔|兰隔|长管隔|C隔|绿ge|兰ge|长管ge|Cge|紫G|绿G|兰G|长管G|"
            r"防晒|防晒|仿晒|晒晒|耐晒|防嗮|仿嗮|晒嗮|耐嗮|防护|仿护|晒护|耐护|小白管|安热|安耐|安热沙|金管|安RE|安耐S|"
            r"素颜|美颜|驻颜|焕颜|贵妇|粉嫩|大红瓶|美白焕肤霜|佳颜霜|黄油|VC粉|SOD蜜|养肤膏|妙龄膏|龙雪膏|龙血膏|龙X|木瓜|瓷肌|保养|珍珠|如意|兰花|"
            r"柔肤|玫瑰|新肌|玫瑰花|拉丝|泡泡|收缩|活力|焕亮|减龄|收缩|焕妍|美肌|生香|冻龄|清莹|蓝光|赋颜|玉泉|化妆|海洋|天然|修护|素颜|神木|极光|固态|丝滑|爽肤|精粹|太阳|神仙|禁忌|菌菇|石榴|活泉|鎏金|流金|洋甘菊|紫苏|菁华|生肌|能量|营养|柔肤|摇摇|肽|原生|定格|启动|美容|美溶|自由之|生机之|雅漾|雅诗兰黛沁|保湿|调理|舒缓|"
            r"(冻|动|dong)\s*(干|gan)|精\s*(华|化|粹|萃)|安\s*瓶|肌底|精华|精化|精粹|精萃|精能液|美容液|美溶液|美容油|美容VE|原液|原生液|原浆|营养液|安瓶|次抛|胶囊|小棕瓶|小灯泡|小白瓶|红腰子|冻干|动干|冻gan|复原蜜|淡斑|淡ban|小黑瓶|润燥精|黑长管|肌活凝露|"
            r"乳|ru|膏|露|霜|胶|油|水|液|雾|粉|泥|浆|贴|膜|奶|泡|沫|冻|线|笔|"
            r"啫喱|慕斯|胶原|磨砂"
            r")"
        )  

        self.suite_regex1 = re.compile(
            fr"""
                ({number}+{unit}.*|{ptype}) # <数字><单位> [任意字] | <ptype>
                \s*[+]                      # +
                (.*?{ptype})?               # [[任意字]<类型>]
                ({number}+{unit})?          # [<数字><单位>]
            """,
            re.X,
        )
        
        self.suite_regex1 = re.compile(
            fr"""
                ({number}+{unit}.*|{ptype}) # <数字><单位> [任意字] | <ptype>
                \s*[+]                      # +
                (.*?{ptype})?               # [[任意字]<类型>]
                ({number}+{unit})?          # [<数字><单位>]
            """,
            re.X,
        )

        self.suite_regex2 = re.compile(
            fr"""
                ({number}+{unit}|{ptype})    # <数字><单位> | <ptype>
                [^*xX]*[*xX]\s*              # [任意字] <*|x|X>
                (?P<count>{number}+){unit2}? # <数字> [单位2]
            """,
            re.X,
        )

        self.suite_regex3 = re.compile(
            fr"""
                {ptype}                              # <产品类别>
                .{{0,10}}?                           # [10个以内字符]
                (?P<count>{number}+){unit2}(装)?     # <数字><单位2>[装]
            """,
            re.X,
        )

        self.suite_regex4 = re.compile(
            fr"""
                (?P<count>{number}+){unit2}(装)?     # <数字><单位2>[装]
                {ptype}                              # <产品类别>
                .{{0,10}}?                           # [10个以内字符]
            """,
            re.X,
        )

        self.exclude_regex = re.compile(
            fr"""
                ([拍买发]{number}{unit}?)?
                [发送赠].*?
                ({number}+{unit}])
            """,
            re.X,
        )

    def __matchWithCount(self, regex, name):
        match = regex.search(name)
        if match:
            count = match.group("count")
            try:
                count = int(count)
                if count > 1:
                    return True
            except ValueError:  # count不是阿拉伯数字
                if count not in ["一", "壹", "单"]:
                    return True
        return False

    def is_valid(self) -> bool:
        return True

    def test(self, row):
        name = row["name"]
        if not isinstance(name, str):
            return False

        name = self.exclude_regex.sub("", name)

        if self.suite_regex.search(name):
            return True
        if self.suite_regex1.search(name):
            return True
        if self.__matchWithCount(self.suite_regex2, name):
            return True
        if self.__matchWithCount(self.suite_regex3, name):
            return True
        if self.__matchWithCount(self.suite_regex4, name):
            return True
        return False


class Classifier:
    def __init__(self, unclassified="TBD") -> None:
        self.rules = []
        self.unclassified = unclassified

    def add_rule(self, rule, prepend=False):
        if prepend:
            self.rules.insert(0, rule)
        else:
            self.rules.append(rule)

    def load_rules(self, excel_file, sheet_name=None):
        logger.info("loading rules from %s, sheet: %s", excel_file, sheet_name)
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        value_col_start = df.columns.get_loc("value")
        logger.info("value column start at: %d", value_col_start)
        for _, row in df.iterrows():
            rule = self.__create_rule(row, value_col_start)
            if rule:
                self.rules.append(rule)
        logger.info("loaded %d rules", len(self.rules))
    
    def load_rules_from_db(self, df):
        value_col_start = df.columns.get_loc("value")
        logger.info("value column start at: %d", value_col_start)
        for _, row in df.iterrows():
            rule = self.__create_rule(row, value_col_start)
            if rule:
                self.rules.append(rule)
        logger.info("loaded %d rules", len(self.rules))

    def __create_rule(self, row, value_col_start):
        group = row["规则组"]
        order = row["规则顺序"]
        result = row["分类结果"]

        category = []
        if isinstance(row["原分类"], str):
            for c in row["原分类"].split(","):
                c = c.strip()
                if c:
                    category.append(c)

        exclude = str(row["规则判断"]).strip() == "不包含"
        process_next = str(row["继续处理"]) in ["是", "Y", "y"]
        values = row[value_col_start:].dropna()
        rule_type = row["规则类型"]

        if rule_type == "关键词":
            rule = KeywordRule(group, order, result, process_next)
            rule.set_keywords(values)
        elif rule_type in ["正则", "正则表达式"]:
            rule = RegexRule(group, order, result, process_next)
            rule.set_regex_list(values)
        elif rule_type == "拼音正则":
            rule = PinyinRegexRule(group, order, result, process_next)
            rule.set_words(values, with_first_letter=True)
        elif rule_type == "无首字拼音正则":
            rule = PinyinRegexRule(group, order, result, process_next)
            rule.set_words(values, with_first_letter=False)
        elif rule_type == "套装":
            rule = SuiteRule(group, order, result, process_next)
        else:
            return None

        if not rule.is_valid():
            return None

        if exclude:
            rule = NegativeTest(rule)
        if category:
            rule = WithCategoryTest(rule, category)

        return rule

    def test(self, row) -> Optional[str]:
        prev_result = True
        for rule in self.rules:
            result = rule.test(row)
            # import pdb;pdb.set_trace()
            # logger.debug(
            #     '%s -> %s: {"c4": "%s", "name", "%s"}',
            #     rule,
            #     result,
            #     row["c4"],
            #     row["name"],
            # )
            if rule.process_next:
                prev_result = prev_result and result
                continue
            if prev_result and result:
                return rule.result
            prev_result = True

        return self.unclassified


# ========================================================================
# 分类规则测试
# ========================================================================
def main():
    logging.basicConfig(level=logging.DEBUG)
    classifier = Classifier()
#    classifier.load_rules("rules.xlsx", sheet_name="category.rule")
    classifier.load_rules("rules.xlsx", sheet_name="brand.rule")

    row = {"c4": "眼霜", "name": "韩国 3CE 9色眼影盘 OVERTAKE眼影盘"}
    result = classifier.test(row)
    print("result:", result)


if __name__ == "__main__":
    main()
