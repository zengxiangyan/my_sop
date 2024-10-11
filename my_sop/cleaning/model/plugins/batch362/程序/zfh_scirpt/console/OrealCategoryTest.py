from OrealCategory import rule_decision, get_rule_list


def main():
    file = '../data/美发四级关键词.xlsx'
    #file = "C:\\Users\\zhang.fenghuan\\Documents\\WXWork\\1688858116332049\\Cache\\File\\2024-08\\240812-美发四级关键词(1).xlsx"
    c4 = ''
    c6 = '发膜'
    c6_trade = ''
    name = '固然堂李若彤 赵雅芝推荐【3袋装】多肽角蛋白还原霜修护护发素发膜'
    rule_list = get_rule_list(file)
    rule = list(filter(lambda r: r['c4'] == c4 and r['c6'] == c6 and r['c6_trade'] == c6_trade, rule_list))
    if len(rule) == 0:
        print('查无此分类')
    result = rule_decision(name, rule[0])
    if result:
        print(f"{name} -----> 规则组={result['rule_group_id']} 规则组顺序={result['rule_order']} 结果={result['result']}")
    else:
        print(f"c4='{c4}' c6='{c6}' c6_trade='{c6_trade}' 没有兜底规则")


if __name__ == '__main__':
    main()
