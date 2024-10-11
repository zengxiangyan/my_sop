import datetime

import pandas as pd
from pandas import json_normalize
import tkinter as tk
from tkinter import filedialog


def choose_file(title='选择文件', filetype=None):
    # 创建一个隐藏的根窗口
    root = tk.Tk()
    root.withdraw()  # 隐藏根窗口

    # 打开文件选择对话框
    file_path = filedialog.askopenfilename(
        title=title,
        filetypes=(
            ("所有文件", "*.*"),
            ("文本文件", "*.txt"),
            ("Python 文件", "*.py"),
            ("Office 文件", "*.doc *.docx *.xls *.xlsx *.csv *.ppt *.pptx")
        )
    )

    return file_path


def file_convert_gbk_to_utf(input_file, output_file):
    try:
        # 读取 GBK 编码的文件内容
        with open(input_file, 'r', encoding='gbk') as f:
            content = f.read()

        # 将内容写入 UTF-8 编码的文件
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(content)

        print("文件转换成功。")
    except FileNotFoundError:
        print("输入文件不存在。")
    except Exception as e:
        print(f"文件转换失败：{e}")


def excel_to_list(excel_file):
    df = pd.read_excel(excel_file)
    df.fillna('', inplace=True)
    keys = df.columns.tolist()
    result = []
    for _, row in df.iterrows():
        row_dict = {key: row[key] for key in keys}
        result.append(row_dict)

    return result


def json_to_excel(json_data, file_path):
    df = json_normalize(json_data)

    df.to_excel(file_path, index=False)
