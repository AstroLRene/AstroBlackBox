# python3 默认采用  UTF-8 编码，所有字符串都是  unicode 字符串
print("你好，世界！")

# 关键字
import keyword
print(keyword.kwlist)
'''
这都是注释
'''
"""
这也是注释
"""

# 单句很长，可以用 \ 反斜杠来实现多行语句，在{}、（）、[]中多行语句不需要
# 同时 \ 是转义字符  在语句前加上  r  转义不起作用
print("this is \n a line")
print(r"this is \n a line")

# 字符串索引 从左往右以 0 开始，从右往左 -1 开始
str="hello"
# 字符串截取 表示从第 0 位开始到 -1 位  但不包含 -1 位   [:]  包前不包后
print(str[0:-1])

# input 表示 等待用户输入
input("\n\n 按下回车后退出")

'''
缩进相同的一组语句构成一个代码块，我们称之代码组。
像if、while、def和class这样的复合语句，首行以关键字开始，以冒号( : )结束，该行之后的一行或多行代码构成代码组。
'''

# print 默认输出是换行的，如果要实现不换行需要在变量末尾加上 end=""：
print("hello",end="")
print("hello")

# 导入包
from sys import path
print(path)

# 在 Python 中，变量就是变量，它没有类型，我们所说的"类型"是变量所指的内存中对象的类型。
# Python允许同时为多个变量赋值， 从后向前赋值，变量都指向同一个内存地址
# 不可变数据（3 个）：Number（数字）、String（字符串）、Tuple（元组）
# 可变数据（3 个）：List（列表）、Dictionary（字典）、Set（集合）。
# String、List、Tuple 都属于 sequence (序列)
# python3 只支持4种 数字类型  int、float、bool、complex（复数）。 没有  Long。bool类型可以和数字相加。

# type() 不会认为子类是一种父类类型
# isinstance() 认为子类是一种父类类型

# 运算符   除法 "/" 返回一个浮点数  除法 "//" 返回一个整数
#   % 取余运算    "**"乘方 运算
#  del  删除一些对象 数据类型不允许改变，否则将重新分配内存空间
# del str
# str * 2 表示字符串两次