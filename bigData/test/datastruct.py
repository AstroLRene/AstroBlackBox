# List 列表 写在 [] 之间，元素之间用 , 分隔。有序的对象集合
# 列表中元素的类型可以不相同，支持数字，字符串甚至可以包含列表（嵌套）
# 列表可以被索引和截取，列表中的元素可以改变
lists = ['abcd',10,10.5]
lists[1] = "hello"
print(lists)

# 元组与列表类似，写在 () 里，元素之间用 , 分隔
# 元组的元素类型也可以不相同，但是 元组得元素不能改变
# 元组只包含一个元素时，需要在元素后面加 ,否则创建的就不是元组类型，是 int 类型
tup = (20,)
tup1 = (10,"hei",10.5,88,"1")
tup2 =()               # 空元组

# 切片操作
print(lists[1:3])
print(tup1[1:3])

# Set集合是无序不重复元素的序列
set1 = set()       #空的set，只能这样创建
set2 = {"tom","jim","mary","tom"}
set3 = {"tom"}
print(set2)
print(set2 - set3)

a = set("hekko")
b = set("jbhb")
print(a - b)   # 差集
print(a | b)   # 并集
print(a & b)   # 交集
print(a ^ b)   # a 和 b 中不同时存在的元素

# Dictionary(字典) 无序的对象结合，字典中的元素通过 key 来存取，不是通过偏移量，key 必须唯一
dictnull = {}      #空字典
# 字典的几种不同创建方式
dict1 = {"name":"astro","age":18,"mobile":"18900250075"}
dict2 = dict([("name","astro"),("age",18),("mobile","18900250075")])
dict3 = dict(name="astro",age=18,mobile="18900250075")
print(dict1)
print(dict1.keys())      #输出所有的 key
print(dict1.values())    #输出所有的 value
print(dict2)
print(dict3)

# 对数据内置的类型进行转换，只需要将数据类型作为函数名即可
test = list(tup1)
print(test)

print(complex(1,2))
print(complex("1+2j"))     # + 号左右不能有空格
print(str(dict3))

print("第2个元素的值为：",lists[2])

# 成员运算符  in 、 not in         判断能否在指定的序列中找到值
# 身份运算符  is 、 not is         判断两个标示符是不是引用同一个对象
#    is 与 == 的区别  ：== 判断值是否相等，满足 == 不一定满足 is