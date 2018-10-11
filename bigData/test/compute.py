# range 含左不含右
for y in range(2,2):
    print(y)
else:
    print("y不在range范围内")

# 单独出现 * 后得参数 必须要用关键字传入 否则报错
def f(a,b,*,c):
    return a+b+c

x = f(1,2,c=3)
print(x)

# 内部作用域想修改外部作用域的变量时，要用到 global 和 nonlocal 关键字
a = 10
def test():
    global a          # global 关键字能够使用外部定义的变量  可以修改外部变量值
    a = 100
    print(a)

test()
print(a)


matrix = [
    [1,2,3,4],
    [5,6,7,8],
    [9,10,11,12]
]
print([[row[i] for row in matrix] for i in range(4)])
for row in matrix:
    print(row)

transposed = []
for i in range(4):
    tran_row = []
    for row in matrix:
        tran_row.append(row[i])
        print(tran_row)
    transposed.append(tran_row)

print(transposed)