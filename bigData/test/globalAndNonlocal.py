gcount = 1
def global_test():
    global gcount
    gcount += 1
    print(gcount)
# global_test()
# print(gcount)

def scope_test():
    def do_local():
        spam = "local spam"  # 此函数定义了另外的一个spam字符串变量，并且生命周期只在此函数内。此处的spam和外层的spam是两个变量，如果写出spam = spam + “local spam” 会报错

    def do_nonlocal():
        nonlocal spam  # 使用外层的spam变量
        spam = "nonlocal spam"    #修改了以后也只能在该函数内部有效

    def do_global():
        global spam
        spam = "global spam"
        print(spam)   # 函数内部的函数修改了 外部变量只能在该内部函数内部有效

    spam = "test spam"
    # do_local()
    # print("After local assignmane:", spam)
    do_nonlocal()
    print("After nonlocal assignment:", spam)
    do_global()
    # print(spam)
    print("After global assignment:", spam)


scope_test()
print("In global scope:", spam)