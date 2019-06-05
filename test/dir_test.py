
import os,sys;

for root,dirs,files in os.walk(r"E:\济宁银行\\第一轮梳理表结构"):
    for file in files:
        #获取文件所属目录
        print(root)
        #获取文件路径
        print(os.path.join(root,file))


if __name__ == '__main__':
    #print(sys.argv[0])
    #print(sys.argv[1])

    st = "DEFAULT 'DB',MB 手机银行；DB 直销银行"

    ss = "default 'DB',"
    print(ss.replace("'","\\'").replace(",","\,").replace(";","\;"))

