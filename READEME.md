#数据库导入工具

这是我用来导入每一行都格式化好的txt文件的小工具，事先写好配置文件，然后启动小程序即可。需导入的文件的扩展名必须是txt

```
java -jar DBImporter.jar -c file.conf

```

命令行参数

```
java -jar DBImporter.jar -c | -h

-c | --config   指定配置文件的路径
-h | --help     帮助

```

配置文件格式

```

target.path=path/to/directory	要导入的文件所在的文件夹
server.name=server:port			服务器地址，要写端口号
server.db=targetDatabase		要导入的数据库
server.username=username		数据库用户
server.password=password		数据库密码
importer.repeatable=[t/f]		是否允许有重复的主键（以第一个字段作为主键，这将强制开启预读模式）
reader.pre-read=[t/f]			是否先把文件预读到内存再导入。
reader.split=char				每一行的字段分隔符
reader.sql=sqlExp				SQL语句，用问号"?"代替字段数据。

```