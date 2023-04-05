# 调用方式
1、首先对每一个用户初始化一个Weibo对象：

```
user = Weibo(weibo_id,since_date)
```
`user_id` 即该用户的微博id，可以是整数也可以是字符串，必填；

`since_date` 是一个整数或者字符串形式的日期，表示爬取的时间范围，默认设置为15，可以不填。since_date值可以是日期，也可以是整数。

如果是整数，代表爬取最近n天的微博，如:
```
user = Weibo(weibo_id=..., since_date=10)
```
代表爬取最近10天的微博，这个说法不是特别准确，准确说是**爬取发布时间从10天前到本程序开始执行时**之间的微博。

如果是日期，代表爬取该日期之后的微博，格式应为“yyyy-mm-dd”，如
```
user = Weibo(weibo_id=..., since_date="2018-01-01")
```

---
代表爬取从2018年1月1日到现在的微博。

2、对已经初始化好的Weibo对象，通过start()方法开始爬取。
```
user.start()
```

爬取的数据会写入`.../weibo/<username>/<user_id>.csv`文件