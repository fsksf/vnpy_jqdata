# vn.py框架的聚宽JQData数据服务接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.2.64.2-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7-blue.svg"/>
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

## 说明

基于jqdatasdk的vnpy从初始化数据插件

* 期货：
  * CFFEX：中国金融期货交易所
  * SHFE：上海期货交易所
  * DCE：大连商品交易所
  * CZCE：郑州商品交易所
  * INE：上海国际能源交易中心
* 股票：
  * SSE：上海证券交易所
  * SZSE：深圳证券交易所

注意：需要使用相应的数据服务权限，可以通过[该页面](https://www.joinquant.com/)注册使用。

## 安装

安装需要基于2.6.0版本以上的[VN Studio](https://www.vnpy.com)。

直接使用pip命令：

```
pip install vnpy_jqdata
```


或者下载解压后在cmd中运行：

```
python setup.py install
```

## 使用

在vn.py中使用JQData时，需要在全局配置中填写以下字段信息：

|名称|含义|必填| 举例     |
|---------|----|---|--------|
|datafeed.name|名称|是| jqdata |
|datafeed.username|用户名|是| 手机号    |
|datafeed.password|密码|是| 注册密码   |
