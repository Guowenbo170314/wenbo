# wenbo
知识图谱创建节点、关系，节点去重
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 21 09:00:20 2021

@author: 86132
"""


#加载需要得第三方库
import csv
import time
import pandas as pd
from pandas import DataFrame
from py2neo import Graph,Node,Relationship,NodeMatcher
import numpy as np
import os
#读取客户信息和客户购物数据

customer_buy = pd.read_csv('D:\\客户社群发现\\customers_buy_add.csv',encoding="gbk")
#print(customer_buy.head())
# 连接Neo4j数据库
graph = Graph('http://localhost:7474/db/data/',username='neo4j',password='guo@zhang170314')
#创建客户、消费地点的节点  
for i in customer_buy.values:
    node = Node('customer',name = i[0],address = i[1])
    relation = Node('address',name = i[1])
    graph.create(node)
    graph.create(relation)
#查找重复节点：
MATCH (p:address)
WITH p.name as name, collect(p) AS nodes 
WHERE size(nodes) >  1
RETURN [ n in nodes | n.name] AS names, size(nodes)
ORDER BY size(nodes) DESC
LIMIT 10
#删除重复节点:
MATCH (p:address)
WITH p 
ORDER BY p.name, size((p)--()) DESC
WITH p.name as name, collect(p) AS nodes 
WHERE size(nodes) >  1
UNWIND nodes[1..] AS n
DETACH DELETE n
 

MATCH (p:customer)
WITH p 
ORDER BY p.name, size((p)--()) DESC
WITH p.name as name, collect(p) AS nodes 
WHERE size(nodes) >  1
UNWIND nodes[1..] AS n
DETACH DELETE n
#创建关系：
MATCH (entity1:customer) , (entity2:address{name:entity1.address}) CREATE (entity1)-[:购买]->(entity2)
