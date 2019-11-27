# 电影推荐系统(Movie Recommend System)

### 项目架构设计(Project Architecture Design):

![image-20191127111530616](README.assets/image-20191127111530616.png)

### 项目组件:

- Flume: 1.7.0
- Tomcat:  8.5.23
- Elasticsearch: 5.6.2
- MongoDB: 3.4.3
- Kafka: 2.12
- Redis: 4.0.2
- Spark: 2.1.1

### 数据模型：

1. Movie表（从testfiles/movies.csv 中导入）

   | 字段名   | 字段类型 | 字段描述     |
   | -------- | -------- | ------------ |
   | mid      | int      | 电影的ID     |
   | name     | String   | 电影的名称   |
   | desci    | String   | 电影的描述   |
   | timelong | String   | 电影时长     |
   | shoot    | String   | 电影拍摄时间 |
   | issue    | String   | 电影发布时间 |
   | language | String   | 电影语言     |
   | genres   | String   | 电影所属类别 |
   | director | String   | 电影导演     |
   | actors   | String   | 电影演员     |

   
   
2. Rating表（用户评分表，从testfiles/ratings.csv中导入）

   | 字段名    | 字段类型 | 字段描述   |
   | --------- | -------- | ---------- |
   | uid       | int      | 用户的ID   |
   | mid       | int      | 电影的ID   |
   | score     | double   | 电影的评分 |
   | timestamp | long     | 评分的时间 |

   

3. Tag表（标签表，从testfiles/tags.csv中导入）

   | 字段名    | 字段类型 | 字段描述   |
   | --------- | -------- | ---------- |
   | uid       | int      | 用户的ID   |
   | mid       | int      | 电影的ID   |
   | tag       | String   | 电影的标签 |
   | timestamp | long     | 评分的时间 |

### 