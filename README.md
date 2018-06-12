#Pattern 性能测试

## 测试环境
```
 flink 版本：1.4.2
 cpu：Intel(R) Core(TM) i7-4790K CPU @ 4.00GHz
 内存：16G （分配给flink的）
```

## 测试背景
控制pattern的输出为1%

## 典型pattern测试
### n分钟m次构造测试程序
Q：怎么控制pattern的输出为1%？

A：先确定pattern 的次数设置阈值是多少，然后，阈值 * 100 * 事件发送间隔时间（5s) 即为异常事件插入时间间隔。即，每发送100个事件后连续插入异常事件，插入异常事件个数大于设定的阈值。需要注意的是，设定事件次数的阈值 * 事件发送间隔时间需要小于等于窗口时间，否则pattern不会触发。

#### pattern代码

~~~Java
Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getString("user").equals("kevin");
                    }
                })
                .timesOrMore(10).greedy()
                .within(Time.minutes(20));
~~~


#### 细分场景
   
   - 满足simple condition的数据很少

   [测试代码](https://github.com/liujiawinds/flink-cep-perf/blob/master/src/main/java/com/hansight/streaming/CEPPerfTest.java)	
     
     速度极快，在一个并行度的情况下都超过42万eps
     	
   job 截图：![dashborad Screenshot](https://github.com/liujiawinds/flink-cep-perf/blob/master/screenshots/99298DAB-16A3-4A02-9CB5-4CE7B23EF346.png)
     
 
   - 满足simple condition的数据很多，但是被次数设定给限制住了（连续产生threshold - 1的异常事件）
		
  [测试代码](https://github.com/liujiawinds/flink-cep-perf/blob/master/src/main/java/com/hansight/streaming/CEPPerfTest2.java)
    
    速度还是很快，在一个并行度11万eps左右
     	
  job 截图：![dashborad Screenshot](https://github.com/liujiawinds/flink-cep-perf/blob/master/screenshots/QQ20180605-172012.png)
     

### N分钟移动M公里

Q: 异常点构造方式？

A: 每一百个事件插入一个异常位置的登录事件。

#### pattern 代码


~~~Java
Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("prev")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject event) throws Exception {
                        return event.getString("event_type").equals("logon");
                    }
                })
                .followedBy("curr")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject currentEvent, Context<JSONObject> ctx) throws Exception {
                        if (!currentEvent.getString("event_type").equals("logon")) {
                            return false;
                        }
                        Iterable<JSONObject> iterator = ctx.getEventsForPattern("prev");
                        JSONObject previousEvent = null;
                        for (JSONObject jsonObject : iterator) {
                            previousEvent = jsonObject;
                        }
                        return CEPUtil.geoDistance(previousEvent.getString("geo"), currentEvent.getString("geo")) > 100;
                    }
                })
                .within(Time.minutes(10));
~~~
#### 细分场景

- 时间范围内的数据很少，绝大多数的事件都被window time给过滤掉了。

[测试代码](https://github.com/liujiawinds/flink-cep-perf/blob/master/src/main/java/com/hansight/streaming/CEPPerfTest3.java)

```
性能情况不错，差不多26w eps
```

 job 截图：![dashborad Screenshot](https://github.com/liujiawinds/flink-cep-perf/blob/master/screenshots/QQ20180607-162121.png)
 
 
 - 时间范围内的数据很多，但是距离不足100km
 
 [测试代码](https://github.com/liujiawinds/flink-cep-perf/blob/master/src/main/java/com/hansight/streaming/CEPPerfTest4.java)

```
性能情况很差了，3.5k eps
```

 job 截图：![dashborad Screenshot](https://github.com/liujiawinds/flink-cep-perf/blob/master/screenshots/QQ20180607-164834.png)
 
 
