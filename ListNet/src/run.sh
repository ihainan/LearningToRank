sbt package && spark-submit \
--class "ListNet" \
--master local \
target/scala-2.10/listnet_2.10-1.0.jar

