# hello-spark

# Further references
- https://hevodata.com/learn/kafka-vs-spark/#t7
- https://www.projectpro.io/article/working-with-spark-rdd-for-fast-data-processing/273
- https://www.projectpro.io/article/apache-spark-architecture-explained-in-detail/338
- https://www.edureka.co/blog/spark-architecture/
- https://spark.apache.org/docs/latest/rdd-programming-guide.html


# Execution
- first go to the docker directory and execute: ```docker-compose up```
- in another shell execute ```mvn clean install && java -Dspring.profiles.active=local -jar target/hello-spark*.jar```
- then execute ```curl --location --request POST 'http://localhost:8081/wordcount?numbers=5%7C20%7C42%7C5%7C20%7C20%7C20%7C20%7C20%7C20%7C20%7C20%7C20%7C20%7C20%7C20%7C20%7C20%7C20%7C20'```