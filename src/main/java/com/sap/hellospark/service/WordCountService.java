package com.sap.hellospark.service;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

@Service
public class WordCountService {

    private final JavaSparkContext sparkContext;
    private final SparkSession sparkSession;

    public WordCountService(JavaSparkContext sparkContext, SparkSession sparkSession) {
        this.sparkContext = sparkContext;
        this.sparkSession = sparkSession;
    }

    public Map<Integer, Long> getFactorials(List<Integer> numbers) {

        StructType structType = new StructType()
                .add("number", DataTypes.IntegerType)
                .add("factorial", DataTypes.IntegerType);

        List<Row> rows = numbers.stream().map(i -> new GenericRowWithSchema(new Integer[]{i, i}, structType)).collect(Collectors.toList());
        JavaRDD<Row> words = sparkContext.parallelize(rows);

        UserDefinedFunction factorialFunction = udf(
                new Factorial(), DataTypes.LongType
        );

        sparkSession.udf().register("factorialFunction", factorialFunction);

        Dataset<Row> dataFrame = sparkSession.createDataFrame(words, structType);
        //>>next line fails in cluster mode. local mode works fine
        //https://stackoverflow.com/questions/28186607/java-lang-classcastexception-using-lambda-expressions-in-spark-job-on-remote-ser/28367602#28367602
        List<Row> result = dataFrame.select(col("number"), factorialFunction.apply(col("factorial"))).collectAsList();
        return result.stream().distinct().collect(Collectors.toMap(e -> (Integer) e.get(0), e -> (Long) e.get(1)));
    }
}
