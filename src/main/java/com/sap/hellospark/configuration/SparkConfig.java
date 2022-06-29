package com.sap.hellospark.configuration;

import com.sap.hellospark.service.Factorial;
import com.sap.hellospark.service.WordCountService;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spark.app.name}")
    private String appName;
    @Value("${spark.master}")
    private String masterUri;

    @Bean
    public JavaSparkContext sparkContext() {
        return new JavaSparkContext(new SparkConf()
                .setAppName(appName)
                .setMaster(masterUri)
                .setJars(new String[]{"target/hello-spark-0.0.1-SNAPSHOT.jar"})
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrationRequired", "true")
                .registerKryoClasses(new Class[]{
                        Factorial.class,
                        WordCountService.class,
                        GenericRowWithSchema.class,
                        StructType.class,
                        StructField.class,
                        StructField[].class,
                        IntegerType$.class,
                        Metadata.class,
                        Integer[].class}));
    }

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName(appName)
                .master(masterUri)
                .getOrCreate();
    }
}
