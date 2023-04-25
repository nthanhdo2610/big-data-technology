package miu.edu.bdt.visualizer;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkService {

    private static SparkConf sparkConf = null;
    private static SparkService INSTANCE = null;
    private static final Logger logger = LoggerFactory.getLogger(SparkService.class);

    private SparkService() {
        sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.set("hive.metastore.uris", Constant.THRIFT_CONNECTION)
                .set("hive.metastore.warehouse.dir", "/user/hive/warehouse")
                .set("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .set("hive.exec.scratchdir", "/tmp/the-current-user-has-permission-to-write-in")
                .set("spark.yarn.security.credentials.hive.enabled", "true")
                .set("spark.sql.hive.metastore.jars", "maven")
                .set("spark.sql.hive.metastore.version", "1.2.1")
                .set("spark.sql.catalogImplementation", "hive")
        ;
    }

    public static SparkService getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new SparkService();
        }
        return INSTANCE;
    }

    public void updateView() {
        logger.info("Spark SQL-Hive Update View!!!");
        try (SparkSession session = SparkSession
                .builder()
                .appName("Spark SQL-Hive")
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate()) {
            session.sparkContext.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
            session.sql(String.format(Constant.DROP_TABLE_SQL, Constant.IOWA_TABLE_NAME));
            Dataset<Row> iowa = session.sql(String.format(Constant.SELECT_WEATHER_IOWA_SQL, Constant.TABLE_NAME));
            iowa.write().saveAsTable(Constant.IOWA_TABLE_NAME);

            // test show top result
            session.sql(Constant.SELECT_LIMIT_10_WEATHER_IOWA_SQL).show();
        } catch (Exception e) {
            logger.error("Spark SQL-Hive Update View Got Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(0);
        }

        logger.info("Spark SQL-Hive Update View Successfully!!!");
    }

}
