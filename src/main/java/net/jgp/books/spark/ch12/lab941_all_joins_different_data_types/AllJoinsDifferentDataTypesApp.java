package net.jgp.books.spark.ch12.lab941_all_joins_different_data_types;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * All joins in a single app, inspired by
 * https://stackoverflow.com/questions/45990633/what-are-the-various-join-types-in-spark.
 * <p>
 * Used in Spark in Action 2e, http://jgp.net/sia
 * Joining indices of different type (int with string) takes longer as the previous example
 * with joins of indices of the same type
 * @author jgp
 */
public class AllJoinsDifferentDataTypesApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        AllJoinsDifferentDataTypesApp app = new AllJoinsDifferentDataTypesApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Processing of invoices")
                .master("local")
                .getOrCreate();

        StructType schemaLeft = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(
                        "id",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "value",
                        DataTypes.StringType,
                        false)});

        StructType schemaRight = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(
                        "idx",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "value",
                        DataTypes.StringType,
                        false)});

        List<Row> rows = new ArrayList<Row>();
        rows.add(RowFactory.create(1, "Value 1"));
        rows.add(RowFactory.create(2, "Value 2"));
        rows.add(RowFactory.create(3, "Value 3"));
        rows.add(RowFactory.create(4, "Value 4"));
        Dataset<Row> dfLeft = spark.createDataFrame(rows, schemaLeft);
        dfLeft.show();

        rows = new ArrayList<Row>();
        rows.add(RowFactory.create("3", "Value 3"));
        rows.add(RowFactory.create("4", "Value 4"));
        rows.add(RowFactory.create("4", "Value 4_1"));
        rows.add(RowFactory.create("5", "Value 5"));
        rows.add(RowFactory.create("6", "Value 6"));
        Dataset<Row> dfRight = spark.createDataFrame(rows, schemaRight);
        dfRight.show();

        String[] joinTypes = new String[]{
                "inner", // v2.0.0. default

                "outer", // v2.0.0
                "full", // v2.1.1
                "full_outer", // v2.1.1

                "left", // v2.1.1
                "left_outer", // v2.0.0

                "right", // v2.1.1
                "right_outer", // v2.0.0

                "left_semi", // v2.0.0, was leftsemi before v2.1.1

                "left_anti", // v2.1.1

                "cross" // v2.2.0
        };

        for (String joinType : joinTypes) {
            System.out.println(joinType.toUpperCase() + " JOIN");
            Dataset<Row> df = dfLeft.join(
                    dfRight,
                    dfLeft.col("id").equalTo(dfRight.col("idx")),
                    joinType);
            df.orderBy(dfLeft.col("id")).show();
        }

        System.out.println("CROSS JOIN (without a column");
        Dataset<Row> df = dfLeft.crossJoin(dfRight);
        df.orderBy(dfLeft.col("id")).show();
    }
}
