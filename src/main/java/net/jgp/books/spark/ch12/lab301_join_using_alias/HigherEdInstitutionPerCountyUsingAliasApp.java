package net.jgp.books.spark.ch12.lab301_join_using_alias;

import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * E X P E R I M E N T A L
 * <p>
 * Performs a join between 3 datasets to build a list of higher education
 * institutions per county.
 *
 * @author jgp
 */
public class HigherEdInstitutionPerCountyUsingAliasApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        HigherEdInstitutionPerCountyUsingAliasApp app =
                new HigherEdInstitutionPerCountyUsingAliasApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creation of the session
        SparkSession spark = SparkSession.builder()
                .appName("Join")
                .master("local")
                .getOrCreate();

        // Ingestion of the census data
        Dataset<Row> censusDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("encoding", "cp1252")
                .load("data/census/PEP_2017_PEPANNRES.csv");
        censusDf = censusDf
                .drop("GEO.id")
                .drop("rescen42010")
                .drop("resbase42010")
                .drop("respop72010")
                .drop("respop72011")
                .drop("respop72012")
                .drop("respop72013")
                .drop("respop72014")
                .drop("respop72015")
                .drop("respop72016")
                .withColumnRenamed("respop72017", "pop2017")
                .withColumnRenamed("GEO.id2", "countyId")
                .withColumnRenamed("GEO.display-label", "county");
        System.out.println("Census data");
        censusDf.sample(0.1).show(3, false);
        censusDf.printSchema();

        // Higher education institution (and yes, there is an Arkansas College
        // of Barbering and Hair Design)
        Dataset<Row> higherEdDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/dapip/InstitutionCampus.csv");
        higherEdDf = higherEdDf
                .filter("LocationType = 'Institution'")
                .withColumn(
                        "addressElements",
                        split(higherEdDf.col("Address"), " "));
        higherEdDf = higherEdDf
                .withColumn(
                        "addressElementCount",
                        size(higherEdDf.col("addressElements")));
        higherEdDf = higherEdDf
                .withColumn(
                        "zip9",
                        element_at(
                                higherEdDf.col("addressElements"),
                                higherEdDf.col("addressElementCount")));
        higherEdDf = higherEdDf
                .withColumn(
                        "splitZipCode",
                        split(higherEdDf.col("zip9"), "-"));
        higherEdDf = higherEdDf
                .withColumn("zip", higherEdDf.col("splitZipCode").getItem(0))
                .withColumnRenamed("LocationName", "location")
                .drop("DapipId")
                .drop("OpeId")
                .drop("ParentName")
                .drop("ParentDapipId")
                .drop("LocationType")
                .drop("Address")
                .drop("GeneralPhone")
                .drop("AdminName")
                .drop("AdminPhone")
                .drop("AdminEmail")
                .drop("Fax")
                .drop("UpdateDate")
                .drop("zip9")
                .drop("addressElements")
                .drop("addressElementCount")
                .drop("splitZipCode")
                .alias("highered"); // higherEdDf now has an alias "highered"
        System.out.println("Higher education institutions (DAPIP)");
        higherEdDf.sample(0.1).show(3, false);
        higherEdDf.printSchema();

        // Zip to county
        Dataset<Row> countyZipDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/hud/COUNTY_ZIP_092018.csv");
        countyZipDf = countyZipDf
                .drop("res_ratio")
                .drop("bus_ratio")
                .drop("oth_ratio")
                .drop("tot_ratio")
                .alias("hud"); // countyZipDf now has an alias "hud"
        System.out.println("Counties / ZIP Codes (HUD)");
        countyZipDf.sample(0.1).show(3, false);
        countyZipDf.printSchema();

        // Institutions per county id
        Dataset<Row> institPerCountyDf = higherEdDf.join(
                countyZipDf,
                higherEdDf.col("zip").equalTo(countyZipDf.col("zip")),
                "inner");
        System.out.println(
                "Higher education institutions left-joined with HUD");
        institPerCountyDf
                .filter(higherEdDf.col("zip").equalTo(27517))
                .show(20, false);
        institPerCountyDf.printSchema();

        // Institutions per county name
        institPerCountyDf = institPerCountyDf.join(
                censusDf,
                institPerCountyDf.col("county").equalTo(censusDf.col("countyId")),
                "left");
        System.out.println(
                "Higher education institutions and county id with census");
        institPerCountyDf
                .filter(higherEdDf.col("zip").equalTo(27517))
                .show(20, false);
        institPerCountyDf
                .filter(higherEdDf.col("zip").equalTo(2138))
                .show(20, false);

        // Final clean up
        institPerCountyDf = institPerCountyDf

                // using the higherEdDf alias to specify which zip column to drop
                .drop("highered.zip") // instead of .drop(higherEdDf.col("zip"))

                //using the df alias to specify which zip column to drop
                .drop("hud.county") //.drop(countyZipDf.col("county"))
                .drop("countyId")
                .distinct();
        System.out.println("Final list");
        institPerCountyDf.show(200, false);
        System.out.println("The combined list has " + institPerCountyDf.count()
                + " elements.");

        // A little more
        // @formatter:off
        //    Dataset<Row> aggDf = institutionPerCountyDf
        //        .groupBy("county", "pop2017")
        //        .count();
        //    aggDf = aggDf.orderBy(aggDf.col("count").desc());
        //    aggDf.show(25, false);
        //
        //    Dataset<Row> popDf = aggDf
        //        .filter("pop2017>30000")
        //        .withColumn("institutionPer10k", expr("count*10000/pop2017"));
        //    popDf = popDf.orderBy(popDf.col("institutionPer10k").desc());
        //    popDf.show(25, false);
        // @formatter:on
    }
}
